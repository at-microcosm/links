use crate::db_types::{db_complete, DbBytes, DbStaticStr, EncodingError, StaticStr};
use crate::store_types::{
    ByCollectionKey, ByCollectionValue, ByCursorSeenKey, ByCursorSeenValue, ByIdKey, ByIdValue,
    JetstreamCursorKey, JetstreamCursorValue, JetstreamEndpointKey, JetstreamEndpointValue,
    ModCursorKey, ModCursorValue, ModQueueItemKey, ModQueueItemStringValue, ModQueueItemValue,
    RollupCursorKey, RollupCursorValue, SeenCounter,
};
use crate::{
    CollectionSamples, CreateRecord, DeleteAccount, Did, EventBatch, ModifyRecord, Nsid, RecordKey,
};
use fjall::{
    Batch as FjallBatch, CompressionType, Config, Keyspace, PartitionCreateOptions, PartitionHandle,
};
use jetstream::events::Cursor;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Receiver;
use tokio::time::{interval_at, sleep};

/// Commit the RW batch immediately if this number of events have been read off the mod queue
const MAX_BATCHED_RW_EVENTS: usize = 18;

/// Commit the RW batch immediately if this number of records is reached
///
/// there are probably some efficiency gains for higher, at cost of more memory.
/// interestingly, this kind of sets a priority weight for the RW loop:
///     - doing more work whenever scheduled means getting more CPU time in general
///
/// this is higher than [MAX_BATCHED_RW_EVENTS] because account-deletes can have lots of items
const MAX_BATCHED_RW_ITEMS: usize = 24;

#[derive(Clone)]
struct Db {
    keyspace: Keyspace,
    partition: PartitionHandle,
}

/**
 * data format, roughly:
 *
 * Global Meta:
 *   ["js_cursor"] => js_cursor(u64), // used as global sequence
 *   ["js_endpoint"] => &str, // checked on startup because jetstream instance cursors are not interchangeable
 *   ["mod_cursor"] => js_cursor(u64);
 *   ["rollup_cursor"] => [js_cursor|collection]; // how far the rollup helper has progressed
 * Mod queue
 *   ["mod_queue"|js_cursor] => one of {
 *      DeleteAccount(did) // delete all account content older than cursor
 *      DeleteRecord(did, collection, rkey) // delete record older than cursor
 *      UpdateRecord(did, collection, rkey, new_record) // delete + put, but don't delete if cursor is newer
 *   }
 * Collection and rollup meta:
 *   ["seen_by_js_cursor_collection"|js_cursor|collection] => u64 // batched total, gets cleaned up by rollup
 *   ["total_by_collection"|collection] => [u64, js_cursor] // rollup; live total requires scanning seen_by_collection after js_cursor
 *   ["hour_by_collection"|hour(u64)|collection] => u64 // rollup from seen_by_js_cursor_collection
 * Samples:
 *   ["by_collection"|collection|js_cursor] => [did|rkey|record]
 *   ["by_id"|did|collection|rkey|js_cursor] => [] // required to support deletes; did first prefix for account deletes.
 *
 * TODO: account privacy preferences. Might wait for the protocol-level (PDS-level?) stuff to land. Will probably do lazy
 * fetching + caching on read.
 **/
#[derive(Clone)]
pub struct Storage {
    /// horrible: gate all db access behind this to force global serialization to avoid deadlock
    db: Db,
}

impl Storage {
    fn init_self(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let keyspace = Config::new(path).fsync_ms(Some(4_000)).open()?;
        let partition = keyspace.open_partition(
            "default",
            PartitionCreateOptions::default().compression(CompressionType::None),
        )?;
        Ok(Self {
            db: Db {
                keyspace,
                partition,
            },
        })
    }

    pub async fn open(
        path: PathBuf,
        endpoint: &str,
        force_endpoint: bool,
    ) -> anyhow::Result<(Self, Option<Cursor>)> {
        let me = tokio::task::spawn_blocking(move || Storage::init_self(path)).await??;

        let js_cursor = me.get_jetstream_cursor().await?;

        if js_cursor.is_some() {
            let Some(JetstreamEndpointValue(stored)) = me.get_jetstream_endpoint().await? else {
                anyhow::bail!("found cursor but missing js_endpoint, refusing to start.");
            };
            if stored != endpoint {
                if force_endpoint {
                    log::warn!("forcing a jetstream switch from {stored:?} to {endpoint:?}");
                    me.set_jetstream_endpoint(endpoint).await?;
                } else {
                    anyhow::bail!("stored js_endpoint {stored:?} differs from provided {endpoint:?}, refusing to start.");
                }
            }
        } else {
            me.set_jetstream_endpoint(endpoint).await?;
        }

        Ok((me, js_cursor))
    }

    /// Jetstream event batch receiver: writes without any reads
    ///
    /// Events that require reads like record updates or delets are written to a queue
    pub async fn receive(&self, mut receiver: Receiver<EventBatch>) -> anyhow::Result<()> {
        // TODO: see rw_loop: enforce single-thread.
        loop {
            let t_sleep = Instant::now();
            sleep(Duration::from_secs_f64(0.8)).await; // TODO: minimize during replay
            let slept_for = t_sleep.elapsed();
            let queue_size = receiver.len();

            if let Some(event_batch) = receiver.recv().await {
                log::trace!("write: received write batch");
                let batch_summary = summarize_batch(&event_batch);

                let last = event_batch.last_jetstream_cursor.clone(); // TODO: get this from the data. track last in consumer. compute or track first.

                let db = &self.db;
                let keyspace = db.keyspace.clone();
                let partition = db.partition.clone();

                let writer_t0 = Instant::now();
                log::trace!("spawn_blocking for write batch");
                tokio::task::spawn_blocking(move || {
                    DBWriter {
                        keyspace,
                        partition,
                    }
                    .write_batch(event_batch, last)
                })
                .await??;
                log::trace!("write: back from blocking task, successfully wrote batch");
                let wrote_for = writer_t0.elapsed();

                println!("{batch_summary}, slept {slept_for: <12?}, wrote {wrote_for: <11?}, queue: {queue_size}");
            } else {
                log::error!("store consumer: receive channel failed (dropped/closed?)");
                anyhow::bail!("receive channel closed");
            }
        }
    }

    /// Read-write loop reads from the queue for record-modifying events and does rollups
    pub async fn rw_loop(&self) -> anyhow::Result<()> {
        // TODO: lock so that only one rw loop can possibly be run. or even better, take a mutable resource thing to enforce at compile time.

        let now = tokio::time::Instant::now();
        let mut time_to_update_events = interval_at(now, Duration::from_secs_f64(0.051));
        let mut time_to_trim_surplus = interval_at(
            now + Duration::from_secs_f64(1.0),
            Duration::from_secs_f64(3.3),
        );
        let mut time_to_roll_up = interval_at(
            now + Duration::from_secs_f64(0.4),
            Duration::from_secs_f64(0.9),
        );

        time_to_update_events.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        time_to_trim_surplus.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        time_to_roll_up.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            let keyspace = self.db.keyspace.clone();
            let partition = self.db.partition.clone();
            tokio::select! {
                _ = time_to_update_events.tick() => {
                    log::debug!("beginning event update task");
                    tokio::task::spawn_blocking(move || Self::update_events(keyspace, partition)).await??;
                    log::debug!("finished event update task");
                }
                _ = time_to_trim_surplus.tick() => {
                    log::debug!("beginning record trim task");
                    tokio::task::spawn_blocking(move || Self::trim_old_events(keyspace, partition)).await??;
                    log::debug!("finished record trim task");
                }
                _ = time_to_roll_up.tick() => {
                    log::debug!("beginning rollup task");
                    tokio::task::spawn_blocking(move || Self::roll_up_counts(keyspace, partition)).await??;
                    log::debug!("finished rollup task");
                },
            }
        }
    }

    fn update_events(keyspace: Keyspace, partition: PartitionHandle) -> anyhow::Result<()> {
        // TODO: lock this to prevent concurrent rw

        log::trace!("rw: getting rw cursor...");
        let mod_cursor =
            get_static::<ModCursorKey, ModCursorValue>(&partition)?.unwrap_or(Cursor::from_start());
        let range = ModQueueItemKey::new(mod_cursor.clone()).range_to_prefix_end()?;

        let mut db_batch = keyspace.batch();
        let mut batched_rw_items = 0;
        let mut any_tasks_found = false;

        log::trace!("rw: iterating newer rw items...");

        for (i, pair) in partition.range(range.clone()).enumerate() {
            log::trace!("rw: iterating {i}");
            any_tasks_found = true;

            if i >= MAX_BATCHED_RW_EVENTS {
                break;
            }

            let (key_bytes, val_bytes) = pair?;
            let mod_key = match db_complete::<ModQueueItemKey>(&key_bytes) {
                Ok(k) => k,
                Err(EncodingError::WrongStaticPrefix(_, _)) => {
                    panic!("wsp: mod queue empty.");
                }
                otherwise => otherwise?,
            };

            let mod_value: ModQueueItemValue =
                db_complete::<ModQueueItemStringValue>(&val_bytes)?.try_into()?;

            log::trace!("rw: iterating {i}: sending to batcher {mod_key:?} => {mod_value:?}");
            batched_rw_items += DBWriter {
                keyspace: keyspace.clone(),
                partition: partition.clone(),
            }
            .write_rw(&mut db_batch, mod_key, mod_value)?;
            log::trace!("rw: iterating {i}: back from batcher.");

            if batched_rw_items >= MAX_BATCHED_RW_ITEMS {
                log::trace!("rw: iterating {i}: batch big enough, breaking out.");
                break;
            }
        }

        if !any_tasks_found {
            log::trace!("rw: skipping batch commit since apparently no items were added (this is normal, skipping is new)");
            // TODO: is this missing a chance to update the cursor?
            return Ok(());
        }

        log::info!("rw: committing rw batch with {batched_rw_items} items (items != total inserts/deletes)...");
        let r = db_batch.commit();
        log::info!("rw: commit result: {r:?}");
        r?;
        Ok(())
    }

    fn trim_old_events(_keyspace: Keyspace, _partition: PartitionHandle) -> anyhow::Result<()> {
        // we *could* keep a collection dirty list in memory to reduce the amount of searching here
        // actually can we use seen_by_js_cursor_collection??
        // *   ["seen_by_js_cursor_collection"|js_cursor|collection] => u64
        // -> the rollup cursor could handle trims.

        // key structure:
        // *   ["by_collection"|collection|js_cursor] => [did|rkey|record]

        // *new* strategy:
        // 1. collect `collection`s seen during rollup
        // 2. for each collected collection:
        // 3. set up prefix iterator
        // 4. reverse and try to walk back MAX_RETAINED steps
        // 5. if we didn't end iteration yet, start deleting records (and their forward links) until we get to the end

        // ... we can probably do even better with cursor ranges too, since we'll have a cursor range from rollup and it's in the by_collection key

        Ok(())
    }

    fn roll_up_counts(_keyspace: Keyspace, _partition: PartitionHandle) -> anyhow::Result<()> {
        Ok(())
    }

    pub async fn get_collection_records(
        &self,
        collection: &Nsid,
        limit: usize,
    ) -> anyhow::Result<Vec<CreateRecord>> {
        let partition = self.db.partition.clone();
        let prefix = ByCollectionKey::prefix_from_collection(collection.clone())?;
        tokio::task::spawn_blocking(move || {
            let mut output = Vec::new();

            for pair in partition.prefix(&prefix).rev().take(limit) {
                let (k_bytes, v_bytes) = pair?;
                let (_, cursor) = db_complete::<ByCollectionKey>(&k_bytes)?.into();
                let (did, rkey, record) = db_complete::<ByCollectionValue>(&v_bytes)?.into();
                output.push(CreateRecord {
                    did,
                    rkey,
                    record,
                    cursor,
                })
            }
            Ok(output)
        })
        .await?
    }

    pub async fn get_meta_info(&self) -> anyhow::Result<StorageInfo> {
        let db = &self.db;
        let keyspace = db.keyspace.clone();
        let partition = db.partition.clone();
        tokio::task::spawn_blocking(move || {
            Ok(StorageInfo {
                keyspace_disk_space: keyspace.disk_space(),
                keyspace_journal_count: keyspace.journal_count(),
                keyspace_sequence: keyspace.instant(),
                partition_approximate_len: partition.approximate_len(),
            })
        })
        .await?
    }

    pub async fn get_collection_total_seen(&self, collection: &Nsid) -> anyhow::Result<u64> {
        let partition = self.db.partition.clone();
        let collection = collection.clone();
        tokio::task::spawn_blocking(move || get_unrolled_collection_seen(&partition, collection))
            .await?
    }

    pub async fn get_top_collections(&self) -> anyhow::Result<HashMap<String, u64>> {
        let partition = self.db.partition.clone();
        tokio::task::spawn_blocking(move || get_unrolled_top_collections(&partition)).await?
    }

    pub async fn get_jetstream_endpoint(&self) -> anyhow::Result<Option<JetstreamEndpointValue>> {
        let partition = self.db.partition.clone();
        tokio::task::spawn_blocking(move || {
            get_static::<JetstreamEndpointKey, JetstreamEndpointValue>(&partition)
        })
        .await?
    }

    async fn set_jetstream_endpoint(&self, endpoint: &str) -> anyhow::Result<()> {
        let partition = self.db.partition.clone();
        let endpoint = endpoint.to_string();
        tokio::task::spawn_blocking(move || {
            insert_static::<JetstreamEndpointKey>(&partition, JetstreamEndpointValue(endpoint))
        })
        .await?
    }

    pub async fn get_jetstream_cursor(&self) -> anyhow::Result<Option<Cursor>> {
        let partition = self.db.partition.clone();
        tokio::task::spawn_blocking(move || {
            get_static::<JetstreamCursorKey, JetstreamCursorValue>(&partition)
        })
        .await?
    }

    pub async fn get_mod_cursor(&self) -> anyhow::Result<Option<Cursor>> {
        let partition = self.db.partition.clone();
        tokio::task::spawn_blocking(move || get_static::<ModCursorKey, ModCursorValue>(&partition))
            .await?
    }
}

/// Get a value from a fixed key
fn get_static<K: StaticStr, V: DbBytes>(partition: &PartitionHandle) -> anyhow::Result<Option<V>> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value = partition
        .get(&key_bytes)?
        .map(|value_bytes| db_complete(&value_bytes))
        .transpose()?;
    Ok(value)
}

/// Set a value to a fixed key
fn insert_static<K: StaticStr>(
    partition: &PartitionHandle,
    value: impl DbBytes,
) -> anyhow::Result<()> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value_bytes = value.to_db_bytes()?;
    partition.insert(&key_bytes, &value_bytes)?;
    Ok(())
}

/// Set a value to a fixed key
fn insert_batch_static<K: StaticStr>(
    batch: &mut FjallBatch,
    partition: &PartitionHandle,
    value: impl DbBytes,
) -> anyhow::Result<()> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value_bytes = value.to_db_bytes()?;
    batch.insert(partition, &key_bytes, &value_bytes);
    Ok(())
}

/// Remove a key
fn remove_batch<K: DbBytes>(
    batch: &mut FjallBatch,
    partition: &PartitionHandle,
    key: K,
) -> Result<(), EncodingError> {
    let key_bytes = key.to_db_bytes()?;
    batch.remove(partition, &key_bytes);
    Ok(())
}

/// Get stats that haven't been rolled up yet
fn get_unrolled_collection_seen(
    partition: &PartitionHandle,
    collection: Nsid,
) -> anyhow::Result<u64> {
    let range =
        if let Some(cursor_value) = get_static::<RollupCursorKey, RollupCursorValue>(partition)? {
            eprintln!("found existing cursor");
            let key: ByCursorSeenKey = cursor_value.into();
            key.range_from()?
        } else {
            eprintln!("cursor from start.");
            ByCursorSeenKey::full_range()?
        };

    let mut collection_total = 0;

    let mut scanned = 0;
    let mut rolled = 0;

    for pair in partition.range(range) {
        let (key_bytes, value_bytes) = pair?;
        let key = db_complete::<ByCursorSeenKey>(&key_bytes)?;
        let val = db_complete::<ByCursorSeenValue>(&value_bytes)?;

        if *key.collection() == collection {
            let SeenCounter(n) = val;
            collection_total += n;
            rolled += 1;
        }
        scanned += 1;
    }

    eprintln!("scanned: {scanned}, rolled: {rolled}");

    Ok(collection_total)
}

fn get_unrolled_top_collections(
    partition: &PartitionHandle,
) -> anyhow::Result<HashMap<String, u64>> {
    let range =
        if let Some(cursor_value) = get_static::<RollupCursorKey, RollupCursorValue>(partition)? {
            eprintln!("found existing cursor");
            let key: ByCursorSeenKey = cursor_value.into();
            key.range_from()?
        } else {
            eprintln!("cursor from start.");
            ByCursorSeenKey::full_range()?
        };

    let mut res = HashMap::new();
    let mut scanned = 0;

    for pair in partition.range(range) {
        let (key_bytes, value_bytes) = pair?;
        let key = db_complete::<ByCursorSeenKey>(&key_bytes)?;
        let SeenCounter(n) = db_complete(&value_bytes)?;

        *res.entry(key.collection().to_string()).or_default() += n;

        scanned += 1;
    }

    eprintln!("scanned: {scanned} seen-counts.");

    Ok(res)
}

impl DBWriter {
    fn write_batch(self, event_batch: EventBatch, last: Option<Cursor>) -> anyhow::Result<()> {
        let mut db_batch = self.keyspace.batch();
        self.add_record_creates(&mut db_batch, event_batch.record_creates)?;
        self.add_record_modifies(&mut db_batch, event_batch.record_modifies)?;
        self.add_account_removes(&mut db_batch, event_batch.account_removes)?;
        if let Some(cursor) = last {
            insert_batch_static::<JetstreamCursorKey>(&mut db_batch, &self.partition, cursor)?;
        }
        log::info!("write: committing write batch...");
        let r = db_batch.commit();
        log::info!("write: commit result: {r:?}");
        r?;
        Ok(())
    }

    fn write_rw(
        self,
        db_batch: &mut FjallBatch,
        mod_key: ModQueueItemKey,
        mod_value: ModQueueItemValue,
    ) -> anyhow::Result<usize> {
        // update the current rw cursor to this item (atomically with the batch if it succeeds)
        let mod_cursor: Cursor = (&mod_key).into();
        insert_batch_static::<ModCursorKey>(db_batch, &self.partition, mod_cursor.clone())?;

        let items_modified = match mod_value {
            ModQueueItemValue::DeleteAccount(did) => {
                log::trace!("rw: batcher: delete account...");
                let (items, finished) = self.delete_account(db_batch, mod_cursor, did)?;
                log::trace!("rw: batcher: back from delete account (finished? {finished})");
                if finished {
                    // only remove the queued rw task if we have actually completed its account removal work
                    remove_batch::<ModQueueItemKey>(db_batch, &self.partition, mod_key)?;
                    items + 1
                } else {
                    items
                }
            }
            ModQueueItemValue::DeleteRecord(did, collection, rkey) => {
                log::trace!("rw: batcher: delete record...");
                let items = self.delete_record(db_batch, mod_cursor, did, collection, rkey)?;
                log::trace!("rw: batcher: back from delete record");
                remove_batch::<ModQueueItemKey>(db_batch, &self.partition, mod_key)?;
                items + 1
            }
            ModQueueItemValue::UpdateRecord(did, collection, rkey, record) => {
                let items =
                    self.update_record(db_batch, mod_cursor, did, collection, rkey, record)?;
                remove_batch::<ModQueueItemKey>(db_batch, &self.partition, mod_key)?;
                items + 1
            }
        };
        Ok(items_modified)
    }

    fn update_record(
        &self,
        db_batch: &mut FjallBatch,
        cursor: Cursor,
        did: Did,
        collection: Nsid,
        rkey: RecordKey,
        record: serde_json::Value,
    ) -> anyhow::Result<usize> {
        // 1. delete any existing versions older than us
        let items_deleted = self.delete_record(
            db_batch,
            cursor.clone(),
            did.clone(),
            collection.clone(),
            rkey.clone(),
        )?;

        // 2. insert the updated version, at our new cursor
        self.add_record(db_batch, cursor, did, collection, rkey, record)?;

        let items_total = items_deleted + 1;
        Ok(items_total)
    }

    fn delete_record(
        &self,
        db_batch: &mut FjallBatch,
        cursor: Cursor,
        did: Did,
        collection: Nsid,
        rkey: RecordKey,
    ) -> anyhow::Result<usize> {
        let key_prefix_bytes =
            ByIdKey::record_prefix(did.clone(), collection.clone(), rkey.clone()).to_db_bytes()?;

        // put the cursor of the actual deletion event in to prevent prefix iter from touching newer docs
        let key_limit =
            ByIdKey::new(did, collection.clone(), rkey, cursor.clone()).to_db_bytes()?;

        let mut items_removed = 0;

        log::trace!("delete_record: iterate over up to current cursor...");

        for (i, pair) in self
            .partition
            .range(key_prefix_bytes..key_limit)
            .enumerate()
        {
            log::trace!("delete_record iter {i}: found");
            // find all (hopefully 1)
            let (key_bytes, _) = pair?;
            let key = db_complete::<ByIdKey>(&key_bytes)?;
            let found_cursor = key.cursor();
            if found_cursor > cursor {
                // we are *only* allowed to delete records that came before the record delete event
                // log::trace!("delete_record: found (and ignoring) newer version(s). key: {key:?}");
                panic!("wtf, found newer version than cursor limit we tried to set.");
                // break;
            }

            // remove the by_id entry
            db_batch.remove(&self.partition, key_bytes);

            // remove its record sample
            let by_collection_key_bytes =
                ByCollectionKey::new(collection.clone(), found_cursor).to_db_bytes()?;
            db_batch.remove(&self.partition, by_collection_key_bytes);

            items_removed += 1;
        }

        // if items_removed > 1 {
        //     log::trace!("odd, removed {items_removed} records for one record removal:");
        //     for (i, pair) in self.partition.prefix(&key_prefix_bytes).enumerate() {
        //         // find all (hopefully 1)
        //         let (key_bytes, _) = pair?;
        //         let found_cursor = db_complete::<ByIdKey>(&key_bytes)?.cursor();
        //         if found_cursor > cursor {
        //             break;
        //         }

        //         let key = db_complete::<ByIdKey>(&key_bytes)?;
        //         log::trace!("  {i}: key {key:?}");
        //     }
        // }
        Ok(items_removed)
    }

    fn delete_account(
        &self,
        db_batch: &mut FjallBatch,
        cursor: Cursor,
        did: Did,
    ) -> anyhow::Result<(usize, bool)> {
        let key_prefix_bytes = ByIdKey::did_prefix(did).to_db_bytes()?;

        let mut items_added = 0;

        for pair in self.partition.prefix(&key_prefix_bytes) {
            let (key_bytes, _) = pair?;

            let (_, collection, _rkey, found_cursor) = db_complete::<ByIdKey>(&key_bytes)?.into();
            if found_cursor > cursor {
                log::trace!(
                    "delete account: found (and ignoring) newer records than the delete event??"
                );
                continue;
            }

            // remove the by_id entry
            db_batch.remove(&self.partition, key_bytes);

            // remove its record sample
            let by_collection_key_bytes =
                ByCollectionKey::new(collection, found_cursor).to_db_bytes()?;
            db_batch.remove(&self.partition, by_collection_key_bytes);

            items_added += 1;
            if items_added >= MAX_BATCHED_RW_ITEMS {
                return Ok((items_added, false)); // there might be more records but we've done enough for this batch
            }
        }

        Ok((items_added, true))
    }

    fn add_record_creates(
        &self,
        db_batch: &mut FjallBatch,
        record_creates: HashMap<Nsid, CollectionSamples>,
    ) -> anyhow::Result<()> {
        for (
            collection,
            CollectionSamples {
                total_seen,
                samples,
            },
        ) in record_creates.into_iter()
        {
            if let Some(last_record) = &samples.back() {
                db_batch.insert(
                    &self.partition,
                    ByCursorSeenKey::new(last_record.cursor.clone(), collection.clone())
                        .to_db_bytes()?,
                    ByCursorSeenValue::new(total_seen as u64).to_db_bytes()?,
                );
            } else {
                log::error!(
                    "collection samples should only exist when at least one sample has been added"
                );
            }

            for CreateRecord {
                did,
                rkey,
                cursor,
                record,
            } in samples.into_iter().rev()
            {
                self.add_record(db_batch, cursor, did, collection.clone(), rkey, record)?;
            }
        }
        Ok(())
    }

    fn add_record(
        &self,
        db_batch: &mut FjallBatch,
        cursor: Cursor,
        did: Did,
        collection: Nsid,
        rkey: RecordKey,
        record: serde_json::Value,
    ) -> anyhow::Result<()> {
        // ["by_collection"|collection|js_cursor] => [did|rkey|record]
        db_batch.insert(
            &self.partition,
            ByCollectionKey::new(collection.clone(), cursor.clone()).to_db_bytes()?,
            ByCollectionValue::new(did.clone(), rkey.clone(), record).to_db_bytes()?,
        );

        // ["by_id"|did|collection|rkey|js_cursor] => [] // required to support deletes; did first prefix for account deletes.
        db_batch.insert(
            &self.partition,
            ByIdKey::new(did, collection.clone(), rkey, cursor).to_db_bytes()?,
            ByIdValue::default().to_db_bytes()?,
        );

        Ok(())
    }

    fn add_record_modifies(
        &self,
        db_batch: &mut FjallBatch,
        record_modifies: Vec<ModifyRecord>,
    ) -> anyhow::Result<()> {
        for modification in record_modifies {
            let (cursor, db_val) = match modification {
                ModifyRecord::Update(u) => (
                    u.cursor,
                    ModQueueItemValue::UpdateRecord(u.did, u.collection, u.rkey, u.record),
                ),
                ModifyRecord::Delete(d) => (
                    d.cursor,
                    ModQueueItemValue::DeleteRecord(d.did, d.collection, d.rkey),
                ),
            };
            db_batch.insert(
                &self.partition,
                ModQueueItemKey::new(cursor).to_db_bytes()?,
                db_val.to_db_bytes()?,
            );
        }
        Ok(())
    }

    fn add_account_removes(
        &self,
        db_batch: &mut FjallBatch,
        account_removes: Vec<DeleteAccount>,
    ) -> anyhow::Result<()> {
        for deletion in account_removes {
            db_batch.insert(
                &self.partition,
                ModQueueItemKey::new(deletion.cursor).to_db_bytes()?,
                ModQueueItemValue::DeleteAccount(deletion.did).to_db_bytes()?,
            );
        }
        Ok(())
    }
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct StorageInfo {
    pub keyspace_disk_space: u64,
    pub keyspace_journal_count: usize,
    pub keyspace_sequence: u64,
    pub partition_approximate_len: usize,
}

struct DBWriter {
    keyspace: Keyspace,
    partition: PartitionHandle,
}

////////// temp stuff to remove:

fn summarize_batch(batch: &EventBatch) -> String {
    let EventBatch {
        record_creates,
        record_modifies,
        account_removes,
        last_jetstream_cursor,
        ..
    } = batch;
    let total_records: usize = record_creates.values().map(|v| v.total_seen).sum();
    let total_samples: usize = record_creates.values().map(|v| v.samples.len()).sum();
    format!(
        "batch of {total_samples: >3} samples from {total_records: >4} records in {: >2} collections, {: >3} modifies, {} acct removes, cursor {: <12?}",
        record_creates.len(),
        record_modifies.len(),
        account_removes.len(),
        last_jetstream_cursor.clone().map(|c| c.elapsed())
    )
}
