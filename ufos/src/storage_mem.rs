use std::sync::Arc;
use std::ops::Bound;

use std::sync::RwLock;
use std::sync::Mutex;
use crate::db_types::{db_complete, DbBytes, DbStaticStr, StaticStr};
use crate::error::StorageError;
use crate::storage::{StorageResult, StorageWhatever, StoreReader, StoreWriter};
use crate::store_types::{
    AllTimeRollupKey, CountsValue, DeleteAccountQueueKey, DeleteAccountQueueVal,
    HourTruncatedCursor, HourlyRollupKey, JetstreamCursorKey, JetstreamCursorValue,
    JetstreamEndpointKey, JetstreamEndpointValue, LiveCountsKey, NewRollupCursorKey,
    NewRollupCursorValue, NsidRecordFeedKey, NsidRecordFeedVal, RecordLocationKey,
    RecordLocationMeta, RecordLocationVal, RecordRawValue, TakeoffKey, TakeoffValue,
    WeekTruncatedCursor, WeeklyRollupKey,
};
use crate::{CommitAction, ConsumerInfo, Did, EventBatch, Nsid, TopCollections, UFOsRecord};
use async_trait::async_trait;
use jetstream::events::Cursor;
use std::collections::HashMap;
use std::collections::BTreeMap;
use std::path::Path;
use std::time::SystemTime;
use lsm_tree::range::prefix_to_range;


const MAX_BATCHED_CLEANUP_SIZE: usize = 1024; // try to commit progress for longer feeds
const MAX_BATCHED_ACCOUNT_DELETE_RECORDS: usize = 1024;
const MAX_BATCHED_ROLLUP_COUNTS: usize = 256;

///
/// new data format, roughly:
///
/// Partion: 'global'
///
///  - Global sequence counter (is the jetstream cursor -- monotonic with many gaps)
///      - key: "js_cursor" (literal)
///      - val: u64
///
///  - Jetstream server endpoint (persisted because the cursor can't be used on another instance without data loss)
///      - key: "js_endpoint" (literal)
///      - val: string (URL of the instance)
///
///  - Launch date
///      - key: "takeoff" (literal)
///      - val: u64 (micros timestamp, not from jetstream for now so not precise)
///
///  - Rollup cursor (bg work: roll stats into hourlies, delete accounts, old record deletes)
///      - key: "rollup_cursor" (literal)
///      - val: u64 (tracks behind js_cursor)
///
///
/// Partition: 'feed'
///
///  - Per-collection list of record references ordered by jetstream cursor
///      - key: nullstr || u64 (collection nsid null-terminated, jetstream cursor)
///      - val: nullstr || nullstr || nullstr (did, rkey, rev. rev is mostly a sanity-check for now.)
///
///
/// Partition: 'records'
///
///  - Actual records by their atproto location
///      - key: nullstr || nullstr || nullstr (did, collection, rkey)
///      - val: u64 || bool || nullstr || rawval (js_cursor, is_update, rev, actual record)
///
///
/// Partition: 'rollups'
///
/// - Live (batched) records counts and dids estimate per collection
///      - key: "live_counts" || u64 || nullstr (js_cursor, nsid)
///      - val: u64 || HLL (count (not cursor), estimator)
///
/// - Hourly total record counts and dids estimate per collection
///      - key: "hourly_counts" || u64 || nullstr (hour, nsid)
///      - val: u64 || HLL (count (not cursor), estimator)
///
/// - Weekly total record counts and dids estimate per collection
///      - key: "weekly_counts" || u64 || nullstr (hour, nsid)
///      - val: u64 || HLL (count (not cursor), estimator)
///
/// - All-time total record counts and dids estimate per collection
///      - key: "ever_counts" || nullstr (nsid)
///      - val: u64 || HLL (count (not cursor), estimator)
///
/// - TODO: sorted indexes for all-times?
///
///
/// Partition: 'queues'
///
///  - Delete account queue
///      - key: "delete_acount" || u64 (js_cursor)
///      - val: nullstr (did)
///
///
/// TODO: moderation actions
/// TODO: account privacy preferences. Might wait for the protocol-level (PDS-level?) stuff to land. Will probably do lazy fetching + caching on read.
#[derive(Debug)]
pub struct MemStorage {}

#[derive(Debug, Default)]
pub struct MemConfig {
    /// drop the db when the storage is dropped
    ///
    /// this is only meant for tests
    #[cfg(test)]
    pub temp: bool,
}

////////////
////////////
////////////
////////////
////////////
////////////

struct BatchSentinel {}

#[derive(Clone)]
struct MemKeyspace {
    keyspace_guard: Arc<RwLock<BatchSentinel>>,
}

impl MemKeyspace {
    pub fn open() -> Self {
        Self { keyspace_guard: Arc::new(RwLock::new(BatchSentinel {})) }
    }
    pub fn open_partition(&self, _name: &str) -> StorageResult<MemPartion> {
        Ok(MemPartion {
            // name: name.to_string(),
            keyspace_guard: self.keyspace_guard.clone(),
            contents: Default::default(),
        })
    }
    pub fn batch(&self) -> MemBatch {
        MemBatch {
            keyspace_guard: self.keyspace_guard.clone(),
            tasks: Vec::new(),
        }
    }
    pub fn instant(&self) -> () {}
}

enum BatchTask {
    Insert {
        p: MemPartion,
        key: Vec<u8>,
        val: Vec<u8>,
    },
    Remove {
        p: MemPartion,
        key: Vec<u8>,
    },
}
struct MemBatch {
    keyspace_guard: Arc<RwLock<BatchSentinel>>,
    tasks: Vec<BatchTask>,
}
impl MemBatch {
    pub fn insert(&mut self, p: &MemPartion, key: &[u8], val: &[u8]) {
        self.tasks.push(BatchTask::Insert {
            p: p.clone(),
            key: key.to_vec(),
            val: val.to_vec(),
        });
    }
    pub fn remove(&mut self, p: &MemPartion, key: &[u8]) {
        self.tasks.push(BatchTask::Remove {
            p: p.clone(),
            key: key.to_vec(),
        });
    }
    pub fn len(&self) -> usize {
        self.tasks.len()
    }
    pub fn commit(&mut self) -> StorageResult<()> {
        let _guard = self.keyspace_guard.write().unwrap();
        for task in &mut self.tasks {
            match task {
                BatchTask::Insert { p, key, val } =>
                    p.contents
                        .try_lock()
                        .unwrap()
                        .insert(key.to_vec(), val.to_vec()),
                BatchTask::Remove { p, key } =>
                    p.contents
                        .try_lock()
                        .unwrap()
                        .remove(key),
            };
        }
        Ok(())
    }
}

#[derive(Clone)]
struct MemPartion {
    // name: String,
    keyspace_guard: Arc<RwLock<BatchSentinel>>,
    contents: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}
impl MemPartion {
    pub fn get(&self, key: &[u8]) -> StorageResult<Option<Vec<u8>>> {
        let _guard = self.keyspace_guard.read().unwrap();
        Ok(self.contents
            .lock()
            .unwrap()
            .get(key)
            .cloned())
    }
    pub fn prefix(&self, pre: &[u8]) -> Vec<StorageResult<(Vec<u8>, Vec<u8>)>> {
        // let prefix_bytes = prefix.to_db_bytes()?;
        let (_, Bound::Excluded(range_end)) = prefix_to_range(&pre) else {
            panic!("bad range thing");
        };

        return self.range(pre.to_vec()..range_end.to_vec()).into()
    }
    pub fn range(&self, r: std::ops::Range<Vec<u8>>) -> Vec<StorageResult<(Vec<u8>, Vec<u8>)>> {
        let _guard = self.keyspace_guard.read().unwrap();
        self.contents
            .lock()
            .unwrap()
            .range(r)
            .map(|(k, v)| Ok((k.clone(), v.clone())))
            .collect()
    }
    pub fn insert(&self, key: &[u8], val: &[u8]) -> StorageResult<()> {
        let _guard = self.keyspace_guard.read().unwrap();
        self.contents
            .lock()
            .unwrap()
            .insert(key.to_vec(), val.to_vec());
        Ok(())
    }
    // pub fn remove(&self, key: &[u8]) -> StorageResult<()> {
    //     let _guard = self.keyspace_guard.read().unwrap();
    //     self.contents
    //         .lock()
    //         .unwrap()
    //         .remove(key);
    //     Ok(())
    // }
    pub fn snapshot_at(&self, _instant: ()) -> Self {
        self.clone()
    }
    pub fn snapshot(&self) -> Self {
        self.clone()
    }
}

////////////
////////////
////////////
////////////
////////////
////////////

impl StorageWhatever<MemReader, MemWriter, MemConfig> for MemStorage {
    fn init(
        _path: impl AsRef<Path>,
        endpoint: String,
        force_endpoint: bool,
        _config: MemConfig,
    ) -> StorageResult<(MemReader, MemWriter, Option<Cursor>)> {
        let keyspace = MemKeyspace::open();

        let global = keyspace.open_partition("global")?;
        let feeds = keyspace.open_partition("feeds")?;
        let records = keyspace.open_partition("records")?;
        let rollups = keyspace.open_partition("rollups")?;
        let queues = keyspace.open_partition("queues")?;

        let js_cursor = get_static_neu::<JetstreamCursorKey, JetstreamCursorValue>(&global)?;

        if js_cursor.is_some() {
            let stored_endpoint =
                get_static_neu::<JetstreamEndpointKey, JetstreamEndpointValue>(&global)?;

            let JetstreamEndpointValue(stored) = stored_endpoint.ok_or(StorageError::InitError(
                "found cursor but missing js_endpoint, refusing to start.".to_string(),
            ))?;

            if stored != endpoint {
                if force_endpoint {
                    log::warn!("forcing a jetstream switch from {stored:?} to {endpoint:?}");
                    insert_static_neu::<JetstreamEndpointKey>(
                        &global,
                        JetstreamEndpointValue(endpoint.to_string()),
                    )?;
                } else {
                    return Err(StorageError::InitError(format!(
                        "stored js_endpoint {stored:?} differs from provided {endpoint:?}, refusing to start.")));
                }
            }
        } else {
            insert_static_neu::<JetstreamEndpointKey>(
                &global,
                JetstreamEndpointValue(endpoint.to_string()),
            )?;
            insert_static_neu::<TakeoffKey>(&global, Cursor::at(SystemTime::now()))?;
            insert_static_neu::<NewRollupCursorKey>(&global, Cursor::from_start())?;
        }

        let reader = MemReader {
            keyspace: keyspace.clone(),
            global: global.clone(),
            feeds: feeds.clone(),
            records: records.clone(),
            rollups: rollups.clone(),
        };
        let writer = MemWriter {
            keyspace,
            global,
            feeds,
            records,
            rollups,
            queues,
        };
        Ok((reader, writer, js_cursor))
    }
}

type MemRKV = StorageResult<(Vec<u8>, Vec<u8>)>;

#[derive(Clone)]
pub struct MemReader {
    keyspace: MemKeyspace,
    global: MemPartion,
    feeds: MemPartion,
    records: MemPartion,
    rollups: MemPartion,
}

/// An iterator that knows how to skip over deleted/invalidated records
struct RecordIterator {
    db_iter: Box<dyn Iterator<Item = MemRKV>>,
    records: MemPartion,
    limit: usize,
    fetched: usize,
}
impl RecordIterator {
    pub fn new(
        feeds: &MemPartion,
        records: MemPartion,
        collection: &Nsid,
        limit: usize,
    ) -> StorageResult<Self> {
        let prefix = NsidRecordFeedKey::from_prefix_to_db_bytes(collection)?;
        let db_iter = feeds.prefix(&prefix).into_iter().rev();
        Ok(Self {
            db_iter: Box::new(db_iter),
            records,
            limit,
            fetched: 0,
        })
    }
    fn get_record(&self, db_next: MemRKV) -> StorageResult<Option<UFOsRecord>> {
        let (key_bytes, val_bytes) = db_next?;
        let feed_key = db_complete::<NsidRecordFeedKey>(&key_bytes)?;
        let feed_val = db_complete::<NsidRecordFeedVal>(&val_bytes)?;
        let location_key: RecordLocationKey = (&feed_key, &feed_val).into();

        let Some(location_val_bytes) = self.records.get(&location_key.to_db_bytes()?)? else {
            // record was deleted (hopefully)
            return Ok(None);
        };

        let (meta, n) = RecordLocationMeta::from_db_bytes(&location_val_bytes)?;

        if meta.cursor() != feed_key.cursor() {
            // older/different version
            return Ok(None);
        }
        if meta.rev != feed_val.rev() {
            // weird...
            log::warn!("record lookup: cursor match but rev did not...? excluding.");
            return Ok(None);
        }
        let Some(raw_value_bytes) = location_val_bytes.get(n..) else {
            log::warn!(
                "record lookup: found record but could not get bytes to decode the record??"
            );
            return Ok(None);
        };
        let rawval = db_complete::<RecordRawValue>(raw_value_bytes)?;
        Ok(Some(UFOsRecord {
            collection: feed_key.collection().clone(),
            cursor: feed_key.cursor(),
            did: feed_val.did().clone(),
            rkey: feed_val.rkey().clone(),
            rev: meta.rev.to_string(),
            record: rawval.try_into()?,
            is_update: meta.is_update,
        }))
    }
}
impl Iterator for RecordIterator {
    type Item = StorageResult<Option<UFOsRecord>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.fetched == self.limit {
            return Some(Ok(None));
        }
        let record = loop {
            let db_next = self.db_iter.next()?; // None short-circuits here
            match self.get_record(db_next) {
                Err(e) => return Some(Err(e)),
                Ok(Some(record)) => break record,
                Ok(None) => continue,
            }
        };
        self.fetched += 1;
        Some(Ok(Some(record)))
    }
}

impl MemReader {
    fn get_storage_stats(&self) -> StorageResult<serde_json::Value> {
        let rollup_cursor =
            get_static_neu::<NewRollupCursorKey, NewRollupCursorValue>(&self.global)?
                .map(|c| c.to_raw_u64());

        Ok(serde_json::json!({
            "rollup_cursor": rollup_cursor,
        }))
    }

    fn get_consumer_info(&self) -> StorageResult<ConsumerInfo> {
        let global = self.global.snapshot();

        let endpoint =
            get_snapshot_static_neu::<JetstreamEndpointKey, JetstreamEndpointValue>(&global)?
                .ok_or(StorageError::BadStateError(
                    "Could not find jetstream endpoint".to_string(),
                ))?
                .0;

        let started_at = get_snapshot_static_neu::<TakeoffKey, TakeoffValue>(&global)?
            .ok_or(StorageError::BadStateError(
                "Could not find jetstream takeoff time".to_string(),
            ))?
            .to_raw_u64();

        let latest_cursor =
            get_snapshot_static_neu::<JetstreamCursorKey, JetstreamCursorValue>(&global)?
                .map(|c| c.to_raw_u64());

        Ok(ConsumerInfo::Jetstream {
            endpoint,
            started_at,
            latest_cursor,
        })
    }

    fn get_top_collections(&self) -> Result<TopCollections, StorageError> {
        // TODO: limit nsid traversal depth
        // TODO: limit nsid traversal breadth
        // TODO: be serious about anything

        // TODO: probably use a stack of segments to reduce to ~log-n merges

        #[derive(Default)]
        struct Blah {
            counts: CountsValue,
            children: HashMap<String, Blah>,
        }
        impl From<&Blah> for TopCollections {
            fn from(bla: &Blah) -> Self {
                Self {
                    total_records: bla.counts.records(),
                    dids_estimate: bla.counts.dids().estimate() as u64,
                    nsid_child_segments: HashMap::from_iter(
                        bla.children.iter().map(|(k, v)| (k.to_string(), v.into())),
                    ),
                }
            }
        }

        let mut b = Blah::default();
        let prefix = AllTimeRollupKey::from_prefix_to_db_bytes(&Default::default())?;
        for kv in self.rollups.prefix(&prefix.to_db_bytes()?) {
            let (key_bytes, val_bytes) = kv?;
            let key = db_complete::<AllTimeRollupKey>(&key_bytes)?;
            let val = db_complete::<CountsValue>(&val_bytes)?;

            let mut node = &mut b;
            node.counts.merge(&val);
            for segment in key.collection().split('.') {
                node = node.children.entry(segment.to_string()).or_default();
                node.counts.merge(&val);
            }
        }

        Ok((&b).into())
    }

    fn get_counts_by_collection(&self, collection: &Nsid) -> StorageResult<(u64, u64)> {
        // 0. grab a snapshot in case rollups happen while we're working
        let instant = self.keyspace.instant();
        let global = self.global.snapshot_at(instant);
        let rollups = self.rollups.snapshot_at(instant);

        // 1. all-time counts
        let all_time_key = AllTimeRollupKey::new(collection).to_db_bytes()?;
        let mut total_counts = rollups
            .get(&all_time_key)?
            .as_deref()
            .map(db_complete::<CountsValue>)
            .transpose()?
            .unwrap_or_default();

        // 2. live counts that haven't been rolled into all-time yet.
        let rollup_cursor =
            get_snapshot_static_neu::<NewRollupCursorKey, NewRollupCursorValue>(&global)?.ok_or(
                StorageError::BadStateError("Could not find current rollup cursor".to_string()),
            )?;

        let full_range = LiveCountsKey::range_from_cursor(rollup_cursor)?;
        for kv in rollups.range(full_range) {
            let (key_bytes, val_bytes) = kv?;
            let key = db_complete::<LiveCountsKey>(&key_bytes)?;
            if key.collection() == collection {
                let counts = db_complete::<CountsValue>(&val_bytes)?;
                total_counts.merge(&counts);
            }
        }
        Ok((
            total_counts.records(),
            total_counts.dids().estimate() as u64,
        ))
    }

    fn get_records_by_collections(
        &self,
        collections: &[Nsid],
        limit: usize,
    ) -> StorageResult<Vec<UFOsRecord>> {
        if collections.is_empty() {
            return Ok(vec![]);
        }
        let mut record_iterators = Vec::new();
        for collection in collections {
            let iter = RecordIterator::new(&self.feeds, self.records.clone(), collection, limit)?;
            record_iterators.push(iter.peekable());
        }
        let mut merged = Vec::new();
        loop {
            let mut latest: Option<(Cursor, usize)> = None; // ugh
            for (i, iter) in record_iterators.iter_mut().enumerate() {
                let Some(it) = iter.peek_mut() else {
                    continue;
                };
                let it = match it {
                    Ok(v) => v,
                    Err(e) => Err(std::mem::replace(e, StorageError::Stolen))?,
                };
                let Some(rec) = it else {
                    break;
                };
                if let Some((cursor, _)) = latest {
                    if rec.cursor > cursor {
                        latest = Some((rec.cursor, i))
                    }
                } else {
                    latest = Some((rec.cursor, i));
                }
            }
            let Some((_, idx)) = latest else {
                break;
            };
            // yeah yeah whateverrrrrrrrrrrrrrrr
            merged.push(record_iterators[idx].next().unwrap().unwrap().unwrap());
        }
        Ok(merged)
    }
}

#[async_trait]
impl StoreReader for MemReader {
    async fn get_storage_stats(&self) -> StorageResult<serde_json::Value> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || MemReader::get_storage_stats(&s)).await?
    }
    async fn get_consumer_info(&self) -> StorageResult<ConsumerInfo> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || MemReader::get_consumer_info(&s)).await?
    }
    async fn get_top_collections(&self) -> Result<TopCollections, StorageError> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || MemReader::get_top_collections(&s)).await?
    }
    async fn get_counts_by_collection(&self, collection: &Nsid) -> StorageResult<(u64, u64)> {
        let s = self.clone();
        let collection = collection.clone();
        tokio::task::spawn_blocking(move || MemReader::get_counts_by_collection(&s, &collection))
            .await?
    }
    async fn get_records_by_collections(
        &self,
        collections: &[Nsid],
        limit: usize,
    ) -> StorageResult<Vec<UFOsRecord>> {
        let s = self.clone();
        let collections = collections.to_vec();
        tokio::task::spawn_blocking(move || {
            MemReader::get_records_by_collections(&s, &collections, limit)
        })
        .await?
    }
}

pub struct MemWriter {
    keyspace: MemKeyspace,
    global: MemPartion,
    feeds: MemPartion,
    records: MemPartion,
    rollups: MemPartion,
    queues: MemPartion,
}

impl MemWriter {
    fn rollup_delete_account(
        &mut self,
        cursor: Cursor,
        key_bytes: &[u8],
        val_bytes: &[u8],
    ) -> StorageResult<usize> {
        let did = db_complete::<DeleteAccountQueueVal>(val_bytes)?;
        self.delete_account(&did)?;
        let mut batch = self.keyspace.batch();
        batch.remove(&self.queues, key_bytes);
        insert_batch_static_neu::<NewRollupCursorKey>(&mut batch, &self.global, cursor)?;
        batch.commit()?;
        Ok(1)
    }

    fn rollup_live_counts(
        &mut self,
        timelies: impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), StorageError>>,
        cursor_exclusive_limit: Option<Cursor>,
        rollup_limit: usize,
    ) -> StorageResult<usize> {
        // current strategy is to buffer counts in mem before writing the rollups
        // we *could* read+write every single batch to rollup.. but their merge is associative so
        // ...so save the db some work up front? is this worth it? who knows...

        log::warn!("sup!!!");

        #[derive(Eq, Hash, PartialEq)]
        enum Rollup {
            Hourly(HourTruncatedCursor),
            Weekly(WeekTruncatedCursor),
            AllTime,
        }

        let mut batch = self.keyspace.batch();
        let mut cursors_advanced = 0;
        let mut last_cursor = Cursor::from_start();
        let mut counts_by_rollup: HashMap<(Nsid, Rollup), CountsValue> = HashMap::new();

        log::warn!("about to loop....");
        for (i, kv) in timelies.enumerate() {
            // log::warn!("loop {i}...");
            if i >= rollup_limit {
                break;
            }

            let (key_bytes, val_bytes) = kv?;
            let key = db_complete::<LiveCountsKey>(&key_bytes)
                    .inspect_err(|e| log::warn!("rlc: key: {e:?}"))?;

            if cursor_exclusive_limit
                .map(|limit| key.cursor() > limit)
                .unwrap_or(false)
            {
                break;
            }

            batch.remove(&self.rollups, &key_bytes);
            let val = db_complete::<CountsValue>(&val_bytes)
                    .inspect_err(|e| log::warn!("rlc: val: {e:?}"))?;
            counts_by_rollup
                .entry((
                    key.collection().clone(),
                    Rollup::Hourly(key.cursor().into()),
                ))
                .or_default()
                .merge(&val);
            counts_by_rollup
                .entry((
                    key.collection().clone(),
                    Rollup::Weekly(key.cursor().into()),
                ))
                .or_default()
                .merge(&val);
            counts_by_rollup
                .entry((key.collection().clone(), Rollup::AllTime))
                .or_default()
                .merge(&val);

            cursors_advanced += 1;
            last_cursor = key.cursor();
        }
        log::warn!("done looping. looping cbr counts(?)..");

        for ((nsid, rollup), counts) in counts_by_rollup {
            log::warn!("######################## cbr loop {nsid:?} {counts:?} ########################");
            let key_bytes = match rollup {
                Rollup::Hourly(hourly_cursor) => {
                    let k = HourlyRollupKey::new(hourly_cursor, &nsid);
                    log::info!("hrly k: {k:?}");
                    k.to_db_bytes()?
                }
                Rollup::Weekly(weekly_cursor) => {
                    let k = WeeklyRollupKey::new(weekly_cursor, &nsid);
                    log::info!("weekly k: {k:?}");
                    k.to_db_bytes()?
                }
                Rollup::AllTime => {
                    let k = AllTimeRollupKey::new(&nsid);
                    log::info!("alltime k: {k:?}");
                    k.to_db_bytes()?
                }
            };
            // log::info!("key bytes: {key_bytes:?}");
            let mut rolled: CountsValue = self
                .rollups
                .get(&key_bytes)?
                .inspect(|v| {
                    let lax = CountsValue::from_db_bytes(v);
                    log::info!("val: len={}, lax={lax:?} first32={:?}", v.len(), v.get(..32));
                })
                .as_deref()
                .map(db_complete::<CountsValue>)
                .transpose()
                .inspect_err(|e| log::warn!("oooh did we break on the rolled thing? {e:?}"))?
                .unwrap_or_default();


            // try to round-trip before inserting, for funsies
            let tripppin = counts.to_db_bytes()?;
            let (and_back, n) = CountsValue::from_db_bytes(&tripppin)?;
            assert_eq!(n, tripppin.len());
            assert_eq!(counts.prefix, and_back.prefix);
            assert_eq!(counts.dids().estimate(), and_back.dids().estimate());
            if counts.records() > 20000000 {
                panic!("COUNTS maybe wtf? {counts:?}")
            }
            // assert_eq!(rolled, and_back);


            rolled.merge(&counts);

            // try to round-trip before inserting, for funsies
            let tripppin = rolled.to_db_bytes()?;
            let (and_back, n) = CountsValue::from_db_bytes(&tripppin)?;
            assert_eq!(n, tripppin.len());
            assert_eq!(rolled.prefix, and_back.prefix);
            assert_eq!(rolled.dids().estimate(), and_back.dids().estimate());
            if rolled.records() > 20000000 {
                panic!("maybe wtf? {rolled:?}")
            }
            // assert_eq!(rolled, and_back);

            batch.insert(&self.rollups, &key_bytes, &rolled.to_db_bytes()?);
        }

        log::warn!("done cbr loop.");

        insert_batch_static_neu::<NewRollupCursorKey>(&mut batch, &self.global, last_cursor)
            .inspect_err(|e| log::warn!("insert neu: {e:?}"))?;

        batch.commit()?;

        log::warn!("ok finished rlc stuff. huh.");
        Ok(cursors_advanced)
    }
}

impl StoreWriter for MemWriter {
    fn insert_batch<const LIMIT: usize>(
        &mut self,
        event_batch: EventBatch<LIMIT>,
    ) -> StorageResult<()> {
        if event_batch.is_empty() {
            return Ok(());
        }

        let mut batch = self.keyspace.batch();

        // would be nice not to have to iterate everything at once here
        let latest = event_batch.latest_cursor().unwrap();

        for (nsid, commits) in event_batch.commits_by_nsid {
            for commit in commits.commits {
                let location_key: RecordLocationKey = (&commit, &nsid).into();

                match commit.action {
                    CommitAction::Cut => {
                        batch.remove(&self.records, &location_key.to_db_bytes()?);
                    }
                    CommitAction::Put(put_action) => {
                        let feed_key = NsidRecordFeedKey::from_pair(nsid.clone(), commit.cursor);
                        let feed_val: NsidRecordFeedVal =
                            (&commit.did, &commit.rkey, commit.rev.as_str()).into();
                        batch.insert(
                            &self.feeds,
                            &feed_key.to_db_bytes()?,
                            &feed_val.to_db_bytes()?,
                        );


                        let location_val: RecordLocationVal =
                            (commit.cursor, commit.rev.as_str(), put_action).into();
                        batch.insert(
                            &self.records,
                            &location_key.to_db_bytes()?,
                            &location_val.to_db_bytes()?,
                        );
                    }
                }
            }
            let live_counts_key: LiveCountsKey = (latest, &nsid).into();
            let counts_value = CountsValue::new(commits.total_seen as u64, commits.dids_estimate);
            batch.insert(
                &self.rollups,
                &live_counts_key.to_db_bytes()?,
                &counts_value.to_db_bytes()?,
            );
        }

        for remove in event_batch.account_removes {
            let queue_key = DeleteAccountQueueKey::new(remove.cursor);
            let queue_val: DeleteAccountQueueVal = remove.did;
            batch.insert(
                &self.queues,
                &queue_key.to_db_bytes()?,
                &queue_val.to_db_bytes()?,
            );
        }

        batch.insert(
            &self.global,
            &DbStaticStr::<JetstreamCursorKey>::default().to_db_bytes()?,
            &latest.to_db_bytes()?,
        );

        batch.commit()?;
        Ok(())
    }

     fn step_rollup(&mut self) -> StorageResult<usize> {
        let rollup_cursor =
            get_static_neu::<NewRollupCursorKey, NewRollupCursorValue>(&self.global)?.ok_or(
                StorageError::BadStateError("Could not find current rollup cursor".to_string()),
            )
            .inspect_err(|e| log::warn!("failed getting rollup cursor: {e:?}"))?;

        // timelies
        let live_counts_range = LiveCountsKey::range_from_cursor(rollup_cursor)
            .inspect_err(|e| log::warn!("live counts range: {e:?}"))?;
        let mut timely_iter = self.rollups.range(live_counts_range).into_iter().peekable();

        let timely_next_cursor = timely_iter
            .peek_mut()
            .map(|kv| -> StorageResult<Cursor> {
                match kv {
                    Err(e) => Err(std::mem::replace(e, StorageError::Stolen))?,
                    Ok((key_bytes, _)) => {
                        let key = db_complete::<LiveCountsKey>(key_bytes)
                            .inspect_err(|e| log::warn!("failed getting key for next timely: {e:?}"))?;
                        Ok(key.cursor())
                    }
                }
            })
            .transpose()
            .inspect_err(|e| log::warn!("something about timely: {e:?}"))?;

        // delete accounts
        let delete_accounts_range =
            DeleteAccountQueueKey::new(rollup_cursor).range_to_prefix_end()?;

        let next_delete = self
            .queues
            .range(delete_accounts_range)
            .into_iter()
            .next()
            .transpose()
            .inspect_err(|e| log::warn!("range for next delete: {e:?}"))?
            .map(|(key_bytes, val_bytes)| {
                db_complete::<DeleteAccountQueueKey>(&key_bytes)
                    .inspect_err(|e| log::warn!("failed inside next delete thing????: {e:?}"))
                    .map(|k| (k.suffix, key_bytes, val_bytes))
            })
            .transpose()
            .inspect_err(|e| log::warn!("failed getting next delete: {e:?}"))?;

        let cursors_stepped = match (timely_next_cursor, next_delete) {
            (
                Some(timely_next_cursor),
                Some((delete_cursor, delete_key_bytes, delete_val_bytes)),
            ) => {
                if timely_next_cursor < delete_cursor {
                    self.rollup_live_counts(
                        timely_iter,
                        Some(delete_cursor),
                        MAX_BATCHED_ROLLUP_COUNTS,
                    )
                    .inspect_err(|e| log::warn!("rolling up live counts: {e:?}"))?
                } else {
                    self.rollup_delete_account(delete_cursor, &delete_key_bytes, &delete_val_bytes)
                    .inspect_err(|e| log::warn!("deleting acocunt: {e:?}"))?
                }
            }
            (Some(_), None) => {
                self.rollup_live_counts(timely_iter, None, MAX_BATCHED_ROLLUP_COUNTS)
                    .inspect_err(|e| log::warn!("rolling up (lasjdflkajs): {e:?}"))?
            }
            (None, Some((delete_cursor, delete_key_bytes, delete_val_bytes))) => {
                self.rollup_delete_account(delete_cursor, &delete_key_bytes, &delete_val_bytes)
                    .inspect_err(|e| log::warn!("deleting acocunt other branch: {e:?}"))?
            }
            (None, None) => 0,
        };

        Ok(cursors_stepped)
    }

    fn trim_collection(
        &mut self,
        collection: &Nsid,
        limit: usize,
        // TODO: could add a start cursor limit to avoid iterating deleted stuff at the start (/end)
    ) -> StorageResult<()> {
        let mut dangling_feed_keys_cleaned = 0;
        let mut records_deleted = 0;

        let mut batch = self.keyspace.batch();

        let prefix = NsidRecordFeedKey::from_prefix_to_db_bytes(collection)?;
        let mut found = 0;
        for kv in self.feeds.prefix(&prefix).into_iter().rev() {
            let (key_bytes, val_bytes) = kv?;
            let feed_key = db_complete::<NsidRecordFeedKey>(&key_bytes)?;
            let feed_val = db_complete::<NsidRecordFeedVal>(&val_bytes)?;
            let location_key: RecordLocationKey = (&feed_key, &feed_val).into();
            let location_key_bytes = location_key.to_db_bytes()?;

            let Some(location_val_bytes) = self.records.get(&location_key_bytes)? else {
                // record was deleted (hopefully)
                batch.remove(&self.feeds, &location_key_bytes);
                dangling_feed_keys_cleaned += 1;
                continue;
            };

            let (meta, _) = RecordLocationMeta::from_db_bytes(&location_val_bytes)?;

            if meta.cursor() != feed_key.cursor() {
                // older/different version
                batch.remove(&self.feeds, &location_key_bytes);
                dangling_feed_keys_cleaned += 1;
                continue;
            }
            if meta.rev != feed_val.rev() {
                // weird...
                log::warn!("record lookup: cursor match but rev did not...? removing.");
                batch.remove(&self.feeds, &location_key_bytes);
                dangling_feed_keys_cleaned += 1;
                continue;
            }

            if batch.len() >= MAX_BATCHED_CLEANUP_SIZE {
                batch.commit()?;
                batch = self.keyspace.batch();
            }

            found += 1;
            if found <= limit {
                continue;
            }

            batch.remove(&self.feeds, &location_key_bytes);
            batch.remove(&self.records, &location_key_bytes);
            records_deleted += 1;
        }

        batch.commit()?;

        log::info!("trim_collection ({collection:?}) removed {dangling_feed_keys_cleaned} dangling feed entries and {records_deleted} records");
        Ok(())
    }

    fn delete_account(&mut self, did: &Did) -> Result<usize, StorageError> {
        let mut records_deleted = 0;
        let mut batch = self.keyspace.batch();
        let prefix = RecordLocationKey::from_prefix_to_db_bytes(did)?;
        for kv in self.records.prefix(&prefix) {
            let (key_bytes, _) = kv?;
            batch.remove(&self.records, &key_bytes);
            records_deleted += 1;
            if batch.len() >= MAX_BATCHED_ACCOUNT_DELETE_RECORDS {
                batch.commit()?;
                batch = self.keyspace.batch();
            }
        }
        batch.commit()?;
        Ok(records_deleted)
    }
}

/// Get a value from a fixed key
fn get_static_neu<K: StaticStr, V: DbBytes>(global: &MemPartion) -> StorageResult<Option<V>> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value = global
        .get(&key_bytes)?
        .map(|value_bytes| db_complete(&value_bytes))
        .transpose()?;
    Ok(value)
}

/// Get a value from a fixed key
fn get_snapshot_static_neu<K: StaticStr, V: DbBytes>(
    global: &MemPartion,
) -> StorageResult<Option<V>> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value = global
        .get(&key_bytes)?
        .map(|value_bytes| db_complete(&value_bytes))
        .transpose()?;
    Ok(value)
}

/// Set a value to a fixed key
fn insert_static_neu<K: StaticStr>(
    global: &MemPartion,
    value: impl DbBytes,
) -> StorageResult<()> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value_bytes = value.to_db_bytes()?;
    global.insert(&key_bytes, &value_bytes)?;
    Ok(())
}

/// Set a value to a fixed key
fn insert_batch_static_neu<K: StaticStr>(
    batch: &mut MemBatch,
    global: &MemPartion,
    value: impl DbBytes,
) -> StorageResult<()> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value_bytes = value.to_db_bytes()?;
    batch.insert(global, &key_bytes, &value_bytes);
    Ok(())
}

#[derive(Debug, serde::Serialize, schemars::JsonSchema)]
pub struct StorageInfo {
    pub keyspace_disk_space: u64,
    pub keyspace_journal_count: usize,
    pub keyspace_sequence: u64,
    pub global_approximate_len: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeleteAccount, RecordKey, UFOsCommit};
    use jetstream::events::{CommitEvent, CommitOp};
    use jetstream::exports::Cid;
    use serde_json::value::RawValue;

    fn fjall_db() -> (MemReader, MemWriter) {
        let (read, write, _) = MemStorage::init(
            tempfile::tempdir().unwrap(),
            "offline test (no real jetstream endpoint)".to_string(),
            false,
            MemConfig { temp: true },
        )
        .unwrap();
        (read, write)
    }

    const TEST_BATCH_LIMIT: usize = 16;

    #[derive(Debug, Default)]
    struct TestBatch {
        pub batch: EventBatch<TEST_BATCH_LIMIT>,
    }

    impl TestBatch {
        #[allow(clippy::too_many_arguments)]
        pub fn create(
            &mut self,
            did: &str,
            collection: &str,
            rkey: &str,
            record: &str,
            rev: Option<&str>,
            cid: Option<Cid>,
            cursor: u64,
        ) -> Nsid {
            let did = Did::new(did.to_string()).unwrap();
            let collection = Nsid::new(collection.to_string()).unwrap();
            let record = RawValue::from_string(record.to_string()).unwrap();
            let cid = cid.unwrap_or(
                "bafyreidofvwoqvd2cnzbun6dkzgfucxh57tirf3ohhde7lsvh4fu3jehgy"
                    .parse()
                    .unwrap(),
            );

            let event = CommitEvent {
                collection,
                rkey: RecordKey::new(rkey.to_string()).unwrap(),
                rev: rev.unwrap_or("asdf").to_string(),
                operation: CommitOp::Create,
                record: Some(record),
                cid: Some(cid),
            };

            let (commit, collection) =
                UFOsCommit::from_commit_info(event, did.clone(), Cursor::from_raw_u64(cursor))
                    .unwrap();

            self.batch
                .commits_by_nsid
                .entry(collection.clone())
                .or_default()
                .truncating_insert(commit)
                .unwrap();

            collection
        }
        #[allow(clippy::too_many_arguments)]
        pub fn update(
            &mut self,
            did: &str,
            collection: &str,
            rkey: &str,
            record: &str,
            rev: Option<&str>,
            cid: Option<Cid>,
            cursor: u64,
        ) -> Nsid {
            let did = Did::new(did.to_string()).unwrap();
            let collection = Nsid::new(collection.to_string()).unwrap();
            let record = RawValue::from_string(record.to_string()).unwrap();
            let cid = cid.unwrap_or(
                "bafyreidofvwoqvd2cnzbun6dkzgfucxh57tirf3ohhde7lsvh4fu3jehgy"
                    .parse()
                    .unwrap(),
            );

            let event = CommitEvent {
                collection,
                rkey: RecordKey::new(rkey.to_string()).unwrap(),
                rev: rev.unwrap_or("asdf").to_string(),
                operation: CommitOp::Update,
                record: Some(record),
                cid: Some(cid),
            };

            let (commit, collection) =
                UFOsCommit::from_commit_info(event, did.clone(), Cursor::from_raw_u64(cursor))
                    .unwrap();

            self.batch
                .commits_by_nsid
                .entry(collection.clone())
                .or_default()
                .truncating_insert(commit)
                .unwrap();

            collection
        }
        #[allow(clippy::too_many_arguments)]
        pub fn delete(
            &mut self,
            did: &str,
            collection: &str,
            rkey: &str,
            rev: Option<&str>,
            cursor: u64,
        ) -> Nsid {
            let did = Did::new(did.to_string()).unwrap();
            let collection = Nsid::new(collection.to_string()).unwrap();
            let event = CommitEvent {
                collection,
                rkey: RecordKey::new(rkey.to_string()).unwrap(),
                rev: rev.unwrap_or("asdf").to_string(),
                operation: CommitOp::Delete,
                record: None,
                cid: None,
            };

            let (commit, collection) =
                UFOsCommit::from_commit_info(event, did, Cursor::from_raw_u64(cursor)).unwrap();

            self.batch
                .commits_by_nsid
                .entry(collection.clone())
                .or_default()
                .truncating_insert(commit)
                .unwrap();

            collection
        }
        pub fn delete_account(&mut self, did: &str, cursor: u64) -> Did {
            let did = Did::new(did.to_string()).unwrap();
            self.batch.account_removes.push(DeleteAccount {
                did: did.clone(),
                cursor: Cursor::from_raw_u64(cursor),
            });
            did
        }
    }

    #[test]
    fn test_hello() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();
        write.insert_batch::<TEST_BATCH_LIMIT>(EventBatch::default())?;
        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("a.b.c".to_string()).unwrap())?;
        assert_eq!(records, 0);
        assert_eq!(dids, 0);
        Ok(())
    }

    #[test]
    fn test_insert_one() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        let collection = batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.b.c",
            "asdf",
            "{}",
            Some("rev-z"),
            None,
            100,
        );
        write.insert_batch(batch.batch)?;

        let (records, dids) = read.get_counts_by_collection(&collection)?;
        assert_eq!(records, 1);
        assert_eq!(dids, 1);
        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("d.e.f".to_string()).unwrap())?;
        assert_eq!(records, 0);
        assert_eq!(dids, 0);

        let records = read.get_records_by_collections(&[collection], 2)?;
        assert_eq!(records.len(), 1);
        let rec = &records[0];
        assert_eq!(rec.record.get(), "{}");
        assert!(!rec.is_update);

        let records =
            read.get_records_by_collections(&[Nsid::new("d.e.f".to_string()).unwrap()], 2)?;
        assert_eq!(records.len(), 0);

        Ok(())
    }

    #[test]
    fn test_get_multi_collection() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.a.a",
            "aaa",
            r#""earliest""#,
            Some("rev-a"),
            None,
            100,
        );
        batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.a.b",
            "aab",
            r#""in between""#,
            Some("rev-ab"),
            None,
            101,
        );
        batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.a.a",
            "aaa-2",
            r#""last""#,
            Some("rev-a-2"),
            None,
            102,
        );
        write.insert_batch(batch.batch)?;

        let records = read.get_records_by_collections(
            &[
                Nsid::new("a.a.a".to_string()).unwrap(),
                Nsid::new("a.a.b".to_string()).unwrap(),
                Nsid::new("a.a.c".to_string()).unwrap(),
            ],
            100,
        )?;
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].record.get(), r#""last""#);
        assert_eq!(
            records[0].collection,
            Nsid::new("a.a.a".to_string()).unwrap()
        );
        assert_eq!(records[1].record.get(), r#""in between""#);
        assert_eq!(
            records[1].collection,
            Nsid::new("a.a.b".to_string()).unwrap()
        );
        assert_eq!(records[2].record.get(), r#""earliest""#);
        assert_eq!(
            records[2].collection,
            Nsid::new("a.a.a".to_string()).unwrap()
        );

        Ok(())
    }

    #[test]
    fn test_update_one() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        let collection = batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.b.c",
            "rkey-asdf",
            "{}",
            Some("rev-a"),
            None,
            100,
        );
        write.insert_batch(batch.batch)?;

        let mut batch = TestBatch::default();
        batch.update(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.b.c",
            "rkey-asdf",
            r#"{"ch":  "ch-ch-ch-changes"}"#,
            Some("rev-z"),
            None,
            101,
        );
        write.insert_batch(batch.batch)?;

        let (records, dids) = read.get_counts_by_collection(&collection)?;
        assert_eq!(records, 1);
        assert_eq!(dids, 1);

        let records = read.get_records_by_collections(&[collection], 2)?;
        assert_eq!(records.len(), 1);
        let rec = &records[0];
        assert_eq!(rec.record.get(), r#"{"ch":  "ch-ch-ch-changes"}"#);
        assert!(rec.is_update);
        Ok(())
    }

    #[test]
    fn test_delete_one() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        let collection = batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.b.c",
            "rkey-asdf",
            "{}",
            Some("rev-a"),
            None,
            100,
        );
        write.insert_batch(batch.batch)?;

        let mut batch = TestBatch::default();
        batch.delete(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.b.c",
            "rkey-asdf",
            Some("rev-z"),
            101,
        );
        write.insert_batch(batch.batch)?;

        let (records, dids) = read.get_counts_by_collection(&collection)?;
        assert_eq!(records, 1);
        assert_eq!(dids, 1);

        let records = read.get_records_by_collections(&[collection], 2)?;
        assert_eq!(records.len(), 0);

        Ok(())
    }

    #[test]
    fn test_collection_trim() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        let mut last_b_cursor;
        for i in 1..=10 {
            last_b_cursor = 11_000 + i;
            batch.create(
                &format!("did:plc:inze6wrmsm7pjl7yta3oig7{}", i % 3),
                "a.a.b",
                &format!("rkey-bbb-{i}"),
                &format!(r#"{{"n": {i}}}"#),
                Some(&format!("rev-bbb-{i}")),
                None,
                last_b_cursor,
            );
        }
        batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.a.c",
            "rkey-ccc",
            "{}",
            Some("rev-ccc"),
            None,
            12_000,
        );

        write.insert_batch(batch.batch)?;

        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.a".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 1);
        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.b".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 10);
        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.c".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 1);
        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.d".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 0);

        write.trim_collection(&Nsid::new("a.a.a".to_string()).unwrap(), 6)?;
        write.trim_collection(&Nsid::new("a.a.b".to_string()).unwrap(), 6)?;
        write.trim_collection(&Nsid::new("a.a.c".to_string()).unwrap(), 6)?;
        write.trim_collection(&Nsid::new("a.a.d".to_string()).unwrap(), 6)?;

        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.a".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 1);
        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.b".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 6);
        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.c".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 1);
        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.d".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 0);

        Ok(())
    }

    #[test]
    fn test_delete_account() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        for i in 1..=2 {
            batch.create(
                "did:plc:person-b",
                "a.a.a",
                &format!("rkey-bbb-{i}"),
                &format!(r#"{{"n": {i}}}"#),
                Some(&format!("rev-bbb-{i}")),
                None,
                11_000 + i,
            );
        }
        write.insert_batch(batch.batch)?;

        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.a".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 3);

        let records_deleted =
            write.delete_account(&Did::new("did:plc:person-b".to_string()).unwrap())?;
        assert_eq!(records_deleted, 2);

        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.a".to_string()).unwrap()], 100)?;
        assert_eq!(records.len(), 1);

        Ok(())
    }

    #[test]
    fn rollup_delete_account_removes_record() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        write.insert_batch(batch.batch)?;

        let mut batch = TestBatch::default();
        batch.delete_account("did:plc:person-a", 9_999); // queue it before the rollup
        write.insert_batch(batch.batch)?;

        write.step_rollup()?;

        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.a".to_string()).unwrap()], 1)?;
        assert_eq!(records.len(), 0);

        Ok(())
    }

    #[test]
    fn rollup_delete_live_count_step() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        write.insert_batch(batch.batch)?;

        let n = write.step_rollup()?;
        assert_eq!(n, 1);

        let mut batch = TestBatch::default();
        batch.delete_account("did:plc:person-a", 10_001);
        write.insert_batch(batch.batch)?;

        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.a".to_string()).unwrap()], 1)?;
        assert_eq!(records.len(), 1);

        let n = write.step_rollup()?;
        assert_eq!(n, 1);

        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.a".to_string()).unwrap()], 1)?;
        assert_eq!(records.len(), 0);

        let mut batch = TestBatch::default();
        batch.delete_account("did:plc:person-a", 9_999);
        write.insert_batch(batch.batch)?;

        let n = write.step_rollup()?;
        assert_eq!(n, 0);

        Ok(())
    }

    #[test]
    fn rollup_multiple_count_batches() -> anyhow::Result<()> {
        let (_read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        write.insert_batch(batch.batch)?;

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aab",
            "{}",
            Some("rev-aab"),
            None,
            10_001,
        );
        write.insert_batch(batch.batch)?;

        let n = write.step_rollup()?;
        assert_eq!(n, 2);

        let n = write.step_rollup()?;
        assert_eq!(n, 0);

        Ok(())
    }

    #[test]
    fn counts_before_and_after_rollup() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        batch.create(
            "did:plc:person-b",
            "a.a.a",
            "rkey-bbb",
            "{}",
            Some("rev-bbb"),
            None,
            10_001,
        );
        write.insert_batch(batch.batch)?;

        let mut batch = TestBatch::default();
        batch.delete_account("did:plc:person-a", 11_000);
        write.insert_batch(batch.batch)?;

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aac",
            "{}",
            Some("rev-aac"),
            None,
            12_000,
        );
        write.insert_batch(batch.batch)?;

        // before any rollup
        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("a.a.a".to_string()).unwrap())?;
        assert_eq!(records, 3);
        assert_eq!(dids, 2);

        // first batch rolled up
        let n = write.step_rollup()?;
        assert_eq!(n, 1);

        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("a.a.a".to_string()).unwrap())?;
        assert_eq!(records, 3);
        assert_eq!(dids, 2);

        // delete account rolled up
        let n = write.step_rollup()?;
        assert_eq!(n, 1);

        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("a.a.a".to_string()).unwrap())?;
        assert_eq!(records, 3);
        assert_eq!(dids, 2);

        // second batch rolled up
        let n = write.step_rollup()?;
        assert_eq!(n, 1);

        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("a.a.a".to_string()).unwrap())?;
        assert_eq!(records, 3);
        assert_eq!(dids, 2);

        // no more rollups left
        let n = write.step_rollup()?;
        assert_eq!(n, 0);

        Ok(())
    }

    #[test]
    fn get_top_collections() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        batch.create(
            "did:plc:person-b",
            "a.a.b",
            "rkey-bbb",
            "{}",
            Some("rev-bbb"),
            None,
            10_001,
        );
        batch.create(
            "did:plc:person-c",
            "a.b.c",
            "rkey-ccc",
            "{}",
            Some("rev-ccc"),
            None,
            10_002,
        );
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aaa-2",
            "{}",
            Some("rev-aaa-2"),
            None,
            10_003,
        );
        write.insert_batch(batch.batch)?;

        let n = write.step_rollup()?;
        assert_eq!(n, 3); // 3 collections

        let tops = read.get_top_collections()?;
        assert_eq!(
            tops,
            TopCollections {
                total_records: 4,
                dids_estimate: 3,
                nsid_child_segments: HashMap::from([(
                    "a".to_string(),
                    TopCollections {
                        total_records: 4,
                        dids_estimate: 3,
                        nsid_child_segments: HashMap::from([
                            (
                                "a".to_string(),
                                TopCollections {
                                    total_records: 3,
                                    dids_estimate: 2,
                                    nsid_child_segments: HashMap::from([
                                        (
                                            "a".to_string(),
                                            TopCollections {
                                                total_records: 2,
                                                dids_estimate: 1,
                                                nsid_child_segments: HashMap::from([]),
                                            },
                                        ),
                                        (
                                            "b".to_string(),
                                            TopCollections {
                                                total_records: 1,
                                                dids_estimate: 1,
                                                nsid_child_segments: HashMap::from([]),
                                            }
                                        ),
                                    ]),
                                },
                            ),
                            (
                                "b".to_string(),
                                TopCollections {
                                    total_records: 1,
                                    dids_estimate: 1,
                                    nsid_child_segments: HashMap::from([(
                                        "c".to_string(),
                                        TopCollections {
                                            total_records: 1,
                                            dids_estimate: 1,
                                            nsid_child_segments: HashMap::from([]),
                                        },
                                    ),]),
                                },
                            ),
                        ]),
                    },
                ),]),
            }
        );
        Ok(())
    }
}
