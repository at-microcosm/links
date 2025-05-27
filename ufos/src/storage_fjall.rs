use crate::db_types::{db_complete, DbBytes, DbStaticStr, StaticStr};
use crate::error::StorageError;
use crate::storage::{StorageResult, StorageWhatever, StoreBackground, StoreReader, StoreWriter};
use crate::store_types::{
    AllTimeDidsKey, AllTimeRecordsKey, AllTimeRollupKey, CountsValue, DeleteAccountQueueKey,
    DeleteAccountQueueVal, HourTruncatedCursor, HourlyDidsKey, HourlyRecordsKey, HourlyRollupKey,
    JetstreamCursorKey, JetstreamCursorValue, JetstreamEndpointKey, JetstreamEndpointValue,
    LiveCountsKey, NewRollupCursorKey, NewRollupCursorValue, NsidRecordFeedKey, NsidRecordFeedVal,
    RecordLocationKey, RecordLocationMeta, RecordLocationVal, RecordRawValue, SketchSecretKey,
    SketchSecretPrefix, TakeoffKey, TakeoffValue, TrimCollectionCursorKey, WeekTruncatedCursor,
    WeeklyDidsKey, WeeklyRecordsKey, WeeklyRollupKey,
};
use crate::{
    CommitAction, ConsumerInfo, Did, EventBatch, Nsid, NsidCount, QueryPeriod, TopCollections,
    UFOsRecord,
};
use async_trait::async_trait;
use fjall::{Batch as FjallBatch, Config, Keyspace, PartitionCreateOptions, PartitionHandle};
use jetstream::events::Cursor;
use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant, SystemTime};

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
///  - Cardinality estimator secret
///      - key: "sketch_secret" (literal)
///      - val: [u8; 16]
///
///  - Rollup cursor (bg work: roll stats into hourlies, delete accounts, old record deletes)
///      - key: "rollup_cursor" (literal)
///      - val: u64 (tracks behind js_cursor)
///
///  - Feed trim cursor (bg work: delete oldest excess records)
///      - key: "trim_cursor" || nullstr (nsid)
///      - val: u64 (earliest previously-removed feed entry jetstream cursor)
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
///
/// - Hourly total record counts and dids estimate per collection
///      - key: "hourly_counts" || u64 || nullstr (hour, nsid)
///      - val: u64 || HLL (count (not cursor), estimator)
///
/// - Hourly record count ranking
///      - key: "hourly_rank_records" || u64 || u64 || nullstr (hour, count, nsid)
///      - val: [empty]
///
/// - Hourly did estimate ranking
///      - key: "hourly_rank_dids" || u64 || u64 || nullstr (hour, dids estimate, nsid)
///      - val: [empty]
///
///
/// - Weekly total record counts and dids estimate per collection
///      - key: "weekly_counts" || u64 || nullstr (week, nsid)
///      - val: u64 || HLL (count (not cursor), estimator)
///
/// - Weekly record count ranking
///      - key: "weekly_rank_records" || u64 || u64 || nullstr (week, count, nsid)
///      - val: [empty]
///
/// - Weekly did estimate ranking
///      - key: "weekly_rank_dids" || u64 || u64 || nullstr (week, dids estimate, nsid)
///      - val: [empty]
///
///
/// - All-time total record counts and dids estimate per collection
///      - key: "ever_counts" || nullstr (nsid)
///      - val: u64 || HLL (count (not cursor), estimator)
///
/// - All-time total record record count ranking
///      - key: "ever_rank_records" || u64 || nullstr (count, nsid)
///      - val: [empty]
///
/// - All-time did estimate ranking
///      - key: "ever_rank_dids" || u64 || nullstr (dids estimate, nsid)
///      - val: [empty]
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
pub struct FjallStorage {}

#[derive(Debug, Default)]
pub struct FjallConfig {
    /// drop the db when the storage is dropped
    ///
    /// this is only meant for tests
    #[cfg(test)]
    pub temp: bool,
}

impl StorageWhatever<FjallReader, FjallWriter, FjallBackground, FjallConfig> for FjallStorage {
    fn init(
        path: impl AsRef<Path>,
        endpoint: String,
        force_endpoint: bool,
        _config: FjallConfig,
    ) -> StorageResult<(FjallReader, FjallWriter, Option<Cursor>, SketchSecretPrefix)> {
        let keyspace = {
            let config = Config::new(path);

            // #[cfg(not(test))]
            // let config = config.fsync_ms(Some(4_000));

            config.open()?
        };

        let global = keyspace.open_partition("global", PartitionCreateOptions::default())?;
        let feeds = keyspace.open_partition("feeds", PartitionCreateOptions::default())?;
        let records = keyspace.open_partition("records", PartitionCreateOptions::default())?;
        let rollups = keyspace.open_partition("rollups", PartitionCreateOptions::default())?;
        let queues = keyspace.open_partition("queues", PartitionCreateOptions::default())?;

        let js_cursor = get_static_neu::<JetstreamCursorKey, JetstreamCursorValue>(&global)?;

        let sketch_secret = if js_cursor.is_some() {
            let stored_endpoint =
                get_static_neu::<JetstreamEndpointKey, JetstreamEndpointValue>(&global)?;
            let JetstreamEndpointValue(stored) = stored_endpoint.ok_or(StorageError::InitError(
                "found cursor but missing js_endpoint, refusing to start.".to_string(),
            ))?;

            let Some(stored_secret) =
                get_static_neu::<SketchSecretKey, SketchSecretPrefix>(&global)?
            else {
                return Err(StorageError::InitError(
                    "found cursor but missing sketch_secret, refusing to start.".to_string(),
                ));
            };

            if stored != endpoint {
                if force_endpoint {
                    log::warn!("forcing a jetstream switch from {stored:?} to {endpoint:?}");
                    insert_static_neu::<JetstreamEndpointKey>(
                        &global,
                        JetstreamEndpointValue(endpoint.to_string()),
                    )?;
                } else {
                    return Err(StorageError::InitError(format!(
                        "stored js_endpoint {stored:?} differs from provided {endpoint:?}, refusing to start without --jetstream-force.")));
                }
            }
            stored_secret
        } else {
            log::info!("initializing a fresh db!");
            init_static_neu::<JetstreamEndpointKey>(
                &global,
                JetstreamEndpointValue(endpoint.to_string()),
            )?;

            log::info!("generating new secret for cardinality sketches...");
            let mut sketch_secret: SketchSecretPrefix = [0u8; 16];
            getrandom::fill(&mut sketch_secret).map_err(|e| {
                StorageError::InitError(format!(
                    "failed to get a random secret for cardinality sketches: {e:?}"
                ))
            })?;
            init_static_neu::<SketchSecretKey>(&global, sketch_secret)?;

            init_static_neu::<TakeoffKey>(&global, Cursor::at(SystemTime::now()))?;
            init_static_neu::<NewRollupCursorKey>(&global, Cursor::from_start())?;

            sketch_secret
        };

        let reader = FjallReader {
            keyspace: keyspace.clone(),
            global: global.clone(),
            feeds: feeds.clone(),
            records: records.clone(),
            rollups: rollups.clone(),
        };
        let writer = FjallWriter {
            bg_taken: Arc::new(AtomicBool::new(false)),
            keyspace,
            global,
            feeds,
            records,
            rollups,
            queues,
        };
        Ok((reader, writer, js_cursor, sketch_secret))
    }
}

type FjallRKV = fjall::Result<(fjall::Slice, fjall::Slice)>;

#[derive(Clone)]
pub struct FjallReader {
    keyspace: Keyspace,
    global: PartitionHandle,
    feeds: PartitionHandle,
    records: PartitionHandle,
    rollups: PartitionHandle,
}

/// An iterator that knows how to skip over deleted/invalidated records
struct RecordIterator {
    db_iter: Box<dyn Iterator<Item = FjallRKV>>,
    records: PartitionHandle,
    limit: usize,
    fetched: usize,
}
impl RecordIterator {
    pub fn new(
        feeds: &PartitionHandle,
        records: PartitionHandle,
        collection: &Nsid,
        limit: usize,
    ) -> StorageResult<Self> {
        let prefix = NsidRecordFeedKey::from_prefix_to_db_bytes(collection)?;
        let db_iter = feeds.prefix(prefix).rev();
        Ok(Self {
            db_iter: Box::new(db_iter),
            records,
            limit,
            fetched: 0,
        })
    }
    fn get_record(&self, db_next: FjallRKV) -> StorageResult<Option<UFOsRecord>> {
        let (key_bytes, val_bytes) = db_next?;
        let feed_key = db_complete::<NsidRecordFeedKey>(&key_bytes)?;
        let feed_val = db_complete::<NsidRecordFeedVal>(&val_bytes)?;
        let location_key: RecordLocationKey = (&feed_key, &feed_val).into();

        let Some(location_val_bytes) = self.records.get(location_key.to_db_bytes()?)? else {
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

impl FjallReader {
    fn get_storage_stats(&self) -> StorageResult<serde_json::Value> {
        let rollup_cursor =
            get_static_neu::<NewRollupCursorKey, NewRollupCursorValue>(&self.global)?
                .map(|c| c.to_raw_u64());

        Ok(serde_json::json!({
            "keyspace_disk_space": self.keyspace.disk_space(),
            "keyspace_journal_count": self.keyspace.journal_count(),
            "keyspace_sequence": self.keyspace.instant(),
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

        let rollup_cursor =
            get_snapshot_static_neu::<NewRollupCursorKey, NewRollupCursorValue>(&global)?
                .map(|c| c.to_raw_u64());

        Ok(ConsumerInfo::Jetstream {
            endpoint,
            started_at,
            latest_cursor,
            rollup_cursor,
        })
    }

    fn get_all_collections(
        &self,
        period: QueryPeriod,
        limit: usize,
        cursor: Option<Vec<u8>>,
    ) -> StorageResult<(Vec<NsidCount>, Option<Vec<u8>>)> {
        Ok(if period.is_all_time() {
            let snapshot = self.rollups.snapshot();

            let start = if let Some(cursor_bytes) = cursor {
                let nsid = db_complete::<Nsid>(&cursor_bytes)?; // TODO: bubble a *client* error type
                Bound::Excluded(
                    AllTimeRollupKey::from_pair(Default::default(), nsid).to_db_bytes()?,
                )
            } else {
                Bound::Included(AllTimeRollupKey::from_prefix_to_db_bytes(
                    &Default::default(),
                )?)
            };

            let end_bytes = AllTimeRollupKey::prefix_range_end(&Default::default())?;
            let end = Bound::Excluded(end_bytes.clone());

            let mut out = Vec::new();
            let mut next_cursor = None;
            log::warn!(
                "ranging snapshot with limit: {limit}, end: {:?}",
                str::from_utf8(&end_bytes)
            );
            for (i, kv) in snapshot.range((start, end)).take(limit).enumerate() {
                let (key_bytes, val_bytes) = kv?;
                let key = db_complete::<AllTimeRollupKey>(&key_bytes)?;
                let db_counts = db_complete::<CountsValue>(&val_bytes)?;
                out.push(NsidCount {
                    nsid: key.collection().to_string(),
                    records: db_counts.records(),
                    dids_estimate: db_counts.dids().estimate() as u64,
                });
                if i == limit - 1 {
                    log::warn!("reached limit, setting next cursor");
                    let nsid_bytes = key.collection().to_db_bytes()?;
                    next_cursor = Some(nsid_bytes);
                }
            }

            (out, next_cursor)
        } else {
            todo!()
        })
    }

    fn get_top_collections_by_count(
        &self,
        limit: usize,
        period: QueryPeriod,
    ) -> StorageResult<Vec<NsidCount>> {
        Ok(if period.is_all_time() {
            let snapshot = self.rollups.snapshot();
            let mut out = Vec::with_capacity(limit);
            let prefix = AllTimeRecordsKey::from_prefix_to_db_bytes(&Default::default())?;
            for kv in snapshot.prefix(prefix).rev().take(limit) {
                let (key_bytes, _) = kv?;
                let key = db_complete::<AllTimeRecordsKey>(&key_bytes)?;
                let rollup_key = AllTimeRollupKey::new(key.collection());
                let db_count_bytes = snapshot.get(rollup_key.to_db_bytes()?)?.expect(
                    "integrity: all-time rank rollup must have corresponding all-time count rollup",
                );
                let db_counts = db_complete::<CountsValue>(&db_count_bytes)?;
                assert_eq!(db_counts.records(), key.count());
                out.push(NsidCount {
                    nsid: key.collection().to_string(),
                    records: db_counts.records(),
                    dids_estimate: db_counts.dids().estimate() as u64,
                });
            }
            out
        } else {
            todo!()
        })
    }

    fn get_top_collections_by_dids(
        &self,
        limit: usize,
        period: QueryPeriod,
    ) -> StorageResult<Vec<NsidCount>> {
        Ok(if period.is_all_time() {
            let snapshot = self.rollups.snapshot();
            let mut out = Vec::with_capacity(limit);
            let prefix = AllTimeDidsKey::from_prefix_to_db_bytes(&Default::default())?;
            for kv in snapshot.prefix(prefix).rev().take(limit) {
                let (key_bytes, _) = kv?;
                let key = db_complete::<AllTimeDidsKey>(&key_bytes)?;
                let rollup_key = AllTimeRollupKey::new(key.collection());
                let db_count_bytes = snapshot.get(rollup_key.to_db_bytes()?)?.expect(
                    "integrity: all-time rank rollup must have corresponding all-time count rollup",
                );
                let db_counts = db_complete::<CountsValue>(&db_count_bytes)?;
                assert_eq!(db_counts.dids().estimate() as u64, key.count());
                out.push(NsidCount {
                    nsid: key.collection().to_string(),
                    records: db_counts.records(),
                    dids_estimate: db_counts.dids().estimate() as u64,
                });
            }
            out
        } else {
            todo!()
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
        expand_each_collection: bool,
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
                    if expand_each_collection {
                        continue;
                    } else {
                        break;
                    }
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
impl StoreReader for FjallReader {
    fn name(&self) -> String {
        "fjall storage v2".into()
    }
    async fn get_storage_stats(&self) -> StorageResult<serde_json::Value> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || FjallReader::get_storage_stats(&s)).await?
    }
    async fn get_consumer_info(&self) -> StorageResult<ConsumerInfo> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || FjallReader::get_consumer_info(&s)).await?
    }
    async fn get_all_collections(
        &self,
        period: QueryPeriod,
        limit: usize,
        cursor: Option<Vec<u8>>,
    ) -> StorageResult<(Vec<NsidCount>, Option<Vec<u8>>)> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || {
            FjallReader::get_all_collections(&s, period, limit, cursor)
        })
        .await?
    }
    async fn get_top_collections_by_count(
        &self,
        limit: usize,
        period: QueryPeriod,
    ) -> StorageResult<Vec<NsidCount>> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || {
            FjallReader::get_top_collections_by_count(&s, limit, period)
        })
        .await?
    }
    async fn get_top_collections_by_dids(
        &self,
        limit: usize,
        period: QueryPeriod,
    ) -> StorageResult<Vec<NsidCount>> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || {
            FjallReader::get_top_collections_by_dids(&s, limit, period)
        })
        .await?
    }
    async fn get_top_collections(&self) -> Result<TopCollections, StorageError> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || FjallReader::get_top_collections(&s)).await?
    }
    async fn get_counts_by_collection(&self, collection: &Nsid) -> StorageResult<(u64, u64)> {
        let s = self.clone();
        let collection = collection.clone();
        tokio::task::spawn_blocking(move || FjallReader::get_counts_by_collection(&s, &collection))
            .await?
    }
    async fn get_records_by_collections(
        &self,
        collections: &[Nsid],
        limit: usize,
        expand_each_collection: bool,
    ) -> StorageResult<Vec<UFOsRecord>> {
        let s = self.clone();
        let collections = collections.to_vec();
        tokio::task::spawn_blocking(move || {
            FjallReader::get_records_by_collections(&s, &collections, limit, expand_each_collection)
        })
        .await?
    }
}

#[derive(Clone)]
pub struct FjallWriter {
    bg_taken: Arc<AtomicBool>,
    keyspace: Keyspace,
    global: PartitionHandle,
    feeds: PartitionHandle,
    records: PartitionHandle,
    rollups: PartitionHandle,
    queues: PartitionHandle,
}

impl FjallWriter {
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
        timelies: impl Iterator<Item = Result<(fjall::Slice, fjall::Slice), fjall::Error>>,
        cursor_exclusive_limit: Option<Cursor>,
        rollup_limit: usize,
    ) -> StorageResult<(usize, HashSet<Nsid>)> {
        // current strategy is to buffer counts in mem before writing the rollups
        // we *could* read+write every single batch to rollup.. but their merge is associative so
        // ...so save the db some work up front? is this worth it? who knows...

        let mut dirty_nsids = HashSet::new();

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

        for (i, kv) in timelies.enumerate() {
            if i >= rollup_limit {
                break;
            }

            let (key_bytes, val_bytes) = kv?;
            let key = db_complete::<LiveCountsKey>(&key_bytes)?;

            if cursor_exclusive_limit
                .map(|limit| key.cursor() > limit)
                .unwrap_or(false)
            {
                break;
            }

            dirty_nsids.insert(key.collection().clone());

            batch.remove(&self.rollups, key_bytes);
            let val = db_complete::<CountsValue>(&val_bytes)?;
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

        // go through each new rollup thing and merge it with whatever might already be in the db
        for ((nsid, rollup), counts) in counts_by_rollup {
            let rollup_key_bytes = match rollup {
                Rollup::Hourly(hourly_cursor) => {
                    HourlyRollupKey::new(hourly_cursor, &nsid).to_db_bytes()?
                }
                Rollup::Weekly(weekly_cursor) => {
                    WeeklyRollupKey::new(weekly_cursor, &nsid).to_db_bytes()?
                }
                Rollup::AllTime => AllTimeRollupKey::new(&nsid).to_db_bytes()?,
            };
            let mut rolled: CountsValue = self
                .rollups
                .get(&rollup_key_bytes)?
                .as_deref()
                .map(db_complete::<CountsValue>)
                .transpose()?
                .unwrap_or_default();

            // now that we have values, we can know the exising ranks
            let before_records_count = rolled.records();
            let before_dids_estimate = rolled.dids().estimate() as u64;

            // update the rollup
            rolled.merge(&counts);

            // replace rank entries
            let (old_records, new_records, dids) = match rollup {
                Rollup::Hourly(hourly_cursor) => {
                    let old_records =
                        HourlyRecordsKey::new(hourly_cursor, before_records_count.into(), &nsid);
                    let new_records = old_records.with_rank(rolled.records().into());
                    let new_estimate = rolled.dids().estimate() as u64;
                    let dids = if new_estimate == before_dids_estimate {
                        None
                    } else {
                        let old_dids =
                            HourlyDidsKey::new(hourly_cursor, before_dids_estimate.into(), &nsid);
                        let new_dids = old_dids.with_rank(new_estimate.into());
                        Some((old_dids.to_db_bytes()?, new_dids.to_db_bytes()?))
                    };
                    (old_records.to_db_bytes()?, new_records.to_db_bytes()?, dids)
                }
                Rollup::Weekly(weekly_cursor) => {
                    let old_records =
                        WeeklyRecordsKey::new(weekly_cursor, before_records_count.into(), &nsid);
                    let new_records = old_records.with_rank(rolled.records().into());
                    let new_estimate = rolled.dids().estimate() as u64;
                    let dids = if new_estimate == before_dids_estimate {
                        None
                    } else {
                        let old_dids =
                            WeeklyDidsKey::new(weekly_cursor, before_dids_estimate.into(), &nsid);
                        let new_dids = old_dids.with_rank(new_estimate.into());
                        Some((old_dids.to_db_bytes()?, new_dids.to_db_bytes()?))
                    };
                    (old_records.to_db_bytes()?, new_records.to_db_bytes()?, dids)
                }
                Rollup::AllTime => {
                    let old_records = AllTimeRecordsKey::new(before_records_count.into(), &nsid);
                    let new_records = old_records.with_rank(rolled.records().into());
                    let new_estimate = rolled.dids().estimate() as u64;
                    let dids = if new_estimate == before_dids_estimate {
                        None
                    } else {
                        let old_dids = AllTimeDidsKey::new(before_dids_estimate.into(), &nsid);
                        let new_dids = old_dids.with_rank(new_estimate.into());
                        Some((old_dids.to_db_bytes()?, new_dids.to_db_bytes()?))
                    };
                    (old_records.to_db_bytes()?, new_records.to_db_bytes()?, dids)
                }
            };

            // replace the ranks
            batch.remove(&self.rollups, &old_records);
            batch.insert(&self.rollups, &new_records, "");
            if let Some((old_dids, new_dids)) = dids {
                batch.remove(&self.rollups, &old_dids);
                batch.insert(&self.rollups, &new_dids, "");
            }

            // replace the rollup
            batch.insert(&self.rollups, &rollup_key_bytes, &rolled.to_db_bytes()?);
        }

        insert_batch_static_neu::<NewRollupCursorKey>(&mut batch, &self.global, last_cursor)?;

        batch.commit()?;
        Ok((cursors_advanced, dirty_nsids))
    }
}

impl StoreWriter<FjallBackground> for FjallWriter {
    fn background_tasks(&mut self, reroll: bool) -> StorageResult<FjallBackground> {
        if self.bg_taken.swap(true, Ordering::SeqCst) {
            Err(StorageError::BackgroundAlreadyStarted)
        } else {
            if reroll {
                log::info!("reroll: resetting rollup cursor...");
                insert_static_neu::<NewRollupCursorKey>(&self.global, Cursor::from_start())?;
                log::info!("reroll: clearing trim cursors...");
                let mut batch = self.keyspace.batch();
                for kv in self
                    .global
                    .prefix(TrimCollectionCursorKey::from_prefix_to_db_bytes(
                        &Default::default(),
                    )?)
                {
                    let (k, _) = kv?;
                    batch.remove(&self.global, k);
                }
                let n = batch.len();
                batch.commit()?;
                log::info!("reroll: cleared {n} trim cursors.");
            }
            Ok(FjallBackground(self.clone()))
        }
    }

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
                            feed_key.to_db_bytes()?,
                            feed_val.to_db_bytes()?,
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
            DbStaticStr::<JetstreamCursorKey>::default().to_db_bytes()?,
            latest.to_db_bytes()?,
        );

        batch.commit()?;
        Ok(())
    }

    fn step_rollup(&mut self) -> StorageResult<(usize, HashSet<Nsid>)> {
        let mut dirty_nsids = HashSet::new();

        let rollup_cursor =
            get_static_neu::<NewRollupCursorKey, NewRollupCursorValue>(&self.global)?.ok_or(
                StorageError::BadStateError("Could not find current rollup cursor".to_string()),
            )?;

        // timelies
        let live_counts_range = LiveCountsKey::range_from_cursor(rollup_cursor)?;
        let mut timely_iter = self.rollups.range(live_counts_range).peekable();

        let timely_next = timely_iter
            .peek_mut()
            .map(|kv| -> StorageResult<LiveCountsKey> {
                match kv {
                    Err(e) => Err(std::mem::replace(e, fjall::Error::Poisoned))?,
                    Ok((key_bytes, _)) => {
                        let key = db_complete::<LiveCountsKey>(key_bytes)?;
                        Ok(key)
                    }
                }
            })
            .transpose()?;

        // delete accounts
        let delete_accounts_range =
            DeleteAccountQueueKey::new(rollup_cursor).range_to_prefix_end()?;

        let next_delete = self
            .queues
            .range(delete_accounts_range)
            .next()
            .transpose()?
            .map(|(key_bytes, val_bytes)| {
                db_complete::<DeleteAccountQueueKey>(&key_bytes)
                    .map(|k| (k.suffix, key_bytes, val_bytes))
            })
            .transpose()?;

        let cursors_stepped = match (timely_next, next_delete) {
            (Some(timely), Some((delete_cursor, delete_key_bytes, delete_val_bytes))) => {
                if timely.cursor() < delete_cursor {
                    let (n, dirty) = self.rollup_live_counts(
                        timely_iter,
                        Some(delete_cursor),
                        MAX_BATCHED_ROLLUP_COUNTS,
                    )?;
                    dirty_nsids.extend(dirty);
                    n
                } else {
                    self.rollup_delete_account(delete_cursor, &delete_key_bytes, &delete_val_bytes)?
                }
            }
            (Some(_), None) => {
                let (n, dirty) =
                    self.rollup_live_counts(timely_iter, None, MAX_BATCHED_ROLLUP_COUNTS)?;
                dirty_nsids.extend(dirty);
                n
            }
            (None, Some((delete_cursor, delete_key_bytes, delete_val_bytes))) => {
                self.rollup_delete_account(delete_cursor, &delete_key_bytes, &delete_val_bytes)?
            }
            (None, None) => 0,
        };

        Ok((cursors_stepped, dirty_nsids))
    }

    fn trim_collection(
        &mut self,
        collection: &Nsid,
        limit: usize,
        full_scan: bool,
    ) -> StorageResult<(usize, usize)> {
        let mut dangling_feed_keys_cleaned = 0;
        let mut records_deleted = 0;

        let live_range = if full_scan {
            let start = NsidRecordFeedKey::from_prefix_to_db_bytes(collection)?;
            let end = NsidRecordFeedKey::prefix_range_end(collection)?;
            start..end
        } else {
            let feed_trim_cursor_key =
                TrimCollectionCursorKey::new(collection.clone()).to_db_bytes()?;
            let trim_cursor = self
                .global
                .get(&feed_trim_cursor_key)?
                .map(|value_bytes| db_complete(&value_bytes))
                .transpose()?
                .unwrap_or(Cursor::from_start());
            NsidRecordFeedKey::from_pair(collection.clone(), trim_cursor).range_to_prefix_end()?
        };

        let mut live_records_found = 0;
        let mut candidate_new_feed_lower_cursor = None;
        let mut ended_early = false;
        let mut batch = self.keyspace.batch();
        for (i, kv) in self.feeds.range(live_range).rev().enumerate() {
            if !full_scan && i > 1_000_000 {
                log::info!("stopping collection trim early: already scanned 1M elements");
                ended_early = true;
                break;
            }
            let (key_bytes, val_bytes) = kv?;
            let feed_key = db_complete::<NsidRecordFeedKey>(&key_bytes)?;
            let feed_val = db_complete::<NsidRecordFeedVal>(&val_bytes)?;
            let location_key: RecordLocationKey = (&feed_key, &feed_val).into();
            let location_key_bytes = location_key.to_db_bytes()?;

            let Some(location_val_bytes) = self.records.get(&location_key_bytes)? else {
                // record was deleted (hopefully)
                batch.remove(&self.feeds, &*key_bytes);
                dangling_feed_keys_cleaned += 1;
                continue;
            };

            let (meta, _) = RecordLocationMeta::from_db_bytes(&location_val_bytes)?;

            if meta.cursor() != feed_key.cursor() {
                // older/different version
                batch.remove(&self.feeds, &*key_bytes);
                dangling_feed_keys_cleaned += 1;
                continue;
            }
            if meta.rev != feed_val.rev() {
                // weird...
                log::warn!("record lookup: cursor match but rev did not...? removing.");
                batch.remove(&self.feeds, &*key_bytes);
                batch.remove(&self.records, &location_key_bytes);
                dangling_feed_keys_cleaned += 1;
                continue;
            }

            if batch.len() >= MAX_BATCHED_CLEANUP_SIZE {
                batch.commit()?;
                batch = self.keyspace.batch();
            }

            live_records_found += 1;
            if live_records_found <= limit {
                continue;
            }
            if candidate_new_feed_lower_cursor.is_none() {
                candidate_new_feed_lower_cursor = Some(feed_key.cursor());
            }

            batch.remove(&self.feeds, key_bytes);
            batch.remove(&self.records, &location_key_bytes);
            records_deleted += 1;
        }

        if !ended_early {
            if let Some(new_cursor) = candidate_new_feed_lower_cursor {
                batch.insert(
                    &self.global,
                    &TrimCollectionCursorKey::new(collection.clone()).to_db_bytes()?,
                    &new_cursor.to_db_bytes()?,
                );
            }
        }

        batch.commit()?;

        log::trace!("trim_collection ({collection:?}) removed {dangling_feed_keys_cleaned} dangling feed entries and {records_deleted} records (ended early? {ended_early})");
        Ok((dangling_feed_keys_cleaned, records_deleted))
    }

    fn delete_account(&mut self, did: &Did) -> Result<usize, StorageError> {
        let mut records_deleted = 0;
        let mut batch = self.keyspace.batch();
        let prefix = RecordLocationKey::from_prefix_to_db_bytes(did)?;
        for kv in self.records.prefix(prefix) {
            let (key_bytes, _) = kv?;
            batch.remove(&self.records, key_bytes);
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

pub struct FjallBackground(FjallWriter);

#[async_trait]
impl StoreBackground for FjallBackground {
    async fn run(mut self, backfill: bool) -> StorageResult<()> {
        let mut dirty_nsids = HashSet::new();

        let mut rollup =
            tokio::time::interval(Duration::from_millis(if backfill { 1 } else { 81 }));
        rollup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        let mut trim =
            tokio::time::interval(Duration::from_millis(if backfill { 3_000 } else { 6_000 }));
        trim.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = rollup.tick() => {
                    let (n, dirty) = self.0.step_rollup().inspect_err(|e| log::error!("rollup error: {e:?}"))?;
                    if n == 0 {
                        rollup.reset_after(Duration::from_millis(1_200)); // we're caught up, take a break
                    }
                    dirty_nsids.extend(dirty);
                    log::trace!("rolled up {n} items ({} collections now dirty)", dirty_nsids.len());
                },
                _ = trim.tick() => {
                    let n = dirty_nsids.len();
                    log::trace!("trimming {n} nsids: {dirty_nsids:?}");
                    let t0 = Instant::now();
                    let (mut total_danglers, mut total_deleted) = (0, 0);
                    for collection in &dirty_nsids {
                        let (danglers, deleted) = self.0.trim_collection(collection, 512, false).inspect_err(|e| log::error!("trim error: {e:?}"))?;
                        total_danglers += danglers;
                        total_deleted += deleted;
                        if total_deleted > 1_000_000 {
                            log::info!("trim stopped early, more than 1M records already deleted.");
                            break;
                        }
                    }
                    log::info!("finished trimming {n} nsids in {:?}: {total_danglers} dangling and {total_deleted} total removed.", t0.elapsed());
                    dirty_nsids.clear();
                },
            };
        }
    }
}

/// Get a value from a fixed key
fn get_static_neu<K: StaticStr, V: DbBytes>(global: &PartitionHandle) -> StorageResult<Option<V>> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value = global
        .get(&key_bytes)?
        .map(|value_bytes| db_complete(&value_bytes))
        .transpose()?;
    Ok(value)
}

/// Get a value from a fixed key
fn get_snapshot_static_neu<K: StaticStr, V: DbBytes>(
    global: &fjall::Snapshot,
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
    global: &PartitionHandle,
    value: impl DbBytes,
) -> StorageResult<()> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    let value_bytes = value.to_db_bytes()?;
    global.insert(&key_bytes, &value_bytes)?;
    Ok(())
}

/// Set a value to a fixed key, erroring if the value already exists
///
/// Intended for single-threaded init: not safe under concurrency, since there
/// is no transaction between checking if the already exists and writing it.
fn init_static_neu<K: StaticStr>(
    global: &PartitionHandle,
    value: impl DbBytes,
) -> StorageResult<()> {
    let key_bytes = DbStaticStr::<K>::default().to_db_bytes()?;
    if global.get(&key_bytes)?.is_some() {
        return Err(StorageError::InitError(format!(
            "init failed: value for key {key_bytes:?} already exists"
        )));
    }
    let value_bytes = value.to_db_bytes()?;
    global.insert(&key_bytes, &value_bytes)?;
    Ok(())
}

/// Set a value to a fixed key
fn insert_batch_static_neu<K: StaticStr>(
    batch: &mut FjallBatch,
    global: &PartitionHandle,
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

////////// temp stuff to remove:

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeleteAccount, RecordKey, UFOsCommit};
    use jetstream::events::{CommitEvent, CommitOp};
    use jetstream::exports::Cid;
    use serde_json::value::RawValue;

    fn fjall_db() -> (FjallReader, FjallWriter) {
        let (read, write, _, _) = FjallStorage::init(
            tempfile::tempdir().unwrap(),
            "offline test (no real jetstream endpoint)".to_string(),
            false,
            FjallConfig { temp: true },
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
                .truncating_insert(commit, &[0u8; 16])
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
                .truncating_insert(commit, &[0u8; 16])
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
                .truncating_insert(commit, &[0u8; 16])
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

        let records = read.get_records_by_collections(&[collection], 2, false)?;
        assert_eq!(records.len(), 1);
        let rec = &records[0];
        assert_eq!(rec.record.get(), "{}");
        assert!(!rec.is_update);

        let records =
            read.get_records_by_collections(&[Nsid::new("d.e.f".to_string()).unwrap()], 2, false)?;
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
            false,
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
    fn test_get_multi_collection_expanded() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        // insert some older ones in aab
        for i in 1..=3 {
            batch.create(
                "did:plc:inze6wrmsm7pjl7yta3oig77",
                "a.a.b",
                &format!("aab-{i}"),
                &format!(r#""b {i}""#),
                Some(&format!("rev-b-{i}")),
                None,
                100 + i,
            );
        }
        // and some newer ones in aaa
        for i in 1..=3 {
            batch.create(
                "did:plc:inze6wrmsm7pjl7yta3oig77",
                "a.a.a",
                &format!("aaa-{i}"),
                &format!(r#""a {i}""#),
                Some(&format!("rev-a-{i}")),
                None,
                200 + i,
            );
        }
        write.insert_batch(batch.batch)?;

        let records = read.get_records_by_collections(
            &[
                Nsid::new("a.a.a".to_string()).unwrap(),
                Nsid::new("a.a.b".to_string()).unwrap(),
                Nsid::new("a.a.c".to_string()).unwrap(),
            ],
            2,
            true,
        )?;
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].record.get(), r#""a 3""#);
        assert_eq!(
            records[0].collection,
            Nsid::new("a.a.a".to_string()).unwrap()
        );

        assert_eq!(records[3].record.get(), r#""b 2""#);
        assert_eq!(
            records[3].collection,
            Nsid::new("a.a.b".to_string()).unwrap()
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

        let records = read.get_records_by_collections(&[collection], 2, false)?;
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

        let records = read.get_records_by_collections(&[collection], 2, false)?;
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

        let records = read.get_records_by_collections(
            &[Nsid::new("a.a.a".to_string()).unwrap()],
            100,
            false,
        )?;
        assert_eq!(records.len(), 1);
        let records = read.get_records_by_collections(
            &[Nsid::new("a.a.b".to_string()).unwrap()],
            100,
            false,
        )?;
        assert_eq!(records.len(), 10);
        let records = read.get_records_by_collections(
            &[Nsid::new("a.a.c".to_string()).unwrap()],
            100,
            false,
        )?;
        assert_eq!(records.len(), 1);
        let records = read.get_records_by_collections(
            &[Nsid::new("a.a.d".to_string()).unwrap()],
            100,
            false,
        )?;
        assert_eq!(records.len(), 0);

        write.trim_collection(&Nsid::new("a.a.a".to_string()).unwrap(), 6, false)?;
        write.trim_collection(&Nsid::new("a.a.b".to_string()).unwrap(), 6, false)?;
        write.trim_collection(&Nsid::new("a.a.c".to_string()).unwrap(), 6, false)?;
        write.trim_collection(&Nsid::new("a.a.d".to_string()).unwrap(), 6, false)?;

        let records = read.get_records_by_collections(
            &[Nsid::new("a.a.a".to_string()).unwrap()],
            100,
            false,
        )?;
        assert_eq!(records.len(), 1);
        let records = read.get_records_by_collections(
            &[Nsid::new("a.a.b".to_string()).unwrap()],
            100,
            false,
        )?;
        assert_eq!(records.len(), 6);
        let records = read.get_records_by_collections(
            &[Nsid::new("a.a.c".to_string()).unwrap()],
            100,
            false,
        )?;
        assert_eq!(records.len(), 1);
        let records = read.get_records_by_collections(
            &[Nsid::new("a.a.d".to_string()).unwrap()],
            100,
            false,
        )?;
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

        let records = read.get_records_by_collections(
            &[Nsid::new("a.a.a".to_string()).unwrap()],
            100,
            false,
        )?;
        assert_eq!(records.len(), 3);

        let records_deleted =
            write.delete_account(&Did::new("did:plc:person-b".to_string()).unwrap())?;
        assert_eq!(records_deleted, 2);

        let records = read.get_records_by_collections(
            &[Nsid::new("a.a.a".to_string()).unwrap()],
            100,
            false,
        )?;
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
            read.get_records_by_collections(&[Nsid::new("a.a.a".to_string()).unwrap()], 1, false)?;
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

        let (n, _) = write.step_rollup()?;
        assert_eq!(n, 1);

        let mut batch = TestBatch::default();
        batch.delete_account("did:plc:person-a", 10_001);
        write.insert_batch(batch.batch)?;

        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.a".to_string()).unwrap()], 1, false)?;
        assert_eq!(records.len(), 1);

        let (n, _) = write.step_rollup()?;
        assert_eq!(n, 1);

        let records =
            read.get_records_by_collections(&[Nsid::new("a.a.a".to_string()).unwrap()], 1, false)?;
        assert_eq!(records.len(), 0);

        let mut batch = TestBatch::default();
        batch.delete_account("did:plc:person-a", 9_999);
        write.insert_batch(batch.batch)?;

        let (n, _) = write.step_rollup()?;
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

        let (n, _) = write.step_rollup()?;
        assert_eq!(n, 2);

        let (n, _) = write.step_rollup()?;
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
        let (n, _) = write.step_rollup()?;
        assert_eq!(n, 1);

        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("a.a.a".to_string()).unwrap())?;
        assert_eq!(records, 3);
        assert_eq!(dids, 2);

        // delete account rolled up
        let (n, _) = write.step_rollup()?;
        assert_eq!(n, 1);

        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("a.a.a".to_string()).unwrap())?;
        assert_eq!(records, 3);
        assert_eq!(dids, 2);

        // second batch rolled up
        let (n, _) = write.step_rollup()?;
        assert_eq!(n, 1);

        let (records, dids) =
            read.get_counts_by_collection(&Nsid::new("a.a.a".to_string()).unwrap())?;
        assert_eq!(records, 3);
        assert_eq!(dids, 2);

        // no more rollups left
        let (n, _) = write.step_rollup()?;
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

        let (n, _) = write.step_rollup()?;
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

    #[test]
    fn get_top_collections_with_parent_nsid() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.a.a.a",
            "aaaa",
            r#""child nsid""#,
            Some("rev-aaaa"),
            None,
            100,
        );
        batch.create(
            "did:plc:inze6wrmsm7pjl7yta3oig77",
            "a.a.a",
            "aaa",
            r#""parent nsid""#,
            Some("rev-aaa"),
            None,
            101,
        );
        write.insert_batch(batch.batch)?;

        let (n, _) = write.step_rollup()?;
        assert_eq!(n, 2); // 3 collections

        let tops = read.get_top_collections()?;
        assert_eq!(
            tops,
            TopCollections {
                total_records: 2,
                dids_estimate: 1,
                nsid_child_segments: HashMap::from([(
                    "a".to_string(),
                    TopCollections {
                        total_records: 2,
                        dids_estimate: 1,
                        nsid_child_segments: HashMap::from([(
                            "a".to_string(),
                            TopCollections {
                                total_records: 2,
                                dids_estimate: 1,
                                nsid_child_segments: HashMap::from([(
                                    "a".to_string(),
                                    TopCollections {
                                        total_records: 2,
                                        dids_estimate: 1,
                                        nsid_child_segments: HashMap::from([(
                                            "a".to_string(),
                                            TopCollections {
                                                total_records: 1,
                                                dids_estimate: 1,
                                                nsid_child_segments: HashMap::from([]),
                                            },
                                        ),]),
                                    },
                                ),]),
                            },
                        ),]),
                    },
                ),]),
            }
        );

        // TODO: handle leaf node counts explicitly, since parent NSIDs can be leaves themselves

        Ok(())
    }
}
