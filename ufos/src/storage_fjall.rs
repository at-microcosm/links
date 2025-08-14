use crate::db_types::{
    db_complete, DbBytes, DbStaticStr, EncodingResult, StaticStr, SubPrefixBytes,
};
use crate::error::StorageError;
use crate::storage::{StorageResult, StorageWhatever, StoreBackground, StoreReader, StoreWriter};
use crate::store_types::{
    AllTimeDidsKey, AllTimeRecordsKey, AllTimeRollupKey, CommitCounts, CountsValue, CursorBucket,
    DeleteAccountQueueKey, DeleteAccountQueueVal, HourTruncatedCursor, HourlyDidsKey,
    HourlyRecordsKey, HourlyRollupKey, HourlyRollupStaticPrefix, JetstreamCursorKey,
    JetstreamCursorValue, JetstreamEndpointKey, JetstreamEndpointValue, LiveCountsKey,
    NewRollupCursorKey, NewRollupCursorValue, NsidRecordFeedKey, NsidRecordFeedVal,
    RecordLocationKey, RecordLocationMeta, RecordLocationVal, RecordRawValue, SketchSecretKey,
    SketchSecretPrefix, TakeoffKey, TakeoffValue, TrimCollectionCursorKey, WeekTruncatedCursor,
    WeeklyDidsKey, WeeklyRecordsKey, WeeklyRollupKey, WithCollection, WithRank, HOUR_IN_MICROS,
    WEEK_IN_MICROS,
};
use crate::{
    nice_duration, CommitAction, ConsumerInfo, Did, EncodingError, EventBatch, JustCount, Nsid,
    NsidCount, NsidPrefix, OrderCollectionsBy, PrefixChild, PrefixCount, UFOsRecord,
};
use async_trait::async_trait;
use fjall::{
    compaction::{Leveled, Strategy},
    Batch as FjallBatch, Config, Keyspace, PartitionCreateOptions, PartitionHandle, Snapshot,
};
use jetstream::events::Cursor;
use lsm_tree::AbstractTree;
use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram, Unit,
};
use std::collections::{HashMap, HashSet};
use std::iter::Peekable;
use std::ops::Bound;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant, SystemTime};

const MAX_BATCHED_ACCOUNT_DELETE_RECORDS: usize = 1024;
const MAX_BATCHED_ROLLUP_COUNTS: usize = 256;

///
/// new data format, roughly:
///
/// Partition: 'global'
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
        // TODO: value separation + gc will probably work
        // then *maybe* also a tall tree to help deletes happen?
        let records = keyspace.open_partition("records", PartitionCreateOptions::default())?;
        // try to make the tree narrower so deletes eventually bottom out and are removed
        // when fjall finally gets weak delete (rocksdb "single delete") that will work way better
        // ...not sure if changing the strategy after creation actually works
        // TODO: also increase block size
        let rollups = keyspace.open_partition(
            "rollups",
            PartitionCreateOptions::default().compaction_strategy(Strategy::Leveled(Leveled {
                target_size: 16 * 2_u32.pow(20), // MiB (default 64)
                level_ratio: 2,                  // this is a little extreme? (default 10)
                ..Default::default()
            })),
        )?;

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
            queues: queues.clone(),
        };
        reader.describe_metrics();
        let writer = FjallWriter {
            bg_taken: Arc::new(AtomicBool::new(false)),
            keyspace,
            global,
            feeds,
            records,
            rollups,
            queues,
        };
        writer.describe_metrics();
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
    queues: PartitionHandle,
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

type GetCounts = Box<dyn FnOnce() -> StorageResult<CountsValue>>;
type GetByterCounts = StorageResult<(Nsid, GetCounts)>;
type NsidCounter = Box<dyn Iterator<Item = GetByterCounts>>;
fn get_lexi_iter<T: WithCollection + DbBytes + 'static>(
    snapshot: &Snapshot,
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
) -> StorageResult<NsidCounter> {
    Ok(Box::new(snapshot.range((start, end)).map(|kv| {
        let (k_bytes, v_bytes) = kv?;
        let key = db_complete::<T>(&k_bytes)?;
        let nsid = key.collection().clone();
        let get_counts: GetCounts = Box::new(move || Ok(db_complete::<CountsValue>(&v_bytes)?));
        Ok((nsid, get_counts))
    })))
}
type GetRollupKey = Arc<dyn Fn(&Nsid) -> EncodingResult<Vec<u8>>>;
fn get_lookup_iter<T: WithCollection + WithRank + DbBytes + 'static>(
    snapshot: lsm_tree::Snapshot,
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
    get_rollup_key: GetRollupKey,
) -> StorageResult<NsidCounter> {
    Ok(Box::new(snapshot.range((start, end)).rev().map(
        move |kv| {
            let (k_bytes, _) = kv?;
            let key = db_complete::<T>(&k_bytes)?;
            let nsid = key.collection().clone();
            let get_counts: GetCounts = Box::new({
                let nsid = nsid.clone();
                let snapshot = snapshot.clone();
                let get_rollup_key = get_rollup_key.clone();
                move || {
                    let db_count_bytes = snapshot.get(get_rollup_key(&nsid)?)?.expect(
                    "integrity: all-time rank rollup must have corresponding all-time count rollup",
                );
                    Ok(db_complete::<CountsValue>(&db_count_bytes)?)
                }
            });
            Ok((nsid, get_counts))
        },
    )))
}

type CollectionSerieses = HashMap<Nsid, Vec<CountsValue>>;

impl FjallReader {
    fn describe_metrics(&self) {
        describe_gauge!(
            "storage_fjall_l0_run_count",
            Unit::Count,
            "number of L0 runs in a partition"
        );
        describe_gauge!(
            "storage_fjall_keyspace_disk_space",
            Unit::Bytes,
            "total storage used according to fjall"
        );
        describe_gauge!(
            "storage_fjall_journal_count",
            Unit::Count,
            "total keyspace journals according to fjall"
        );
        describe_gauge!(
            "storage_fjall_keyspace_sequence",
            Unit::Count,
            "fjall keyspace sequence"
        );
    }

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

    fn get_earliest_hour(&self, rollups: Option<&Snapshot>) -> StorageResult<HourTruncatedCursor> {
        let cursor = rollups
            .unwrap_or(&self.rollups.snapshot())
            .prefix(HourlyRollupStaticPrefix::default().to_db_bytes()?)
            .next()
            .transpose()?
            .map(|(key_bytes, _)| db_complete::<HourlyRollupKey>(&key_bytes))
            .transpose()?
            .map(|key| key.cursor())
            .unwrap_or_else(|| Cursor::from_start().into());
        Ok(cursor)
    }

    fn get_lexi_collections(
        &self,
        snapshot: Snapshot,
        limit: usize,
        cursor: Option<Vec<u8>>,
        buckets: Vec<CursorBucket>,
    ) -> StorageResult<(Vec<NsidCount>, Option<Vec<u8>>)> {
        let cursor_nsid = cursor.as_deref().map(db_complete::<Nsid>).transpose()?;
        let mut iters: Vec<Peekable<NsidCounter>> = Vec::with_capacity(buckets.len());
        for bucket in &buckets {
            let it: NsidCounter = match bucket {
                CursorBucket::Hour(t) => {
                    let start = cursor_nsid
                        .as_ref()
                        .map(|nsid| HourlyRollupKey::after_nsid(*t, nsid))
                        .unwrap_or_else(|| HourlyRollupKey::start(*t))?;
                    let end = HourlyRollupKey::end(*t)?;
                    get_lexi_iter::<HourlyRollupKey>(&snapshot, start, end)?
                }
                CursorBucket::Week(t) => {
                    let start = cursor_nsid
                        .as_ref()
                        .map(|nsid| WeeklyRollupKey::after_nsid(*t, nsid))
                        .unwrap_or_else(|| WeeklyRollupKey::start(*t))?;
                    let end = WeeklyRollupKey::end(*t)?;
                    get_lexi_iter::<WeeklyRollupKey>(&snapshot, start, end)?
                }
                CursorBucket::AllTime => {
                    let start = cursor_nsid
                        .as_ref()
                        .map(AllTimeRollupKey::after_nsid)
                        .unwrap_or_else(AllTimeRollupKey::start)?;
                    let end = AllTimeRollupKey::end()?;
                    get_lexi_iter::<AllTimeRollupKey>(&snapshot, start, end)?
                }
            };
            iters.push(it.peekable());
        }

        let mut out = Vec::new();
        let mut current_nsid = None;
        for _ in 0..limit {
            // double-scan the iters for each element: this could be eliminated but we're starting simple.
            // first scan: find the lowest nsid
            // second scan: take + merge, and advance all iters with lowest nsid
            let mut lowest: Option<Nsid> = None;
            for iter in &mut iters {
                if let Some(bla) = iter.peek_mut() {
                    let (nsid, _) = match bla {
                        Ok(v) => v,
                        Err(e) => Err(std::mem::replace(e, StorageError::Stolen))?,
                    };
                    lowest = match lowest {
                        Some(ref current) if nsid.as_str() > current.as_str() => lowest,
                        _ => Some(nsid.clone()),
                    };
                }
            }
            current_nsid = lowest.clone();
            let Some(nsid) = lowest else { break };

            let mut merged = CountsValue::default();
            for iter in &mut iters {
                // unwrap: potential fjall error was already checked & bailed over when peeking in the first loop
                if let Some(Ok((_, get_counts))) = iter.next_if(|v| v.as_ref().unwrap().0 == nsid) {
                    let counts = get_counts()?;
                    merged.merge(&counts);
                }
            }
            out.push(NsidCount::new(&nsid, &merged));
        }

        let next_cursor = current_nsid.map(|s| s.to_db_bytes()).transpose()?;
        Ok((out, next_cursor))
    }

    fn get_ordered_collections(
        &self,
        snapshot: Snapshot,
        limit: usize,
        order: OrderCollectionsBy,
        buckets: Vec<CursorBucket>,
    ) -> StorageResult<Vec<NsidCount>> {
        let mut iters: Vec<NsidCounter> = Vec::with_capacity(buckets.len());

        for bucket in buckets {
            let it: NsidCounter = match (&order, bucket) {
                (OrderCollectionsBy::RecordsCreated, CursorBucket::Hour(t)) => {
                    get_lookup_iter::<HourlyRecordsKey>(
                        snapshot.clone(),
                        HourlyRecordsKey::start(t)?,
                        HourlyRecordsKey::end(t)?,
                        Arc::new({
                            move |collection| HourlyRollupKey::new(t, collection).to_db_bytes()
                        }),
                    )?
                }
                (OrderCollectionsBy::DidsEstimate, CursorBucket::Hour(t)) => {
                    get_lookup_iter::<HourlyDidsKey>(
                        snapshot.clone(),
                        HourlyDidsKey::start(t)?,
                        HourlyDidsKey::end(t)?,
                        Arc::new({
                            move |collection| HourlyRollupKey::new(t, collection).to_db_bytes()
                        }),
                    )?
                }
                (OrderCollectionsBy::RecordsCreated, CursorBucket::Week(t)) => {
                    get_lookup_iter::<WeeklyRecordsKey>(
                        snapshot.clone(),
                        WeeklyRecordsKey::start(t)?,
                        WeeklyRecordsKey::end(t)?,
                        Arc::new({
                            move |collection| WeeklyRollupKey::new(t, collection).to_db_bytes()
                        }),
                    )?
                }
                (OrderCollectionsBy::DidsEstimate, CursorBucket::Week(t)) => {
                    get_lookup_iter::<WeeklyDidsKey>(
                        snapshot.clone(),
                        WeeklyDidsKey::start(t)?,
                        WeeklyDidsKey::end(t)?,
                        Arc::new({
                            move |collection| WeeklyRollupKey::new(t, collection).to_db_bytes()
                        }),
                    )?
                }
                (OrderCollectionsBy::RecordsCreated, CursorBucket::AllTime) => {
                    get_lookup_iter::<AllTimeRecordsKey>(
                        snapshot.clone(),
                        AllTimeRecordsKey::start()?,
                        AllTimeRecordsKey::end()?,
                        Arc::new(|collection| AllTimeRollupKey::new(collection).to_db_bytes()),
                    )?
                }
                (OrderCollectionsBy::DidsEstimate, CursorBucket::AllTime) => {
                    get_lookup_iter::<AllTimeDidsKey>(
                        snapshot.clone(),
                        AllTimeDidsKey::start()?,
                        AllTimeDidsKey::end()?,
                        Arc::new(|collection| AllTimeRollupKey::new(collection).to_db_bytes()),
                    )?
                }
                (OrderCollectionsBy::Lexi { .. }, _) => unreachable!(),
            };
            iters.push(it);
        }

        // overfetch by taking a bit more than the limit
        // merge by collection
        // sort by requested order, take limit, discard all remaining
        //
        // this isn't guaranteed to be correct, but it will hopefully be close most of the time:
        // - it's possible that some NSIDs might score low during some time-buckets, and miss being merged
        // - overfetching hopefully helps a bit by catching nsids near the threshold more often, but. yeah.
        //
        // this thing is heavy, there's probably a better way
        let mut ranked: HashMap<Nsid, CountsValue> = HashMap::with_capacity(limit * 2);
        for iter in iters {
            for pair in iter.take((limit as f64 * 1.3).ceil() as usize) {
                let (nsid, get_counts) = pair?;
                let counts = get_counts()?;
                ranked.entry(nsid).or_default().merge(&counts);
            }
        }
        let mut ranked: Vec<(Nsid, CountsValue)> = ranked.into_iter().collect();
        match order {
            OrderCollectionsBy::RecordsCreated => ranked.sort_by_key(|(_, c)| c.counts().creates),
            OrderCollectionsBy::DidsEstimate => ranked.sort_by_key(|(_, c)| c.dids().estimate()),
            OrderCollectionsBy::Lexi { .. } => unreachable!(),
        }
        let counts = ranked
            .into_iter()
            .rev()
            .take(limit)
            .map(|(nsid, cv)| NsidCount::new(&nsid, &cv))
            .collect();
        Ok(counts)
    }

    fn get_collections(
        &self,
        limit: usize,
        order: OrderCollectionsBy,
        since: Option<HourTruncatedCursor>,
        until: Option<HourTruncatedCursor>,
    ) -> StorageResult<(Vec<NsidCount>, Option<Vec<u8>>)> {
        let snapshot = self.rollups.snapshot();
        let buckets = if let (None, None) = (since, until) {
            vec![CursorBucket::AllTime]
        } else {
            let mut lower = self.get_earliest_hour(Some(&snapshot))?;
            if let Some(specified) = since {
                if specified > lower {
                    lower = specified;
                }
            }
            let upper = until.unwrap_or_else(|| Cursor::at(SystemTime::now()).into());
            CursorBucket::buckets_spanning(lower, upper)
        };
        match order {
            OrderCollectionsBy::Lexi { cursor } => {
                self.get_lexi_collections(snapshot, limit, cursor, buckets)
            }
            _ => Ok((
                self.get_ordered_collections(snapshot, limit, order, buckets)?,
                None,
            )),
        }
    }

    fn get_lexi_prefix(
        &self,
        snapshot: Snapshot,
        prefix: NsidPrefix,
        limit: usize,
        cursor: Option<Vec<u8>>,
        buckets: Vec<CursorBucket>,
    ) -> StorageResult<(JustCount, Vec<PrefixChild>, Option<Vec<u8>>)> {
        // let prefix_sub_with_null = prefix.as_str().to_string().to_db_bytes()?;
        let prefix_sub = String::sub_prefix(&prefix.terminated())?; // with trailing dot to ensure full segment match
        let cursor_child = cursor
            .as_deref()
            .map(|encoded_bytes| {
                let decoded: String = db_complete(encoded_bytes)?;
                // TODO: write some tests for cursors, there's probably bugs here
                let as_sub_prefix_with_null = decoded.to_db_bytes()?;
                Ok::<_, EncodingError>(as_sub_prefix_with_null)
            })
            .transpose()?;
        let mut iters: Vec<NsidCounter> = Vec::with_capacity(buckets.len());
        for bucket in &buckets {
            let it: NsidCounter = match bucket {
                CursorBucket::Hour(t) => {
                    let start = cursor_child
                        .as_ref()
                        .map(|child| HourlyRollupKey::after_nsid_prefix(*t, child))
                        .unwrap_or_else(|| HourlyRollupKey::after_nsid_prefix(*t, &prefix_sub))?;
                    let end = HourlyRollupKey::nsid_prefix_end(*t, &prefix_sub)?;
                    get_lexi_iter::<HourlyRollupKey>(&snapshot, start, end)?
                }
                CursorBucket::Week(t) => {
                    let start = cursor_child
                        .as_ref()
                        .map(|child| WeeklyRollupKey::after_nsid_prefix(*t, child))
                        .unwrap_or_else(|| WeeklyRollupKey::after_nsid_prefix(*t, &prefix_sub))?;
                    let end = WeeklyRollupKey::nsid_prefix_end(*t, &prefix_sub)?;
                    get_lexi_iter::<WeeklyRollupKey>(&snapshot, start, end)?
                }
                CursorBucket::AllTime => {
                    let start = cursor_child
                        .as_ref()
                        .map(|child| AllTimeRollupKey::after_nsid_prefix(child))
                        .unwrap_or_else(|| AllTimeRollupKey::after_nsid_prefix(&prefix_sub))?;
                    let end = AllTimeRollupKey::nsid_prefix_end(&prefix_sub)?;
                    get_lexi_iter::<AllTimeRollupKey>(&snapshot, start, end)?
                }
            };
            iters.push(it);
        }

        // with apologies
        let mut iters: Vec<_> = iters
            .into_iter()
            .map(|it| {
                it.map(|bla| {
                    bla.map(|(nsid, v)| {
                        let Some(child) = Child::from_prefix(&nsid, &prefix) else {
                            panic!("failed from_prefix: {nsid:?} {prefix:?} (bad iter bounds?)");
                        };
                        (child, v)
                    })
                })
                .peekable()
            })
            .collect();

        let mut items = Vec::new();
        let mut prefix_count = CountsValue::default();
        #[derive(Debug, Clone, PartialEq)]
        enum Child {
            FullNsid(Nsid),
            ChildPrefix(String),
        }
        impl Child {
            fn from_prefix(nsid: &Nsid, prefix: &NsidPrefix) -> Option<Self> {
                if prefix.is_group_of(nsid) {
                    return Some(Child::FullNsid(nsid.clone()));
                }
                let suffix = nsid.as_str().strip_prefix(&format!("{}.", prefix.0))?;
                let (segment, _) = suffix.split_once('.').unwrap();
                let child_prefix = format!("{}.{segment}", prefix.0);
                Some(Child::ChildPrefix(child_prefix))
            }
            fn is_before(&self, other: &Child) -> bool {
                match (self, other) {
                    (Child::FullNsid(s), Child::ChildPrefix(o)) if s.as_str() == o => true,
                    (Child::ChildPrefix(s), Child::FullNsid(o)) if s == o.as_str() => false,
                    (Child::FullNsid(s), Child::FullNsid(o)) => s.as_str() < o.as_str(),
                    (Child::ChildPrefix(s), Child::ChildPrefix(o)) => s < o,
                    (Child::FullNsid(s), Child::ChildPrefix(o)) => s.to_string() < *o,
                    (Child::ChildPrefix(s), Child::FullNsid(o)) => *s < o.to_string(),
                }
            }
            fn into_inner(self) -> String {
                match self {
                    Child::FullNsid(s) => s.to_string(),
                    Child::ChildPrefix(s) => s,
                }
            }
        }
        let mut current_child: Option<Child> = None;
        for _ in 0..limit {
            // double-scan the iters for each element: this could be eliminated but we're starting simple.
            // first scan: find the lowest nsid
            // second scan: take + merge, and advance all iters with lowest nsid
            let mut lowest: Option<Child> = None;
            for iter in &mut iters {
                if let Some(bla) = iter.peek_mut() {
                    let (child, _) = match bla {
                        Ok(v) => v,
                        Err(e) => Err(std::mem::replace(e, StorageError::Stolen))?,
                    };

                    lowest = match lowest {
                        Some(ref current) if current.is_before(child) => lowest,
                        _ => Some(child.clone()),
                    };
                }
            }
            current_child = lowest.clone();
            let Some(child) = lowest else { break };

            let mut merged = CountsValue::default();
            for iter in &mut iters {
                // unwrap: potential fjall error was already checked & bailed over when peeking in the first loop
                while let Some(Ok((_, get_counts))) =
                    iter.next_if(|v| v.as_ref().unwrap().0 == child)
                {
                    let counts = get_counts()?;
                    prefix_count.merge(&counts);
                    merged.merge(&counts);
                }
            }
            items.push(match child {
                Child::FullNsid(nsid) => PrefixChild::Collection(NsidCount::new(&nsid, &merged)),
                Child::ChildPrefix(prefix) => {
                    PrefixChild::Prefix(PrefixCount::new(&prefix, &merged))
                }
            });
        }

        // TODO: could serialize the prefix count (with sketch) into the cursor so that uniqs can actually count up?
        // ....er the sketch is probably too big
        // TODO: this is probably buggy on child-type boundaries bleh
        let next_cursor = current_child
            .map(|s| s.into_inner().to_db_bytes())
            .transpose()?;

        Ok(((&prefix_count).into(), items, next_cursor))
    }

    fn get_prefix(
        &self,
        prefix: NsidPrefix,
        limit: usize,
        order: OrderCollectionsBy,
        since: Option<HourTruncatedCursor>,
        until: Option<HourTruncatedCursor>,
    ) -> StorageResult<(JustCount, Vec<PrefixChild>, Option<Vec<u8>>)> {
        let snapshot = self.rollups.snapshot();
        let buckets = if let (None, None) = (since, until) {
            vec![CursorBucket::AllTime]
        } else {
            let mut lower = self.get_earliest_hour(Some(&snapshot))?;
            if let Some(specified) = since {
                if specified > lower {
                    lower = specified;
                }
            }
            let upper = until.unwrap_or_else(|| Cursor::at(SystemTime::now()).into());
            CursorBucket::buckets_spanning(lower, upper)
        };
        match order {
            OrderCollectionsBy::Lexi { cursor } => {
                self.get_lexi_prefix(snapshot, prefix, limit, cursor, buckets)
            }
            _ => todo!(),
        }
    }

    /// - step: output series time step, in seconds
    fn get_timeseries(
        &self,
        collections: Vec<Nsid>,
        since: HourTruncatedCursor,
        until: Option<HourTruncatedCursor>,
        step: u64,
    ) -> StorageResult<(Vec<HourTruncatedCursor>, CollectionSerieses)> {
        if step > WEEK_IN_MICROS {
            panic!("week-stepping is todo");
        }
        let until = until.unwrap_or_else(|| Cursor::at(SystemTime::now()).into());
        let Ok(dt) = Cursor::from(until).duration_since(&Cursor::from(since)) else {
            return Ok((
                // empty: until < since
                vec![],
                collections.into_iter().map(|c| (c, vec![])).collect(),
            ));
        };
        let n_hours = (dt.as_micros() as u64) / HOUR_IN_MICROS;
        let mut counts_by_hour = Vec::with_capacity(n_hours as usize);
        let snapshot = self.rollups.snapshot();
        for hour in (0..n_hours).map(|i| since.nth_next(i)) {
            let mut counts = Vec::with_capacity(collections.len());
            for nsid in &collections {
                let count = snapshot
                    .get(&HourlyRollupKey::new(hour, nsid).to_db_bytes()?)?
                    .as_deref()
                    .map(db_complete::<CountsValue>)
                    .transpose()?
                    .unwrap_or_default();
                counts.push(count);
            }
            counts_by_hour.push((hour, counts));
        }

        let step_hours = step / (HOUR_IN_MICROS / 1_000_000);
        let mut output_hours = Vec::with_capacity(step_hours as usize);
        let mut output_series: CollectionSerieses = collections
            .iter()
            .map(|c| (c.clone(), Vec::with_capacity(step_hours as usize)))
            .collect();

        for chunk in counts_by_hour.chunks(step_hours as usize) {
            output_hours.push(chunk[0].0); // always guaranteed to have at least one element in a chunks chunk
            for (i, collection) in collections.iter().enumerate() {
                let mut c = CountsValue::default();
                for (_, counts) in chunk {
                    c.merge(&counts[i]);
                }
                output_series
                    .get_mut(collection)
                    .expect("output series is initialized with all collections")
                    .push(c);
            }
        }

        Ok((output_hours, output_series))
    }

    fn get_collection_counts(
        &self,
        collection: &Nsid,
        since: HourTruncatedCursor,
        until: Option<HourTruncatedCursor>,
    ) -> StorageResult<JustCount> {
        // grab snapshots in case rollups happen while we're working
        let rollups = self.rollups.snapshot();

        let until = until.unwrap_or_else(|| Cursor::at(SystemTime::now()).into());
        let buckets = CursorBucket::buckets_spanning(since, until);
        let mut total_counts = CountsValue::default();

        for bucket in buckets {
            let key = match bucket {
                CursorBucket::Hour(t) => HourlyRollupKey::new(t, collection).to_db_bytes()?,
                CursorBucket::Week(t) => WeeklyRollupKey::new(t, collection).to_db_bytes()?,
                CursorBucket::AllTime => unreachable!(), // TODO: fall back on this if the time span spans the whole dataset?
            };
            let count = rollups
                .get(&key)?
                .as_deref()
                .map(db_complete::<CountsValue>)
                .transpose()?
                .unwrap_or_default();
            total_counts.merge(&count);
        }

        Ok((&total_counts).into())
    }

    fn get_records_by_collections(
        &self,
        collections: HashSet<Nsid>,
        limit: usize,
        expand_each_collection: bool,
    ) -> StorageResult<Vec<UFOsRecord>> {
        if collections.is_empty() {
            return Ok(vec![]);
        }
        let mut record_iterators = Vec::new();
        for collection in collections {
            let iter = RecordIterator::new(&self.feeds, self.records.clone(), &collection, limit)?;
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

    fn search_collections(&self, terms: Vec<String>) -> StorageResult<Vec<NsidCount>> {
        let start = AllTimeRollupKey::start()?;
        let end = AllTimeRollupKey::end()?;
        let mut matches = Vec::new();
        let limit = 16; // TODO: param
        for kv in self.rollups.range((start, end)) {
            let (key_bytes, val_bytes) = kv?;
            let key = db_complete::<AllTimeRollupKey>(&key_bytes)?;
            let nsid = key.collection();
            for term in &terms {
                if nsid.contains(term) {
                    let counts = db_complete::<CountsValue>(&val_bytes)?;
                    matches.push(NsidCount::new(nsid, &counts));
                    break;
                }
            }
            if matches.len() >= limit {
                break;
            }
        }
        // TODO: indicate incomplete results
        Ok(matches)
    }
}

#[async_trait]
impl StoreReader for FjallReader {
    fn name(&self) -> String {
        "fjall storage v2".into()
    }
    fn update_metrics(&self) {
        gauge!("storage_fjall_l0_run_count", "partition" => "global")
            .set(self.global.tree.l0_run_count() as f64);
        gauge!("storage_fjall_l0_run_count", "partition" => "feeds")
            .set(self.feeds.tree.l0_run_count() as f64);
        gauge!("storage_fjall_l0_run_count", "partition" => "records")
            .set(self.records.tree.l0_run_count() as f64);
        gauge!("storage_fjall_l0_run_count", "partition" => "rollups")
            .set(self.rollups.tree.l0_run_count() as f64);
        gauge!("storage_fjall_l0_run_count", "partition" => "queues")
            .set(self.queues.tree.l0_run_count() as f64);
        gauge!("storage_fjall_keyspace_disk_space").set(self.keyspace.disk_space() as f64);
        gauge!("storage_fjall_journal_count").set(self.keyspace.journal_count() as f64);
        gauge!("storage_fjall_keyspace_sequence").set(self.keyspace.instant() as f64);
    }
    async fn get_storage_stats(&self) -> StorageResult<serde_json::Value> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || FjallReader::get_storage_stats(&s)).await?
    }
    async fn get_consumer_info(&self) -> StorageResult<ConsumerInfo> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || FjallReader::get_consumer_info(&s)).await?
    }
    async fn get_collections(
        &self,
        limit: usize,
        order: OrderCollectionsBy,
        since: Option<HourTruncatedCursor>,
        until: Option<HourTruncatedCursor>,
    ) -> StorageResult<(Vec<NsidCount>, Option<Vec<u8>>)> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || {
            FjallReader::get_collections(&s, limit, order, since, until)
        })
        .await?
    }
    async fn get_prefix(
        &self,
        prefix: NsidPrefix,
        limit: usize,
        order: OrderCollectionsBy,
        since: Option<HourTruncatedCursor>,
        until: Option<HourTruncatedCursor>,
    ) -> StorageResult<(JustCount, Vec<PrefixChild>, Option<Vec<u8>>)> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || {
            FjallReader::get_prefix(&s, prefix, limit, order, since, until)
        })
        .await?
    }
    async fn get_timeseries(
        &self,
        collections: Vec<Nsid>,
        since: HourTruncatedCursor,
        until: Option<HourTruncatedCursor>,
        step: u64,
    ) -> StorageResult<(Vec<HourTruncatedCursor>, CollectionSerieses)> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || {
            FjallReader::get_timeseries(&s, collections, since, until, step)
        })
        .await?
    }
    async fn get_collection_counts(
        &self,
        collection: &Nsid,
        since: HourTruncatedCursor,
        until: Option<HourTruncatedCursor>,
    ) -> StorageResult<JustCount> {
        let s = self.clone();
        let collection = collection.clone();
        tokio::task::spawn_blocking(move || {
            FjallReader::get_collection_counts(&s, &collection, since, until)
        })
        .await?
    }
    async fn get_records_by_collections(
        &self,
        collections: HashSet<Nsid>,
        limit: usize,
        expand_each_collection: bool,
    ) -> StorageResult<Vec<UFOsRecord>> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || {
            FjallReader::get_records_by_collections(&s, collections, limit, expand_each_collection)
        })
        .await?
    }
    async fn search_collections(&self, terms: Vec<String>) -> StorageResult<Vec<NsidCount>> {
        let s = self.clone();
        tokio::task::spawn_blocking(move || FjallReader::search_collections(&s, terms)).await?
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
    fn describe_metrics(&self) {
        describe_histogram!(
            "storage_insert_batch_db_batch_items",
            Unit::Count,
            "how many items are in the fjall batch for batched inserts"
        );
        describe_histogram!(
            "storage_rollup_counts_db_batch_items",
            Unit::Count,
            "how many items are in the fjall batch for a timlies rollup"
        );
        describe_counter!(
            "storage_delete_account_partial_commits",
            Unit::Count,
            "fjall checkpoint commits for cleaning up accounts with too many records"
        );
        describe_counter!(
            "storage_delete_account_completions",
            Unit::Count,
            "total count of account deletes handled"
        );
        describe_counter!(
            "storage_delete_account_records_deleted",
            Unit::Count,
            "total records deleted when handling account deletes"
        );
        describe_histogram!(
            "storage_trim_dirty_nsids",
            Unit::Count,
            "number of NSIDs trimmed"
        );
        describe_histogram!(
            "storage_trim_duration",
            Unit::Microseconds,
            "how long it took to trim the dirty NSIDs"
        );
        describe_counter!(
            "storage_trim_removed",
            Unit::Count,
            "how many records were removed during trim"
        );
    }
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
            let before_creates_count = rolled.counts().creates;
            let before_dids_estimate = rolled.dids().estimate() as u64;

            // update the rollup
            rolled.merge(&counts);

            // new ranks
            let new_creates_count = rolled.counts().creates;
            let new_dids_estimate = rolled.dids().estimate() as u64;

            // update create-ranked secondary index if rank changed
            if new_creates_count != before_creates_count {
                let (old_k, new_k) = match rollup {
                    Rollup::Hourly(cursor) => (
                        HourlyRecordsKey::new(cursor, before_creates_count.into(), &nsid)
                            .to_db_bytes()?,
                        HourlyRecordsKey::new(cursor, new_creates_count.into(), &nsid)
                            .to_db_bytes()?,
                    ),
                    Rollup::Weekly(cursor) => (
                        WeeklyRecordsKey::new(cursor, before_creates_count.into(), &nsid)
                            .to_db_bytes()?,
                        WeeklyRecordsKey::new(cursor, new_creates_count.into(), &nsid)
                            .to_db_bytes()?,
                    ),
                    Rollup::AllTime => (
                        AllTimeRecordsKey::new(before_creates_count.into(), &nsid).to_db_bytes()?,
                        AllTimeRecordsKey::new(new_creates_count.into(), &nsid).to_db_bytes()?,
                    ),
                };
                // remove_weak is allowed here because the secondary ranking index only ever inserts once at a key
                batch.remove_weak(&self.rollups, &old_k);
                batch.insert(&self.rollups, &new_k, "");
            }

            // update dids-ranked secondary index if rank changed
            if new_dids_estimate != before_dids_estimate {
                let (old_k, new_k) = match rollup {
                    Rollup::Hourly(cursor) => (
                        HourlyDidsKey::new(cursor, before_dids_estimate.into(), &nsid)
                            .to_db_bytes()?,
                        HourlyDidsKey::new(cursor, new_dids_estimate.into(), &nsid)
                            .to_db_bytes()?,
                    ),
                    Rollup::Weekly(cursor) => (
                        WeeklyDidsKey::new(cursor, before_dids_estimate.into(), &nsid)
                            .to_db_bytes()?,
                        WeeklyDidsKey::new(cursor, new_dids_estimate.into(), &nsid)
                            .to_db_bytes()?,
                    ),
                    Rollup::AllTime => (
                        AllTimeDidsKey::new(before_dids_estimate.into(), &nsid).to_db_bytes()?,
                        AllTimeDidsKey::new(new_dids_estimate.into(), &nsid).to_db_bytes()?,
                    ),
                };
                // remove_weak is allowed here because the secondary ranking index only ever inserts once at a key
                batch.remove_weak(&self.rollups, &old_k);
                batch.insert(&self.rollups, &new_k, "");
            }

            // replace the main counts rollup
            batch.insert(&self.rollups, &rollup_key_bytes, &rolled.to_db_bytes()?);
        }

        insert_batch_static_neu::<NewRollupCursorKey>(&mut batch, &self.global, last_cursor)?;

        histogram!("storage_rollup_counts_db_batch_items").record(batch.len() as f64);
        batch.commit()?;
        Ok((cursors_advanced, dirty_nsids))
    }
}

impl StoreWriter<FjallBackground> for FjallWriter {
    fn background_tasks(&mut self, reroll: bool) -> StorageResult<FjallBackground> {
        if self.bg_taken.swap(true, Ordering::SeqCst) {
            return Err(StorageError::BackgroundAlreadyStarted);
        }
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
            let counts_value = CountsValue::new(
                CommitCounts {
                    creates: commits.creates as u64,
                    updates: commits.updates as u64,
                    deletes: commits.deletes as u64,
                },
                commits.dids_estimate,
            );
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

        histogram!("storage_insert_batch_db_batch_items").record(batch.len() as f64);
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
    ) -> StorageResult<(usize, usize, bool)> {
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
        let ended_early = false;
        let mut current_cursor: Option<Cursor> = None;
        for (i, kv) in self.feeds.range(live_range).rev().enumerate() {
            if i > 0 && i % 500_000 == 0 {
                log::info!(
                    "trim: at {i} for {:?} (now at {})",
                    collection.to_string(),
                    current_cursor
                        .map(|c| c
                            .elapsed()
                            .map(nice_duration)
                            .unwrap_or("[not past]".into()))
                        .unwrap_or("??".into()),
                );
            }
            let (key_bytes, val_bytes) = kv?;
            let feed_key = db_complete::<NsidRecordFeedKey>(&key_bytes)?;
            let feed_val = db_complete::<NsidRecordFeedVal>(&val_bytes)?;
            let location_key: RecordLocationKey = (&feed_key, &feed_val).into();
            let location_key_bytes = location_key.to_db_bytes()?;

            let Some(location_val_bytes) = self.records.get(&location_key_bytes)? else {
                // record was deleted (hopefully)
                self.feeds.remove(&*key_bytes)?;
                dangling_feed_keys_cleaned += 1;
                continue;
            };

            let (meta, _) = RecordLocationMeta::from_db_bytes(&location_val_bytes)?;
            current_cursor = Some(meta.cursor());

            if meta.cursor() != feed_key.cursor() {
                // older/different version
                self.feeds.remove(&*key_bytes)?;
                dangling_feed_keys_cleaned += 1;
                continue;
            }
            if meta.rev != feed_val.rev() {
                // weird...
                log::warn!("record lookup: cursor match but rev did not...? removing.");
                self.records.remove(&location_key_bytes)?;
                self.feeds.remove(&*key_bytes)?;
                dangling_feed_keys_cleaned += 1;
                continue;
            }

            live_records_found += 1;
            if live_records_found <= limit {
                continue;
            }
            if candidate_new_feed_lower_cursor.is_none() {
                candidate_new_feed_lower_cursor = Some(feed_key.cursor());
            }

            self.feeds.remove(&location_key_bytes)?;
            self.feeds.remove(key_bytes)?;
            records_deleted += 1;
        }

        if !ended_early {
            if let Some(new_cursor) = candidate_new_feed_lower_cursor {
                self.global.insert(
                    &TrimCollectionCursorKey::new(collection.clone()).to_db_bytes()?,
                    &new_cursor.to_db_bytes()?,
                )?;
            }
        }

        log::trace!("trim_collection ({collection:?}) removed {dangling_feed_keys_cleaned} dangling feed entries and {records_deleted} records (ended early? {ended_early})");
        Ok((dangling_feed_keys_cleaned, records_deleted, ended_early))
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
                counter!("storage_delete_account_partial_commits").increment(1);
                batch.commit()?;
                batch = self.keyspace.batch();
            }
        }
        counter!("storage_delete_account_completions").increment(1);
        counter!("storage_delete_account_records_deleted").increment(records_deleted as u64);
        batch.commit()?;
        Ok(records_deleted)
    }
}

pub struct FjallBackground(FjallWriter);

#[async_trait]
impl StoreBackground for FjallBackground {
    async fn run(mut self, backfill: bool) -> StorageResult<()> {
        let mut dirty_nsids = HashSet::new();

        // backfill condition here is iffy -- longer is good when doing the main ingest and then collection trims
        // shorter once those are done helps things catch up
        // the best setting for non-backfill is non-obvious.. it can be pretty slow and still be fine
        let mut rollup =
            tokio::time::interval(Duration::from_micros(if backfill { 100 } else { 32_000 }));
        rollup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        // backfill condition again iffy. collection trims should probably happen in their own phase.
        let mut trim = tokio::time::interval(Duration::from_secs(if backfill { 18 } else { 9 }));
        trim.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = rollup.tick() => {
                    let mut db = self.0.clone();
                    let (n, dirty) = tokio::task::spawn_blocking(move || db.step_rollup()).await??;
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
                    let mut completed = HashSet::new();
                    for collection in &dirty_nsids {
                        let mut db = self.0.clone();
                        let c = collection.clone();
                        let (danglers, deleted, ended_early) = tokio::task::spawn_blocking(move || db.trim_collection(&c, 512, false)).await??;
                        total_danglers += danglers;
                        total_deleted += deleted;
                        if !ended_early {
                            completed.insert(collection.clone());
                        }
                        if total_deleted > 10_000_000 {
                            log::info!("trim stopped early, more than 10M records already deleted.");
                            break;
                        }
                    }
                    let dt = t0.elapsed();
                    log::trace!("finished trimming {n} nsids in {dt:?}: {total_danglers} dangling and {total_deleted} total removed.");
                    histogram!("storage_trim_dirty_nsids").record(completed.len() as f64);
                    histogram!("storage_trim_duration").record(dt.as_micros() as f64);
                    counter!("storage_trim_removed", "dangling" => "true").increment(total_danglers as u64);
                    if total_deleted >= total_danglers {
                        counter!("storage_trim_removed", "dangling" => "false").increment((total_deleted - total_danglers) as u64);
                    } else {
                        // TODO: probably think through what's happening here
                        log::warn!("weird trim case: more danglers than deleted? metric will be missing for dangling=false. deleted={total_deleted} danglers={total_danglers}");
                    }
                    for c in completed {
                        dirty_nsids.remove(&c);
                    }
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
    fn beginning() -> HourTruncatedCursor {
        Cursor::from_start().into()
    }

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
        let JustCount {
            creates,
            dids_estimate,
            ..
        } = read.get_collection_counts(
            &Nsid::new("a.b.c".to_string()).unwrap(),
            beginning(),
            None,
        )?;
        assert_eq!(creates, 0);
        assert_eq!(dids_estimate, 0);
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
        write.step_rollup()?;

        let JustCount {
            creates,
            dids_estimate,
            ..
        } = read.get_collection_counts(&collection, beginning(), None)?;
        assert_eq!(creates, 1);
        assert_eq!(dids_estimate, 1);
        let JustCount {
            creates,
            dids_estimate,
            ..
        } = read.get_collection_counts(
            &Nsid::new("d.e.f".to_string()).unwrap(),
            beginning(),
            None,
        )?;
        assert_eq!(creates, 0);
        assert_eq!(dids_estimate, 0);

        let records = read.get_records_by_collections([collection].into(), 2, false)?;
        assert_eq!(records.len(), 1);
        let rec = &records[0];
        assert_eq!(rec.record.get(), "{}");
        assert!(!rec.is_update);

        let records = read.get_records_by_collections(
            [Nsid::new("d.e.f".to_string()).unwrap()].into(),
            2,
            false,
        )?;
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
            HashSet::from([
                Nsid::new("a.a.a".to_string()).unwrap(),
                Nsid::new("a.a.b".to_string()).unwrap(),
                Nsid::new("a.a.c".to_string()).unwrap(),
            ]),
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
            HashSet::from([
                Nsid::new("a.a.a".to_string()).unwrap(),
                Nsid::new("a.a.b".to_string()).unwrap(),
                Nsid::new("a.a.c".to_string()).unwrap(),
            ]),
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
        write.step_rollup()?;

        let JustCount {
            creates,
            dids_estimate,
            ..
        } = read.get_collection_counts(&collection, beginning(), None)?;
        assert_eq!(creates, 1);
        assert_eq!(dids_estimate, 1);

        let records = read.get_records_by_collections([collection].into(), 2, false)?;
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
        write.step_rollup()?;

        let JustCount {
            creates,
            dids_estimate,
            ..
        } = read.get_collection_counts(&collection, beginning(), None)?;
        assert_eq!(creates, 1);
        assert_eq!(dids_estimate, 1);

        let records = read.get_records_by_collections([collection].into(), 2, false)?;
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
            HashSet::from([Nsid::new("a.a.a".to_string()).unwrap()]),
            100,
            false,
        )?;
        assert_eq!(records.len(), 1);
        let records = read.get_records_by_collections(
            HashSet::from([Nsid::new("a.a.b".to_string()).unwrap()]),
            100,
            false,
        )?;
        assert_eq!(records.len(), 10);
        let records = read.get_records_by_collections(
            HashSet::from([Nsid::new("a.a.c".to_string()).unwrap()]),
            100,
            false,
        )?;
        assert_eq!(records.len(), 1);
        let records = read.get_records_by_collections(
            HashSet::from([Nsid::new("a.a.d".to_string()).unwrap()]),
            100,
            false,
        )?;
        assert_eq!(records.len(), 0);

        write.trim_collection(&Nsid::new("a.a.a".to_string()).unwrap(), 6, false)?;
        write.trim_collection(&Nsid::new("a.a.b".to_string()).unwrap(), 6, false)?;
        write.trim_collection(&Nsid::new("a.a.c".to_string()).unwrap(), 6, false)?;
        write.trim_collection(&Nsid::new("a.a.d".to_string()).unwrap(), 6, false)?;

        let records = read.get_records_by_collections(
            HashSet::from([Nsid::new("a.a.a".to_string()).unwrap()]),
            100,
            false,
        )?;
        assert_eq!(records.len(), 1);
        let records = read.get_records_by_collections(
            HashSet::from([Nsid::new("a.a.b".to_string()).unwrap()]),
            100,
            false,
        )?;
        assert_eq!(records.len(), 6);
        let records = read.get_records_by_collections(
            HashSet::from([Nsid::new("a.a.c".to_string()).unwrap()]),
            100,
            false,
        )?;
        assert_eq!(records.len(), 1);
        let records = read.get_records_by_collections(
            HashSet::from([Nsid::new("a.a.d".to_string()).unwrap()]),
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
            HashSet::from([Nsid::new("a.a.a".to_string()).unwrap()]),
            100,
            false,
        )?;
        assert_eq!(records.len(), 3);

        let records_deleted =
            write.delete_account(&Did::new("did:plc:person-b".to_string()).unwrap())?;
        assert_eq!(records_deleted, 2);

        let records = read.get_records_by_collections(
            HashSet::from([Nsid::new("a.a.a".to_string()).unwrap()]),
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

        let records = read.get_records_by_collections(
            [Nsid::new("a.a.a".to_string()).unwrap()].into(),
            1,
            false,
        )?;
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

        let records = read.get_records_by_collections(
            [Nsid::new("a.a.a".to_string()).unwrap()].into(),
            1,
            false,
        )?;
        assert_eq!(records.len(), 1);

        let (n, _) = write.step_rollup()?;
        assert_eq!(n, 1);

        let records = read.get_records_by_collections(
            [Nsid::new("a.a.a".to_string()).unwrap()].into(),
            1,
            false,
        )?;
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
        let JustCount {
            creates,
            dids_estimate,
            ..
        } = read.get_collection_counts(
            &Nsid::new("a.a.a".to_string()).unwrap(),
            beginning(),
            None,
        )?;
        assert_eq!(creates, 0);
        assert_eq!(dids_estimate, 0);

        // first batch rolled up
        let (n, _) = write.step_rollup()?;
        assert_eq!(n, 1);

        let JustCount {
            creates,
            dids_estimate,
            ..
        } = read.get_collection_counts(
            &Nsid::new("a.a.a".to_string()).unwrap(),
            beginning(),
            None,
        )?;
        assert_eq!(creates, 2);
        assert_eq!(dids_estimate, 2);

        // delete account rolled up
        let (n, _) = write.step_rollup()?;
        assert_eq!(n, 1);

        let JustCount {
            creates,
            dids_estimate,
            ..
        } = read.get_collection_counts(
            &Nsid::new("a.a.a".to_string()).unwrap(),
            beginning(),
            None,
        )?;
        assert_eq!(creates, 2);
        assert_eq!(dids_estimate, 2);

        // second batch rolled up
        let (n, _) = write.step_rollup()?;
        assert_eq!(n, 1);

        let JustCount {
            creates,
            dids_estimate,
            ..
        } = read.get_collection_counts(
            &Nsid::new("a.a.a".to_string()).unwrap(),
            beginning(),
            None,
        )?;
        assert_eq!(creates, 3);
        assert_eq!(dids_estimate, 2);

        // no more rollups left
        let (n, _) = write.step_rollup()?;
        assert_eq!(n, 0);

        Ok(())
    }

    #[test]
    fn get_prefix_children_lexi_empty() {
        let (read, _) = fjall_db();
        let (
            JustCount {
                creates,
                dids_estimate,
                ..
            },
            children,
            cursor,
        ) = read
            .get_prefix(
                NsidPrefix::new("aaa.aaa").unwrap(),
                10,
                OrderCollectionsBy::Lexi { cursor: None },
                None,
                None,
            )
            .unwrap();

        assert_eq!(creates, 0);
        assert_eq!(dids_estimate, 0);
        assert_eq!(children, vec![]);
        assert_eq!(cursor, None);
    }

    #[test]
    fn get_prefix_excludes_exact_collection() -> anyhow::Result<()> {
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
        write.step_rollup()?;

        let (
            JustCount {
                creates,
                dids_estimate,
                ..
            },
            children,
            cursor,
        ) = read.get_prefix(
            NsidPrefix::new("a.a.a").unwrap(),
            10,
            OrderCollectionsBy::Lexi { cursor: None },
            None,
            None,
        )?;
        assert_eq!(creates, 0);
        assert_eq!(dids_estimate, 0);
        assert_eq!(children, vec![]);
        assert_eq!(cursor, None);
        Ok(())
    }

    #[test]
    fn get_prefix_excludes_neighbour_collection() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.aa",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        write.insert_batch(batch.batch)?;
        write.step_rollup()?;

        let (
            JustCount {
                creates,
                dids_estimate,
                ..
            },
            children,
            cursor,
        ) = read.get_prefix(
            NsidPrefix::new("a.a.a").unwrap(),
            10,
            OrderCollectionsBy::Lexi { cursor: None },
            None,
            None,
        )?;
        assert_eq!(creates, 0);
        assert_eq!(dids_estimate, 0);
        assert_eq!(children, vec![]);
        assert_eq!(cursor, None);
        Ok(())
    }

    #[test]
    fn get_prefix_includes_child_collection() -> anyhow::Result<()> {
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
        write.step_rollup()?;

        let (
            JustCount {
                creates,
                dids_estimate,
                ..
            },
            children,
            cursor,
        ) = read.get_prefix(
            NsidPrefix::new("a.a").unwrap(),
            10,
            OrderCollectionsBy::Lexi { cursor: None },
            None,
            None,
        )?;
        assert_eq!(creates, 1);
        assert_eq!(dids_estimate, 1);
        assert_eq!(
            children,
            vec![PrefixChild::Collection(NsidCount {
                nsid: "a.a.a".to_string(),
                creates: 1,
                updates: 0,
                deletes: 0,
                dids_estimate: 1
            }),]
        );
        assert_eq!(cursor, None);
        Ok(())
    }

    #[test]
    fn get_prefix_includes_child_prefix() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a.a",
            "rkey-aaaa",
            "{}",
            Some("rev-aaaa"),
            None,
            10_000,
        );
        write.insert_batch(batch.batch)?;
        write.step_rollup()?;

        let (
            JustCount {
                creates,
                dids_estimate,
                ..
            },
            children,
            cursor,
        ) = read.get_prefix(
            NsidPrefix::new("a.a").unwrap(),
            10,
            OrderCollectionsBy::Lexi { cursor: None },
            None,
            None,
        )?;
        assert_eq!(creates, 1);
        assert_eq!(dids_estimate, 1);
        assert_eq!(
            children,
            vec![PrefixChild::Prefix(PrefixCount {
                prefix: "a.a.a".to_string(),
                creates: 1,
                updates: 0,
                deletes: 0,
                dids_estimate: 1,
            }),]
        );
        assert_eq!(cursor, None);
        Ok(())
    }

    #[test]
    fn get_prefix_merges_child_prefixes() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        batch.create(
            "did:plc:person-a",
            "a.a.a.a",
            "rkey-aaaa",
            "{}",
            Some("rev-aaaa"),
            None,
            10_000,
        );
        batch.create(
            "did:plc:person-a",
            "a.a.a.b",
            "rkey-aaab",
            "{}",
            Some("rev-aaab"),
            None,
            10_001,
        );
        write.insert_batch(batch.batch)?;
        write.step_rollup()?;

        let (
            JustCount {
                creates,
                dids_estimate,
                ..
            },
            children,
            cursor,
        ) = read.get_prefix(
            NsidPrefix::new("a.a").unwrap(),
            10,
            OrderCollectionsBy::Lexi { cursor: None },
            None,
            None,
        )?;
        assert_eq!(creates, 2);
        assert_eq!(dids_estimate, 1);
        assert_eq!(
            children,
            vec![PrefixChild::Prefix(PrefixCount {
                prefix: "a.a.a".to_string(),
                creates: 2,
                updates: 0,
                deletes: 0,
                dids_estimate: 1
            }),]
        );
        assert_eq!(cursor, None);
        Ok(())
    }

    #[test]
    fn get_prefix_exact_and_child_and_prefix() -> anyhow::Result<()> {
        let (read, mut write) = fjall_db();

        let mut batch = TestBatch::default();
        // exact:
        batch.create(
            "did:plc:person-a",
            "a.a.a",
            "rkey-aaa",
            "{}",
            Some("rev-aaa"),
            None,
            10_000,
        );
        // child:
        batch.create(
            "did:plc:person-a",
            "a.a.a.a",
            "rkey-aaaa",
            "{}",
            Some("rev-aaaa"),
            None,
            10_001,
        );
        // prefix:
        batch.create(
            "did:plc:person-a",
            "a.a.a.a.a",
            "rkey-aaaaa",
            "{}",
            Some("rev-aaaaa"),
            None,
            10_002,
        );
        write.insert_batch(batch.batch)?;
        write.step_rollup()?;

        let (
            JustCount {
                creates,
                dids_estimate,
                ..
            },
            children,
            cursor,
        ) = read.get_prefix(
            NsidPrefix::new("a.a.a").unwrap(),
            10,
            OrderCollectionsBy::Lexi { cursor: None },
            None,
            None,
        )?;
        assert_eq!(creates, 2);
        assert_eq!(dids_estimate, 1);
        assert_eq!(
            children,
            vec![
                PrefixChild::Collection(NsidCount {
                    nsid: "a.a.a.a".to_string(),
                    creates: 1,
                    updates: 0,
                    deletes: 0,
                    dids_estimate: 1
                }),
                PrefixChild::Prefix(PrefixCount {
                    prefix: "a.a.a.a".to_string(),
                    creates: 1,
                    updates: 0,
                    deletes: 0,
                    dids_estimate: 1
                }),
            ]
        );
        assert_eq!(cursor, None);
        Ok(())
    }
}
