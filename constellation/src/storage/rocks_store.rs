use super::{ActionableEvent, LinkReader, LinkStorage, PagedAppendingCollection, StorageStats};
use crate::{CountsByCount, Did, RecordId};
use anyhow::{bail, Result};
use bincode::Options as BincodeOptions;
use links::CollectedLink;
use metrics::{counter, describe_counter, describe_histogram, histogram, Unit};
use ratelimit::Ratelimiter;
use rocksdb::backup::{BackupEngine, BackupEngineOptions};
use rocksdb::{
    AsColumnFamilyRef, ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MergeOperands,
    MultiThreaded, Options, PrefixRange, ReadOptions, WriteBatch,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

static DID_IDS_CF: &str = "did_ids";
static TARGET_IDS_CF: &str = "target_ids";
static TARGET_LINKERS_CF: &str = "target_links";
static LINK_TARGETS_CF: &str = "link_targets";

static JETSTREAM_CURSOR_KEY: &str = "jetstream_cursor";

// todo: actually understand and set these options probably better
fn rocks_opts_base() -> Options {
    let mut opts = Options::default();
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.create_if_missing(true);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd); // this probably doesn't work because it hasn't been enabled
                                                                            // TODO: actually enable the bottommost compression. but after other changes run for a bit in case zstd is cpu- or mem-expensive.
    opts
}
fn get_db_opts() -> Options {
    let mut opts = rocks_opts_base();
    opts.create_missing_column_families(true);
    opts.increase_parallelism(4); // todo: make configurable if anyone else actually runs a different instance. start at # of cores
                                  // consider doing optimize_level_style_compaction or optimize_universal_style_compaction
    opts
}
fn get_db_read_opts() -> Options {
    let mut opts = Options::default();
    opts.optimize_for_point_lookup(16_384); // mb (run this on big machines)
    opts
}

#[derive(Debug, Clone)]
pub struct RocksStorage {
    pub db: Arc<DBWithThreadMode<MultiThreaded>>, // TODO: mov seqs here (concat merge op will be fun)
    did_id_table: IdTable<Did, DidIdValue, true>,
    target_id_table: IdTable<TargetKey, TargetId, false>,
    is_writer: bool,
    backup_task: Arc<Option<thread::JoinHandle<Result<()>>>>,
}

trait IdTableValue: ValueFromRocks + Clone {
    fn new(v: u64) -> Self;
    fn id(&self) -> u64;
}
#[derive(Debug, Clone)]
struct IdTableBase<Orig, IdVal: IdTableValue>
where
    Orig: KeyFromRocks,
    for<'a> &'a Orig: AsRocksKey,
{
    _key_marker: PhantomData<Orig>,
    _val_marker: PhantomData<IdVal>,
    name: String,
    id_seq: Arc<AtomicU64>,
}
impl<Orig, IdVal: IdTableValue> IdTableBase<Orig, IdVal>
where
    Orig: KeyFromRocks,
    for<'a> &'a Orig: AsRocksKey,
{
    fn cf_descriptor(&self) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(&self.name, rocks_opts_base())
    }
    fn init<const WITH_REVERSE: bool>(
        self,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<IdTable<Orig, IdVal, WITH_REVERSE>> {
        if db.cf_handle(&self.name).is_none() {
            bail!("failed to get cf handle from db -- was the db open with our .cf_descriptor()?");
        }
        let priv_id_seq = if let Some(seq_bytes) = db.get(self.seq_key())? {
            if seq_bytes.len() != 8 {
                bail!(
                    "reading bytes for u64 id seq {:?}: found the wrong number of bytes",
                    self.seq_key()
                );
            }
            let mut buf: [u8; 8] = [0; 8];
            seq_bytes.as_slice().read_exact(&mut buf)?;
            let last_seq = u64::from_le_bytes(buf);
            last_seq + 1
        } else {
            1
        };
        self.id_seq.store(priv_id_seq, Ordering::SeqCst);
        Ok(IdTable {
            base: self,
            priv_id_seq,
        })
    }
    fn seq_key(&self) -> Vec<u8> {
        let mut k = b"__id_seq_key_plz_be_unique:".to_vec();
        k.extend(self.name.as_bytes());
        k
    }
}
#[derive(Debug, Clone)]
struct IdTable<Orig, IdVal: IdTableValue, const WITH_REVERSE: bool>
where
    Orig: KeyFromRocks,
    for<'a> &'a Orig: AsRocksKey,
{
    base: IdTableBase<Orig, IdVal>,
    priv_id_seq: u64,
}
impl<Orig: Clone, IdVal: IdTableValue, const WITH_REVERSE: bool> IdTable<Orig, IdVal, WITH_REVERSE>
where
    Orig: KeyFromRocks,
    for<'v> &'v IdVal: AsRocksValue,
    for<'k> &'k Orig: AsRocksKey,
{
    #[must_use]
    fn setup(name: &str) -> IdTableBase<Orig, IdVal> {
        IdTableBase::<Orig, IdVal> {
            _key_marker: PhantomData,
            _val_marker: PhantomData,
            name: name.into(),
            id_seq: Arc::new(AtomicU64::new(0)), // zero is "uninint", first seq num will be 1
        }
    }
    fn get_id_val(
        &self,
        db: &DBWithThreadMode<MultiThreaded>,
        orig: &Orig,
    ) -> Result<Option<IdVal>> {
        let cf = db.cf_handle(&self.base.name).unwrap();
        if let Some(_id_bytes) = db.get_cf(&cf, _rk(orig))? {
            Ok(Some(_vr(&_id_bytes)?))
        } else {
            Ok(None)
        }
    }
    fn __get_or_create_id_val<CF>(
        &mut self,
        cf: &CF,
        db: &DBWithThreadMode<MultiThreaded>,
        batch: &mut WriteBatch,
        orig: &Orig,
    ) -> Result<IdVal>
    where
        CF: AsColumnFamilyRef,
    {
        Ok(self.get_id_val(db, orig)?.unwrap_or_else(|| {
            let prev_priv_seq = self.priv_id_seq;
            self.priv_id_seq += 1;
            let prev_public_seq = self.base.id_seq.swap(self.priv_id_seq, Ordering::SeqCst);
            assert_eq!(
                prev_public_seq, prev_priv_seq,
                "public seq may have been modified??"
            );
            let id_value = IdVal::new(self.priv_id_seq);
            batch.put(self.base.seq_key(), self.priv_id_seq.to_le_bytes());
            batch.put_cf(cf, _rk(orig), _rv(&id_value));
            id_value
        }))
    }
    fn estimate_count(&self) -> u64 {
        self.base.id_seq.load(Ordering::SeqCst) - 1 // -1 because seq zero is reserved
    }
}
impl<Orig: Clone, IdVal: IdTableValue> IdTable<Orig, IdVal, true>
where
    Orig: KeyFromRocks,
    for<'v> &'v IdVal: AsRocksValue,
    for<'k> &'k Orig: AsRocksKey,
{
    fn get_or_create_id_val(
        &mut self,
        db: &DBWithThreadMode<MultiThreaded>,
        batch: &mut WriteBatch,
        orig: &Orig,
    ) -> Result<IdVal> {
        let cf = db.cf_handle(&self.base.name).unwrap();
        let id_val = self.__get_or_create_id_val(&cf, db, batch, orig)?;
        // TODO: assert that the original is never a u64 that could collide
        batch.put_cf(&cf, id_val.id().to_be_bytes(), _rk(orig)); // reversed rk/rv on purpose here :/
        Ok(id_val)
    }

    fn get_val_from_id(
        &self,
        db: &DBWithThreadMode<MultiThreaded>,
        id: u64,
    ) -> Result<Option<Orig>> {
        let cf = db.cf_handle(&self.base.name).unwrap();
        if let Some(orig_bytes) = db.get_cf(&cf, id.to_be_bytes())? {
            // HACK ish
            Ok(Some(_kr(&orig_bytes)?))
        } else {
            Ok(None)
        }
    }
}
impl<Orig: Clone, IdVal: IdTableValue> IdTable<Orig, IdVal, false>
where
    Orig: KeyFromRocks,
    for<'v> &'v IdVal: AsRocksValue,
    for<'k> &'k Orig: AsRocksKey,
{
    fn get_or_create_id_val(
        &mut self,
        db: &DBWithThreadMode<MultiThreaded>,
        batch: &mut WriteBatch,
        orig: &Orig,
    ) -> Result<IdVal> {
        let cf = db.cf_handle(&self.base.name).unwrap();
        self.__get_or_create_id_val(&cf, db, batch, orig)
    }
}

impl IdTableValue for DidIdValue {
    fn new(v: u64) -> Self {
        DidIdValue(DidId(v), true)
    }
    fn id(&self) -> u64 {
        self.0 .0
    }
}
impl IdTableValue for TargetId {
    fn new(v: u64) -> Self {
        TargetId(v)
    }
    fn id(&self) -> u64 {
        self.0
    }
}

impl RocksStorage {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Self::describe_metrics();
        RocksStorage::open_readmode(path, false)
    }

    pub fn open_readonly(path: impl AsRef<Path>) -> Result<Self> {
        RocksStorage::open_readmode(path, true)
    }

    fn open_readmode(path: impl AsRef<Path>, readonly: bool) -> Result<Self> {
        let did_id_table = IdTable::<_, _, true>::setup(DID_IDS_CF);
        let target_id_table = IdTable::<_, _, false>::setup(TARGET_IDS_CF);

        let cfs = vec![
            // id reference tables
            did_id_table.cf_descriptor(),
            target_id_table.cf_descriptor(),
            // the reverse links:
            ColumnFamilyDescriptor::new(TARGET_LINKERS_CF, {
                let mut opts = rocks_opts_base();
                opts.set_merge_operator_associative(
                    "merge_op_extend_did_ids",
                    Self::merge_op_extend_did_ids,
                );
                opts
            }),
            // unfortunately we also need forward links to handle deletes
            ColumnFamilyDescriptor::new(LINK_TARGETS_CF, rocks_opts_base()),
        ];

        let db = if readonly {
            DBWithThreadMode::open_cf_descriptors_read_only(&get_db_read_opts(), path, cfs, false)?
        } else {
            DBWithThreadMode::open_cf_descriptors(&get_db_opts(), path, cfs)?
        };

        let db = Arc::new(db);
        let did_id_table = did_id_table.init(&db)?;
        let target_id_table = target_id_table.init(&db)?;
        Ok(Self {
            db,
            did_id_table,
            target_id_table,
            is_writer: !readonly,
            backup_task: None.into(),
        })
    }

    pub fn start_backup(
        &mut self,
        path: PathBuf,
        auto: Option<(u64, Option<usize>)>,
        stay_alive: CancellationToken,
    ) -> Result<()> {
        let task = if let Some((interval_hrs, copies)) = auto {
            eprintln!("backups: starting background task...");
            self.backup_task(path, interval_hrs, copies, stay_alive)
        } else {
            eprintln!("backups: starting a one-off backup...");
            thread::spawn({
                let db = self.db.clone();
                move || Self::do_backup(db, path)
            })
        };
        self.backup_task = Arc::new(Some(task));
        Ok(())
    }

    fn backup_task(
        &self,
        path: PathBuf,
        interval_hrs: u64,
        copies: Option<usize>,
        stay_alive: CancellationToken,
    ) -> std::thread::JoinHandle<Result<()>> {
        let db = self.db.clone();
        thread::spawn(move || {
            let limit =
                Ratelimiter::builder(1, Duration::from_secs(interval_hrs * 60 * 60)).build()?;
            let minimum_sleep = Duration::from_secs(1);

            'quit: loop {
                if let Err(sleep) = limit.try_wait() {
                    eprintln!("backups: background: next backup scheduled in {sleep:?}");
                    let waiting = Instant::now();
                    loop {
                        let remaining = sleep - waiting.elapsed();
                        if stay_alive.is_cancelled() {
                            break 'quit;
                        } else if remaining <= Duration::ZERO {
                            break;
                        } else if remaining < minimum_sleep {
                            thread::sleep(remaining);
                            break;
                        } else {
                            thread::sleep(minimum_sleep);
                        }
                    }
                }
                eprintln!("backups: background: starting backup...");
                if let Err(e) = Self::do_backup(db.clone(), &path) {
                    eprintln!("backups: background: backup failed: {e:?}");
                    // todo: metrics
                } else {
                    eprintln!("backups: background: backup succeeded yay");
                }
                if let Some(copies) = copies {
                    eprintln!("backups: background: trimming to {copies} saved backups...");
                    if let Err(e) = Self::trim_backups(copies, &path) {
                        eprintln!("backups: background: failed to trim backups: {e:?}");
                    } else {
                        eprintln!("backups: background: trimming worked!")
                    }
                }
            }

            Ok(())
        })
    }

    fn do_backup(db: Arc<DBWithThreadMode<MultiThreaded>>, path: impl AsRef<Path>) -> Result<()> {
        let mut engine =
            BackupEngine::open(&BackupEngineOptions::new(path)?, &rocksdb::Env::new()?)?;
        eprintln!("backups: starting backup...");
        let t0 = Instant::now();
        if let Err(e) = engine.create_new_backup(&db) {
            eprintln!("backups: oh no, backup failed: {e:?}");
        } else {
            eprintln!("backups: yay, backup worked?");
        }
        eprintln!(
            "backups: backup finished after {:.2}s",
            t0.elapsed().as_secs_f32()
        );
        Ok(())
    }

    fn trim_backups(num_backups_to_keep: usize, path: impl AsRef<Path>) -> Result<()> {
        let mut engine =
            BackupEngine::open(&BackupEngineOptions::new(path)?, &rocksdb::Env::new()?)?;
        engine.purge_old_backups(num_backups_to_keep)?;
        Ok(())
    }

    fn describe_metrics() {
        describe_histogram!(
            "storage_rocksdb_read_seconds",
            Unit::Seconds,
            "duration of the read stage of actions"
        );
        describe_histogram!(
            "storage_rocksdb_action_seconds",
            Unit::Seconds,
            "duration of read + write of actions"
        );
        describe_counter!(
            "storage_rocksdb_batch_ops_total",
            Unit::Count,
            "total batched operations from actions"
        );
        describe_histogram!(
            "storage_rocksdb_delete_account_ops",
            Unit::Count,
            "total batched ops for account deletions"
        );
    }

    fn merge_op_extend_did_ids(
        key: &[u8],
        existing: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut linkers: Vec<_> = if let Some(existing_bytes) = existing {
            match _vr(existing_bytes) {
                Ok(TargetLinkers(mut existing_linkers)) => {
                    existing_linkers.reserve(operands.len());
                    existing_linkers
                }
                Err(e) => {
                    eprintln!("bug? could not deserialize existing target linkers: {e:?}. key={key:?}. continuing, but data will be lost!");
                    if existing_bytes.len() < 1000 {
                        eprintln!("dropping: {existing_bytes:?}");
                    } else {
                        eprintln!("(too long to print)");
                    }
                    Vec::with_capacity(operands.len())
                }
            }
        } else {
            Vec::with_capacity(operands.len())
        };
        for new_linkers in operands {
            match _vr(new_linkers) {
                Ok(TargetLinkers(new_linkers)) => linkers.extend(new_linkers),
                Err(e) => {
                    eprintln!("bug? could not deserialize new target linkers: {e:?}. key={key:?}. continuing, but data will be lost!");
                    if new_linkers.len() < 1000 {
                        eprintln!("skipping: {new_linkers:?}");
                    } else {
                        eprintln!("(too long to print)");
                    }
                }
            }
        }
        Some(_rv(&TargetLinkers(linkers)))
    }

    fn prefix_iter_cf<K, V, CF, P>(
        &self,
        cf: &CF,
        pre: P,
    ) -> impl Iterator<Item = (K, V)> + use<'_, K, V, CF, P>
    where
        K: KeyFromRocks,
        V: ValueFromRocks,
        CF: AsColumnFamilyRef,
        for<'a> &'a P: AsRocksKeyPrefix<K>,
    {
        let mut read_opts = ReadOptions::default();
        read_opts.set_iterate_range(PrefixRange(_rkp(&pre))); // TODO verify: inclusive bounds?
        self.db
            .iterator_cf_opt(cf, read_opts, IteratorMode::Start)
            .map_while(Result::ok)
            .map_while(|(k, v)| Some((_kr(&k).ok()?, _vr(&v).ok()?)))
    }

    fn update_did_id_value<F>(&self, batch: &mut WriteBatch, did: &Did, update: F) -> Result<bool>
    where
        F: FnOnce(DidIdValue) -> Option<DidIdValue>,
    {
        let cf = self.db.cf_handle(DID_IDS_CF).unwrap();
        let Some(did_id_value) = self.did_id_table.get_id_val(&self.db, did)? else {
            return Ok(false);
        };
        let Some(new_did_id_value) = update(did_id_value) else {
            return Ok(false);
        };
        batch.put_cf(&cf, _rk(did), _rv(&new_did_id_value));
        Ok(true)
    }
    fn delete_did_id_value(&self, batch: &mut WriteBatch, did: &Did) {
        let cf = self.db.cf_handle(DID_IDS_CF).unwrap();
        batch.delete_cf(&cf, _rk(did));
    }

    fn get_target_linkers(&self, target_id: &TargetId) -> Result<TargetLinkers> {
        let cf = self.db.cf_handle(TARGET_LINKERS_CF).unwrap();
        let Some(linkers_bytes) = self.db.get_cf(&cf, _rk(target_id))? else {
            return Ok(TargetLinkers::default());
        };
        _vr(&linkers_bytes)
    }
    /// zero out every duplicate did. bit of a hack, looks the same as deleted, but eh
    fn get_distinct_target_linkers(&self, target_id: &TargetId) -> Result<TargetLinkers> {
        let mut seen = HashSet::new();
        let mut linkers = self.get_target_linkers(target_id)?;
        for (did_id, _) in linkers.0.iter_mut() {
            if seen.contains(did_id) {
                did_id.0 = 0;
            } else {
                seen.insert(*did_id);
            }
        }
        Ok(linkers)
    }
    fn merge_target_linker(
        &self,
        batch: &mut WriteBatch,
        target_id: &TargetId,
        linker_did_id: &DidId,
        linker_rkey: &RKey,
    ) {
        let cf = self.db.cf_handle(TARGET_LINKERS_CF).unwrap();
        batch.merge_cf(
            &cf,
            _rk(target_id),
            _rv(&TargetLinkers(vec![(*linker_did_id, linker_rkey.clone())])),
        );
    }
    fn update_target_linkers<F>(
        &self,
        batch: &mut WriteBatch,
        target_id: &TargetId,
        update: F,
    ) -> Result<bool>
    where
        F: FnOnce(TargetLinkers) -> Option<TargetLinkers>,
    {
        let cf = self.db.cf_handle(TARGET_LINKERS_CF).unwrap();
        let existing_linkers = self.get_target_linkers(target_id)?;
        let Some(new_linkers) = update(existing_linkers) else {
            return Ok(false);
        };
        batch.put_cf(&cf, _rk(target_id), _rv(&new_linkers));
        Ok(true)
    }

    fn put_link_targets(
        &self,
        batch: &mut WriteBatch,
        record_link_key: &RecordLinkKey,
        targets: &RecordLinkTargets,
    ) {
        // todo: we are almost idempotent to link creates with this blind write, but we'll still
        // merge in the reverse index. we could read+modify+write here but it'll be SLOWWWWW on
        // the path that we need to be fast. we could go back to a merge op and probably be
        // consistent. or we can accept just a littttttle inconsistency and be idempotent on
        // forward links but not reverse, slightly messing up deletes :/
        // _maybe_ we could run in slow idempotent r-m-w mode during firehose catch-up at the start,
        // then switch to the fast version?
        let cf = self.db.cf_handle(LINK_TARGETS_CF).unwrap();
        batch.put_cf(&cf, _rk(record_link_key), _rv(targets));
    }
    fn get_record_link_targets(
        &self,
        record_link_key: &RecordLinkKey,
    ) -> Result<Option<RecordLinkTargets>> {
        let cf = self.db.cf_handle(LINK_TARGETS_CF).unwrap();
        if let Some(bytes) = self.db.get_cf(&cf, _rk(record_link_key))? {
            Ok(Some(_vr(&bytes)?))
        } else {
            Ok(None)
        }
    }
    fn delete_record_link(&self, batch: &mut WriteBatch, record_link_key: &RecordLinkKey) {
        let cf = self.db.cf_handle(LINK_TARGETS_CF).unwrap();
        batch.delete_cf(&cf, _rk(record_link_key));
    }
    fn iter_links_for_did_id(
        &self,
        did_id: &DidId,
    ) -> impl Iterator<Item = (RecordLinkKey, RecordLinkTargets)> + use<'_> {
        let cf = self.db.cf_handle(LINK_TARGETS_CF).unwrap();
        self.prefix_iter_cf(&cf, RecordLinkKeyDidIdPrefix(*did_id))
    }
    fn iter_targets_for_target(
        &self,
        target: &Target,
    ) -> impl Iterator<Item = (TargetKey, TargetId)> + use<'_> {
        let cf = self.db.cf_handle(TARGET_IDS_CF).unwrap();
        self.prefix_iter_cf(&cf, TargetIdTargetPrefix(target.clone()))
    }

    //
    // higher-level event action handlers
    //

    fn add_links(
        &mut self,
        record_id: &RecordId,
        links: &[CollectedLink],
        batch: &mut WriteBatch,
    ) -> Result<()> {
        let DidIdValue(did_id, _) =
            self.did_id_table
                .get_or_create_id_val(&self.db, batch, &record_id.did)?;

        let record_link_key = RecordLinkKey(
            did_id,
            Collection(record_id.collection()),
            RKey(record_id.rkey()),
        );
        let mut record_link_targets = RecordLinkTargets::with_capacity(links.len());

        for CollectedLink { target, path } in links {
            let target_key = TargetKey(
                Target(target.clone().into_string()),
                Collection(record_id.collection()),
                RPath(path.clone()),
            );
            let target_id =
                self.target_id_table
                    .get_or_create_id_val(&self.db, batch, &target_key)?;
            self.merge_target_linker(batch, &target_id, &did_id, &RKey(record_id.rkey()));

            record_link_targets.add(RecordLinkTarget(RPath(path.clone()), target_id))
        }

        self.put_link_targets(batch, &record_link_key, &record_link_targets);
        Ok(())
    }

    fn remove_links(&mut self, record_id: &RecordId, batch: &mut WriteBatch) -> Result<()> {
        let Some(DidIdValue(linking_did_id, _)) =
            self.did_id_table.get_id_val(&self.db, &record_id.did)?
        else {
            return Ok(()); // we don't know her: nothing to do
        };

        let record_link_key = RecordLinkKey(
            linking_did_id,
            Collection(record_id.collection()),
            RKey(record_id.rkey()),
        );
        let Some(record_link_targets) = self.get_record_link_targets(&record_link_key)? else {
            return Ok(()); // we don't have these links
        };

        // we do read -> modify -> write here: could merge-op in the deletes instead?
        // otherwise it's another single-thread-constraining thing.
        for RecordLinkTarget(_, target_id) in record_link_targets.0 {
            self.update_target_linkers(batch, &target_id, |mut linkers| {
                if linkers.0.is_empty() {
                    eprintln!("bug? linked target was missing when removing links");
                }
                if !linkers.remove_linker(&linking_did_id, &RKey(record_id.rkey.clone())) {
                    eprintln!("bug? linked target was missing a link when removing links");
                }
                Some(linkers)
            })?;
        }

        self.delete_record_link(batch, &record_link_key);
        Ok(())
    }

    fn set_account(&mut self, did: &Did, active: bool, batch: &mut WriteBatch) -> Result<()> {
        // this needs to be read-modify-write since the did_id needs to stay the same,
        // which has a benefit of allowing to avoid adding entries for dids we don't
        // need. reading on dids needs to be cheap anyway for the current design, and
        // did active/inactive updates are low-freq in the firehose so, eh, it's fine.
        self.update_did_id_value(batch, did, |current_value| {
            Some(DidIdValue(current_value.did_id(), active))
        })?;
        Ok(())
    }

    fn delete_account(&mut self, did: &Did, batch: &mut WriteBatch) -> Result<usize> {
        let mut total_batched_ops = 0;
        let Some(DidIdValue(did_id, _)) = self.did_id_table.get_id_val(&self.db, did)? else {
            return Ok(total_batched_ops); // ignore updates for dids we don't know about
        };
        self.delete_did_id_value(batch, did);
        // TODO: also delete the reverse!!

        // use a separate batch for all their links, since it can be a lot and make us crash at around 1GiB batch size.
        // this should still hopefully be crash-safe: as long as we don't actually delete the DidId entry until after all links are cleared.
        // the above .delete_did_id_value is batched, so it shouldn't be written until we've returned from this fn successfully
        // TODO: queue a background delete task or whatever
        // TODO: test delete account with more links than chunk size
        let stuff: Vec<_> = self.iter_links_for_did_id(&did_id).collect();
        for chunk in stuff.chunks(1024) {
            let mut mini_batch = WriteBatch::default();

            for (record_link_key, links) in chunk {
                self.delete_record_link(&mut mini_batch, record_link_key); // _could_ use delete range here instead of individual deletes, but since we have to scan anyway it's not obvious if it's better

                for RecordLinkTarget(_, target_link_id) in links.0.iter() {
                    self.update_target_linkers(&mut mini_batch, target_link_id, |mut linkers| {
                        if !linkers.remove_linker(&did_id, &record_link_key.2) {
                            eprintln!("bug? could not find linker when removing links while deleting an account");
                        }
                        Some(linkers)
                    })?;
                }
            }
            total_batched_ops += mini_batch.len();
            self.db.write(mini_batch)?; // todo
        }
        Ok(total_batched_ops)
    }
}

impl Drop for RocksStorage {
    fn drop(&mut self) {
        if self.is_writer {
            println!("rocksdb writer: cleaning up for shutdown...");
            if let Err(e) = self.db.flush_wal(true) {
                eprintln!("rocks: flushing wal failed: {e:?}");
            }
            if let Err(e) = self.db.flush_opt(&{
                let mut opt = rocksdb::FlushOptions::default();
                opt.set_wait(true);
                opt
            }) {
                eprintln!("rocks: flushing memtables failed: {e:?}");
            }
            match Arc::get_mut(&mut self.backup_task) {
                Some(maybe_task) => {
                    if let Some(task) = maybe_task.take() {
                        eprintln!("waiting for backup task to complete...");
                        if let Err(e) = task.join() {
                            eprintln!("failed to join backup task: {e:?}");
                        }
                    }
                }
                None => eprintln!("rocks: failed to get backup task, likely a bug."),
            }
            self.db.cancel_all_background_work(true);
        }
    }
}

impl AsRocksValue for u64 {}
impl ValueFromRocks for u64 {}

impl LinkStorage for RocksStorage {
    fn get_cursor(&mut self) -> Result<Option<u64>> {
        self.db
            .get(JETSTREAM_CURSOR_KEY)?
            .map(|b| _vr(&b))
            .transpose()
    }

    fn push(&mut self, event: &ActionableEvent, cursor: u64) -> Result<()> {
        // normal ops
        let mut batch = WriteBatch::default();
        let t0 = Instant::now();
        if let Some(action) = match event {
            ActionableEvent::CreateLinks { record_id, links } => {
                self.add_links(record_id, links, &mut batch)?;
                Some("create_links")
            }
            ActionableEvent::UpdateLinks {
                record_id,
                new_links,
            } => {
                self.remove_links(record_id, &mut batch)?;
                self.add_links(record_id, new_links, &mut batch)?;
                Some("update_links")
            }
            ActionableEvent::DeleteRecord(record_id) => {
                self.remove_links(record_id, &mut batch)?;
                Some("delete_record")
            }
            ActionableEvent::ActivateAccount(did) => {
                self.set_account(did, true, &mut batch)?;
                Some("set_account_status")
            }
            ActionableEvent::DeactivateAccount(did) => {
                self.set_account(did, false, &mut batch)?;
                Some("set_account_status")
            }
            ActionableEvent::DeleteAccount(_) => None, // delete account is handled specially
        } {
            let t_read = t0.elapsed();
            batch.put(JETSTREAM_CURSOR_KEY.as_bytes(), _rv(cursor));
            let batch_ops = batch.len();
            self.db.write(batch)?;
            let t_total = t0.elapsed();

            histogram!("storage_rocksdb_read_seconds", "action" => action)
                .record(t_read.as_secs_f64());
            histogram!("storage_rocksdb_action_seconds", "action" => action)
                .record(t_total.as_secs_f64());
            counter!("storage_rocksdb_batch_ops_total", "action" => action)
                .increment(batch_ops as u64);
        }

        // special metrics for account deletion which can be arbitrarily expensive
        let mut outer_batch = WriteBatch::default();
        let t0 = Instant::now();
        if let ActionableEvent::DeleteAccount(did) = event {
            let inner_batch_ops = self.delete_account(did, &mut outer_batch)?;
            let total_batch_ops = inner_batch_ops + outer_batch.len();
            self.db.write(outer_batch)?;
            let t_total = t0.elapsed();

            histogram!("storage_rocksdb_action_seconds", "action" => "delete_account")
                .record(t_total.as_secs_f64());
            counter!("storage_rocksdb_batch_ops_total", "action" => "delete_account")
                .increment(total_batch_ops as u64);
            histogram!("storage_rocksdb_delete_account_ops").record(total_batch_ops as f64);
        }

        Ok(())
    }

    fn to_readable(&mut self) -> impl LinkReader {
        let mut readable = self.clone();
        readable.is_writer = false;
        readable
    }
}

impl LinkReader for RocksStorage {
    fn get_count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        let target_key = TargetKey(
            Target(target.to_string()),
            Collection(collection.to_string()),
            RPath(path.to_string()),
        );
        if let Some(target_id) = self.target_id_table.get_id_val(&self.db, &target_key)? {
            let (alive, _) = self.get_target_linkers(&target_id)?.count();
            Ok(alive)
        } else {
            Ok(0)
        }
    }

    fn get_distinct_did_count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        let target_key = TargetKey(
            Target(target.to_string()),
            Collection(collection.to_string()),
            RPath(path.to_string()),
        );
        if let Some(target_id) = self.target_id_table.get_id_val(&self.db, &target_key)? {
            Ok(self.get_target_linkers(&target_id)?.count_distinct_dids())
        } else {
            Ok(0)
        }
    }

    fn get_links(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        limit: u64,
        until: Option<u64>,
        filter_dids: &HashSet<Did>,
    ) -> Result<PagedAppendingCollection<RecordId>> {
        let target_key = TargetKey(
            Target(target.to_string()),
            Collection(collection.to_string()),
            RPath(path.to_string()),
        );

        let Some(target_id) = self.target_id_table.get_id_val(&self.db, &target_key)? else {
            return Ok(PagedAppendingCollection {
                version: (0, 0),
                items: Vec::new(),
                next: None,
                total: 0,
            });
        };

        let mut linkers = self.get_target_linkers(&target_id)?;
        if !filter_dids.is_empty() {
            let mut did_filter = HashSet::new();
            for did in filter_dids {
                let Some(DidIdValue(did_id, active)) =
                    self.did_id_table.get_id_val(&self.db, did)?
                else {
                    eprintln!("failed to find a did_id for {did:?}");
                    continue;
                };
                if !active {
                    eprintln!("excluding inactive did from filtered results");
                    continue;
                }
                did_filter.insert(did_id);
            }
            linkers.0.retain(|linker| did_filter.contains(&linker.0));
        }

        let (alive, gone) = linkers.count();
        let total = alive + gone;
        let end = until.map(|u| std::cmp::min(u, total)).unwrap_or(total) as usize;
        let begin = end.saturating_sub(limit as usize);
        let next = if begin == 0 { None } else { Some(begin as u64) };

        let did_id_rkeys = linkers.0[begin..end].iter().rev().collect::<Vec<_>>();

        let mut items = Vec::with_capacity(did_id_rkeys.len());
        // TODO: use get-many (or multi-get or whatever it's called)
        for (did_id, rkey) in did_id_rkeys {
            if did_id.is_empty() {
                continue;
            }
            if let Some(did) = self.did_id_table.get_val_from_id(&self.db, did_id.0)? {
                let Some(DidIdValue(_, active)) = self.did_id_table.get_id_val(&self.db, &did)?
                else {
                    eprintln!("failed to look up did_value from did_id {did_id:?}: {did:?}: data consistency bug?");
                    continue;
                };
                if !active {
                    continue;
                }
                items.push(RecordId {
                    did,
                    collection: collection.to_string(),
                    rkey: rkey.0.clone(),
                });
            } else {
                eprintln!("failed to look up did from did_id {did_id:?}");
            }
        }

        Ok(PagedAppendingCollection {
            version: (total, gone),
            items,
            next,
            total: alive,
        })
    }

    fn get_distinct_dids(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        limit: u64,
        until: Option<u64>,
    ) -> Result<PagedAppendingCollection<Did>> {
        let target_key = TargetKey(
            Target(target.to_string()),
            Collection(collection.to_string()),
            RPath(path.to_string()),
        );

        let Some(target_id) = self.target_id_table.get_id_val(&self.db, &target_key)? else {
            return Ok(PagedAppendingCollection {
                version: (0, 0),
                items: Vec::new(),
                next: None,
                total: 0,
            });
        };

        let linkers = self.get_distinct_target_linkers(&target_id)?;

        let (alive, gone) = linkers.count();
        let total = alive + gone;
        let end = until.map(|u| std::cmp::min(u, total)).unwrap_or(total) as usize;
        let begin = end.saturating_sub(limit as usize);
        let next = if begin == 0 { None } else { Some(begin as u64) };

        let did_id_rkeys = linkers.0[begin..end].iter().rev().collect::<Vec<_>>();

        let mut items = Vec::with_capacity(did_id_rkeys.len());
        // TODO: use get-many (or multi-get or whatever it's called)
        for (did_id, _) in did_id_rkeys {
            if did_id.is_empty() {
                continue;
            }
            if let Some(did) = self.did_id_table.get_val_from_id(&self.db, did_id.0)? {
                let Some(DidIdValue(_, active)) = self.did_id_table.get_id_val(&self.db, &did)?
                else {
                    eprintln!("failed to look up did_value from did_id {did_id:?}: {did:?}: data consistency bug?");
                    continue;
                };
                if !active {
                    continue;
                }
                items.push(did);
            } else {
                eprintln!("failed to look up did from did_id {did_id:?}");
            }
        }

        Ok(PagedAppendingCollection {
            version: (total, gone),
            items,
            next,
            total: alive,
        })
    }

    fn get_all_record_counts(&self, target: &str) -> Result<HashMap<String, HashMap<String, u64>>> {
        let mut out: HashMap<String, HashMap<String, u64>> = HashMap::new();
        for (target_key, target_id) in self.iter_targets_for_target(&Target(target.into())) {
            let TargetKey(_, Collection(ref collection), RPath(ref path)) = target_key;
            let (count, _) = self.get_target_linkers(&target_id)?.count();
            out.entry(collection.into())
                .or_default()
                .insert(path.clone(), count);
        }
        Ok(out)
    }

    fn get_all_counts(
        &self,
        target: &str,
    ) -> Result<HashMap<String, HashMap<String, CountsByCount>>> {
        let mut out: HashMap<String, HashMap<String, CountsByCount>> = HashMap::new();
        for (target_key, target_id) in self.iter_targets_for_target(&Target(target.into())) {
            let TargetKey(_, Collection(ref collection), RPath(ref path)) = target_key;
            let target_linkers = self.get_target_linkers(&target_id)?;
            let (records, _) = target_linkers.count();
            let distinct_dids = target_linkers.count_distinct_dids();
            out.entry(collection.into()).or_default().insert(
                path.clone(),
                CountsByCount {
                    records,
                    distinct_dids,
                },
            );
        }
        Ok(out)
    }

    fn get_stats(&self) -> Result<StorageStats> {
        let dids = self.did_id_table.estimate_count();
        let targetables = self.target_id_table.estimate_count();
        let lr_cf = self.db.cf_handle(LINK_TARGETS_CF).unwrap();
        let linking_records = self
            .db
            .property_value_cf(&lr_cf, rocksdb::properties::ESTIMATE_NUM_KEYS)?
            .map(|s| s.parse::<u64>())
            .transpose()?
            .unwrap_or(0);
        Ok(StorageStats {
            dids,
            targetables,
            linking_records,
        })
    }
}

trait AsRocksKey: Serialize {}
trait AsRocksKeyPrefix<K: KeyFromRocks>: Serialize {}
trait AsRocksValue: Serialize {}
trait KeyFromRocks: for<'de> Deserialize<'de> {}
trait ValueFromRocks: for<'de> Deserialize<'de> {}

// did_id table
impl AsRocksKey for &Did {}
impl AsRocksValue for &DidIdValue {}
impl ValueFromRocks for DidIdValue {}

// temp
impl KeyFromRocks for Did {}
impl AsRocksKey for &DidId {}

// target_ids table
impl AsRocksKey for &TargetKey {}
impl AsRocksKeyPrefix<TargetKey> for &TargetIdTargetPrefix {}
impl AsRocksValue for &TargetId {}
impl KeyFromRocks for TargetKey {}
impl ValueFromRocks for TargetId {}

// target_links table
impl AsRocksKey for &TargetId {}
impl AsRocksValue for &TargetLinkers {}
impl ValueFromRocks for TargetLinkers {}

// record_link_targets table
impl AsRocksKey for &RecordLinkKey {}
impl AsRocksKeyPrefix<RecordLinkKey> for &RecordLinkKeyDidIdPrefix {}
impl AsRocksValue for &RecordLinkTargets {}
impl KeyFromRocks for RecordLinkKey {}
impl ValueFromRocks for RecordLinkTargets {}

pub fn _bincode_opts() -> impl BincodeOptions {
    bincode::DefaultOptions::new().with_big_endian() // happier db -- numeric prefixes in lsm
}
fn _rk(k: impl AsRocksKey) -> Vec<u8> {
    _bincode_opts().serialize(&k).unwrap()
}
fn _rkp<K: KeyFromRocks>(kp: impl AsRocksKeyPrefix<K>) -> Vec<u8> {
    _bincode_opts().serialize(&kp).unwrap()
}
fn _rv(v: impl AsRocksValue) -> Vec<u8> {
    _bincode_opts().serialize(&v).unwrap()
}
fn _kr<T: KeyFromRocks>(bytes: &[u8]) -> Result<T> {
    Ok(_bincode_opts().deserialize(bytes)?)
}
fn _vr<T: ValueFromRocks>(bytes: &[u8]) -> Result<T> {
    Ok(_bincode_opts().deserialize(bytes)?)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Collection(pub String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RPath(pub String);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RKey(pub String);

impl RKey {
    fn empty() -> Self {
        RKey("".to_string())
    }
}

// did ids
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DidId(pub u64);

impl DidId {
    fn empty() -> Self {
        DidId(0)
    }
    fn is_empty(&self) -> bool {
        self.0 == 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DidIdValue(DidId, bool); // active or not

impl DidIdValue {
    fn did_id(&self) -> DidId {
        let Self(id, _) = self;
        *id
    }
}

// target ids
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TargetId(u64); // key

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Target(pub String); // the actual target/uri

// targets (uris, dids, etc.): the reverse index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetKey(pub Target, pub Collection, pub RPath);

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TargetLinkers(pub Vec<(DidId, RKey)>);

impl TargetLinkers {
    fn remove_linker(&mut self, did: &DidId, rkey: &RKey) -> bool {
        if let Some(entry) = self.0.iter_mut().rfind(|d| **d == (*did, rkey.clone())) {
            *entry = (DidId::empty(), RKey::empty());
            true
        } else {
            false
        }
    }
    pub fn count(&self) -> (u64, u64) {
        // (linkers, deleted links)
        let total = self.0.len() as u64;
        let alive = self.0.iter().filter(|(DidId(id), _)| *id != 0).count() as u64;
        let gone = total - alive;
        (alive, gone)
    }
    fn count_distinct_dids(&self) -> u64 {
        self.0
            .iter()
            .filter_map(|(DidId(id), _)| if *id == 0 { None } else { Some(id) })
            .collect::<HashSet<_>>()
            .len() as u64
    }
}

// forward links to targets so we can delete links
#[derive(Debug, Serialize, Deserialize)]
struct RecordLinkKey(DidId, Collection, RKey);

// does this even work????
#[derive(Debug, Serialize, Deserialize)]
struct RecordLinkKeyDidIdPrefix(DidId);

#[derive(Debug, Serialize, Deserialize)]
struct TargetIdTargetPrefix(Target);

#[derive(Debug, Serialize, Deserialize)]
struct RecordLinkTarget(RPath, TargetId);

#[derive(Debug, Default, Serialize, Deserialize)]
struct RecordLinkTargets(Vec<RecordLinkTarget>);

impl RecordLinkTargets {
    fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }
    fn add(&mut self, target: RecordLinkTarget) {
        self.0.push(target)
    }
}

#[cfg(test)]
mod tests {
    use super::super::ActionableEvent;
    use super::*;
    use links::Link;
    use tempfile::tempdir;

    #[test]
    fn rocks_delete_iterator_regression() -> Result<()> {
        let mut store = RocksStorage::new(tempdir()?)?;

        // create a link from the deleter account
        store.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:will-shortly-delete".into(),
                    collection: "a.b.c".into(),
                    rkey: "asdf".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("example.com".into()),
                    path: ".uri".into(),
                }],
            },
            0,
        )?;

        // and a different link from a separate, new account (later in didid prefix iteration)
        store.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:someone-else".into(),
                    collection: "a.b.c".into(),
                    rkey: "asdf".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("another.example.com".into()),
                    path: ".uri".into(),
                }],
            },
            0,
        )?;

        // now delete the first account (this is where the buggy version explodes)
        store.push(
            &ActionableEvent::DeleteAccount("did:plc:will-shortly-delete".into()),
            0,
        )?;

        Ok(())
    }

    #[test]
    fn rocks_prefix_iteration_helper() -> Result<()> {
        #[derive(Serialize, Deserialize)]
        struct Key(u8, u8);

        #[derive(Serialize)]
        struct KeyPrefix(u8);

        #[derive(Serialize, Deserialize)]
        struct Value(());

        impl AsRocksKey for &Key {}
        impl AsRocksKeyPrefix<Key> for &KeyPrefix {}
        impl AsRocksValue for &Value {}

        impl KeyFromRocks for Key {}
        impl ValueFromRocks for Value {}

        let data = RocksStorage::new(tempdir()?)?;
        let cf = data.db.cf_handle(DID_IDS_CF).unwrap();
        let mut batch = WriteBatch::default();

        // not our prefix
        batch.put_cf(&cf, _rk(&Key(0x01, 0x00)), _rv(&Value(())));
        batch.put_cf(&cf, _rk(&Key(0x01, 0xFF)), _rv(&Value(())));

        // our prefix!
        for i in 0..=0xFF {
            batch.put_cf(&cf, _rk(&Key(0x02, i)), _rv(&Value(())));
        }

        // not our prefix
        batch.put_cf(&cf, _rk(&Key(0x03, 0x00)), _rv(&Value(())));
        batch.put_cf(&cf, _rk(&Key(0x03, 0xFF)), _rv(&Value(())));

        data.db.write(batch)?;

        let mut okays: [bool; 256] = [false; 256];
        for (i, (k, Value(_))) in data.prefix_iter_cf(&cf, KeyPrefix(0x02)).enumerate() {
            assert!(i < 256);
            assert_eq!(k.0, 0x02, "prefix iterated key has the right prefix");
            assert_eq!(k.1 as usize, i, "prefixed keys are iterated in exact order");
            okays[k.1 as usize] = true;
        }
        assert!(okays.iter().all(|b| *b), "every key was iterated");

        Ok(())
    }

    // TODO: add tests for key prefixes actually prefixing (bincode encoding _should_...)
}
