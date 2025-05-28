use crate::store_types::{HourTruncatedCursor, SketchSecretPrefix};
use crate::{error::StorageError, ConsumerInfo, Cursor, EventBatch, NsidCount, UFOsRecord};
use async_trait::async_trait;
use jetstream::exports::{Did, Nsid};
use std::collections::HashSet;
use std::path::Path;
use tokio::sync::mpsc::Receiver;

pub type StorageResult<T> = Result<T, StorageError>;

pub trait StorageWhatever<R: StoreReader, W: StoreWriter<B>, B: StoreBackground, C> {
    fn init(
        path: impl AsRef<Path>,
        endpoint: String,
        force_endpoint: bool,
        config: C,
    ) -> StorageResult<(R, W, Option<Cursor>, SketchSecretPrefix)>
    where
        Self: Sized;
}

pub trait StoreWriter<B: StoreBackground>: Send + Sync
where
    Self: 'static,
{
    fn background_tasks(&mut self, reroll: bool) -> StorageResult<B>;

    fn receive_batches<const LIMIT: usize>(
        mut self,
        mut batches: Receiver<EventBatch<LIMIT>>,
    ) -> impl std::future::Future<Output = StorageResult<()>> + Send
    where
        Self: Sized,
    {
        async {
            tokio::task::spawn_blocking(move || {
                while let Some(event_batch) = batches.blocking_recv() {
                    self.insert_batch(event_batch)?;
                }
                Ok::<(), StorageError>(())
            })
            .await?
        }
    }

    fn insert_batch<const LIMIT: usize>(
        &mut self,
        event_batch: EventBatch<LIMIT>,
    ) -> StorageResult<()>;

    fn step_rollup(&mut self) -> StorageResult<(usize, HashSet<Nsid>)>;

    fn trim_collection(
        &mut self,
        collection: &Nsid,
        limit: usize,
        full_scan: bool,
    ) -> StorageResult<(usize, usize)>;

    fn delete_account(&mut self, did: &Did) -> StorageResult<usize>;
}

#[async_trait]
pub trait StoreBackground: Send + Sync {
    async fn run(mut self, backfill: bool) -> StorageResult<()>;
}

#[async_trait]
pub trait StoreReader: Send + Sync {
    fn name(&self) -> String;

    async fn get_storage_stats(&self) -> StorageResult<serde_json::Value>;

    async fn get_consumer_info(&self) -> StorageResult<ConsumerInfo>;

    async fn get_all_collections(
        &self,
        limit: usize,
        cursor: Option<Vec<u8>>,
        since: Option<HourTruncatedCursor>,
        until: Option<HourTruncatedCursor>,
    ) -> StorageResult<(Vec<NsidCount>, Option<Vec<u8>>)>;

    async fn get_top_collections_by_count(
        &self,
        limit: usize,
        since: Option<HourTruncatedCursor>,
        until: Option<HourTruncatedCursor>,
    ) -> StorageResult<Vec<NsidCount>>;

    async fn get_top_collections_by_dids(
        &self,
        limit: usize,
        since: Option<HourTruncatedCursor>,
        until: Option<HourTruncatedCursor>,
    ) -> StorageResult<Vec<NsidCount>>;

    async fn get_counts_by_collection(&self, collection: &Nsid) -> StorageResult<(u64, u64)>;

    async fn get_records_by_collections(
        &self,
        collections: HashSet<Nsid>,
        limit: usize,
        expand_each_collection: bool,
    ) -> StorageResult<Vec<UFOsRecord>>;
}
