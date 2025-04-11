// use crate::store_types::CountsValue;
use crate::{error::StorageError, ConsumerInfo, Cursor, EventBatch, TopCollections, UFOsRecord};
use async_trait::async_trait;
use jetstream::exports::{Did, Nsid};
use std::path::Path;

pub type StorageResult<T> = Result<T, StorageError>;

pub trait StorageWhatever<R: StoreReader, W: StoreWriter, C> {
    fn init(
        path: impl AsRef<Path>,
        endpoint: String,
        force_endpoint: bool,
        config: C,
    ) -> StorageResult<(R, W, Option<Cursor>)>
    where
        Self: Sized;
}

pub trait StoreWriter: Send + Sync {
    fn insert_batch<const LIMIT: usize>(
        &mut self,
        event_batch: EventBatch<LIMIT>,
    ) -> StorageResult<()>;

    fn step_rollup(&mut self) -> StorageResult<usize>;

    fn trim_collection(&mut self, collection: &Nsid, limit: usize) -> StorageResult<()>;

    fn delete_account(&mut self, did: &Did) -> StorageResult<usize>;
}

#[async_trait]
pub trait StoreReader: Send + Sync {
    async fn get_storage_stats(&self) -> StorageResult<serde_json::Value>;

    async fn get_consumer_info(&self) -> StorageResult<ConsumerInfo>;

    async fn get_top_collections(&self) -> StorageResult<TopCollections>;

    async fn get_counts_by_collection(&self, collection: &Nsid) -> StorageResult<(u64, u64)>;

    async fn get_records_by_collections(
        &self,
        collections: &[Nsid],
        limit: usize,
    ) -> StorageResult<Vec<UFOsRecord>>;
}
