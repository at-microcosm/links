use crate::{error::StorageError, Cursor, EventBatch, UFOsRecord};
use jetstream::exports::Nsid;
use std::path::Path;

pub type StorageResult<T> = Result<T, StorageError>;

pub trait StorageWhatever<R: StoreReader, W: StoreWriter, C> {
    // TODO: extract this
    fn init(
        path: impl AsRef<Path>,
        endpoint: String,
        force_endpoint: bool,
        config: C,
    ) -> StorageResult<(R, W, Option<Cursor>)>
    where
        Self: Sized;
}

pub trait StoreWriter {
    fn insert_batch(&mut self, event_batch: EventBatch) -> StorageResult<()>;
    fn trim_collection(&mut self, collection: &Nsid, limit: usize) -> StorageResult<()>;
}

pub trait StoreReader: Clone {
    fn get_counts_by_collection(&self, collection: &Nsid) -> StorageResult<(u64, u64)>;
    fn get_records_by_collections(
        &self,
        collections: &[&Nsid],
        limit: usize,
    ) -> StorageResult<Vec<UFOsRecord>>;
}
