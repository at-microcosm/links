use crate::db_types::EncodingError;
use crate::UFOsCommit;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FirehoseEventError {
    #[error("Create/Update commit operation missing record data")]
    CruMissingRecord,
    #[error("Account event missing account info")]
    AccountEventMissingAccount,
    #[error("Commit event missing commit info")]
    CommitEventMissingCommit,
}

#[derive(Debug, Error)]
pub enum BatchInsertError {
    #[error("Batch is full and no creates are left to be truncated")]
    BatchFull(UFOsCommit),
    #[error("Bug: tried to index beyond batch limit: {0}")]
    BatchOverflow(usize),
    #[error("Bug: non-terminating head advancement??")]
    BatchForever,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("Failed to initialize: {0}")]
    InitError(String),
    #[error("DB seems to be in a bad state: {0}")]
    BadStateError(String),
    #[error("Fjall error")]
    FjallError(#[from] fjall::Error),
    #[error("LSM-tree error (from fjall)")]
    FjallLsmError(#[from] fjall::LsmError),
    #[error("Bytes encoding error")]
    EncodingError(#[from] EncodingError),
    #[error("If you ever see this, there's a bug in the code. The error was stolen")]
    Stolen,
    #[error("Failed to join tokio task: {0}")]
    JoinError(#[from] tokio::task::JoinError),
}
