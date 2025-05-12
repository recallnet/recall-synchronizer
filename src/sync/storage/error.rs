use thiserror::Error;

/// Errors that can occur when interacting with the sync storage
#[derive(Error, Debug)]
pub enum SyncStorageError {
    #[error("Failed to open storage: {0}")]
    OpenError(String),

    #[error("Storage operation failed: {0}")]
    OperationError(String),

    #[error("Object with key {0} not found")]
    ObjectNotFound(String),

    #[error("Storage is locked")]
    Locked,

    #[error("Other storage error: {0}")]
    Other(#[from] anyhow::Error),
}