use thiserror::Error;

/// Errors that can occur when interacting with S3 storage
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Object with key {0} not found")]
    ObjectNotFound(String),

    #[error("Access denied for object {0}: {1}")]
    AccessDenied(String, String),

    #[error("Failed to read object {0}: {1}")]
    ReadError(String, String),

    #[error("Other storage error: {0}")]
    Other(#[from] anyhow::Error),
}
