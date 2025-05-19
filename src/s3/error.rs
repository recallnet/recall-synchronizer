use thiserror::Error;

/// Errors that can occur when interacting with S3 storage
#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum StorageError {
    #[error("Failed to connect to storage: {0}")]
    ConnectionError(String),

    #[error("Object with key {0} not found")]
    ObjectNotFound(String),

    #[error("Access denied for object {0}: {1}")]
    AccessDenied(String, String),

    #[error("Failed to read object {0}: {1}")]
    ReadError(String, String),

    #[error("Network error: {0}")]
    NetworkError(String),

    #[error("Configuration error: {0}")]
    ConfigurationError(String),

    #[error("Other storage error: {0}")]
    Other(#[from] anyhow::Error),
}
