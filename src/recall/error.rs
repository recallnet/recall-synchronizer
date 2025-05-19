use thiserror::Error;

/// Errors related to Recall blockchain storage operations
#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum RecallError {
    /// A blob was not found
    #[error("Blob not found: {0}")]
    BlobNotFound(String),

    /// Connection error to Recall network
    #[error("Connection error: {0}")]
    Connection(String),

    /// Error during data operations
    #[error("Operation error: {0}")]
    Operation(String),

    /// Error reading data from Recall network
    #[error("Read error for {0}: {1}")]
    Read(String, String),

    /// Error writing data to Recall network
    #[error("Write error for {0}: {1}")]
    Write(String, String),

    /// Error deleting data from Recall network
    #[error("Delete error for {0}: {1}")]
    Delete(String, String),

    /// Access denied (unauthorized)
    #[error("Access denied for {0}: {1}")]
    AccessDenied(String, String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Other unspecified errors
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
