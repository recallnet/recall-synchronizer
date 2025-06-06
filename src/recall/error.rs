use thiserror::Error;

/// Errors related to Recall blockchain storage operations
#[derive(Error, Debug)]
pub enum RecallError {
    /// A blob was not found
    #[error("Blob not found: {0}")]
    #[cfg(test)]
    BlobNotFound(String),

    /// Connection error to Recall network
    #[error("Connection error: {0}")]
    Connection(String),

    /// Error during data operations
    #[error("Operation error: {0}")]
    Operation(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Other unspecified errors
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
