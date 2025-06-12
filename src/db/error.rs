use thiserror::Error;

/// Errors that can occur when interacting with the database
#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Failed to connect to database: {0}")]
    ConnectionError(String),

    #[error("Query execution failed: {0}")]
    QueryError(String),

    #[error("Failed to deserialize database row: {0}")]
    DeserializationError(String),

    #[error("Other database error: {0}")]
    Other(#[from] anyhow::Error),
}
