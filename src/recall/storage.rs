use crate::recall::error::RecallError;
use async_trait::async_trait;
use std::sync::Arc;

/// Storage trait defining the interface for reading and writing data to the Recall blockchain
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    /// Store a blob on the Recall network
    ///
    /// * `key` - The key/path for the blob
    /// * `data` - The blob data to store
    async fn add_blob(&self, key: &str, data: Vec<u8>) -> Result<(), RecallError>;

    /// Check if a blob exists on the Recall network
    ///
    /// * `key` - The key/path to check
    async fn has_blob(&self, key: &str) -> Result<bool, RecallError>;

    /// List all blobs with a given prefix
    ///
    /// * `prefix` - The prefix to filter by
    async fn list_blobs(&self, prefix: &str) -> Result<Vec<String>, RecallError>;

    /// Delete a blob (test-only)
    #[cfg(test)]
    async fn delete_blob(&self, key: &str) -> Result<(), RecallError>;

    /// Clear all blobs with a given prefix (test-only)
    #[cfg(test)]
    async fn clear_prefix(&self, prefix: &str) -> Result<(), RecallError>;

    /// Get a blob from the Recall network (test-only)
    ///
    /// * `key` - The key/path to retrieve
    #[cfg(test)]
    async fn get_blob(&self, key: &str) -> Result<Vec<u8>, RecallError>;
}

/// Implementation of RecallStorage trait for Arc<T> where T implements RecallStorage
///
/// This allows sharing storage instances across threads and components efficiently.
/// The Arc wrapper provides thread-safe reference counting.
#[async_trait]
impl<T: Storage + ?Sized> Storage for Arc<T> {
    async fn add_blob(&self, key: &str, data: Vec<u8>) -> Result<(), RecallError> {
        (**self).add_blob(key, data).await
    }

    async fn has_blob(&self, key: &str) -> Result<bool, RecallError> {
        (**self).has_blob(key).await
    }

    async fn list_blobs(&self, prefix: &str) -> Result<Vec<String>, RecallError> {
        (**self).list_blobs(prefix).await
    }

    #[cfg(test)]
    async fn delete_blob(&self, key: &str) -> Result<(), RecallError> {
        (**self).delete_blob(key).await
    }

    #[cfg(test)]
    async fn clear_prefix(&self, prefix: &str) -> Result<(), RecallError> {
        (**self).clear_prefix(prefix).await
    }

    #[cfg(test)]
    async fn get_blob(&self, key: &str) -> Result<Vec<u8>, RecallError> {
        (**self).get_blob(key).await
    }
}
