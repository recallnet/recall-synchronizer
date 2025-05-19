use crate::s3::error::StorageError;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

/// Storage trait defining the interface for reading data objects from S3-compatible storage
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    /// Get an object by its key
    ///
    /// * `key` - The object key to retrieve
    async fn get_object(&self, key: &str) -> Result<Bytes, StorageError>;

    /// Add an object to storage (test-only)
    #[cfg(test)]
    async fn add_object(&self, key: &str, data: Bytes) -> Result<(), StorageError>;

    /// Remove an object from storage (test-only)
    #[cfg(test)]
    async fn remove_object(&self, key: &str) -> Result<(), StorageError>;

    /// Check if a bucket exists (test-only)
    #[cfg(test)]
    async fn has_bucket(&self, bucket: &str) -> Result<bool, StorageError>;

    /// Create a bucket (test-only)
    #[cfg(test)]
    async fn create_bucket(&self, bucket: &str) -> Result<(), StorageError>;
}

/// Implementation of Storage trait for Arc<T> where T implements Storage
///
/// This allows sharing storage instances across threads and components efficiently.
/// The Arc wrapper provides thread-safe reference counting.
#[async_trait]
impl<T: Storage + ?Sized> Storage for Arc<T> {
    async fn get_object(&self, key: &str) -> Result<Bytes, StorageError> {
        (**self).get_object(key).await
    }

    #[cfg(test)]
    async fn add_object(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
        (**self).add_object(key, data).await
    }

    #[cfg(test)]
    async fn remove_object(&self, key: &str) -> Result<(), StorageError> {
        (**self).remove_object(key).await
    }

    #[cfg(test)]
    async fn has_bucket(&self, bucket: &str) -> Result<bool, StorageError> {
        (**self).has_bucket(bucket).await
    }

    #[cfg(test)]
    async fn create_bucket(&self, bucket: &str) -> Result<(), StorageError> {
        (**self).create_bucket(bucket).await
    }
}
