use crate::sync::storage::error::SyncStorageError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;

/// SyncStorage trait defining the interface for managing synchronization state
#[async_trait]
pub trait SyncStorage: Send + Sync + 'static {
    /// Mark an object as successfully synchronized to Recall
    async fn mark_object_synced(
        &self,
        object_key: &str,
        sync_timestamp: DateTime<Utc>,
    ) -> Result<(), SyncStorageError>;

    /// Check if an object has been synchronized
    async fn is_object_synced(&self, object_key: &str) -> Result<bool, SyncStorageError>;

    /// Update the timestamp of the last successful synchronization run
    async fn update_last_sync_timestamp(
        &self,
        timestamp: DateTime<Utc>,
    ) -> Result<(), SyncStorageError>;

    /// Get the timestamp of the last successful synchronization run
    async fn get_last_sync_timestamp(&self) -> Result<Option<DateTime<Utc>>, SyncStorageError>;
}

/// Implementation of SyncStorage trait for Arc<T> where T implements SyncStorage
///
/// This allows sharing storage instances across threads and components efficiently.
/// The Arc wrapper provides thread-safe reference counting, enabling multiple
/// parts of the application to share the same storage instance.
#[async_trait]
impl<T: SyncStorage + ?Sized> SyncStorage for Arc<T> {
    async fn mark_object_synced(
        &self,
        object_key: &str,
        sync_timestamp: DateTime<Utc>,
    ) -> Result<(), SyncStorageError> {
        (**self)
            .mark_object_synced(object_key, sync_timestamp)
            .await
    }

    async fn is_object_synced(&self, object_key: &str) -> Result<bool, SyncStorageError> {
        (**self).is_object_synced(object_key).await
    }

    async fn update_last_sync_timestamp(
        &self,
        timestamp: DateTime<Utc>,
    ) -> Result<(), SyncStorageError> {
        (**self).update_last_sync_timestamp(timestamp).await
    }

    async fn get_last_sync_timestamp(&self) -> Result<Option<DateTime<Utc>>, SyncStorageError> {
        (**self).get_last_sync_timestamp().await
    }
}
