use crate::sync::storage::error::SyncStorageError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// SyncStorage trait defining the interface for managing synchronization state
#[async_trait]
pub trait SyncStorage: Send + Sync + 'static {
    /// Mark an object as successfully synchronized to Recall
    async fn mark_object_synced(&self, object_key: &str, sync_timestamp: DateTime<Utc>) 
        -> Result<(), SyncStorageError>;

    /// Check if an object has been synchronized
    async fn is_object_synced(&self, object_key: &str) -> Result<bool, SyncStorageError>;

    /// Update the timestamp of the last successful synchronization run
    async fn update_last_sync_timestamp(&self, timestamp: DateTime<Utc>) 
        -> Result<(), SyncStorageError>;

    /// Get the timestamp of the last successful synchronization run
    async fn get_last_sync_timestamp(&self) -> Result<Option<DateTime<Utc>>, SyncStorageError>;
}