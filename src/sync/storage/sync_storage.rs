use crate::sync::storage::error::SyncStorageError;
use crate::sync::storage::models::{SyncRecord, SyncStatus};
use async_trait::async_trait;
use std::sync::Arc;
use uuid::Uuid;

/// SyncStorage trait defining the interface for managing synchronization state
#[async_trait]
pub trait SyncStorage: Send + Sync + 'static {
    /// Add a new object to track for synchronization
    async fn add_object(&self, record: SyncRecord) -> Result<(), SyncStorageError>;

    /// Set the status of an object by its ID
    async fn set_object_status(&self, id: Uuid, status: SyncStatus)
        -> Result<(), SyncStorageError>;

    /// Get an object by its ID
    async fn get_object(&self, id: Uuid) -> Result<Option<SyncRecord>, SyncStorageError>;

    /// Get all objects with a given status
    async fn get_objects_with_status(
        &self,
        status: SyncStatus,
    ) -> Result<Vec<SyncRecord>, SyncStorageError>;

    /// Get the last queued object (most recent by timestamp)
    /// This helps the synchronizer know where to start fetching next
    async fn get_last_object(&self) -> Result<Option<SyncRecord>, SyncStorageError>;

    /// Get the last synced object ID for a competition or globally
    /// This helps handle objects with the same timestamp
    /// - When competition_id is None, returns the global last synced ID
    /// - When competition_id is Some, returns the last synced ID for that competition
    async fn get_last_synced_object_id(
        &self,
        competition_id: Option<Uuid>,
    ) -> Result<Option<Uuid>, SyncStorageError>;

    /// Set the last synced object ID for a competition or globally
    /// - When competition_id is None, sets the global last synced ID
    /// - When competition_id is Some, sets the last synced ID for that competition
    async fn set_last_synced_object_id(
        &self,
        id: Uuid,
        competition_id: Option<Uuid>,
    ) -> Result<(), SyncStorageError>;

    /// Clear all test data (test-only)
    #[cfg(test)]
    async fn clear_data(&self) -> Result<(), SyncStorageError>;
}

/// Implementation of SyncStorage trait for Arc<T> where T implements SyncStorage
///
/// This allows sharing storage instances across threads and components efficiently.
/// The Arc wrapper provides thread-safe reference counting, enabling multiple
/// parts of the application to share the same storage instance.
#[async_trait]
impl<T: SyncStorage + ?Sized> SyncStorage for Arc<T> {
    async fn add_object(&self, record: SyncRecord) -> Result<(), SyncStorageError> {
        (**self).add_object(record).await
    }

    async fn set_object_status(
        &self,
        id: Uuid,
        status: SyncStatus,
    ) -> Result<(), SyncStorageError> {
        (**self).set_object_status(id, status).await
    }

    async fn get_object(&self, id: Uuid) -> Result<Option<SyncRecord>, SyncStorageError> {
        (**self).get_object(id).await
    }

    async fn get_objects_with_status(
        &self,
        status: SyncStatus,
    ) -> Result<Vec<SyncRecord>, SyncStorageError> {
        (**self).get_objects_with_status(status).await
    }

    async fn get_last_object(&self) -> Result<Option<SyncRecord>, SyncStorageError> {
        (**self).get_last_object().await
    }

    async fn get_last_synced_object_id(
        &self,
        competition_id: Option<Uuid>,
    ) -> Result<Option<Uuid>, SyncStorageError> {
        (**self).get_last_synced_object_id(competition_id).await
    }

    async fn set_last_synced_object_id(
        &self,
        id: Uuid,
        competition_id: Option<Uuid>,
    ) -> Result<(), SyncStorageError> {
        (**self).set_last_synced_object_id(id, competition_id).await
    }

    #[cfg(test)]
    async fn clear_data(&self) -> Result<(), SyncStorageError> {
        (**self).clear_data().await
    }
}
