use crate::sync::storage::error::SyncStorageError;
use crate::sync::storage::models::{SyncRecord, SyncStatus, FailureType};
use async_trait::async_trait;
use std::sync::Arc;
use uuid::Uuid;

/// SyncStorage trait defining the interface for managing synchronization state
#[async_trait]
pub trait SyncStorage: Send + Sync + 'static {
    /// Add a new object to track for synchronization
    async fn add_object(&self, record: SyncRecord) -> Result<(), SyncStorageError>;

    /// Add a new object if it doesn't exist, or update the existing one if it does
    async fn upsert_object(&self, record: SyncRecord) -> Result<(), SyncStorageError>;

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

    /// Get the last synced object ID
    /// This helps handle objects with the same timestamp
    async fn get_last_synced_object_id(&self) -> Result<Option<Uuid>, SyncStorageError>;

    /// Set the last synced object ID
    async fn set_last_synced_object_id(&self, id: Uuid) -> Result<(), SyncStorageError>;

    /// Clear all data
    async fn clear_all(&self) -> Result<(), SyncStorageError>;

    /// Record a failure for an object with retry information
    async fn record_failure(
        &self,
        id: Uuid,
        failure_type: FailureType,
        error_message: String,
        is_permanent: bool,
    ) -> Result<(), SyncStorageError>;

    /// Get objects that have failed and can be retried (status = Failed)
    async fn get_failed_objects(&self) -> Result<Vec<SyncRecord>, SyncStorageError>;

    /// Get objects that have failed permanently (status = FailedPermanently)
    async fn get_permanently_failed_objects(&self) -> Result<Vec<SyncRecord>, SyncStorageError>;

    /// Clear the data field for a successfully synchronized object
    async fn clear_object_data(&self, id: Uuid) -> Result<(), SyncStorageError>;
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

    async fn upsert_object(&self, record: SyncRecord) -> Result<(), SyncStorageError> {
        (**self).upsert_object(record).await
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

    async fn get_last_synced_object_id(&self) -> Result<Option<Uuid>, SyncStorageError> {
        (**self).get_last_synced_object_id().await
    }

    async fn set_last_synced_object_id(&self, id: Uuid) -> Result<(), SyncStorageError> {
        (**self).set_last_synced_object_id(id).await
    }

    async fn clear_all(&self) -> Result<(), SyncStorageError> {
        (**self).clear_all().await
    }

    async fn record_failure(
        &self,
        id: Uuid,
        failure_type: FailureType,
        error_message: String,
        is_permanent: bool,
    ) -> Result<(), SyncStorageError> {
        (**self).record_failure(id, failure_type, error_message, is_permanent).await
    }

    async fn get_failed_objects(&self) -> Result<Vec<SyncRecord>, SyncStorageError> {
        (**self).get_failed_objects().await
    }

    async fn get_permanently_failed_objects(&self) -> Result<Vec<SyncRecord>, SyncStorageError> {
        (**self).get_permanently_failed_objects().await
    }

    async fn clear_object_data(&self, id: Uuid) -> Result<(), SyncStorageError> {
        (**self).clear_object_data(id).await
    }
}
