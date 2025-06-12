use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Status of synchronization for an object
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncStatus {
    /// Object is pending synchronization
    PendingSync,
    /// Object is currently being processed
    Processing,
    /// Object has been successfully synchronized
    Complete,
}

/// Record representing an object to be synchronized
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncRecord {
    /// UUID from ObjectIndex (maps to object_index.id)
    pub id: Uuid,
    /// S3 object key from ObjectIndex
    pub object_key: String,
    /// S3 bucket name from ObjectIndex
    pub bucket_name: String,
    /// Timestamp when object was last modified (from object_index.object_last_modified_at)
    pub timestamp: DateTime<Utc>,
    /// Current synchronization status
    pub status: SyncStatus,
}

impl SyncRecord {
    /// Create a new SyncRecord with PendingSync status
    pub fn new(
        id: Uuid,
        object_key: String,
        bucket_name: String,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            object_key,
            bucket_name,
            timestamp,
            status: SyncStatus::PendingSync,
        }
    }

    /// Create a SyncRecord with a specific status
    pub fn with_status(
        id: Uuid,
        object_key: String,
        bucket_name: String,
        timestamp: DateTime<Utc>,
        status: SyncStatus,
    ) -> Self {
        Self {
            id,
            object_key,
            bucket_name,
            timestamp,
            status,
        }
    }
}
