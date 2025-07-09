use crate::db::data_type::DataType;
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
    /// Object failed to synchronize and will be retried
    Failed,
    /// Object failed and exceeded maximum retry attempts
    FailedPermanently,
}

/// Type of failure encountered during synchronization
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureType {
    /// Failed to retrieve data from S3
    S3DataRetrieval,
    /// Failed to store data in Recall
    RecallStorage,
    /// Unknown/other failure
    Other,
}

/// Record representing an object to be synchronized
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncRecord {
    /// UUID from ObjectIndex (maps to object_index.id)
    pub id: Uuid,
    /// Competition ID
    pub competition_id: Option<Uuid>,
    /// Agent ID
    pub agent_id: Option<Uuid>,
    /// Data type
    pub data_type: DataType,
    /// Timestamp when object was last modified (from object_index.object_last_modified_at)
    pub timestamp: DateTime<Utc>,
    /// Current synchronization status
    pub status: SyncStatus,
    /// Number of retry attempts made
    pub retry_count: u32,
    /// Last error message (if any)
    pub last_error: Option<String>,
    /// Type of failure (if any)
    pub failure_type: Option<FailureType>,
    /// Timestamp of first attempt
    pub first_attempt_at: DateTime<Utc>,
    /// Timestamp of last attempt
    pub last_attempt_at: DateTime<Utc>,
    /// S3 object key (if any)
    pub object_key: Option<String>,
    /// Size in bytes (if any)
    pub size_bytes: Option<i64>,
    /// Metadata (if any)
    pub metadata: Option<serde_json::Value>,
    /// Event timestamp (if any)
    pub event_timestamp: Option<DateTime<Utc>>,
    /// Direct data (if any)
    pub data: Option<Vec<u8>>,
}

impl SyncRecord {
    /// Create a new SyncRecord with specified status
    pub fn new(
        id: Uuid,
        competition_id: Option<Uuid>,
        agent_id: Option<Uuid>,
        data_type: DataType,
        timestamp: DateTime<Utc>,
        status: SyncStatus,
    ) -> Self {
        let now = Utc::now();
        Self {
            id,
            competition_id,
            agent_id,
            data_type,
            timestamp,
            status,
            retry_count: 0,
            last_error: None,
            failure_type: None,
            first_attempt_at: now,
            last_attempt_at: now,
            object_key: None,
            size_bytes: None,
            metadata: None,
            event_timestamp: None,
            data: None,
        }
    }
    
    /// Create a new SyncRecord from an ObjectIndex
    pub fn from_object_index(object: &crate::db::ObjectIndex, status: SyncStatus) -> Self {
        let now = Utc::now();
        Self {
            id: object.id,
            competition_id: object.competition_id,
            agent_id: object.agent_id,
            data_type: object.data_type.clone(),
            timestamp: object.created_at,
            status,
            retry_count: 0,
            last_error: None,
            failure_type: None,
            first_attempt_at: now,
            last_attempt_at: now,
            object_key: object.object_key.clone(),
            size_bytes: object.size_bytes,
            metadata: object.metadata.clone(),
            event_timestamp: object.event_timestamp,
            data: object.data.clone(),
        }
    }
    
    /// Convert SyncRecord to ObjectIndex
    pub fn to_object_index(&self) -> crate::db::ObjectIndex {
        crate::db::ObjectIndex {
            id: self.id,
            object_key: self.object_key.clone(),
            competition_id: self.competition_id,
            agent_id: self.agent_id,
            data_type: self.data_type.clone(),
            size_bytes: self.size_bytes,
            metadata: self.metadata.clone(),
            event_timestamp: self.event_timestamp,
            created_at: self.timestamp,
            data: self.data.clone(),
        }
    }
}

