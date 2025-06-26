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
}

impl SyncRecord {
    /// Create a new SyncRecord with PendingSync status
    pub fn new(
        id: Uuid,
        competition_id: Option<Uuid>,
        agent_id: Option<Uuid>,
        data_type: DataType,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            competition_id,
            agent_id,
            data_type,
            timestamp,
            status: SyncStatus::PendingSync,
        }
    }

    /// Create a SyncRecord with a specific status
    pub fn with_status(
        id: Uuid,
        competition_id: Option<Uuid>,
        agent_id: Option<Uuid>,
        data_type: DataType,
        timestamp: DateTime<Utc>,
        status: SyncStatus,
    ) -> Self {
        Self {
            id,
            competition_id,
            agent_id,
            data_type,
            timestamp,
            status,
        }
    }
}
