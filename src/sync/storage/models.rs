use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Represents an object that has been synchronized
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncedObject {
    pub object_key: String,
    pub synced_at: DateTime<Utc>,
}

/// Represents the global state of synchronization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncState {
    pub last_sync_timestamp: Option<DateTime<Utc>>,
}
