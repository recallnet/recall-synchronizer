use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents an object in the object_index table
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectIndex {
    pub id: Uuid,
    pub object_key: String,
    pub bucket_name: String,
    pub competition_id: Uuid,
    pub agent_id: Uuid,
    pub data_type: String,
    pub size_bytes: Option<i64>,
    pub content_hash: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub event_timestamp: Option<DateTime<Utc>>,
    pub object_last_modified_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
