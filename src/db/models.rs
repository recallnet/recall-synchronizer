use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents an object in the object_index table (S3 storage mode)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectIndex {
    pub id: Uuid,
    pub object_key: String,
    pub bucket_name: String,
    pub competition_id: Uuid,
    pub agent_id: Uuid,
    pub data_type: String,
    pub size_bytes: Option<i64>,
    pub metadata: Option<serde_json::Value>,
    pub event_timestamp: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Object stored directly in database with data column
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectIndexDirect {
    pub id: Uuid,
    pub competition_id: Uuid,
    pub agent_id: Uuid,
    pub data_type: String,
    pub size_bytes: Option<i64>,
    pub metadata: Option<serde_json::Value>,
    pub event_timestamp: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub data: Vec<u8>,
}
