use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents an object in the object_index table
/// When S3 is configured: object_key and bucket_name are required
/// When S3 is not configured: data field is required
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectIndex {
    pub id: Uuid,
    pub object_key: Option<String>,
    pub bucket_name: Option<String>,
    pub competition_id: Uuid,
    pub agent_id: Uuid,
    pub data_type: String,
    pub size_bytes: Option<i64>,
    pub metadata: Option<serde_json::Value>,
    pub event_timestamp: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub data: Option<Vec<u8>>,
}
