use crate::db::data_type::DataType;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents an object in the object_index table
/// When S3 is configured: object_key is required
/// When S3 is not configured: data field is required
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectIndex {
    pub id: Uuid,
    pub object_key: Option<String>,
    pub competition_id: Option<Uuid>,
    pub agent_id: Option<Uuid>,
    pub data_type: DataType,
    pub size_bytes: Option<i64>,
    pub metadata: Option<serde_json::Value>,
    pub event_timestamp: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub data: Option<Vec<u8>>,
}
