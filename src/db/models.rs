use serde::{Deserialize, Serialize};
use sqlx::types::chrono::{DateTime, Utc};

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectInfo {
    pub key: String,
    pub updated_at: DateTime<Utc>,
    pub competition_id: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub size_bytes: Option<i64>,
}
