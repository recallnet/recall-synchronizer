use crate::config::{load_config, Config};
use crate::db::ObjectIndex;
use chrono::{DateTime, Utc};
use uuid::Uuid;

pub mod wallet_pool;
pub use wallet_pool::get_next_wallet;

/// Check if a test is enabled via environment variable
fn is_test_enabled(env_var: &str) -> bool {
    std::env::var(env_var)
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false)
}

/// Check if database tests are enabled via environment variable
pub fn is_db_enabled() -> bool {
    is_test_enabled("ENABLE_DB_TESTS")
}

/// Check if SQLite tests are enabled via environment variable
pub fn is_sqlite_enabled() -> bool {
    is_test_enabled("ENABLE_SQLITE_TESTS")
}

/// Check if S3 tests are enabled via environment variable
pub fn is_s3_enabled() -> bool {
    is_test_enabled("ENABLE_S3_TESTS")
}

/// Check if Recall tests are enabled via environment variable
pub fn is_recall_enabled() -> bool {
    is_test_enabled("ENABLE_RECALL_TESTS")
}

/// Load test configuration from config.toml
pub fn load_test_config() -> Result<Config, anyhow::Error> {
    let config_path = "config.toml";

    // Load the config file using the standard config loader
    load_config(config_path).map_err(|e| anyhow::anyhow!("Failed to load config.toml: {}", e))
}

/// Creates a test ObjectIndex with S3 storage mode (object_key and bucket_name)
///
/// # Arguments
///
/// * `object_key` - The unique key for the object
/// * `created_at` - The last modified timestamp
///
/// Other parameters can be customized after creation if needed
pub fn create_test_object_index_s3(object_key: &str, created_at: DateTime<Utc>) -> ObjectIndex {
    ObjectIndex {
        id: Uuid::new_v4(),
        object_key: Some(object_key.to_string()),
        bucket_name: Some("test-bucket".to_string()),
        competition_id: Uuid::new_v4(),
        agent_id: Uuid::new_v4(),
        data_type: "TEST_DATA".to_string(),
        size_bytes: Some(1024),
        metadata: None,
        event_timestamp: Some(created_at),
        created_at,
        data: None,
    }
}

/// Creates a test ObjectIndex with direct storage mode (data field)
///
/// # Arguments
///
/// * `competition_id` - The competition ID
/// * `agent_id` - The agent ID
/// * `data` - The data to store directly
///
/// Other parameters can be customized after creation if needed
pub fn create_test_object_index_direct(
    competition_id: String,
    agent_id: String,
    data: Vec<u8>,
) -> ObjectIndex {
    let now = Utc::now();
    ObjectIndex {
        id: Uuid::new_v4(),
        competition_id: Uuid::parse_str(&competition_id).unwrap_or_else(|_| Uuid::new_v4()),
        agent_id: Uuid::parse_str(&agent_id).unwrap_or_else(|_| Uuid::new_v4()),
        data_type: "TEST_DATA".to_string(),
        size_bytes: Some(data.len() as i64),
        metadata: None,
        event_timestamp: Some(now),
        created_at: now,
        data: Some(data),
        object_key: None,
        bucket_name: None,
    }
}
