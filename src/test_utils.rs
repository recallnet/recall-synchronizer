use crate::config::{load_config, Config};
use crate::db::ObjectIndex;
use chrono::{DateTime, Utc};
use uuid::Uuid;

pub mod wallet_pool;
pub use wallet_pool::get_next_wallet;

/// Check if database tests are enabled via environment variable
pub fn is_db_enabled() -> bool {
    std::env::var("ENABLE_DB_TESTS")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false)
}

/// Check if SQLite tests are enabled via environment variable
pub fn is_sqlite_enabled() -> bool {
    std::env::var("ENABLE_SQLITE_TESTS")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false)
}

/// Check if S3 tests are enabled via environment variable
pub fn is_s3_enabled() -> bool {
    std::env::var("ENABLE_S3_TESTS")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false)
}

/// Check if Recall tests are enabled via environment variable
pub fn is_recall_enabled() -> bool {
    std::env::var("ENABLE_RECALL_TESTS")
        .map(|v| v.to_lowercase() == "true")
        .unwrap_or(false)
}

/// Load test configuration from test_config.toml
pub fn load_test_config() -> Result<Config, anyhow::Error> {
    let config_path = "test_config.toml";

    // Load the config file using the standard config loader
    load_config(config_path).map_err(|e| anyhow::anyhow!("Failed to load test_config.toml: {}", e))
}

/// Creates a test ObjectIndex with default values
///
/// # Arguments
///
/// * `object_key` - The unique key for the object
/// * `modified_at` - The last modified timestamp
///
/// Other parameters can be customized after creation if needed
pub fn create_test_object_index(object_key: &str, modified_at: DateTime<Utc>) -> ObjectIndex {
    ObjectIndex {
        id: Uuid::new_v4(),
        object_key: object_key.to_string(),
        bucket_name: "test-bucket".to_string(),
        competition_id: Some(Uuid::new_v4()),
        agent_id: Some(Uuid::new_v4()),
        data_type: "TEST_DATA".to_string(),
        size_bytes: Some(1024),
        content_hash: Some(format!(
            "hash_{}",
            object_key.chars().rev().take(5).collect::<String>()
        )), // Use reversed part of key for unique hash
        metadata: None,
        event_timestamp: Some(modified_at),
        object_last_modified_at: modified_at,
        created_at: modified_at,
        updated_at: modified_at,
    }
}
