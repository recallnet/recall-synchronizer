use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub database: DatabaseConfig,
    pub s3: S3Config,
    pub recall: RecallConfig,
    pub sync: SyncConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RecallConfig {
    pub private_key: String,
    pub network: String,
    pub config_path: Option<String>,
    pub bucket: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncConfig {
    pub interval_seconds: u64,
    pub batch_size: usize,
    pub workers: usize,
    pub retry_limit: usize,
    pub retry_delay_seconds: u64,
    pub state_db_path: String,
}

pub fn load_config(path: &str) -> Result<Config> {
    let config_path = Path::new(path);
    let config_text =
        fs::read_to_string(config_path).context(format!("Failed to read config file: {}", path))?;

    let mut config: Config = config::Config::builder()
        .add_source(config::File::from_str(
            &config_text,
            config::FileFormat::Toml,
        ))
        .build()?
        .try_deserialize()?;

    // Apply environment variable overrides for Recall configuration
    if let Ok(network) = std::env::var("RECALL_NETWORK") {
        config.recall.network = network;
    }

    if let Ok(config_file) = std::env::var("RECALL_NETWORK_FILE") {
        config.recall.config_path = Some(config_file);
    }

    if let Ok(private_key) = std::env::var("RECALL_PRIVATE_KEY") {
        config.recall.private_key = private_key;
    }

    if let Ok(bucket) = std::env::var("RECALL_BUCKET_ADDRESS") {
        config.recall.bucket = Some(bucket);
    }

    Ok(config)
}
