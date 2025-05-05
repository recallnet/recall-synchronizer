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

#[derive(Debug, Serialize, Deserialize)]
pub struct RecallConfig {
    pub endpoint: String,
    pub private_key: String,
    pub prefix: Option<String>,
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

    let config: Config = config::Config::builder()
        .add_source(config::File::from_str(
            &config_text,
            config::FileFormat::Toml,
        ))
        .build()?
        .try_deserialize()?;

    Ok(config)
}
