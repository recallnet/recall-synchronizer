use anyhow::Result;
use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub database: DatabaseConfig,
    pub s3: S3Config,
    pub recall: RecallConfig,
    pub sync: SyncConfig,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
}

#[derive(Debug, Deserialize)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RecallConfig {
    pub endpoint: String,
    pub private_key: String,
}

#[derive(Debug, Deserialize)]
pub struct SyncConfig {
    pub interval_seconds: u64,
    pub batch_size: usize,
    pub workers: usize,
    pub state_db_path: String,
}

pub fn load_config(path: &str) -> Result<Config> {
    let config_text = fs::read_to_string(Path::new(path))?;
    let config: Config = toml::from_str(&config_text)?;
    Ok(config)
}
