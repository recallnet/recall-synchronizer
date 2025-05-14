use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct TestConfig {
    pub database: DatabaseTestConfig,
    pub sqlite: SqliteTestConfig,
    #[allow(dead_code)]
    pub s3: S3TestConfig,
    #[allow(dead_code)]
    pub recall: RecallTestConfig,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseTestConfig {
    pub enabled: bool,
    pub url: String,
}

#[derive(Debug, Deserialize)]
pub struct SqliteTestConfig {
    pub enabled: bool,
    #[allow(dead_code)]
    pub path: String,
}

#[derive(Debug, Deserialize)]
pub struct S3TestConfig {
    #[allow(dead_code)]
    pub enabled: bool,
    #[allow(dead_code)]
    pub endpoint: String,
    #[allow(dead_code)]
    pub region: String,
    #[allow(dead_code)]
    pub bucket: String,
    #[allow(dead_code)]
    pub access_key_id: String,
    #[allow(dead_code)]
    pub secret_access_key: String,
}

#[derive(Debug, Deserialize)]
pub struct RecallTestConfig {
    #[allow(dead_code)]
    pub enabled: bool,
    #[allow(dead_code)]
    pub endpoint: String,
}

/// Load test configuration from test_config.toml
/// Environment variables will override config file settings
pub fn load_test_config() -> TestConfig {
    let config_path = Path::new("test_config.toml");

    // Start with file-based config or default
    let mut config = if !config_path.exists() {
        default_test_config()
    } else {
        // Read the config file
        let config_content = match fs::read_to_string(config_path) {
            Ok(content) => content,
            Err(_) => {
                println!("Warning: Failed to read test_config.toml, using default config");
                return default_test_config();
            }
        };

        // Parse the config file
        match toml::from_str(&config_content) {
            Ok(config) => config,
            Err(e) => {
                println!("Warning: Failed to parse test_config.toml: {}", e);
                default_test_config()
            }
        }
    };

    // Override with environment variables if present
    if let Ok(val) = std::env::var("ENABLE_DB_TESTS") {
        config.database.enabled = val.to_lowercase() == "true";
    }

    if let Ok(val) = std::env::var("ENABLE_SQLITE_TESTS") {
        config.sqlite.enabled = val.to_lowercase() == "true";
    }

    if let Ok(val) = std::env::var("ENABLE_S3_TESTS") {
        config.s3.enabled = val.to_lowercase() == "true";
    }

    if let Ok(val) = std::env::var("ENABLE_RECALL_TESTS") {
        config.recall.enabled = val.to_lowercase() == "true";
    }

    config
}

/// Default test configuration when file is missing or invalid
fn default_test_config() -> TestConfig {
    TestConfig {
        database: DatabaseTestConfig {
            enabled: false,
            url: "postgresql://recall:recall_password@localhost:5432/recall_competitions"
                .to_string(),
        },
        sqlite: SqliteTestConfig {
            enabled: true,
            path: "".to_string(),
        },
        s3: S3TestConfig {
            enabled: false,
            endpoint: "http://localhost:9000".to_string(),
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            access_key_id: "minioadmin".to_string(),
            secret_access_key: "minioadmin".to_string(),
        },
        recall: RecallTestConfig {
            enabled: false,
            endpoint: "http://localhost:8080".to_string(),
        },
    }
}
