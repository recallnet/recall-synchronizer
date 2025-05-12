// src/main.rs
use anyhow::{Context, Result};
use clap::Parser;
use std::process;
use std::sync::Arc;
use tracing::{error, info, Level};

mod config;
mod db;
mod recall;
mod s3;
mod sync;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to configuration file
    #[arg(short, long, value_name = "FILE", default_value = "config.toml")]
    config: String,

    /// Reset synchronization state
    #[arg(long)]
    reset: bool,

    /// Filter by competition ID
    #[arg(long)]
    competition_id: Option<String>,

    /// Synchronize only data updated since this timestamp (RFC3339 format)
    #[arg(long)]
    since: Option<String>,

    /// Use fake in-memory implementations (for testing)
    #[arg(long)]
    fake: bool,

    /// Show verbose output
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let cli = Cli::parse();

    // Initialize logging
    let log_level = if cli.verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };
    tracing_subscriber::fmt().with_max_level(log_level).init();

    // Display startup banner
    info!("Recall Data Synchronizer v{}", env!("CARGO_PKG_VERSION"));
    info!("Loading configuration from: {}", cli.config);

    // Load configuration
    let config = match config::load_config(&cli.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            process::exit(1);
        }
    };

    if cli.fake {
        // Use fake implementations for testing
        info!("Using fake in-memory implementations for testing");
        run_with_fake_implementations(config, cli.reset, cli.competition_id, cli.since).await
    } else {
        // Use real implementations
        info!("Using real database and storage implementations");
        run_with_real_implementations(config, cli.reset, cli.competition_id, cli.since).await
    }
}

/// Run the synchronizer with real database and storage implementations
async fn run_with_real_implementations(
    config: config::Config,
    reset: bool,
    competition_id: Option<String>,
    since: Option<String>,
) -> Result<()> {
    // Initialize and run synchronizer
    match sync::Synchronizer::new(config, reset).await {
        Ok(synchronizer) => {
            info!("Synchronizer initialized successfully");

            if let Err(e) = synchronizer.run(competition_id, since).await {
                error!("Synchronizer failed: {}", e);
                process::exit(1);
            }
        }
        Err(e) => {
            error!("Failed to initialize synchronizer: {}", e);
            process::exit(1);
        }
    }

    Ok(())
}

/// Run the synchronizer with fake in-memory implementations
async fn run_with_fake_implementations(
    config: config::Config,
    reset: bool,
    competition_id: Option<String>,
    since: Option<String>,
) -> Result<()> {
    // Create fake database
    let database = db::FakeDatabase::new();

    // Add some test data
    use chrono::Utc;
    use uuid::Uuid;

    let test_object = db::ObjectIndex {
        id: Uuid::new_v4(),
        object_key: "test-object-1".to_string(),
        bucket_name: "test-bucket".to_string(),
        competition_id: Some(Uuid::new_v4()),
        agent_id: Some(Uuid::new_v4()),
        data_type: "TEST_DATA".to_string(),
        size_bytes: Some(1024),
        content_hash: Some("abcdef1234567890".to_string()),
        metadata: None,
        event_timestamp: Some(Utc::now()),
        object_last_modified_at: Utc::now(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    database.fake_add_object(test_object);

    // Create fake sync storage
    let sync_storage = sync::storage::FakeSyncStorage::new();

    // Create S3 connector
    let s3_connector = s3::S3Connector::new(&config.s3)
        .await
        .context("Failed to initialize S3 connector")?;

    // Create Recall connector
    let recall_connector = recall::RecallConnector::new(&config.recall)
        .await
        .context("Failed to initialize Recall connector")?;

    // Create synchronizer with fake implementations
    let synchronizer = sync::Synchronizer::with_storage(
        database,
        sync_storage,
        s3_connector,
        recall_connector,
        config,
        reset,
    );

    info!("Synchronizer with fake implementations initialized successfully");

    // Run the synchronizer
    if let Err(e) = synchronizer.run(competition_id, since).await {
        error!("Synchronizer failed: {}", e);
        process::exit(1);
    }

    Ok(())
}