// src/main.rs
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use std::process;
use tracing::{error, info, Level};

mod config;
mod db;
mod recall;
mod s3;
mod sync;
#[cfg(test)]
mod test_utils;

use crate::db::postgres::PostgresDatabase;
use crate::recall::RecallBlockchain;
use crate::sync::storage::SqliteSyncStorage;
use crate::sync::synchronizer::Synchronizer;

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

    // Use real implementations
    info!("Using real database and storage implementations");
    run(config, cli.reset, cli.competition_id, cli.since).await
}

/// Run the synchronizer with real database and storage implementations
async fn run(
    config: config::Config,
    reset: bool,
    competition_id: Option<String>,
    since: Option<String>,
) -> Result<()> {
    // Parse the since parameter if provided
    let since_time = if let Some(ts) = since {
        Some(
            DateTime::parse_from_rfc3339(&ts)
                .context(format!("Failed to parse timestamp: {}", ts))?
                .with_timezone(&Utc),
        )
    } else {
        None
    };

    // Create specific implementations
    let database = PostgresDatabase::new(&config.database.url).await?;
    let sync_storage = SqliteSyncStorage::new(&config.sync.state_db_path)?;
    let s3_storage = crate::s3::S3Storage::new(&config.s3).await?;
    let recall_storage = RecallBlockchain::new(&config.recall).await?;

    // Create the synchronizer with specific implementations
    let synchronizer = Synchronizer::new(
        database,
        sync_storage,
        s3_storage,
        recall_storage,
        config,
    );

    info!("Synchronizer initialized successfully");

    // If reset flag is set, reset the synchronizer state
    if reset {
        info!("Reset flag is set, clearing synchronization state");
        synchronizer.reset().await?;
    }

    // Run the synchronizer
    if let Err(e) = synchronizer.run(competition_id, since_time).await {
        error!("Synchronizer failed: {}", e);
        process::exit(1);
    }

    Ok(())
}
