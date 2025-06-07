// src/main.rs
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
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
    #[arg(
        short,
        long,
        value_name = "FILE",
        default_value = "config.toml",
        global = true
    )]
    config: String,

    /// Show verbose output
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the synchronizer
    Run {
        /// Filter by competition ID
        #[arg(long)]
        competition_id: Option<String>,

        /// Synchronize only data updated since this timestamp (RFC3339 format)
        #[arg(long)]
        since: Option<String>,
    },
    /// Reset synchronization state
    Reset,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let log_level = if cli.verbose {
        Level::DEBUG
    } else {
        Level::INFO
    };
    tracing_subscriber::fmt().with_max_level(log_level).init();

    info!("Recall Data Synchronizer v{}", env!("CARGO_PKG_VERSION"));
    info!("Loading configuration from: {}", cli.config);

    let config = match config::load_config(&cli.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            process::exit(1);
        }
    };

    match cli.command {
        Commands::Run {
            competition_id,
            since,
        } => run_synchronizer(config, competition_id, since).await,
        Commands::Reset => reset_synchronizer(config).await,
    }
}

/// Run the synchronizer with real database and storage implementations
async fn run_synchronizer(
    config: config::Config,
    competition_id: Option<String>,
    since: Option<String>,
) -> Result<()> {
    let since_time = if let Some(ts) = since {
        Some(
            DateTime::parse_from_rfc3339(&ts)
                .context(format!("Failed to parse timestamp: {}", ts))?
                .with_timezone(&Utc),
        )
    } else {
        None
    };

    let synchronizer = initialize_synchronizer(config).await?;

    if let Err(e) = synchronizer.run(competition_id, since_time).await {
        error!("Synchronizer failed: {}", e);
        process::exit(1);
    }

    Ok(())
}

/// Reset the synchronizer state
async fn reset_synchronizer(config: config::Config) -> Result<()> {
    let synchronizer = initialize_synchronizer(config).await?;

    info!("Resetting synchronization state...");

    synchronizer.reset().await?;

    info!("Synchronization state has been reset successfully");

    Ok(())
}

async fn initialize_synchronizer(
    config: config::Config,
) -> Result<
    Synchronizer<PostgresDatabase, SqliteSyncStorage, s3::S3Storage, RecallBlockchain>,
    anyhow::Error,
> {
    let database = PostgresDatabase::new(&config.database.url).await?;
    let sync_storage = SqliteSyncStorage::new(&config.sync_storage.db_path)?;
    let s3_storage = crate::s3::S3Storage::new(&config.s3).await?;
    let recall_storage = RecallBlockchain::new(&config.recall).await?;
    let synchronizer = Synchronizer::new(
        database,
        sync_storage,
        s3_storage,
        recall_storage,
        config.sync,
    );

    info!("Synchronizer initialized successfully");

    Ok(synchronizer)
}
