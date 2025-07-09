// src/main.rs
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use std::process;
use tracing::{error, info};

mod config;
mod db;
mod logging;
mod recall;
mod s3;
mod sync;
#[cfg(test)]
mod test_utils;

use crate::db::postgres::PostgresDatabase;
use crate::recall::RecallBlockchain;
use crate::s3::S3Storage;
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

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the synchronizer once
    Run {
        /// Synchronize only data updated since this timestamp (RFC3339 format)
        #[arg(long)]
        since: Option<String>,
        /// Process only failed objects (ignore new objects)
        #[arg(long)]
        only_failed: bool,
    },
    /// Start the synchronizer to run continuously at specified interval
    Start {
        /// Interval in seconds between synchronization runs
        #[arg(short, long, value_name = "SECONDS")]
        interval: u64,

        /// Synchronize only data updated since this timestamp (RFC3339 format)
        #[arg(long)]
        since: Option<String>,
    },
    /// Reset synchronization state
    Reset,
    /// List failed objects
    ListFailed {
        /// Show only permanently failed objects
        #[arg(long)]
        permanent_only: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let config = match config::load_config(&cli.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("Failed to load configuration: {e}");
            process::exit(1);
        }
    };

    let _guard = logging::init_logging(config.logging.as_ref())?;

    match cli.command {
        Commands::Run { since, only_failed } => run_synchronizer(config, since, only_failed).await,
        Commands::Start { interval, since } => start_synchronizer(config, interval, since).await,
        Commands::Reset => reset_synchronizer(config).await,
        Commands::ListFailed { permanent_only } => list_failed_objects(config, permanent_only).await,
    }
}

/// Run the synchronizer with real database and storage implementations
async fn run_synchronizer(config: config::Config, since: Option<String>, only_failed: bool) -> Result<()> {
    let since_time = if let Some(ts) = since {
        Some(
            DateTime::parse_from_rfc3339(&ts)
                .context(format!("Failed to parse timestamp: {ts}"))?
                .with_timezone(&Utc),
        )
    } else {
        None
    };

    let synchronizer = initialize_synchronizer(config).await?;

    if let Err(e) = synchronizer.run(since_time, only_failed).await {
        error!("Synchronizer failed: {e}");
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

/// List failed objects from the sync storage
async fn list_failed_objects(config: config::Config, permanent_only: bool) -> Result<()> {
    let synchronizer = initialize_synchronizer(config).await?;
    
    let failed_objects = if permanent_only {
        synchronizer.get_permanently_failed_objects().await?
    } else {
        synchronizer.get_failed_objects().await?
    };
    
    if failed_objects.is_empty() {
        let status_type = if permanent_only { "permanently failed" } else { "failed" };
        info!("No {status_type} objects found");
        return Ok(());
    }
    
    let status_type = if permanent_only { "permanently failed" } else { "failed" };
    info!("Found {} {status_type} objects:", failed_objects.len());
    
    for record in failed_objects {
        let error_msg = record.last_error.as_deref().unwrap_or("No error message");
        let failure_type = record.failure_type.as_ref().map(|ft| format!("{:?}", ft)).unwrap_or("Unknown".to_string());
        
        println!("ID: {}", record.id);
        println!("  Status: {:?}", record.status);
        println!("  Data Type: {}", record.data_type);
        println!("  Retry Count: {}", record.retry_count);
        println!("  Failure Type: {}", failure_type);
        println!("  Last Error: {}", error_msg);
        println!("  First Attempt: {}", record.first_attempt_at.format("%Y-%m-%d %H:%M:%S UTC"));
        println!("  Last Attempt: {}", record.last_attempt_at.format("%Y-%m-%d %H:%M:%S UTC"));
        println!();
    }
    
    Ok(())
}

/// Start the synchronizer to run continuously at specified interval
async fn start_synchronizer(
    config: config::Config,
    interval: u64,
    since: Option<String>,
) -> Result<()> {
    let since_time = if let Some(ts) = since {
        Some(
            DateTime::parse_from_rfc3339(&ts)
                .context(format!("Failed to parse timestamp: {ts}"))?
                .with_timezone(&Utc),
        )
    } else {
        None
    };

    let synchronizer = initialize_synchronizer(config).await?;

    info!("Starting synchronizer with interval of {interval} seconds");

    if let Err(e) = synchronizer.start(interval, since_time).await {
        error!("Synchronizer failed: {e}");
        process::exit(1);
    }

    Ok(())
}

type SynchronizerInstance =
    Synchronizer<PostgresDatabase, SqliteSyncStorage, S3Storage, RecallBlockchain>;

async fn initialize_synchronizer(config: config::Config) -> Result<SynchronizerInstance> {
    let sync_storage = SqliteSyncStorage::new(&config.sync_storage.db_path)?;
    let recall_storage = RecallBlockchain::new(&config.recall).await?;

    let synchronizer = if let Some(s3_config) = config.s3 {
        // S3 mode - use generic synchronizer with S3
        info!("Initializing S3-based synchronizer");
        let database =
            PostgresDatabase::new(&config.database.url, crate::db::pg_schema::SchemaMode::S3)
                .await?;
        let s3_storage = crate::s3::S3Storage::new(&s3_config).await?;
        Synchronizer::with_s3(
            database,
            sync_storage,
            s3_storage,
            recall_storage,
            config.sync,
        )
    } else {
        // Direct mode - use generic synchronizer without S3
        info!("Initializing direct database synchronizer (no S3)");
        let database = PostgresDatabase::new(
            &config.database.url,
            crate::db::pg_schema::SchemaMode::Direct,
        )
        .await?;
        Synchronizer::without_s3(database, sync_storage, recall_storage, config.sync)
    };

    info!("Synchronizer initialized successfully");
    Ok(synchronizer)
}
