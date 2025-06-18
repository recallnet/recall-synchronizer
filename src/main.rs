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
use crate::db::Database;
use crate::recall::RecallBlockchain;
use crate::s3::Storage as S3Storage;
use crate::sync::storage::{SqliteSyncStorage, SyncStorage};
use crate::sync::synchronizer::Synchronizer;
use async_trait::async_trait;

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

/// Trait for synchronizer operations to enable dynamic dispatch
#[async_trait]
trait SynchronizerOps: Send + Sync {
    async fn run(&self, since: Option<DateTime<Utc>>) -> Result<()>;

    async fn start(&self, interval: u64, since: Option<DateTime<Utc>>) -> Result<()>;

    async fn reset(&self) -> Result<()>;
}

/// Implement SynchronizerOps for any Synchronizer type
#[async_trait]
impl<D, S, ST, RS> SynchronizerOps for Synchronizer<D, S, ST, RS>
where
    D: Database,
    S: SyncStorage,
    ST: S3Storage,
    RS: recall::Storage,
{
    async fn run(&self, since: Option<DateTime<Utc>>) -> Result<()> {
        Synchronizer::run(self, since).await
    }

    async fn start(
        &self,
        interval: u64,
        since: Option<DateTime<Utc>>,
    ) -> Result<()> {
        Synchronizer::start(self, interval, since).await
    }

    async fn reset(&self) -> Result<()> {
        Synchronizer::reset(self).await
    }
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the synchronizer once
    Run {
        /// Synchronize only data updated since this timestamp (RFC3339 format)
        #[arg(long)]
        since: Option<String>,
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
        Commands::Run { since } => run_synchronizer(config, since).await,
        Commands::Start { interval, since } => start_synchronizer(config, interval, since).await,
        Commands::Reset => reset_synchronizer(config).await,
    }
}

/// Run the synchronizer with real database and storage implementations
async fn run_synchronizer(config: config::Config, since: Option<String>) -> Result<()> {
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

    if let Err(e) = synchronizer.run(since_time).await {
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

/// Start the synchronizer to run continuously at specified interval
async fn start_synchronizer(
    config: config::Config,
    interval: u64,
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

    info!(
        "Starting synchronizer with interval of {} seconds",
        interval
    );

    if let Err(e) = synchronizer.start(interval, since_time).await {
        error!("Synchronizer failed: {}", e);
        process::exit(1);
    }

    Ok(())
}

async fn initialize_synchronizer(config: config::Config) -> Result<Box<dyn SynchronizerOps>> {
    let sync_storage = SqliteSyncStorage::new(&config.sync_storage.db_path)?;
    let recall_storage = RecallBlockchain::new(&config.recall).await?;

    let synchronizer: Box<dyn SynchronizerOps> = if let Some(s3_config) = config.s3 {
        // S3 mode - use generic synchronizer with S3
        info!("Initializing S3-based synchronizer");
        let database =
            PostgresDatabase::new(&config.database.url, crate::db::pg_schema::SchemaMode::S3)
                .await?;
        let s3_storage = crate::s3::S3Storage::new(&s3_config).await?;
        let sync = Synchronizer::with_s3(
            database,
            sync_storage,
            s3_storage,
            recall_storage,
            config.sync,
        );
        Box::new(sync)
    } else {
        // Direct mode - use generic synchronizer without S3
        info!("Initializing direct database synchronizer (no S3)");
        let database = PostgresDatabase::new(
            &config.database.url,
            crate::db::pg_schema::SchemaMode::Direct,
        )
        .await?;
        let sync = Synchronizer::<_, _, crate::s3::S3Storage, _>::without_s3(
            database,
            sync_storage,
            recall_storage,
            config.sync,
        );
        Box::new(sync)
    };

    info!("Synchronizer initialized successfully");
    Ok(synchronizer)
}
