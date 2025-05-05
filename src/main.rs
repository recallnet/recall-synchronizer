// src/main.rs
use anyhow::{Context, Result};
use clap::Parser;
use std::process;
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

    // Initialize and run synchronizer
    match sync::Synchronizer::new(config, cli.reset).await {
        Ok(synchronizer) => {
            info!("Synchronizer initialized successfully");

            if let Some(comp_id) = &cli.competition_id {
                info!("Filtering by competition ID: {}", comp_id);
            }

            if let Some(since) = &cli.since {
                info!("Synchronizing data since: {}", since);
            }

            if let Err(e) = synchronizer.run(cli.competition_id, cli.since).await {
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
