use anyhow::Result;
use clap::Parser;

mod config;
mod db;
mod recall;
mod s3;
mod sync;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long, value_name = "FILE")]
    config: Option<String>,

    #[arg(long)]
    reset: bool,

    #[arg(long)]
    competition_id: Option<String>,

    #[arg(long)]
    since: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let cli = Cli::parse();

    // Load configuration
    let config_path = cli.config.unwrap_or_else(|| "config.toml".to_string());
    let config = config::load_config(&config_path)?;

    // Initialize synchronizer
    let synchronizer = sync::Synchronizer::new(config, cli.reset).await?;

    // Run the synchronizer
    synchronizer.run(cli.competition_id, cli.since).await?;

    Ok(())
}
