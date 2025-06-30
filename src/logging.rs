use std::path::Path;
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_rolling_file::{RollingConditionBase, RollingFileAppender};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, Layer};

/// Guard wrapper that ensures logs are flushed on drop
pub struct LogGuard(Option<WorkerGuard>);

impl Drop for LogGuard {
    fn drop(&mut self) {
        if let Some(guard) = self.0.take() {
            // Explicitly drop the guard to flush logs
            drop(guard);
            // Give a delay to ensure flush completes
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
    }
}

/// Initialize logging to console and optionally to file
/// Returns a LogGuard that must be kept alive for the duration of the program
pub fn init_logging(
    config: Option<&crate::config::LoggingConfig>,
) -> Result<LogGuard, anyhow::Error> {
    let level = if let Some(config) = config {
        match config.level.to_lowercase().as_str() {
            "trace" => Level::TRACE,
            "debug" => Level::DEBUG,
            "info" => Level::INFO,
            "warn" | "warning" => Level::WARN,
            "error" => Level::ERROR,
            _ => Level::INFO, // Default to INFO for invalid levels
        }
    } else {
        Level::INFO // Default level when no config
    };

    // If no config provided, only log to console
    if config.is_none() {
        tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .with_writer(std::io::stdout)
                    .with_ansi(true)
                    .with_filter(tracing_subscriber::filter::LevelFilter::from_level(level)),
            )
            .init();

        return Ok(LogGuard(None));
    }

    let config = config.unwrap();

    if let Some(parent) = Path::new(&config.path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let file_appender = RollingFileAppender::new(
        &config.path,
        RollingConditionBase::new().max_size(config.size * 1024 * 1024),
        config.max_files,
    )
    .map_err(|e| anyhow::anyhow!("Failed to create rolling file appender: {}", e))?;

    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(std::io::stdout)
                .with_ansi(true)
                .with_filter(tracing_subscriber::filter::LevelFilter::from_level(level)),
        )
        .with(
            fmt::layer()
                .with_writer(non_blocking)
                .with_ansi(false)
                .with_filter(tracing_subscriber::filter::LevelFilter::from_level(level)),
        )
        .init();

    Ok(LogGuard(Some(guard)))
}
