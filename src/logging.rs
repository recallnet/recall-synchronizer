use std::path::Path;
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
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

/// Initialize logging to both console and file
/// Returns a LogGuard that must be kept alive for the duration of the program
pub fn init_logging(log_file: &str, verbose: bool) -> Result<LogGuard, anyhow::Error> {
    if let Some(parent) = Path::new(log_file).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    if let Err(e) = std::fs::File::create(log_file) {
        // If we can't create/truncate the file, log to stderr but continue
        eprintln!("Warning: Could not truncate log file {}: {}", log_file, e);
    }

    let level = if verbose { Level::DEBUG } else { Level::INFO };

    let dir = Path::new(log_file)
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let file_name = Path::new(log_file)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("logs.log");

    let file_appender = tracing_appender::rolling::never(dir, file_name);
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
