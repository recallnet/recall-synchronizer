use anyhow::{Context, Result};
use sqlx::types::chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tokio::time;
use tracing::{debug, error, info, instrument, warn};

use crate::config::Config;
use crate::db::DbConnector;
use crate::recall::RecallConnector;
use crate::s3::S3Connector;

pub mod job;
pub mod state;

use job::{JobError, SyncJob};
use state::SyncState;

pub struct Synchronizer {
    db: Arc<DbConnector>,
    s3: Arc<S3Connector>,
    recall: Arc<RecallConnector>,
    state: Arc<SyncState>,
    config: Config,
}

impl Synchronizer {
    pub async fn new(config: Config, reset: bool) -> Result<Self> {
        let db = Arc::new(
            DbConnector::new(&config.database)
                .await
                .context("Failed to initialize database connector")?,
        );

        let s3 = Arc::new(
            S3Connector::new(&config.s3)
                .await
                .context("Failed to initialize S3 connector")?,
        );

        let recall = Arc::new(
            RecallConnector::new(&config.recall)
                .await
                .context("Failed to initialize Recall connector")?,
        );

        let state = Arc::new(
            SyncState::new(&config.sync.state_db_path, reset)
                .await
                .context("Failed to initialize state manager")?,
        );

        Ok(Self {
            db,
            s3,
            recall,
            state,
            config,
        })
    }

    #[instrument(skip(self))]
    pub async fn run(
        &self,
        competition_id: Option<String>,
        since_str: Option<String>,
    ) -> Result<()> {
        // Parse since string if provided
        let mut since = if let Some(s) = since_str {
            Some(
                DateTime::parse_from_rfc3339(&s)
                    .context("Failed to parse since timestamp")?
                    .with_timezone(&Utc),
            )
        } else {
            // Get last sync time from state
            self.state.get_last_sync_time().await?
        };

        info!(
            "Starting synchronization{}{}",
            since.map_or("".to_string(), |t| format!(" since {}", t)),
            competition_id
                .as_ref()
                .map_or("".to_string(), |id| format!(" for competition {}", id))
        );

        // Print stats
        let (synced_count, last_sync) = self.state.get_stats().await?;
        info!(
            "Current state: {} objects synced, last sync: {}",
            synced_count,
            last_sync.map_or("never".to_string(), |t| t.to_rfc3339())
        );

        let mut interval = time::interval(Duration::from_secs(self.config.sync.interval_seconds));

        // Main sync loop
        loop {
            interval.tick().await;
            debug!("Checking for updates...");

            // Get updated objects
            let objects = self
                .db
                .get_updated_objects(
                    since,
                    competition_id.clone(),
                    self.config.sync.batch_size as u32,
                )
                .await?;

            if objects.is_empty() {
                debug!("No new objects to sync");
                continue;
            }

            let objects_len = objects.len();
            info!("Processing {} objects", objects_len);

            // Process objects in parallel with worker pool
            let (tx, mut rx) = mpsc::channel(objects_len);
            let semaphore = Arc::new(Semaphore::new(self.config.sync.workers));

            for object in objects {
                let tx = tx.clone();
                let db = self.db.clone();
                let s3 = self.s3.clone();
                let recall = self.recall.clone();
                let state = self.state.clone();
                let sem = semaphore.clone();
                let retry_limit = self.config.sync.retry_limit;
                let retry_delay = Duration::from_secs(self.config.sync.retry_delay_seconds);

                tokio::spawn(async move {
                    let _permit = sem.acquire().await.unwrap();
                    let mut job = SyncJob::new(object.clone());

                    // Retry logic
                    let mut result = job.execute(&db, &s3, &recall, &state).await;

                    let mut attempts = 1;
                    while let Err(e) = &result {
                        if attempts >= retry_limit {
                            warn!(
                                "[Job {}] Giving up after {} attempts: {}",
                                job.id, attempts, e
                            );
                            break;
                        }

                        warn!(
                            "[Job {}] Attempt {} failed: {}. Retrying in {} seconds...",
                            job.id,
                            attempts,
                            e,
                            retry_delay.as_secs()
                        );

                        // Wait before retrying
                        time::sleep(retry_delay).await;

                        // Retry
                        attempts += 1;
                        result = job.execute(&db, &s3, &recall, &state).await;
                    }

                    let _ = tx
                        .send((object.key.clone(), object.updated_at, result))
                        .await;
                });
            }

            drop(tx); // Drop sender to close channel when all workers are done

            // Wait for all workers to complete
            let mut success_count = 0;
            let mut latest_timestamp: Option<DateTime<Utc>> = None;

            while let Some((key, timestamp, result)) = rx.recv().await {
                match result {
                    Ok(()) => {
                        success_count += 1;
                        // Keep track of the latest timestamp
                        if let Some(current) = latest_timestamp {
                            if timestamp > current {
                                latest_timestamp = Some(timestamp);
                            }
                        } else {
                            latest_timestamp = Some(timestamp);
                        }
                        debug!("Successfully synced: {}", key);
                    }
                    Err(e) => {
                        error!("Failed to sync object {}: {}", key, e);
                    }
                }
            }

            info!(
                "Completed batch: {} of {} objects synced successfully",
                success_count, objects_len
            );

            // Update the last sync time
            if let Some(timestamp) = latest_timestamp {
                self.state.update_last_sync_time(timestamp).await?;
                since = Some(timestamp);
            }

            // Report any failed jobs
            let failed_jobs = self.state.get_failed_jobs(5).await?;
            if !failed_jobs.is_empty() {
                warn!("Top failed jobs: {:?}", failed_jobs);
            }
        }
    }
}
