use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use crate::config::SyncConfig;
use crate::db::{Database, ObjectIndex};
use crate::recall::Storage as RecallStorage;
use crate::s3::Storage as S3Storage;
use crate::sync::storage::{SyncRecord, SyncStatus, SyncStorage};

/// Main synchronizer that orchestrates the data synchronization process
#[derive(Clone)]
pub struct Synchronizer<D: Database, S: SyncStorage, ST: S3Storage, RS: RecallStorage> {
    database: Arc<D>,
    sync_storage: Arc<S>,
    s3_storage: Arc<ST>,
    recall_storage: Arc<RS>,
    config: SyncConfig,
}

impl<D: Database, S: SyncStorage, ST: S3Storage, RS: RecallStorage> Synchronizer<D, S, ST, RS> {
    /// Creates a new Synchronizer instance
    pub fn new(
        database: D,
        sync_storage: S,
        s3_storage: ST,
        recall_storage: RS,
        sync_config: SyncConfig,
    ) -> Self {
        Synchronizer {
            database: Arc::new(database),
            sync_storage: Arc::new(sync_storage),
            s3_storage: Arc::new(s3_storage),
            recall_storage: Arc::new(recall_storage),
            config: sync_config,
        }
    }

    /// Test-specific constructor that accepts Arc-wrapped storage implementations
    #[cfg(test)]
    pub fn with_storage(
        database: Arc<D>,
        sync_storage: Arc<S>,
        s3_storage: Arc<ST>,
        recall_storage: Arc<RS>,
        sync_config: SyncConfig,
    ) -> Self {
        Synchronizer {
            database,
            sync_storage,
            s3_storage,
            recall_storage,
            config: sync_config,
        }
    }

    /// Gets the timestamp and ID to sync from
    async fn get_sync_state(
        &self,
        since: Option<DateTime<Utc>>,
        competition_id: Option<uuid::Uuid>,
    ) -> Result<(Option<DateTime<Utc>>, Option<uuid::Uuid>)> {
        if let Some(ts) = since {
            // If user provides a specific timestamp, start from there with no ID filter
            Ok((Some(ts), None))
        } else {
            // Use the appropriate last synced ID based on whether we have a competition filter
            if let Some(last_id) = self
                .sync_storage
                .get_last_synced_object_id(competition_id)
                .await?
            {
                if let Some(last_record) = self.sync_storage.get_object(last_id).await? {
                    Ok((Some(last_record.timestamp), Some(last_id)))
                } else {
                    Ok((None, None))
                }
            } else {
                Ok((None, None))
            }
        }
    }

    /// Fetches objects from the database that need to be synchronized
    async fn fetch_objects_to_sync(
        &self,
        since_time: Option<DateTime<Utc>>,
        after_id: Option<uuid::Uuid>,
        limit: Option<u32>,
        competition_id: Option<uuid::Uuid>,
    ) -> Result<Vec<ObjectIndex>> {
        let batch_size = limit.unwrap_or(self.config.batch_size as u32);
        self.database
            .get_objects(batch_size, since_time, after_id, competition_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch objects to sync: {}", e))
    }

    /// Checks if an object should be processed based on its current status
    async fn should_process_object(&self, object: &ObjectIndex) -> Result<bool> {
        match self.sync_storage.get_object(object.id).await? {
            Some(record) => match record.status {
                SyncStatus::Complete => {
                    debug!(
                        "Object {} already synchronized, skipping",
                        object.object_key
                    );
                    Ok(false)
                }
                SyncStatus::Processing => {
                    debug!(
                        "Object {} is already being processed, skipping",
                        object.object_key
                    );
                    Ok(false)
                }
                _ => Ok(true),
            },
            None => Ok(true),
        }
    }

    /// Synchronizes a single object from S3 to Recall
    async fn sync_object(&self, object: &ObjectIndex) -> Result<()> {
        let sync_record = SyncRecord::new(
            object.id,
            object.object_key.clone(),
            object.bucket_name.clone(),
            object.object_last_modified_at,
        );

        self.sync_storage.add_object(sync_record).await?;

        self.sync_storage
            .set_object_status(object.id, SyncStatus::Processing)
            .await?;

        match self.s3_storage.get_object(&object.object_key).await {
            Ok(data) => {
                match self
                    .recall_storage
                    .add_blob(&object.object_key, data.to_vec())
                    .await
                {
                    Ok(()) => {
                        info!("Successfully synchronized {} to Recall", object.object_key);

                        self.sync_storage
                            .set_object_status(object.id, SyncStatus::Complete)
                            .await?;
                    }
                    Err(e) => {
                        error!("Failed to submit {} to Recall: {}", object.object_key, e);
                        // TODO: Implement retry logic
                    }
                }
            }
            Err(e) => {
                error!("Failed to get {} from S3: {}", object.object_key, e);
                // TODO: Implement retry logic
            }
        }

        Ok(())
    }

    /// Parse competition ID from string to UUID
    fn parse_competition_id(competition_id: &Option<String>) -> Result<Option<uuid::Uuid>> {
        match competition_id {
            Some(id) => {
                info!("Filtering by competition ID: {}", id);
                Ok(Some(uuid::Uuid::parse_str(id).context(format!(
                    "Invalid competition ID format: {}",
                    id
                ))?))
            }
            None => Ok(None),
        }
    }

    /// Process a batch of objects and return the last synced object and count
    async fn process_object_batch<'a>(
        &self,
        objects: &'a [ObjectIndex],
    ) -> Result<(Option<&'a ObjectIndex>, usize)> {
        let mut last_synced_object: Option<&'a ObjectIndex> = None;
        let mut batch_processed = 0;

        for object in objects {
            if self.should_process_object(object).await? {
                self.sync_object(object).await?;

                if let Some(record) = self.sync_storage.get_object(object.id).await? {
                    if record.status == SyncStatus::Complete {
                        last_synced_object = Some(object);
                        batch_processed += 1;
                    }
                }
            }
        }

        Ok((last_synced_object, batch_processed))
    }

    /// Runs the synchronization process
    pub async fn run(
        &self,
        competition_id: Option<String>,
        since: Option<DateTime<Utc>>,
    ) -> Result<()> {
        info!("Starting synchronization");

        let competition_uuid = Self::parse_competition_id(&competition_id)?;

        let (mut current_since_time, mut current_after_id) =
            self.get_sync_state(since, competition_uuid).await?;

        if let Some(ts) = current_since_time {
            info!("Synchronizing data since: {}", ts);
        } else {
            info!("No previous sync timestamp found, syncing all objects");
        }

        let batch_size = self.config.batch_size;
        let mut total_processed = 0;

        loop {
            // Calculate how many more objects we can process
            let remaining_quota = batch_size.saturating_sub(total_processed);
            if remaining_quota == 0 {
                debug!("Batch quota filled. Total processed: {}", total_processed);
                break;
            }

            // Fetch objects, but limit to our remaining quota
            let fetch_limit = std::cmp::min(batch_size as u32, remaining_quota as u32);
            let objects = self
                .fetch_objects_to_sync(
                    current_since_time,
                    current_after_id,
                    Some(fetch_limit),
                    competition_uuid,
                )
                .await?;

            if objects.is_empty() {
                if total_processed == 0 {
                    info!("No objects found to synchronize");
                } else {
                    info!(
                        "Synchronization completed. Processed {} objects",
                        total_processed
                    );
                }
                return Ok(());
            }

            info!("Found {} objects to synchronize", objects.len());

            // Process the batch of objects
            let (last_synced_object, batch_processed) = self.process_object_batch(&objects).await?;
            total_processed += batch_processed;

            if let Some(last_object) = last_synced_object {
                self.sync_storage
                    .set_last_synced_object_id(last_object.id, competition_uuid)
                    .await?;
            }

            // Check if we should continue to the next batch
            if total_processed >= batch_size {
                break;
            }

            // Update state for next iteration if there might be more objects
            if let Some(last_obj) = objects.last() {
                current_since_time = Some(last_obj.object_last_modified_at);
                current_after_id = Some(last_obj.id);

                debug!(
                    "Total processed: {}, continuing to fill batch of {}",
                    total_processed, batch_size
                );
            } else {
                break;
            }
        }

        info!("Synchronization completed");
        Ok(())
    }

    /// Resets the synchronization state by clearing all sync records
    pub async fn reset(&self) -> Result<()> {
        info!("Resetting synchronization state");
        self.sync_storage.clear_all().await?;
        info!("Synchronization state has been reset");
        Ok(())
    }

    /// Starts the synchronizer to run continuously at the specified interval
    pub async fn start(
        &self,
        interval_seconds: u64,
        competition_id: Option<String>,
        since: Option<DateTime<Utc>>,
    ) -> Result<()> {
        let mut interval_timer = interval(Duration::from_secs(interval_seconds));

        info!("Synchronizer started with {}s interval", interval_seconds);

        loop {
            interval_timer.tick().await;

            info!("Starting synchronization run");
            match self.run(competition_id.clone(), since).await {
                Ok(()) => {
                    debug!("Synchronization run completed successfully");
                }
                Err(e) => {
                    warn!("Synchronization run failed: {}", e);
                    // Continue to next interval even if this run failed
                }
            }
        }
    }
}
