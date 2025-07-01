use anyhow::Result;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use crate::config::SyncConfig;
use crate::db::{Database, ObjectIndex};
use crate::recall::Storage as RecallStorage;
use crate::s3::Storage as S3Storage;
use crate::sync::storage::{SyncRecord, SyncStatus, SyncStorage};

/// Synchronizer that works with ObjectIndex database
#[derive(Clone)]
pub struct Synchronizer<D, S, ST, RS>
where
    D: Database,
    S: SyncStorage,
    ST: S3Storage,
    RS: RecallStorage,
{
    database: Arc<D>,
    sync_storage: Arc<S>,
    s3_storage: Option<Arc<ST>>,
    recall_storage: Arc<RS>,
    config: SyncConfig,
}

impl<D, S, ST, RS> Synchronizer<D, S, ST, RS>
where
    D: Database,
    S: SyncStorage,
    ST: S3Storage,
    RS: RecallStorage,
{
    /// Creates a new synchronizer with S3 storage
    pub fn with_s3(
        database: D,
        sync_storage: S,
        s3_storage: ST,
        recall_storage: RS,
        sync_config: SyncConfig,
    ) -> Self {
        Synchronizer {
            database: Arc::new(database),
            sync_storage: Arc::new(sync_storage),
            s3_storage: Some(Arc::new(s3_storage)),
            recall_storage: Arc::new(recall_storage),
            config: sync_config,
        }
    }

    /// Creates a new synchronizer without S3 storage (direct mode)
    pub fn without_s3(
        database: D,
        sync_storage: S,
        recall_storage: RS,
        sync_config: SyncConfig,
    ) -> Self {
        Synchronizer {
            database: Arc::new(database),
            sync_storage: Arc::new(sync_storage),
            s3_storage: None,
            recall_storage: Arc::new(recall_storage),
            config: sync_config,
        }
    }

    /// Test-specific constructor that accepts Arc-wrapped storage implementations
    #[cfg(test)]
    pub fn with_storage(
        database: Arc<D>,
        sync_storage: Arc<S>,
        s3_storage: Option<Arc<ST>>,
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
    ) -> Result<(Option<DateTime<Utc>>, Option<uuid::Uuid>)> {
        if let Some(ts) = since {
            // If user provides a specific timestamp, start from there with no ID filter
            Ok((Some(ts), None))
        } else {
            // Get the global last synced ID
            if let Some(last_id) = self.sync_storage.get_last_synced_object_id().await? {
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
    ) -> Result<Vec<ObjectIndex>> {
        let fetch_size = limit.unwrap_or(self.config.batch_size as u32);

        let objects = self
            .database
            .get_objects(fetch_size, since_time, after_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch objects to sync: {e}"))?;

        Ok(objects)
    }

    /// Checks if an object should be processed based on its current status
    async fn should_process_object(&self, object: &ObjectIndex) -> Result<bool> {
        let object_id = object.id;
        match self.sync_storage.get_object(object_id).await? {
            Some(record) => match record.status {
                SyncStatus::Complete => {
                    debug!("Object {object_id} already synchronized, skipping");
                    Ok(false)
                }
                SyncStatus::Processing => {
                    debug!("Object {object_id} is already being processed, skipping");
                    Ok(false)
                }
                _ => Ok(true),
            },
            None => Ok(true),
        }
    }

    /// Constructs the Recall key in the required format
    /// Format: [<competition_id>/][<agent_id>/]<data_type>/<uuid>
    /// Competition ID and Agent ID are omitted if not present
    fn construct_recall_key(object: &ObjectIndex) -> String {
        let mut parts = Vec::new();

        if let Some(competition_id) = &object.competition_id {
            parts.push(competition_id.to_string());
        }

        if let Some(agent_id) = &object.agent_id {
            parts.push(agent_id.to_string());
        }

        parts.push(object.data_type.to_string());
        parts.push(object.id.to_string());

        parts.join("/")
    }

    /// Synchronizes a single object to Recall
    async fn sync_object(&self, object: &ObjectIndex) -> Result<()> {
        let object_id = object.id;
        let competition_id = object.competition_id;
        let agent_id = object.agent_id;
        let data_type = object.data_type;
        let created_at = object.created_at;

        // Create sync record with detailed information
        let sync_record =
            SyncRecord::new(object_id, competition_id, agent_id, data_type, created_at);

        self.sync_storage.add_object(sync_record).await?;
        self.sync_storage
            .set_object_status(object_id, SyncStatus::Processing)
            .await?;

        // Get data based on storage type
        let data_result = if let Some(data) = &object.data {
            // Object has embedded data
            Ok(data.clone())
        } else if let Some(s3_key) = &object.object_key {
            // Object uses S3 storage
            match &self.s3_storage {
                Some(s3) => s3
                    .get_object(s3_key)
                    .await
                    .map(|bytes| bytes.to_vec())
                    .map_err(|e| anyhow::anyhow!("Failed to get object from S3: {e}")),
                None => Err(anyhow::anyhow!("S3 storage required but not configured")),
            }
        } else {
            Err(anyhow::anyhow!(
                "Object has neither embedded data nor S3 key"
            ))
        };

        match data_result {
            Ok(data) => {
                // Use the recall key for storing in Recall
                let recall_key = Self::construct_recall_key(object);
                match self.recall_storage.add_blob(&recall_key, data).await {
                    Ok(()) => {
                        info!("Successfully synchronized to Recall as {recall_key}");
                        self.sync_storage
                            .set_object_status(object_id, SyncStatus::Complete)
                            .await?;
                    }
                    Err(e) => {
                        error!("Failed to submit {recall_key} to Recall: {e}");
                        // TODO: Implement retry logic
                    }
                }
            }
            Err(e) => {
                error!("Failed to get data: {e}");
                // TODO: Implement retry logic
            }
        }

        Ok(())
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

                let object_id = object.id;
                if let Some(record) = self.sync_storage.get_object(object_id).await? {
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
    pub async fn run(&self, since: Option<DateTime<Utc>>) -> Result<()> {
        info!("Starting synchronization");

        let (mut current_since_time, mut current_after_id) = self.get_sync_state(since).await?;

        if let Some(ts) = current_since_time {
            info!("Synchronizing data since: {ts}");
        } else {
            info!("No previous sync timestamp found, syncing all objects");
        }

        let batch_size = self.config.batch_size;
        let mut total_processed = 0;

        loop {
            // Calculate how many more objects we can process
            let remaining_quota = batch_size.saturating_sub(total_processed);
            if remaining_quota == 0 {
                debug!("Batch quota filled. Total processed: {total_processed}");
                break;
            }

            // Fetch objects, but limit to our remaining quota
            let fetch_limit = std::cmp::min(batch_size as u32, remaining_quota as u32);
            let objects = self
                .fetch_objects_to_sync(current_since_time, current_after_id, Some(fetch_limit))
                .await?;

            if objects.is_empty() {
                if total_processed == 0 {
                    info!("No objects found to synchronize");
                } else {
                    info!("Synchronization completed. Processed {total_processed} objects");
                }
                return Ok(());
            }

            info!("Found {} objects to synchronize", objects.len());

            let (last_synced_object, batch_processed) = self.process_object_batch(&objects).await?;
            total_processed += batch_processed;

            if let Some(last_object) = last_synced_object {
                let object_id = last_object.id;
                self.sync_storage
                    .set_last_synced_object_id(object_id)
                    .await?;
            }

            if total_processed >= batch_size {
                break;
            }

            // Update state for next iteration if there might be more objects
            if let Some(last_obj) = objects.last() {
                current_since_time = Some(last_obj.created_at);
                current_after_id = Some(last_obj.id);

                debug!(
                    "Total processed: {total_processed}, continuing to fill batch of {batch_size}"
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
    pub async fn start(&self, interval_seconds: u64, since: Option<DateTime<Utc>>) -> Result<()> {
        let mut interval_timer = interval(Duration::from_secs(interval_seconds));

        info!("Synchronizer started with {interval_seconds}s interval");

        loop {
            interval_timer.tick().await;

            info!("Starting synchronization run");
            match self.run(since).await {
                Ok(()) => {
                    debug!("Synchronization run completed successfully");
                }
                Err(e) => {
                    warn!("Synchronization run failed: {e}");
                    // Continue to next interval even if this run failed
                }
            }
        }
    }
}
