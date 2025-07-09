use anyhow::Result;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use crate::config::SyncConfig;
use crate::db::{Database, ObjectIndex};
use crate::recall::Storage as RecallStorage;
use crate::s3::Storage as S3Storage;
use crate::sync::storage::models::FailureType;
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

    /// Fetch objects from database with retry on failure
    async fn fetch_objects_with_retry(
        &self,
        since: Option<DateTime<Utc>>,
        limit: usize,
    ) -> Result<Vec<ObjectIndex>> {
        const MAX_DB_RETRIES: u32 = 3;
        const DB_RETRY_DELAY_MS: u64 = 500;

        for attempt in 1..=MAX_DB_RETRIES {
            match self
                .fetch_objects_to_sync(since, None, Some(limit as u32))
                .await
            {
                Ok(objects) => return Ok(objects),
                Err(e) => {
                    warn!(
                        "Database fetch attempt {}/{} failed: {}",
                        attempt, MAX_DB_RETRIES, e
                    );
                    if attempt < MAX_DB_RETRIES {
                        tokio::time::sleep(Duration::from_millis(
                            DB_RETRY_DELAY_MS * attempt as u64,
                        ))
                        .await;
                    } else {
                        error!("All database fetch attempts failed, checking for existing failed objects");
                        return Ok(Vec::new());
                    }
                }
            }
        }

        Ok(Vec::new())
    }

    /// Get object indices for failed records that need retry
    async fn get_object_indices_for_failed_records(&self) -> Result<Vec<ObjectIndex>> {
        let failed_records = self.sync_storage.get_failed_objects().await?;
        let object_indices = failed_records
            .into_iter()
            .map(|record| record.to_object_index())
            .collect();

        Ok(object_indices)
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
                SyncStatus::FailedPermanently => {
                    debug!("Object {object_id} has failed permanently, skipping");
                    Ok(false)
                }
                SyncStatus::PendingSync | SyncStatus::Failed => Ok(true),
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

        let sync_record = SyncRecord::from_object_index(object, SyncStatus::Processing);
        self.sync_storage.upsert_object(sync_record).await?;

        // Get data based on storage type
        let data_result = if let Some(data) = &object.data {
            Ok(data.clone())
        } else if let Some(s3_key) = &object.object_key {
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
                let recall_key = Self::construct_recall_key(object);
                match self.recall_storage.add_blob(&recall_key, data).await {
                    Ok(()) => {
                        info!("Successfully synchronized to Recall as {recall_key}");
                        self.sync_storage
                            .set_object_status(object_id, SyncStatus::Complete)
                            .await?;
                        // Clear embedded data after successful sync to save space
                        self.sync_storage.clear_object_data(object_id).await?;
                    }
                    Err(e) => {
                        error!("Failed to submit {recall_key} to Recall: {e}");
                        self.sync_storage
                            .record_failure(
                                object_id,
                                FailureType::RecallStorage,
                                e.to_string(),
                                false,
                            )
                            .await?;
                    }
                }
            }
            Err(e) => {
                error!("Failed to get data: {e}");
                self.sync_storage
                    .record_failure(
                        object_id,
                        FailureType::S3DataRetrieval,
                        e.to_string(),
                        false,
                    )
                    .await?;
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
    pub async fn run(&self, since: Option<DateTime<Utc>>, only_failed: bool) -> Result<()> {
        info!("Starting synchronization");

        let batch_size = self.config.batch_size;
        let mut total_processed = 0;

        loop {
            let mut objects_to_process = Vec::new();

            if only_failed {
                let failed_objects = self.get_object_indices_for_failed_records().await?;
                objects_to_process.extend(failed_objects);
            } else {
                let failed_objects = self.get_object_indices_for_failed_records().await?;
                let failed_count = failed_objects.len();
                objects_to_process.extend(failed_objects);

                let remaining_quota = batch_size.saturating_sub(failed_count);
                if remaining_quota > 0 {
                    let new_objects = self
                        .fetch_objects_with_retry(since, remaining_quota)
                        .await?;
                    objects_to_process.extend(new_objects);
                }
            }

            if objects_to_process.is_empty() {
                info!("No objects found to synchronize");
                break;
            }

            let failed_count = if only_failed {
                objects_to_process.len()
            } else {
                self.sync_storage.get_failed_objects().await?.len()
            };

            info!(
                "Processing {} objects ({} retries + {} new)",
                objects_to_process.len(),
                failed_count,
                objects_to_process.len() - failed_count
            );

            let (last_synced_object, batch_processed) =
                self.process_object_batch(&objects_to_process).await?;
            total_processed += batch_processed;

            if let Some(last_object) = last_synced_object {
                self.sync_storage
                    .set_last_synced_object_id(last_object.id)
                    .await?;
            }

            if only_failed || total_processed >= batch_size {
                break;
            }
        }

        info!("Synchronization completed. Processed {total_processed} objects");
        Ok(())
    }

    /// Resets the synchronization state by clearing all sync records
    pub async fn reset(&self) -> Result<()> {
        info!("Resetting synchronization state");
        self.sync_storage.clear_all().await?;
        info!("Synchronization state has been reset");
        Ok(())
    }

    /// Get failed objects that can be retried
    pub async fn get_failed_objects(&self) -> Result<Vec<SyncRecord>> {
        self.sync_storage
            .get_failed_objects()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get failed objects: {e}"))
    }

    /// Get permanently failed objects
    pub async fn get_permanently_failed_objects(&self) -> Result<Vec<SyncRecord>> {
        self.sync_storage
            .get_permanently_failed_objects()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get permanently failed objects: {e}"))
    }

    /// Starts the synchronizer to run continuously at the specified interval
    pub async fn start(&self, interval_seconds: u64, since: Option<DateTime<Utc>>) -> Result<()> {
        let mut interval_timer = interval(Duration::from_secs(interval_seconds));

        info!("Synchronizer started with {interval_seconds}s interval");

        loop {
            interval_timer.tick().await;

            info!("Starting synchronization run");
            match self.run(since, false).await {
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
