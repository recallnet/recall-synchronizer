use anyhow::Result;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::db::models::ObjectIndex;
use crate::db::Database;
use crate::recall::RecallStorage;
use crate::s3::Storage;
use crate::sync::storage::{SyncRecord, SyncStatus, SyncStorage};

#[allow(clippy::enum_variant_names, dead_code)]
#[derive(Debug, Error)]
pub enum JobError {
    #[error("Failed to fetch object from S3: {0}")]
    S3Error(#[from] anyhow::Error),

    #[error("Failed to store object to Recall: {0}")]
    RecallError(String),

    #[error("Failed to update sync storage: {0}")]
    StorageError(String),

    #[error("Database error: {0}")]
    DatabaseError(String),
}

#[allow(dead_code)]
pub struct SyncJob {
    pub id: Uuid,
    pub object_index: ObjectIndex,
}

#[allow(dead_code)]
impl SyncJob {
    pub fn new(object_index: ObjectIndex) -> Self {
        Self {
            id: Uuid::new_v4(),
            object_index,
        }
    }

    pub async fn execute<ST: Storage, RS: RecallStorage, SS: SyncStorage>(
        &self,
        s3_storage: &Arc<ST>,
        recall_storage: &Arc<RS>,
        sync_storage: &Arc<SS>,
    ) -> Result<(), JobError> {
        let object_key = &self.object_index.object_key;
        let object_id = self.object_index.id;

        info!("Starting sync job for object: {}", object_key);

        // Create a sync record
        let sync_record = SyncRecord::new(
            object_id,
            object_key.clone(),
            self.object_index.bucket_name.clone(),
            self.object_index.object_last_modified_at,
        );

        // Add the record to sync storage
        sync_storage
            .add_object(sync_record)
            .await
            .map_err(|e| JobError::StorageError(e.to_string()))?;

        // Update status to Processing
        sync_storage
            .set_object_status(object_id, SyncStatus::Processing)
            .await
            .map_err(|e| JobError::StorageError(e.to_string()))?;

        // Download the object from S3
        let object_data = s3_storage.get_object(object_key).await.map_err(|e| {
            JobError::S3Error(anyhow::anyhow!("Failed to get object from S3: {}", e))
        })?;

        debug!(
            "Downloaded object from S3: {} (size: {} bytes)",
            object_key,
            object_data.len()
        );

        // Upload the object to Recall
        let recall_id = recall_storage
            .add_blob(object_key, object_data.to_vec())
            .await
            .map_err(|e| JobError::RecallError(e.to_string()))?;

        info!(
            "Successfully uploaded object to Recall: {} -> {}",
            object_key, recall_id
        );

        // Update the sync status to Complete
        sync_storage
            .set_object_status(object_id, SyncStatus::Complete)
            .await
            .map_err(|e| JobError::StorageError(e.to_string()))?;

        Ok(())
    }

    pub async fn retry<ST: Storage, RS: RecallStorage, SS: SyncStorage>(
        &self,
        s3_storage: &Arc<ST>,
        recall_storage: &Arc<RS>,
        sync_storage: &Arc<SS>,
        retry_count: u32,
    ) -> Result<(), JobError> {
        let max_retries = 3;

        for attempt in 0..max_retries {
            match self
                .execute(s3_storage, recall_storage, sync_storage)
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    error!(
                        "Sync job failed for {}: {} (attempt {}/{})",
                        self.object_index.object_key,
                        e,
                        attempt + 1,
                        max_retries
                    );

                    if attempt < max_retries - 1 {
                        // Exponential backoff
                        let delay_secs = 2u64.pow(attempt);
                        tokio::time::sleep(tokio::time::Duration::from_secs(delay_secs)).await;
                    }
                }
            }
        }

        Err(JobError::StorageError(format!(
            "Failed after {} attempts",
            retry_count
        )))
    }
}

#[allow(dead_code)]
pub struct JobScheduler<D: Database, ST: Storage, RS: RecallStorage, SS: SyncStorage> {
    database: Arc<D>,
    s3_storage: Arc<ST>,
    recall_storage: Arc<RS>,
    sync_storage: Arc<SS>,
}

#[allow(dead_code)]
impl<D: Database, ST: Storage, RS: RecallStorage, SS: SyncStorage> JobScheduler<D, ST, RS, SS> {
    pub fn new(
        database: Arc<D>,
        s3_storage: Arc<ST>,
        recall_storage: Arc<RS>,
        sync_storage: Arc<SS>,
    ) -> Self {
        Self {
            database,
            s3_storage,
            recall_storage,
            sync_storage,
        }
    }

    pub async fn run_batch(&self, batch_size: u32) -> Result<u32, JobError> {
        // Get the last sync timestamp
        let last_sync = self
            .sync_storage
            .get_last_object()
            .await
            .map_err(|e| JobError::StorageError(e.to_string()))?
            .map(|record| record.timestamp);

        // Fetch objects that need to be synced
        let objects = self
            .database
            .get_objects_to_sync(batch_size, last_sync)
            .await
            .map_err(|e| JobError::DatabaseError(e.to_string()))?;

        if objects.is_empty() {
            info!("No objects to sync");
            return Ok(0);
        }

        info!("Found {} objects to sync", objects.len());

        let mut successful_count = 0;

        for object in objects {
            let job = SyncJob::new(object);

            match job
                .retry(
                    &self.s3_storage,
                    &self.recall_storage,
                    &self.sync_storage,
                    3,
                )
                .await
            {
                Ok(()) => successful_count += 1,
                Err(e) => error!("Failed to sync object: {}", e),
            }
        }

        info!("Successfully synced {} objects", successful_count);
        Ok(successful_count)
    }

    pub async fn run_continuous(&self, interval_seconds: u64, batch_size: u32) {
        let interval = tokio::time::Duration::from_secs(interval_seconds);
        let mut interval_timer = tokio::time::interval(interval);

        loop {
            interval_timer.tick().await;

            match self.run_batch(batch_size).await {
                Ok(count) => {
                    if count > 0 {
                        info!("Batch sync completed: {} objects", count);
                    }
                }
                Err(e) => error!("Batch sync failed: {}", e),
            }
        }
    }
}