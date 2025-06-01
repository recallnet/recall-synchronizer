use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tracing::{debug, error, info};

use crate::config::Config;
use crate::db::{Database, ObjectIndex};
use crate::recall::Storage as RecallStorage;
use crate::s3::Storage as S3Storage;
use crate::sync::storage::{SyncRecord, SyncStatus, SyncStorage};

/// Main synchronizer that orchestrates the data synchronization process
pub struct Synchronizer<D: Database, S: SyncStorage, ST: S3Storage, RS: RecallStorage> {
    database: Arc<D>,
    sync_storage: Arc<S>,
    s3_storage: Arc<ST>,
    recall_storage: Arc<RS>,
    config: Config,
    reset: bool,
}

impl<D: Database, S: SyncStorage, ST: S3Storage, RS: RecallStorage> Synchronizer<D, S, ST, RS> {
    /// Creates a new Synchronizer instance
    pub fn new(
        database: D,
        sync_storage: S,
        s3_storage: ST,
        recall_storage: RS,
        config: Config,
        reset: bool,
    ) -> Self {
        Synchronizer {
            database: Arc::new(database),
            sync_storage: Arc::new(sync_storage),
            s3_storage: Arc::new(s3_storage),
            recall_storage: Arc::new(recall_storage),
            config,
            reset,
        }
    }

    /// Test-specific constructor that accepts Arc-wrapped storage implementations
    #[cfg(test)]
    pub fn with_storage(
        database: Arc<D>,
        sync_storage: Arc<S>,
        s3_storage: Arc<ST>,
        recall_storage: Arc<RS>,
        config: Config,
        reset: bool,
    ) -> Self {
        Synchronizer {
            database,
            sync_storage,
            s3_storage,
            recall_storage,
            config,
            reset,
        }
    }

    /// Parses the provided timestamp string or gets the last sync timestamp
    async fn get_sync_timestamp(&self, since: Option<String>) -> Result<Option<DateTime<Utc>>> {
        if let Some(ts) = since {
            let timestamp = DateTime::parse_from_rfc3339(&ts)
                .context(format!("Failed to parse provided timestamp: {}", ts))?
                .with_timezone(&Utc);
            Ok(Some(timestamp))
        } else {
            // If no since parameter was provided, use the last synced object's timestamp
            if let Some(last_record) = self.sync_storage.get_last_object().await? {
                Ok(Some(last_record.timestamp))
            } else {
                Ok(None)
            }
        }
    }

    /// Fetches objects from the database that need to be synchronized
    async fn fetch_objects_to_sync(
        &self,
        since_time: Option<DateTime<Utc>>,
    ) -> Result<Vec<ObjectIndex>> {
        let batch_size = self.config.sync.batch_size as u32;
        self.database
            .get_objects_to_sync(batch_size, since_time)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch objects to sync: {}", e))
    }

    /// Filters objects by competition ID if specified
    fn filter_objects_by_competition(
        &self,
        objects: Vec<ObjectIndex>,
        competition_id: Option<String>,
    ) -> Result<Vec<ObjectIndex>> {
        if let Some(comp_id) = competition_id {
            let comp_uuid = uuid::Uuid::parse_str(&comp_id)
                .context(format!("Invalid competition ID format: {}", comp_id))?;
            Ok(objects
                .into_iter()
                .filter(|obj| obj.competition_id == Some(comp_uuid))
                .collect())
        } else {
            Ok(objects)
        }
    }

    /// Checks if an object should be processed based on its current status
    async fn should_process_object(&self, object: &ObjectIndex) -> Result<bool> {
        match self.sync_storage.get_object_status(object.id).await? {
            Some(SyncStatus::Complete) => {
                debug!(
                    "Object {} already synchronized, skipping",
                    object.object_key
                );
                Ok(false)
            }
            Some(SyncStatus::Processing) => {
                debug!(
                    "Object {} is already being processed, skipping",
                    object.object_key
                );
                Ok(false)
            }
            _ => Ok(true),
        }
    }

    /// Synchronizes a single object from S3 to Recall
    async fn sync_object(&self, object: &ObjectIndex) -> Result<()> {
        // Create a record in sync storage
        let sync_record = SyncRecord::new(
            object.id,
            object.object_key.clone(),
            object.bucket_name.clone(),
            object.object_last_modified_at,
        );

        // Add the object to sync storage with PendingSync status
        self.sync_storage.add_object(sync_record).await?;

        // Update status to Processing
        self.sync_storage
            .set_object_status(object.id, SyncStatus::Processing)
            .await?;

        // Get the object from S3
        match self.s3_storage.get_object(&object.object_key).await {
            Ok(data) => {
                // Submit to Recall
                match self
                    .recall_storage
                    .add_blob(&object.object_key, data.to_vec())
                    .await
                {
                    Ok(()) => {
                        info!("Successfully synchronized {} to Recall", object.object_key);

                        // Update status to Complete
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

    /// Runs the synchronization process
    pub async fn run(&self, competition_id: Option<String>, since: Option<String>) -> Result<()> {
        info!("Starting synchronization");

        // Log parameters for debugging
        if let Some(id) = &competition_id {
            info!("Filtering by competition ID: {}", id);
        }

        // Parse the since parameter
        let since_time = self.get_sync_timestamp(since).await?;

        if let Some(ts) = &since_time {
            info!("Synchronizing data since: {}", ts);
        } else {
            info!("No previous sync timestamp found, syncing all objects");
        }

        if self.reset {
            info!("Reset mode is enabled, clearing synchronization state");
            // TODO: Implement reset logic when integrating with sync storage
        }

        // Get objects to sync
        let objects = self.fetch_objects_to_sync(since_time).await?;

        if objects.is_empty() {
            info!("No objects found to synchronize");
            return Ok(());
        }

        // Apply competition_id filter if specified
        let filtered_objects = self.filter_objects_by_competition(objects, competition_id)?;

        info!("Found {} objects to synchronize", filtered_objects.len());

        // Process each object
        for object in filtered_objects {
            if self.should_process_object(&object).await? {
                self.sync_object(&object).await?;
            }
        }

        info!("Synchronization completed");
        Ok(())
    }
}
