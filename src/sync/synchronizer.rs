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

    /// Gets the timestamp and ID to sync from
    async fn get_sync_state(
        &self,
        since: Option<DateTime<Utc>>,
    ) -> Result<(Option<DateTime<Utc>>, Option<uuid::Uuid>)> {
        if let Some(ts) = since {
            // If user provides a specific timestamp, start from there with no ID filter
            Ok((Some(ts), None))
        } else if let Some(last_id) = self.sync_storage.get_last_synced_object_id().await? {
            if let Some(last_record) = self.sync_storage.get_object(last_id).await? {
                Ok((Some(last_record.timestamp), Some(last_id)))
            } else {
                Ok((None, None))
            }
        } else {
            Ok((None, None))
        }
    }

    /// Fetches objects from the database that need to be synchronized
    async fn fetch_objects_to_sync(
        &self,
        since_time: Option<DateTime<Utc>>,
        after_id: Option<uuid::Uuid>,
    ) -> Result<Vec<ObjectIndex>> {
        let batch_size = self.config.sync.batch_size as u32;
        self.database
            .get_objects_to_sync_with_id(batch_size, since_time, after_id)
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

    /// Runs the synchronization process
    pub async fn run(
        &self,
        competition_id: Option<String>,
        since: Option<DateTime<Utc>>,
    ) -> Result<()> {
        info!("Starting synchronization");

        if let Some(id) = &competition_id {
            info!("Filtering by competition ID: {}", id);
        }

        let (since_time, after_id) = self.get_sync_state(since).await?;

        if let Some(ts) = &since_time {
            info!("Synchronizing data since: {}", ts);
        } else {
            info!("No previous sync timestamp found, syncing all objects");
        }

        if self.reset {
            info!("Reset mode is enabled, clearing synchronization state");
            // TODO: Implement reset logic when integrating with sync storage
        }

        let objects = self.fetch_objects_to_sync(since_time, after_id).await?;

        if objects.is_empty() {
            info!("No objects found to synchronize");
            return Ok(());
        }

        let filtered_objects = self.filter_objects_by_competition(objects, competition_id)?;

        info!("Found {} objects to synchronize", filtered_objects.len());

        // Process objects and track the last successfully synced object
        // We need to track the last object we processed to properly continue
        // from where we left off, especially when multiple objects have the same timestamp
        let mut last_synced_object: Option<&ObjectIndex> = None;

        for object in &filtered_objects {
            if self.should_process_object(object).await? {
                self.sync_object(object).await?;

                if let Some(record) = self.sync_storage.get_object(object.id).await? {
                    if record.status == SyncStatus::Complete {
                        last_synced_object = Some(object);
                    }
                }
            }
        }

        if let Some(last_object) = last_synced_object {
            self.sync_storage
                .set_last_synced_object_id(last_object.id)
                .await?;
        }

        info!("Synchronization completed");
        Ok(())
    }
}
