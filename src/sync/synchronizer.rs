use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tracing::{debug, info};

use crate::config::Config;
use crate::db::Database;
use crate::db::PostgresDatabase;
use crate::recall::RecallStorage;
use crate::s3::Storage;
use crate::sync::storage::SqliteSyncStorage;
use crate::sync::storage::{SyncRecord, SyncStatus, SyncStorage};

/// Main synchronizer that orchestrates the data synchronization process
pub struct Synchronizer<D: Database, S: SyncStorage, ST: Storage, RS: RecallStorage> {
    database: Arc<D>,
    sync_storage: Arc<S>,
    s3_storage: Arc<ST>,
    recall_storage: Arc<RS>,
    config: Config,
    reset: bool,
}

impl<D: Database, S: SyncStorage, ST: Storage, RS: RecallStorage> Synchronizer<D, S, ST, RS> {
    /// Returns a reference to the sync storage
    #[allow(dead_code)]
    pub fn get_sync_storage(&self) -> &Arc<S> {
        &self.sync_storage
    }

    /// Test version that works with different trait implementations
    #[allow(dead_code)]
    pub fn with_storage(
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

    /// Runs the synchronization process
    pub async fn run(&self, competition_id: Option<String>, since: Option<String>) -> Result<()> {
        info!("Starting synchronization");

        // Log parameters for debugging
        if let Some(id) = &competition_id {
            info!("Filtering by competition ID: {}", id);
        }

        // Parse the since parameter if provided
        let since_time = if let Some(ts) = &since {
            Some(
                DateTime::parse_from_rfc3339(ts)
                    .context(format!("Failed to parse provided timestamp: {}", ts))?
                    .with_timezone(&Utc),
            )
        } else {
            // If no since parameter was provided, use the last synced object's timestamp
            if let Some(last_record) = self.sync_storage.get_last_object().await? {
                Some(last_record.timestamp)
            } else {
                None
            }
        };

        if let Some(ts) = &since_time {
            info!("Synchronizing data since: {}", ts);
        } else {
            info!("No previous sync timestamp found, syncing all objects");
        }

        if self.reset {
            info!("Reset mode is enabled, clearing synchronization state");
            // We'll implement reset logic when integrating with sync storage
        }

        // Get objects to sync
        let batch_size = self.config.sync.batch_size as u32;
        let objects = self
            .database
            .get_objects_to_sync(batch_size, since_time)
            .await?;

        if objects.is_empty() {
            info!("No objects found to synchronize");
            return Ok(());
        }

        // Apply competition_id filter if specified
        let filtered_objects = if let Some(comp_id) = competition_id {
            let comp_uuid = uuid::Uuid::parse_str(&comp_id)
                .context(format!("Invalid competition ID format: {}", comp_id))?;
            objects
                .into_iter()
                .filter(|obj| obj.competition_id == Some(comp_uuid))
                .collect::<Vec<_>>()
        } else {
            objects
        };

        info!("Found {} objects to synchronize", filtered_objects.len());

        // Process each object
        for object in filtered_objects {
            // Check if the object has already been synced
            match self.sync_storage.get_object_status(object.id).await? {
                Some(SyncStatus::Complete) => {
                    debug!(
                        "Object {} already synchronized, skipping",
                        object.object_key
                    );
                    continue;
                }
                Some(SyncStatus::Processing) => {
                    debug!(
                        "Object {} is already being processed, skipping",
                        object.object_key
                    );
                    continue;
                }
                _ => {}
            }

            // Create a record in sync storage
            let sync_record = SyncRecord::new(
                object.id,
                object.object_key.clone(),
                object.bucket_name.clone(),
                object.object_last_modified_at,
            );

            // Add the object to sync storage with PendingSync status
            self.sync_storage.add_object(sync_record.clone()).await?;

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
                            eprintln!("Failed to submit {} to Recall: {}", object.object_key, e);
                            // Consider retry logic here
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to get {} from S3: {}", object.object_key, e);
                    // Consider retry logic here
                }
            }
        }

        info!("Synchronization completed");
        Ok(())
    }
}

impl
    Synchronizer<
        crate::db::postgres::PostgresDatabase,
        SqliteSyncStorage,
        crate::s3::S3Storage,
        crate::recall::RecallBlockchain,
    >
{
    /// Creates a new Synchronizer instance with real implementations
    pub async fn new(
        database: crate::db::postgres::PostgresDatabase,
        config: Config,
        reset: bool,
    ) -> Result<Self> {
        let sync_storage = SqliteSyncStorage::new(&config.sync.state_db_path)?;
        let s3_storage = crate::s3::S3Storage::new(&config.s3).await?;
        let recall_storage = crate::recall::RecallBlockchain::new(&config.recall).await?;

        Ok(Synchronizer {
            database: Arc::new(database),
            sync_storage: Arc::new(sync_storage),
            s3_storage: Arc::new(s3_storage),
            recall_storage: Arc::new(recall_storage),
            config,
            reset,
        })
    }
}

/// Default implementation for PostgreSQL database and SQLite sync storage
pub type DefaultSynchronizer = Synchronizer<
    PostgresDatabase,
    SqliteSyncStorage,
    crate::s3::S3Storage,
    crate::recall::RecallBlockchain,
>;

impl DefaultSynchronizer {
    /// Creates a synchronizer with default implementations
    pub async fn default(config: Config, reset: bool) -> Result<Self> {
        let database = PostgresDatabase::new(&config.database.url).await?;
        Synchronizer::new(database, config, reset).await
    }
}
