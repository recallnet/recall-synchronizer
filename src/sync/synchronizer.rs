use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tracing::{debug, info};

use crate::config::Config;
use crate::db::Database;
use crate::db::PostgresDatabase;
use crate::recall::RecallConnector;
use crate::s3::Storage;
use crate::sync::storage::SqliteSyncStorage;
use crate::sync::storage::{SyncRecord, SyncStatus, SyncStorage};

#[cfg(test)]
use crate::recall::test_utils::FakeRecallConnector;

// Define a trait for Recall access
#[cfg(test)]
pub trait RecallAccess {
    fn store_object<'a>(
        &'a self,
        key: &'a str,
        data: &'a [u8],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send + 'a>>;
}

#[cfg(test)]
impl RecallAccess for RecallConnector {
    fn store_object<'a>(
        &'a self,
        key: &'a str,
        data: &'a [u8],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send + 'a>>
    {
        Box::pin(self.store_object(key, data))
    }
}

#[cfg(test)]
impl RecallAccess for FakeRecallConnector {
    fn store_object<'a>(
        &'a self,
        key: &'a str,
        data: &'a [u8],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<String>> + Send + 'a>>
    {
        Box::pin(self.store_object(key, data))
    }
}

/// Main synchronizer that orchestrates the data synchronization process
#[cfg(not(test))]
pub struct Synchronizer<D: Database, S: SyncStorage, ST: Storage> {
    database: Arc<D>,
    sync_storage: Arc<S>,
    s3_storage: Arc<ST>,
    recall_connector: Arc<RecallConnector>,
    config: Config,
    reset: bool,
}

/// Test version of the synchronizer that works with both real and fake implementations
#[cfg(test)]
pub struct Synchronizer<D: Database, S: SyncStorage, ST: Storage> {
    database: Arc<D>,
    sync_storage: Arc<S>,
    s3_storage: Arc<ST>,
    recall_connector: Arc<dyn RecallAccess + Send + Sync>,
    config: Config,
    reset: bool,
}

#[cfg(test)]
impl<D: Database, S: SyncStorage, ST: Storage> Synchronizer<D, S, ST> {
    /// Test version that works with dynamic trait objects
    pub fn with_storage<
        T2: RecallAccess + Send + Sync + 'static,
    >(
        database: D,
        sync_storage: S,
        s3_storage: ST,
        recall_connector: T2,
        config: Config,
        reset: bool,
    ) -> Self {
        Synchronizer {
            database: Arc::new(database),
            sync_storage: Arc::new(sync_storage),
            s3_storage: Arc::new(s3_storage),
            recall_connector: Arc::new(recall_connector),
            config,
            reset,
        }
    }
}

#[cfg(not(test))]
impl<D: Database, S: SyncStorage, ST: Storage> Synchronizer<D, S, ST> {
    /// Returns a reference to the sync storage
    #[allow(dead_code)]
    pub fn get_sync_storage(&self) -> &Arc<S> {
        &self.sync_storage
    }
}

#[cfg(test)]
impl<D: Database, S: SyncStorage, ST: Storage> Synchronizer<D, S, ST> {
    /// Returns a reference to the sync storage
    #[allow(dead_code)]
    pub fn get_sync_storage(&self) -> &Arc<S> {
        &self.sync_storage
    }
}

#[cfg(not(test))]
impl<D: Database, S: SyncStorage, ST: Storage> Synchronizer<D, S, ST> {
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
                    debug!("Object {} already synchronized, skipping", object.object_key);
                    continue;
                }
                Some(SyncStatus::Processing) => {
                    debug!("Object {} is already being processed, skipping", object.object_key);
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
                        .recall_connector
                        .store_object(&object.object_key, &data)
                        .await
                    {
                        Ok(recall_id) => {
                            info!(
                                "Successfully synchronized {} to Recall with ID: {}",
                                object.object_key, recall_id
                            );

                            // Update status to Complete
                            self.sync_storage
                                .set_object_status(object.id, SyncStatus::Complete)
                                .await?;
                        }
                        Err(e) => {
                            eprintln!(
                                "Failed to submit {} to Recall: {}",
                                object.object_key, e
                            );
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

#[cfg(test)]
impl<D: Database, S: SyncStorage, ST: Storage> Synchronizer<D, S, ST> {
    /// Test version of run that uses trait objects
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
                    debug!("Object {} already synchronized, skipping", object.object_key);
                    continue;
                }
                Some(SyncStatus::Processing) => {
                    debug!("Object {} is already being processed, skipping", object.object_key);
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
                        .recall_connector
                        .store_object(&object.object_key, &data)
                        .await
                    {
                        Ok(recall_id) => {
                            info!(
                                "Successfully synchronized {} to Recall with ID: {}",
                                object.object_key, recall_id
                            );

                            // Update status to Complete
                            self.sync_storage
                                .set_object_status(object.id, SyncStatus::Complete)
                                .await?;
                        }
                        Err(e) => {
                            eprintln!(
                                "Failed to submit {} to Recall: {}",
                                object.object_key, e
                            );
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

#[cfg(not(test))]
impl<D: Database, S: SyncStorage, ST: Storage> Synchronizer<D, S, ST> {
    /// Creates a new Synchronizer instance
    pub async fn new(database: D, config: Config, reset: bool) -> Result<Self> {
        let sync_storage = SqliteSyncStorage::new(&config.sync.state_db_path).await?;
        let s3_storage = crate::s3::S3Storage::new(&config.s3).await?;
        let recall_connector = RecallConnector::new(&config.recall)?;

        Ok(Synchronizer {
            database: Arc::new(database),
            sync_storage: Arc::new(sync_storage),
            s3_storage: Arc::new(s3_storage),
            recall_connector: Arc::new(recall_connector),
            config,
            reset,
        })
    }
}

#[cfg(test)]
impl<D: Database, S: SyncStorage, ST: Storage> Synchronizer<D, S, ST> {
    /// Creates a new test Synchronizer instance
    pub async fn new(_database: D, _config: Config, _reset: bool) -> Result<Self> {
        unimplemented!("Use with_storage for test instances")
    }
}

/// Default implementation for PostgreSQL database and SQLite sync storage
pub type DefaultSynchronizer = Synchronizer<PostgresDatabase, SqliteSyncStorage, crate::s3::S3Storage>;

impl DefaultSynchronizer {
    /// Creates a synchronizer with default implementations
    pub async fn default(config: Config, reset: bool) -> Result<Self> {
        let database = PostgresDatabase::new(&config.database.url).await?;
        Synchronizer::new(database, config, reset).await
    }
}