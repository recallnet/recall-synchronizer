use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::sync::Arc;
use tracing::{debug, info};

use crate::config::Config;
use crate::db::Database;
use crate::db::PostgresDatabase;
use crate::recall::RecallConnector;
use crate::s3::S3Connector;
use crate::sync::storage::SqliteSyncStorage;
use crate::sync::storage::SyncStorage;

#[cfg(test)]
use crate::recall::test_utils::FakeRecallConnector;
#[cfg(test)]
use crate::s3::test_utils::FakeS3Connector;

// Define a trait for S3 access that both real and fake implementations can satisfy
#[cfg(test)]
pub trait S3Access {
    fn get_object<'a>(
        &'a self,
        key: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    >;
}

#[cfg(test)]
impl S3Access for S3Connector {
    fn get_object<'a>(
        &'a self,
        key: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    > {
        Box::pin(self.get_object(key))
    }
}

#[cfg(test)]
impl S3Access for FakeS3Connector {
    fn get_object<'a>(
        &'a self,
        key: &'a str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    > {
        Box::pin(self.get_object(key))
    }
}

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
pub struct Synchronizer<D: Database, S: SyncStorage> {
    database: Arc<D>,
    sync_storage: Arc<S>,
    s3_connector: Arc<S3Connector>,
    recall_connector: Arc<RecallConnector>,
    config: Config,
    reset: bool,
}

/// Test version of the synchronizer that works with both real and fake implementations
#[cfg(test)]
pub struct Synchronizer<D: Database, S: SyncStorage> {
    database: Arc<D>,
    sync_storage: Arc<S>,
    s3_connector: Arc<dyn S3Access + Send + Sync>,
    recall_connector: Arc<dyn RecallAccess + Send + Sync>,
    config: Config,
    reset: bool,
}

#[cfg(not(test))]
impl<D: Database, S: SyncStorage> Synchronizer<D, S> {
    /// Creates a new synchronizer instance with the provided database and sync storage
    pub fn with_storage(
        database: D,
        sync_storage: S,
        s3_connector: S3Connector,
        recall_connector: RecallConnector,
        config: Config,
        reset: bool,
    ) -> Self {
        Synchronizer {
            database: Arc::new(database),
            sync_storage: Arc::new(sync_storage),
            s3_connector: Arc::new(s3_connector),
            recall_connector: Arc::new(recall_connector),
            config,
            reset,
        }
    }
}

#[cfg(test)]
impl<D: Database, S: SyncStorage> Synchronizer<D, S> {
    /// Test version that works with dynamic trait objects
    pub fn with_storage<
        T1: S3Access + Send + Sync + 'static,
        T2: RecallAccess + Send + Sync + 'static,
    >(
        database: D,
        sync_storage: S,
        s3_connector: T1,
        recall_connector: T2,
        config: Config,
        reset: bool,
    ) -> Self {
        Synchronizer {
            database: Arc::new(database),
            sync_storage: Arc::new(sync_storage),
            s3_connector: Arc::new(s3_connector),
            recall_connector: Arc::new(recall_connector),
            config,
            reset,
        }
    }
}

#[cfg(not(test))]
impl<D: Database, S: SyncStorage> Synchronizer<D, S> {
    /// Returns a reference to the sync storage
    #[allow(dead_code)]
    pub fn get_sync_storage(&self) -> &Arc<S> {
        &self.sync_storage
    }
}

#[cfg(test)]
impl<D: Database, S: SyncStorage> Synchronizer<D, S> {
    /// Returns a reference to the sync storage
    #[allow(dead_code)]
    pub fn get_sync_storage(&self) -> &Arc<S> {
        &self.sync_storage
    }
}

#[cfg(not(test))]
impl<D: Database, S: SyncStorage> Synchronizer<D, S> {
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
            // If no since parameter was provided, use the last sync timestamp
            self.sync_storage.get_last_sync_timestamp().await?
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

        info!("Found {} objects to synchronize", objects.len());

        // Process each object
        let mut synced_count = 0;
        let now = Utc::now();

        for object in objects {
            // Check if already synced (to handle restarts)
            if self
                .sync_storage
                .is_object_synced(&object.object_key)
                .await?
            {
                debug!("Object already synced: {}", object.object_key);
                continue;
            }

            // Download from S3
            debug!("Fetching object from S3: {}", object.object_key);
            let data = self.s3_connector.get_object(&object.object_key).await?;

            // Upload to Recall
            debug!("Storing object to Recall: {}", object.object_key);
            let cid = self
                .recall_connector
                .store_object(&object.object_key, &data)
                .await?;

            // Mark as synced
            self.sync_storage
                .mark_object_synced(&object.object_key, now)
                .await?;

            info!(
                "Successfully synced object: {} -> {}",
                object.object_key, cid
            );
            synced_count += 1;
        }

        // Update last sync timestamp
        self.sync_storage.update_last_sync_timestamp(now).await?;

        info!(
            "Synchronization completed successfully. Synced {} objects.",
            synced_count
        );
        Ok(())
    }
}

#[cfg(test)]
impl<D: Database, S: SyncStorage> Synchronizer<D, S> {
    /// Runs the synchronization process (test version)
    pub async fn run(&self, competition_id: Option<String>, since: Option<String>) -> Result<()> {
        info!("Starting synchronization (test version)");

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
            // If no since parameter was provided, use the last sync timestamp
            self.sync_storage.get_last_sync_timestamp().await?
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

        info!("Found {} objects to synchronize", objects.len());

        // Process each object
        let mut synced_count = 0;
        let now = Utc::now();

        for object in objects {
            // Check if already synced (to handle restarts)
            if self
                .sync_storage
                .is_object_synced(&object.object_key)
                .await?
            {
                debug!("Object already synced: {}", object.object_key);
                continue;
            }

            // Download from S3 using the trait method
            debug!("Fetching object from S3: {}", object.object_key);
            let data = self.s3_connector.get_object(&object.object_key).await?;

            // Upload to Recall using the trait method
            debug!("Storing object to Recall: {}", object.object_key);
            let cid = self
                .recall_connector
                .store_object(&object.object_key, &data)
                .await?;

            // Mark as synced
            self.sync_storage
                .mark_object_synced(&object.object_key, now)
                .await?;

            info!(
                "Successfully synced object: {} -> {}",
                object.object_key, cid
            );
            synced_count += 1;
        }

        // Update last sync timestamp
        self.sync_storage.update_last_sync_timestamp(now).await?;

        info!(
            "Synchronization completed successfully. Synced {} objects.",
            synced_count
        );
        Ok(())
    }
}

impl Synchronizer<PostgresDatabase, SqliteSyncStorage> {
    /// Creates a new synchronizer instance from the provided configuration
    pub async fn new(config: Config, reset: bool) -> Result<Self> {
        info!("Initializing synchronizer with default implementations");

        // Initialize database connection
        let database = PostgresDatabase::new(&config.database.url)
            .await
            .context("Failed to connect to PostgreSQL database")?;

        // Initialize sync storage
        let sync_storage = SqliteSyncStorage::new(&config.sync.state_db_path)
            .context("Failed to initialize SQLite sync storage")?;

        // Initialize S3 connector
        let s3_connector = S3Connector::new(&config.s3)
            .await
            .context("Failed to initialize S3 connector")?;

        // Initialize Recall connector
        let recall_connector = RecallConnector::new(&config.recall)
            .await
            .context("Failed to initialize Recall connector")?;

        Ok(Synchronizer {
            database: Arc::new(database),
            sync_storage: Arc::new(sync_storage),
            s3_connector: Arc::new(s3_connector),
            recall_connector: Arc::new(recall_connector),
            config,
            reset,
        })
    }
}
