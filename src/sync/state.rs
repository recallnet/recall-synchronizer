use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::sync::storage::{SyncStorage, SqliteSyncStorage, FakeSyncStorage};

/// SyncState provides a wrapper for the SyncStorage trait with additional functionality
/// specifically for the synchronizer. This maintains backward compatibility while
/// delegating to the SyncStorage trait for core operations.
pub struct SyncState {
    storage: Arc<dyn SyncStorage>,
}

impl SyncState {
    /// Create a new SyncState using the SqliteSyncStorage implementation
    pub fn new(path: &str, reset: bool) -> Result<Self> {
        let path = Path::new(path);

        // Handle reset if requested
        if reset && path.exists() {
            std::fs::remove_file(path)?;
            info!("Reset state database at {}", path.display());
        }

        let storage = SqliteSyncStorage::new(path.to_str().unwrap())
            .context(format!("Failed to initialize sync storage at {}", path.display()))?;

        info!("Initialized state database at {}", path.display());
        Ok(Self {
            storage: Arc::new(storage),
        })
    }

    /// Create a new in-memory SyncState for testing
    #[cfg(test)]
    pub fn new_in_memory() -> Self {
        let storage = FakeSyncStorage::new();
        Self {
            storage: Arc::new(storage),
        }
    }

    /// Check if an object has been processed
    pub async fn is_processed(&self, key: &str) -> Result<bool> {
        self.storage
            .is_object_synced(key)
            .await
            .context(format!("Failed to check if key is processed: {}", key))
    }

    /// Mark an object as processed with its CID
    pub async fn mark_processed(
        &self,
        key: &str,
        _cid: &str, // CID is not used in the SyncStorage trait but kept for compatibility
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        self.storage
            .mark_object_synced(key, timestamp)
            .await
            .context(format!("Failed to mark key as processed: {}", key))
    }

    /// Get the timestamp of the last successful synchronization
    pub async fn get_last_sync_time(&self) -> Result<Option<DateTime<Utc>>> {
        self.storage
            .get_last_sync_timestamp()
            .await
            .context("Failed to get last sync time")
    }

    /// Update the timestamp of the last successful synchronization
    pub async fn update_last_sync_time(&self, timestamp: DateTime<Utc>) -> Result<()> {
        self.storage
            .update_last_sync_timestamp(timestamp)
            .await
            .context("Failed to update last sync time")?;

        debug!("Updated last sync time to {}", timestamp);
        Ok(())
    }

    /// Get statistics about synchronized objects
    pub async fn get_stats(&self) -> Result<(usize, Option<DateTime<Utc>>)> {
        // This functionality is not directly part of the SyncStorage trait
        // For now, we'll return placeholder stats
        let last_sync = self.get_last_sync_time().await?;

        // In a real implementation, we could query for all synced objects
        // and count them, but that's not trivial with our current trait design
        Ok((0, last_sync))
    }

    /// Get a list of failed jobs
    pub async fn get_failed_jobs(&self, _limit: usize) -> Result<Vec<String>> {
        // This functionality is not directly part of the SyncStorage trait
        // For a production implementation, we would need to extend the trait
        // or maintain additional state
        Ok(Vec::new())
    }

    /// Get the underlying storage implementation
    pub fn get_storage(&self) -> Arc<dyn SyncStorage> {
        self.storage.clone()
    }
}
