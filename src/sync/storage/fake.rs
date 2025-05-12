use crate::sync::storage::error::SyncStorageError;
use crate::sync::storage::sync_storage::SyncStorage;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// A fake in-memory implementation of the SyncStorage trait for testing
pub struct FakeSyncStorage {
    synced_objects: Arc<RwLock<HashMap<String, DateTime<Utc>>>>,
    last_sync_timestamp: Arc<RwLock<Option<DateTime<Utc>>>>,
}

impl FakeSyncStorage {
    /// Create a new empty FakeSyncStorage
    pub fn new() -> Self {
        FakeSyncStorage {
            synced_objects: Arc::new(RwLock::new(HashMap::new())),
            last_sync_timestamp: Arc::new(RwLock::new(None)),
        }
    }

    /// Clear all synced objects
    pub fn fake_clear_synced_objects(&self) {
        let mut synced = self.synced_objects.write().unwrap();
        synced.clear();
    }

    /// Set the last sync timestamp directly (for testing)
    pub fn fake_set_last_sync_timestamp(&self, timestamp: Option<DateTime<Utc>>) {
        let mut last_sync = self.last_sync_timestamp.write().unwrap();
        *last_sync = timestamp;
    }
}

#[async_trait]
impl SyncStorage for FakeSyncStorage {
    async fn mark_object_synced(&self, object_key: &str, sync_timestamp: DateTime<Utc>) 
        -> Result<(), SyncStorageError> {
        let mut synced = self.synced_objects.write().unwrap();
        synced.insert(object_key.to_string(), sync_timestamp);
        Ok(())
    }

    async fn is_object_synced(&self, object_key: &str) -> Result<bool, SyncStorageError> {
        let synced = self.synced_objects.read().unwrap();
        Ok(synced.contains_key(object_key))
    }

    async fn update_last_sync_timestamp(&self, timestamp: DateTime<Utc>) 
        -> Result<(), SyncStorageError> {
        let mut last_sync = self.last_sync_timestamp.write().unwrap();
        *last_sync = Some(timestamp);
        Ok(())
    }

    async fn get_last_sync_timestamp(&self) -> Result<Option<DateTime<Utc>>, SyncStorageError> {
        let last_sync = self.last_sync_timestamp.read().unwrap();
        Ok(*last_sync)
    }
}