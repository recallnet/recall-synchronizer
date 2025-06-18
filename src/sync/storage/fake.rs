use crate::sync::storage::error::SyncStorageError;
use crate::sync::storage::models::{SyncRecord, SyncStatus};
use crate::sync::storage::sync_storage::SyncStorage;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

/// A fake in-memory implementation of the SyncStorage trait for testing
#[derive(Clone)]
pub struct FakeSyncStorage {
    records: Arc<RwLock<HashMap<Uuid, SyncRecord>>>,
    last_synced_id: Arc<RwLock<Option<Uuid>>>,
}

impl FakeSyncStorage {
    /// Create a new empty FakeSyncStorage
    pub fn new() -> Self {
        FakeSyncStorage {
            records: Arc::new(RwLock::new(HashMap::new())),
            last_synced_id: Arc::new(RwLock::new(None)),
        }
    }
}

#[async_trait]
impl SyncStorage for FakeSyncStorage {
    async fn add_object(&self, record: SyncRecord) -> Result<(), SyncStorageError> {
        let mut records = self.records.write().unwrap();
        records.insert(record.id, record);
        Ok(())
    }

    async fn set_object_status(
        &self,
        id: Uuid,
        status: SyncStatus,
    ) -> Result<(), SyncStorageError> {
        let mut records = self.records.write().unwrap();
        if let Some(record) = records.get_mut(&id) {
            record.status = status;
            Ok(())
        } else {
            Err(SyncStorageError::ObjectNotFound(id.to_string()))
        }
    }

    async fn get_object(&self, id: Uuid) -> Result<Option<SyncRecord>, SyncStorageError> {
        let records = self.records.read().unwrap();
        Ok(records.get(&id).cloned())
    }

    async fn get_objects_with_status(
        &self,
        status: SyncStatus,
    ) -> Result<Vec<SyncRecord>, SyncStorageError> {
        let records = self.records.read().unwrap();
        let mut matching_records: Vec<SyncRecord> = records
            .values()
            .filter(|record| record.status == status)
            .cloned()
            .collect();
        // Sort by timestamp to match SQLite behavior
        matching_records.sort_by_key(|record| record.timestamp);
        Ok(matching_records)
    }

    async fn get_last_object(&self) -> Result<Option<SyncRecord>, SyncStorageError> {
        let records = self.records.read().unwrap();
        // Find the record with the most recent timestamp
        Ok(records
            .values()
            .max_by_key(|record| record.timestamp)
            .cloned())
    }

    async fn get_last_synced_object_id(&self) -> Result<Option<Uuid>, SyncStorageError> {
        let last_id = self.last_synced_id.read().unwrap();
        Ok(*last_id)
    }

    async fn set_last_synced_object_id(&self, id: Uuid) -> Result<(), SyncStorageError> {
        let mut last_id = self.last_synced_id.write().unwrap();
        *last_id = Some(id);
        Ok(())
    }

    async fn clear_all(&self) -> Result<(), SyncStorageError> {
        let mut records = self.records.write().unwrap();
        records.clear();
        let mut last_id = self.last_synced_id.write().unwrap();
        *last_id = None;
        Ok(())
    }
}
