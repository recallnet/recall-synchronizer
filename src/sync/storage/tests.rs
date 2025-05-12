#[cfg(test)]
mod tests {
    use crate::sync::storage::{SyncStorage, FakeSyncStorage, SqliteSyncStorage, SyncStorageError};
    use chrono::{DateTime, Duration, Utc};
    use tempfile::tempdir;
    use std::sync::Arc;
    use async_trait::async_trait;

    // Add trait implementation for Arc<T> where T implements SyncStorage
    #[async_trait]
    impl<T: SyncStorage + Send + Sync + 'static> SyncStorage for Arc<T> {
        async fn mark_object_synced(&self, object_key: &str, sync_timestamp: DateTime<Utc>) 
            -> Result<(), SyncStorageError> {
            (**self).mark_object_synced(object_key, sync_timestamp).await
        }

        async fn is_object_synced(&self, object_key: &str) -> Result<bool, SyncStorageError> {
            (**self).is_object_synced(object_key).await
        }

        async fn update_last_sync_timestamp(&self, timestamp: DateTime<Utc>) 
            -> Result<(), SyncStorageError> {
            (**self).update_last_sync_timestamp(timestamp).await
        }

        async fn get_last_sync_timestamp(&self) -> Result<Option<DateTime<Utc>>, SyncStorageError> {
            (**self).get_last_sync_timestamp().await
        }
    }

    // Helper function to create test storage implementations
    fn get_test_storages() -> Vec<Box<dyn Fn() -> Box<dyn SyncStorage + Send + Sync>>> {
        let mut storages: Vec<Box<dyn Fn() -> Box<dyn SyncStorage + Send + Sync>>> = vec![
            // Always include the FakeSyncStorage
            Box::new(|| {
                let storage = FakeSyncStorage::new();
                Box::new(storage)
            }),
        ];

        // Add the SQLite implementation with a temporary file
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let db_path = temp_dir.path().join("sync_test.db");
        let db_path_str = db_path.to_str().unwrap().to_string();
        
        storages.push(Box::new(move || {
            // Static reference to be shared between test runs
            static mut SQLITE_STORAGE: Option<Arc<SqliteSyncStorage>> = None;
            
            // Create instance once
            if unsafe { SQLITE_STORAGE.is_none() } {
                let storage = SqliteSyncStorage::new(&db_path_str)
                    .expect("Failed to create SQLite storage");
                
                unsafe {
                    SQLITE_STORAGE = Some(Arc::new(storage));
                }
            }
            
            let storage = unsafe { SQLITE_STORAGE.as_ref().unwrap().clone() };
            Box::new(storage)
        }));

        storages
    }

    #[tokio::test]
    async fn test_mark_and_check_object_synced() {
        for storage_factory in get_test_storages() {
            let storage = storage_factory();
            
            // Test object synced status before marking
            let is_synced = storage.is_object_synced("test/object.jsonl").await.unwrap();
            assert!(!is_synced, "Object should not be synced initially");
            
            // Mark object as synced
            let now = Utc::now();
            storage.mark_object_synced("test/object.jsonl", now).await.unwrap();
            
            // Check object synced status after marking
            let is_synced = storage.is_object_synced("test/object.jsonl").await.unwrap();
            assert!(is_synced, "Object should be synced after marking");
        }
    }

    #[tokio::test]
    async fn test_last_sync_timestamp() {
        for storage_factory in get_test_storages() {
            let storage = storage_factory();
            
            // Initially, last sync timestamp should be None
            let timestamp = storage.get_last_sync_timestamp().await.unwrap();
            assert!(timestamp.is_none(), "Initial last sync timestamp should be None");
            
            // Update last sync timestamp
            let now = Utc::now();
            storage.update_last_sync_timestamp(now).await.unwrap();
            
            // Check last sync timestamp after update
            let timestamp = storage.get_last_sync_timestamp().await.unwrap();
            assert!(timestamp.is_some(), "Last sync timestamp should be set after update");
            
            // Compare timestamps with some tolerance for precision differences
            if let Some(ts) = timestamp {
                let diff = (ts - now).num_milliseconds().abs();
                assert!(diff < 5, "Timestamp difference should be very small, was {}ms", diff);
            }
        }
    }

    #[tokio::test]
    async fn test_multiple_objects() {
        for storage_factory in get_test_storages() {
            let storage = storage_factory();
            
            // Mark multiple objects as synced
            let now = Utc::now();
            storage.mark_object_synced("test/object1.jsonl", now).await.unwrap();
            storage.mark_object_synced("test/object2.jsonl", now + Duration::seconds(1)).await.unwrap();
            
            // Check both objects are synced
            let is_synced1 = storage.is_object_synced("test/object1.jsonl").await.unwrap();
            let is_synced2 = storage.is_object_synced("test/object2.jsonl").await.unwrap();
            assert!(is_synced1, "Object 1 should be synced");
            assert!(is_synced2, "Object 2 should be synced");
            
            // Check non-existent object is not synced
            let is_synced3 = storage.is_object_synced("test/object3.jsonl").await.unwrap();
            assert!(!is_synced3, "Non-existent object should not be synced");
        }
    }
}