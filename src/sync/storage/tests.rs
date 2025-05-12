#[cfg(test)]
mod tests {
    use crate::sync::storage::{SyncStorage, FakeSyncStorage, SqliteSyncStorage, SyncStorageError};
    use chrono::{DateTime, Duration, Utc};
    use tempfile::tempdir;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use async_trait::async_trait;

    // We implement SyncStorage for Arc<T> to simplify sharing of storage implementations
    // across test cases while still maintaining proper ownership semantics.
    // This allows our test_storages() function to return Box<dyn SyncStorage>
    // where the storage might be an Arc<SqliteSyncStorage> or any other wrapped implementation.
    //
    // Without this, we would need to:
    // 1. Either recreate the storage for each test (expensive), OR
    // 2. Manually implement each function to unwrap, call, and then re-wrap
    //
    // This delegation pattern is cleaner and follows standard Rust conventions.
    #[async_trait]
    impl<T: SyncStorage + Send + Sync + 'static> SyncStorage for Arc<T> {
        async fn mark_object_synced(&self, object_key: &str, sync_timestamp: DateTime<Utc>)
            -> Result<(), SyncStorageError> {
            // Deref coercion to call the implementation on the inner type
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
            // Using a similar pattern as in the database tests, but with a safer approach.
            // We're avoiding double-nested unsafe blocks by using better encapsulation.
            Box::new(get_or_create_sqlite_storage(&db_path_str))
        }));

        storages
    }

    // Static reference to be shared between test runs
    // SAFETY: This is only used during testing and follows a singleton pattern
    // with write-once, read-many semantics.
    static mut SQLITE_STORAGE: Option<Arc<SqliteSyncStorage>> = None;
    static INIT_DONE: AtomicBool = AtomicBool::new(false);

    /// Creates or returns a cached SQLite database connection for testing
    ///
    /// This function provides a singleton pattern that ensures we use the same
    /// SQLite connection across test cases, avoiding expensive recreation.
    fn get_or_create_sqlite_storage(db_path: &str) -> Arc<SqliteSyncStorage> {
        // Check if already initialized via atomic flag
        if INIT_DONE.load(std::sync::atomic::Ordering::SeqCst) {
            // SAFETY: Safe because we only read after initialization, and initialization
            // happens exactly once due to the INIT_DONE atomic flag.
            return unsafe { SQLITE_STORAGE.as_ref().unwrap().clone() };
        }

        // Create new storage
        let storage = SqliteSyncStorage::new(db_path)
            .expect("Failed to create SQLite storage");
        let storage = Arc::new(storage);

        // Store it for future use
        // SAFETY: We're writing to this exactly once, and all future access is read-only.
        // The INIT_DONE atomic flag ensures this.
        unsafe {
            SQLITE_STORAGE = Some(storage.clone());
        }

        // Mark as initialized
        INIT_DONE.store(true, std::sync::atomic::Ordering::SeqCst);

        storage
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

            // Check last sync timestamp after first update
            let timestamp = storage.get_last_sync_timestamp().await.unwrap();
            assert!(timestamp.is_some(), "Last sync timestamp should be set after update");

            // Compare timestamps with some tolerance for precision differences
            if let Some(ts) = timestamp {
                let diff = (ts - now).num_milliseconds().abs();
                assert!(diff < 5, "Timestamp difference should be very small, was {}ms", diff);
            }

            // Update again with a newer timestamp (simulate a second sync cycle)
            let newer_now = Utc::now() + Duration::seconds(60);
            storage.update_last_sync_timestamp(newer_now).await.unwrap();

            // Verify the timestamp was updated to the newer time
            let updated_timestamp = storage.get_last_sync_timestamp().await.unwrap();
            assert!(updated_timestamp.is_some(), "Last sync timestamp should still be set after second update");

            // Check that the timestamp was actually updated to the newer value
            if let Some(ts) = updated_timestamp {
                // Check it's close to the newer timestamp
                let diff = (ts - newer_now).num_milliseconds().abs();
                assert!(diff < 5, "Timestamp difference should be very small, was {}ms", diff);

                // Check it's substantially different from the first timestamp
                if let Some(first_ts) = timestamp {
                    let diff_between_updates = (ts - first_ts).num_seconds();
                    assert!(diff_between_updates >= 59,
                        "Second timestamp should be significantly newer than first (expected ~60s difference, got {}s)",
                        diff_between_updates);
                }
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