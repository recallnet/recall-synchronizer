use crate::sync::storage::models::{SyncRecord, SyncStatus};
use crate::sync::storage::{FakeSyncStorage, SqliteSyncStorage, SyncStorage};
use crate::test_utils::load_test_config;
use chrono::{Duration, Utc};
use std::sync::Arc;
use uuid::Uuid;

// Type alias to simplify the complex type for storage factory functions
type StorageFactory =
    Box<dyn Fn() -> futures::future::BoxFuture<'static, Box<dyn SyncStorage + Send + Sync>>>;

/// Creates a test SyncRecord with default values
fn create_test_record(object_key: &str, timestamp: chrono::DateTime<Utc>) -> SyncRecord {
    SyncRecord::new(
        Uuid::new_v4(),
        object_key.to_string(),
        "test-bucket".to_string(),
        timestamp,
    )
}

/// Sets up test data with multiple records at different timestamps
async fn setup_test_data(storage: &dyn SyncStorage) -> Vec<SyncRecord> {
    let base_time = Utc::now() - Duration::hours(10);
    let mut test_data = Vec::new();

    // Clear any existing data
    storage.clear_all().await.unwrap();

    // Create 15 records with different timestamps
    for i in 0..15 {
        let object_key = format!("test/object_{:02}.jsonl", i);
        let timestamp = base_time + Duration::minutes(i * 30);
        let record = create_test_record(&object_key, timestamp);

        storage.add_object(record.clone()).await.unwrap();
        test_data.push(record);
    }

    test_data
}

/// Helper function to create test storage implementations
fn get_test_storages() -> Vec<StorageFactory> {
    let mut storages: Vec<StorageFactory> = vec![
        // Always include the FakeSyncStorage
        Box::new(|| {
            Box::pin(async {
                Box::new(FakeSyncStorage::new()) as Box<dyn SyncStorage + Send + Sync>
            })
        }),
    ];

    let test_config = load_test_config();

    if test_config.sqlite.enabled {
        storages.push(Box::new(|| {
            Box::pin(async move {
                // Create an in-memory SQLite storage for testing
                // Using ":memory:" creates a database that exists only in RAM
                let storage = SqliteSyncStorage::new(":memory:")
                    .expect("Failed to create in-memory SQLite storage");
                Box::new(storage) as Box<dyn SyncStorage + Send + Sync>
            })
        }));
    }

    storages
}

#[tokio::test]
async fn add_object_creates_new_record() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let record = create_test_record("test/object.jsonl", Utc::now());
        let id = record.id;

        storage.add_object(record).await.unwrap();

        // Check that the object exists with PendingSync status
        let retrieved = storage.get_object(id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().status, SyncStatus::PendingSync);
    }
}

#[tokio::test]
async fn set_object_status_updates_existing_record() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let record = create_test_record("test/object.jsonl", Utc::now());
        let id = record.id;

        storage.add_object(record).await.unwrap();

        // Update status to Processing
        storage
            .set_object_status(id, SyncStatus::Processing)
            .await
            .unwrap();
        let retrieved = storage.get_object(id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().status, SyncStatus::Processing);

        // Update status to Complete
        storage
            .set_object_status(id, SyncStatus::Complete)
            .await
            .unwrap();
        let retrieved = storage.get_object(id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().status, SyncStatus::Complete);
    }
}

#[tokio::test]
async fn set_object_status_fails_for_nonexistent_record() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let nonexistent_id = Uuid::new_v4();
        let result = storage
            .set_object_status(nonexistent_id, SyncStatus::Complete)
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::sync::storage::error::SyncStorageError::ObjectNotFound(ref id) if id == &nonexistent_id.to_string()
        ));
    }
}

#[tokio::test]
async fn get_object_returns_none_for_nonexistent_record() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let nonexistent_id = Uuid::new_v4();
        let retrieved = storage.get_object(nonexistent_id).await.unwrap();

        assert_eq!(retrieved, None);
    }
}

#[tokio::test]
async fn get_objects_with_status_returns_matching_records() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        let test_data = setup_test_data(storage.as_ref()).await;

        // Set different statuses for different records
        storage
            .set_object_status(test_data[0].id, SyncStatus::Processing)
            .await
            .unwrap();
        storage
            .set_object_status(test_data[1].id, SyncStatus::Processing)
            .await
            .unwrap();
        storage
            .set_object_status(test_data[2].id, SyncStatus::Complete)
            .await
            .unwrap();
        storage
            .set_object_status(test_data[3].id, SyncStatus::Complete)
            .await
            .unwrap();
        storage
            .set_object_status(test_data[4].id, SyncStatus::Complete)
            .await
            .unwrap();

        // Get records by status
        let pending_records = storage
            .get_objects_with_status(SyncStatus::PendingSync)
            .await
            .unwrap();
        let processing_records = storage
            .get_objects_with_status(SyncStatus::Processing)
            .await
            .unwrap();
        let complete_records = storage
            .get_objects_with_status(SyncStatus::Complete)
            .await
            .unwrap();

        assert_eq!(pending_records.len(), 10); // 15 - 5 with other statuses
        assert_eq!(processing_records.len(), 2);
        assert_eq!(complete_records.len(), 3);

        // Verify the correct records are returned
        assert!(processing_records.iter().any(|r| r.id == test_data[0].id));
        assert!(processing_records.iter().any(|r| r.id == test_data[1].id));
        assert!(complete_records.iter().any(|r| r.id == test_data[2].id));
        assert!(complete_records.iter().any(|r| r.id == test_data[3].id));
        assert!(complete_records.iter().any(|r| r.id == test_data[4].id));
    }
}

#[tokio::test]
async fn get_last_object_returns_most_recent_by_timestamp() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let base_time = Utc::now() - Duration::hours(5);

        // Add records with different timestamps
        let record1 = create_test_record("test/object1.jsonl", base_time);
        let record2 = create_test_record("test/object2.jsonl", base_time + Duration::hours(1));
        let record3 = create_test_record("test/object3.jsonl", base_time + Duration::hours(2));

        storage.add_object(record1).await.unwrap();
        storage.add_object(record2.clone()).await.unwrap();
        storage.add_object(record3.clone()).await.unwrap();

        let last_object = storage.get_last_object().await.unwrap();
        assert!(last_object.is_some());

        let last = last_object.unwrap();
        assert_eq!(last.id, record3.id);
        assert_eq!(last.object_key, record3.object_key);
    }
}

#[tokio::test]
async fn get_last_object_returns_none_when_empty() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let last_object = storage.get_last_object().await.unwrap();
        assert!(last_object.is_none());
    }
}

#[tokio::test]
async fn clear_all_removes_all_records() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        let test_data = setup_test_data(storage.as_ref()).await;

        // Verify some data exists
        let retrieved = storage.get_object(test_data[0].id).await.unwrap();
        assert!(retrieved.is_some());

        // Clear all data
        storage.clear_all().await.unwrap();

        // Verify all data is gone
        for record in &test_data {
            let retrieved = storage.get_object(record.id).await.unwrap();
            assert_eq!(retrieved, None);
        }

        // Verify get_last_object returns None
        let last_object = storage.get_last_object().await.unwrap();
        assert!(last_object.is_none());
    }
}

#[tokio::test]
async fn concurrent_status_updates_do_not_corrupt_data() {
    use tokio::task::JoinSet;

    for storage_factory in get_test_storages() {
        let storage = Arc::new(storage_factory().await);
        storage.clear_all().await.unwrap();

        // Create a single record
        let record = create_test_record("test/concurrent.jsonl", Utc::now());
        let id = record.id;
        storage.add_object(record).await.unwrap();

        let mut tasks = JoinSet::new();

        // Spawn multiple concurrent status updates
        for i in 0..10 {
            let storage = storage.clone();
            let status = if i % 2 == 0 {
                SyncStatus::Processing
            } else {
                SyncStatus::Complete
            };

            tasks.spawn(async move {
                storage.set_object_status(id, status).await.unwrap();
            });
        }

        // Wait for all tasks to complete
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }

        // Verify the object still exists with a valid status
        let final_record = storage.get_object(id).await.unwrap();
        assert!(final_record.is_some());
        assert!(matches!(
            final_record.unwrap().status,
            SyncStatus::Processing | SyncStatus::Complete
        ));
    }
}

#[tokio::test]
async fn get_objects_with_status_returns_records_ordered_by_timestamp() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let base_time = Utc::now() - Duration::hours(10);
        let mut records = Vec::new();

        // Add records in random order
        let timestamps = [
            base_time + Duration::hours(2),
            base_time,
            base_time + Duration::hours(5),
            base_time + Duration::hours(1),
            base_time + Duration::hours(3),
        ];

        for (i, timestamp) in timestamps.iter().enumerate() {
            let record = create_test_record(&format!("test/object_{}.jsonl", i), *timestamp);
            records.push(record.clone());
            storage.add_object(record).await.unwrap();
        }

        // Get records ordered by status (which orders by timestamp)
        let pending_records = storage
            .get_objects_with_status(SyncStatus::PendingSync)
            .await
            .unwrap();

        // Verify they are ordered by timestamp (ascending)
        for i in 1..pending_records.len() {
            assert!(
                pending_records[i].timestamp >= pending_records[i - 1].timestamp,
                "Records should be ordered by timestamp"
            );
        }

        // Verify get_last_object returns the newest
        let last_object = storage.get_last_object().await.unwrap().unwrap();
        assert_eq!(last_object.timestamp, base_time + Duration::hours(5));
    }
}

#[tokio::test]
async fn get_last_synced_object_id_returns_none_when_not_set() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let last_id = storage.get_last_synced_object_id(None).await.unwrap();
        assert_eq!(last_id, None);
    }
}

#[tokio::test]
async fn set_last_synced_object_id_overwrites_previous_value() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let first_id = Uuid::new_v4();
        storage
            .set_last_synced_object_id(first_id, None)
            .await
            .unwrap();

        let retrieved_id = storage.get_last_synced_object_id(None).await.unwrap();
        assert_eq!(retrieved_id, Some(first_id));

        let second_id = Uuid::new_v4();
        storage
            .set_last_synced_object_id(second_id, None)
            .await
            .unwrap();

        let retrieved_id = storage.get_last_synced_object_id(None).await.unwrap();
        assert_eq!(retrieved_id, Some(second_id));
        assert_ne!(retrieved_id, Some(first_id));
    }
}

#[tokio::test]
async fn clear_all_removes_all_last_synced_object_ids() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let comp1_id = Uuid::new_v4();
        let comp2_id = Uuid::new_v4();

        // Set multiple competition and global last synced IDs
        storage
            .set_last_synced_object_id(Uuid::new_v4(), None)
            .await
            .unwrap();
        storage
            .set_last_synced_object_id(Uuid::new_v4(), Some(comp1_id))
            .await
            .unwrap();
        storage
            .set_last_synced_object_id(Uuid::new_v4(), Some(comp2_id))
            .await
            .unwrap();

        // Clear all data
        storage.clear_all().await.unwrap();

        // All should be cleared
        assert_eq!(storage.get_last_synced_object_id(None).await.unwrap(), None);
        assert_eq!(
            storage
                .get_last_synced_object_id(Some(comp1_id))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            storage
                .get_last_synced_object_id(Some(comp2_id))
                .await
                .unwrap(),
            None
        );
    }
}

#[tokio::test]
async fn set_last_synced_object_id_with_competition_stores_separately() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let comp1_id = Uuid::new_v4();
        let comp2_id = Uuid::new_v4();
        let obj1_id = Uuid::new_v4();
        let obj2_id = Uuid::new_v4();
        let obj3_id = Uuid::new_v4();

        storage
            .set_last_synced_object_id(obj1_id, None)
            .await
            .unwrap();
        storage
            .set_last_synced_object_id(obj2_id, Some(comp1_id))
            .await
            .unwrap();
        storage
            .set_last_synced_object_id(obj3_id, Some(comp2_id))
            .await
            .unwrap();

        assert_eq!(
            storage
                .get_last_synced_object_id(Some(Uuid::new_v4()))
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            storage.get_last_synced_object_id(None).await.unwrap(),
            Some(obj1_id)
        );
        assert_eq!(
            storage
                .get_last_synced_object_id(Some(comp1_id))
                .await
                .unwrap(),
            Some(obj2_id)
        );
        assert_eq!(
            storage
                .get_last_synced_object_id(Some(comp2_id))
                .await
                .unwrap(),
            Some(obj3_id)
        );

        // Update one competition's ID shouldn't affect others
        let new_obj_id = Uuid::new_v4();
        storage
            .set_last_synced_object_id(new_obj_id, Some(comp1_id))
            .await
            .unwrap();

        assert_eq!(
            storage.get_last_synced_object_id(None).await.unwrap(),
            Some(obj1_id)
        );
        assert_eq!(
            storage
                .get_last_synced_object_id(Some(comp1_id))
                .await
                .unwrap(),
            Some(new_obj_id)
        );
        assert_eq!(
            storage
                .get_last_synced_object_id(Some(comp2_id))
                .await
                .unwrap(),
            Some(obj3_id)
        );
    }
}
