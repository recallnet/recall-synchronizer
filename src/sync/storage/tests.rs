use crate::sync::storage::models::{SyncRecord, SyncStatus};
use crate::sync::storage::{FakeSyncStorage, SqliteSyncStorage, SyncStorage};
use crate::test_utils::is_sqlite_enabled;
use chrono::{Duration, Utc};
use std::sync::Arc;
use uuid::Uuid;

// Type alias to simplify the complex type for storage factory functions
type StorageFactory =
    Box<dyn Fn() -> futures::future::BoxFuture<'static, Box<dyn SyncStorage + Send + Sync>>>;

/// Creates a test SyncRecord with default values
fn create_test_record(_object_key: &str, timestamp: chrono::DateTime<Utc>) -> SyncRecord {
    SyncRecord::new(
        Uuid::new_v4(),
        Uuid::new_v4(),
        Uuid::new_v4(),
        "TEST_DATA".to_string(),
        timestamp,
    )
}

/// Sets up test data with multiple records at different timestamps
async fn setup_test_data(storage: &dyn SyncStorage) -> Vec<SyncRecord> {
    let base_time = Utc::now() - Duration::hours(10);
    let mut test_data = Vec::new();

    storage.clear_all().await.unwrap();

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
    let mut storages: Vec<StorageFactory> = vec![Box::new(|| {
        Box::pin(async { Box::new(FakeSyncStorage::new()) as Box<dyn SyncStorage + Send + Sync> })
    })];

    if is_sqlite_enabled() {
        storages.push(Box::new(|| {
            Box::pin(async move {
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

        let retrieved = storage.get_object(id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(
            retrieved.unwrap().status,
            SyncStatus::PendingSync,
            "Expected status to be PendingSync after adding object"
        );
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

        storage
            .set_object_status(id, SyncStatus::Processing)
            .await
            .unwrap();
        let retrieved = storage.get_object(id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(
            retrieved.unwrap().status,
            SyncStatus::Processing,
            "Expected status to be Processing after update"
        );

        storage
            .set_object_status(id, SyncStatus::Complete)
            .await
            .unwrap();
        let retrieved = storage.get_object(id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(
            retrieved.unwrap().status,
            SyncStatus::Complete,
            "Expected status to be Complete after second update"
        );
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

        assert!(
            result.is_err(),
            "Expected error when setting status for nonexistent record"
        );
        assert!(
            matches!(
                result.unwrap_err(),
                crate::sync::storage::error::SyncStorageError::ObjectNotFound(ref id) if id == &nonexistent_id.to_string()
            ),
            "Expected ObjectNotFound error for nonexistent record"
        );
    }
}

#[tokio::test]
async fn get_object_returns_none_for_nonexistent_record() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let nonexistent_id = Uuid::new_v4();
        let retrieved = storage.get_object(nonexistent_id).await.unwrap();

        assert_eq!(
            retrieved, None,
            "Expected None when trying to get a nonexistent object"
        );
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

        let record1 = create_test_record("test/object1.jsonl", base_time);
        let record2 = create_test_record("test/object2.jsonl", base_time + Duration::hours(1));
        let record3 = create_test_record("test/object3.jsonl", base_time + Duration::hours(2));

        storage.add_object(record1).await.unwrap();
        storage.add_object(record2.clone()).await.unwrap();
        storage.add_object(record3.clone()).await.unwrap();

        let last_object = storage.get_last_object().await.unwrap();
        assert!(
            last_object.is_some(),
            "Expected Some when objects exist in storage"
        );

        let last = last_object.unwrap();
        assert_eq!(
            last.id, record3.id,
            "Expected last object to be the most recent"
        );
        assert_eq!(
            last.competition_id, record3.competition_id,
            "Expected last competition_id to match the most recent record"
        );
    }
}

#[tokio::test]
async fn get_last_object_returns_none_when_empty() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let last_object = storage.get_last_object().await.unwrap();
        assert!(
            last_object.is_none(),
            "Expected None when no objects exist in storage"
        );
    }
}

#[tokio::test]
async fn clear_all_removes_all_records() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        let test_data = setup_test_data(storage.as_ref()).await;

        let retrieved = storage.get_object(test_data[0].id).await.unwrap();
        assert!(retrieved.is_some());

        storage.clear_all().await.unwrap();

        for record in &test_data {
            let retrieved = storage.get_object(record.id).await.unwrap();
            assert_eq!(
                retrieved, None,
                "Record {} should be removed after clear_all",
                record.id
            );
        }

        let last_object = storage.get_last_object().await.unwrap();
        assert!(
            last_object.is_none(),
            "Last object should be None after clear_all"
        );
    }
}

#[tokio::test]
async fn concurrent_status_updates_do_not_corrupt_data() {
    use tokio::task::JoinSet;

    for storage_factory in get_test_storages() {
        let storage = Arc::new(storage_factory().await);
        storage.clear_all().await.unwrap();

        let record = create_test_record("test/concurrent.jsonl", Utc::now());
        let id = record.id;
        storage.add_object(record).await.unwrap();

        let mut tasks = JoinSet::new();

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

        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }

        let final_record = storage.get_object(id).await.unwrap();
        assert!(final_record.is_some());
        assert!(
            matches!(
                final_record.unwrap().status,
                SyncStatus::Processing | SyncStatus::Complete
            ),
            "Expected status to be Processing or Complete"
        );
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

        let pending_records = storage
            .get_objects_with_status(SyncStatus::PendingSync)
            .await
            .unwrap();

        for i in 1..pending_records.len() {
            assert!(
                pending_records[i].timestamp >= pending_records[i - 1].timestamp,
                "Records should be ordered by timestamp"
            );
        }

        let last_object = storage.get_last_object().await.unwrap().unwrap();
        assert_eq!(
            last_object.timestamp,
            base_time + Duration::hours(5),
            "Last object should be the one with the latest timestamp"
        );
    }
}

#[tokio::test]
async fn get_last_synced_object_id_returns_none_when_not_set() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let last_id = storage.get_last_synced_object_id(None).await.unwrap();
        assert_eq!(last_id, None, "Expected None when no last synced ID is set");
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
        assert_eq!(
            retrieved_id,
            Some(first_id),
            "Expected to retrieve the first ID"
        );

        let second_id = Uuid::new_v4();
        storage
            .set_last_synced_object_id(second_id, None)
            .await
            .unwrap();

        let retrieved_id = storage.get_last_synced_object_id(None).await.unwrap();
        assert_eq!(
            retrieved_id,
            Some(second_id),
            "Expected to retrieve the second ID"
        );
        assert_ne!(
            retrieved_id,
            Some(first_id),
            "Expected the second ID to overwrite the first"
        );
    }
}

#[tokio::test]
async fn clear_all_removes_all_last_synced_object_ids() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let comp1_id = Uuid::new_v4();
        let comp2_id = Uuid::new_v4();

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

        storage.clear_all().await.unwrap();

        assert_eq!(
            storage.get_last_synced_object_id(None).await.unwrap(),
            None,
            "Expected None after clear_all for global last synced ID"
        );
        assert_eq!(
            storage
                .get_last_synced_object_id(Some(comp1_id))
                .await
                .unwrap(),
            None,
            "Expected None after clear_all for competition 1 last synced ID"
        );
        assert_eq!(
            storage
                .get_last_synced_object_id(Some(comp2_id))
                .await
                .unwrap(),
            None,
            "Expected None after clear_all for competition 2 last synced ID"
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
            Some(obj1_id),
            "Expected to retrieve the global last synced ID"
        );
        assert_eq!(
            storage
                .get_last_synced_object_id(Some(comp1_id))
                .await
                .unwrap(),
            Some(obj2_id),
            "Expected to retrieve the second object's ID for competition 1"
        );
        assert_eq!(
            storage
                .get_last_synced_object_id(Some(comp2_id))
                .await
                .unwrap(),
            Some(obj3_id),
            "Expected to retrieve the third object's ID for competition 2"
        );

        // Update one competition's ID shouldn't affect others
        let new_obj_id = Uuid::new_v4();
        storage
            .set_last_synced_object_id(new_obj_id, Some(comp1_id))
            .await
            .unwrap();

        assert_eq!(
            storage.get_last_synced_object_id(None).await.unwrap(),
            Some(obj1_id),
            "Expected to retrieve the global last synced ID"
        );
        assert_eq!(
            storage
                .get_last_synced_object_id(Some(comp1_id))
                .await
                .unwrap(),
            Some(new_obj_id),
            "Expected to retrieve the updated object's ID for competition 1"
        );
        assert_eq!(
            storage
                .get_last_synced_object_id(Some(comp2_id))
                .await
                .unwrap(),
            Some(obj3_id),
            "Expected to retrieve the third object's ID for competition 2"
        );
    }
}
