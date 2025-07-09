use crate::db::data_type::DataType;
use crate::sync::storage::models::{FailureType, SyncRecord, SyncStatus};
use crate::sync::storage::{FakeSyncStorage, SqliteSyncStorage, SyncStorage};
use crate::test_utils::is_sqlite_enabled;
use chrono::{Duration, Utc};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

// Type alias to simplify the complex type for storage factory functions
type StorageFactory =
    Box<dyn Fn() -> futures::future::BoxFuture<'static, Box<dyn SyncStorage + Send + Sync>>>;

/// Creates a test SyncRecord with default values
fn create_test_record(_object_key: &str, timestamp: chrono::DateTime<Utc>) -> SyncRecord {
    SyncRecord::new(
        Uuid::new_v4(),
        Some(Uuid::new_v4()),
        Some(Uuid::new_v4()),
        "trade".into(),
        timestamp,
        SyncStatus::PendingSync,
    )
}

/// Sets up test data with multiple records at different timestamps
async fn setup_test_data(storage: &dyn SyncStorage) -> Vec<SyncRecord> {
    let base_time = Utc::now() - Duration::hours(10);
    let mut test_data = Vec::new();

    storage.clear_all().await.unwrap();

    for i in 0..15 {
        let object_key = format!("test/object_{i:02}.jsonl");
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
async fn clear_all_removes_all_records_and_state() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        let test_data = setup_test_data(storage.as_ref()).await;

        // Set last synced object ID
        storage
            .set_last_synced_object_id(Uuid::new_v4())
            .await
            .unwrap();

        let retrieved = storage.get_object(test_data[0].id).await.unwrap();
        assert!(retrieved.is_some());

        storage.clear_all().await.unwrap();

        // Verify all records are removed
        for record in &test_data {
            let retrieved = storage.get_object(record.id).await.unwrap();
            assert_eq!(
                retrieved, None,
                "Record {} should be removed after clear_all",
                record.id
            );
        }

        // Verify last object is None
        let last_object = storage.get_last_object().await.unwrap();
        assert!(
            last_object.is_none(),
            "Last object should be None after clear_all"
        );

        // Verify last synced object ID is cleared
        assert_eq!(
            storage.get_last_synced_object_id().await.unwrap(),
            None,
            "Expected None after clear_all for last synced ID"
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
            let record = create_test_record(&format!("test/object_{i}.jsonl"), *timestamp);
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

        let last_id = storage.get_last_synced_object_id().await.unwrap();
        assert_eq!(last_id, None, "Expected None when no last synced ID is set");
    }
}

#[tokio::test]
async fn set_last_synced_object_id_overwrites_previous_value() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let first_id = Uuid::new_v4();
        storage.set_last_synced_object_id(first_id).await.unwrap();

        let retrieved_id = storage.get_last_synced_object_id().await.unwrap();
        assert_eq!(
            retrieved_id,
            Some(first_id),
            "Expected to retrieve the first ID"
        );

        let second_id = Uuid::new_v4();
        storage.set_last_synced_object_id(second_id).await.unwrap();

        let retrieved_id = storage.get_last_synced_object_id().await.unwrap();
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
async fn record_failure_with_temporary_failure() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let record = create_test_record("test/object.jsonl", Utc::now());
        let id = record.id;

        storage.add_object(record).await.unwrap();

        storage
            .record_failure(
                id,
                FailureType::S3DataRetrieval,
                "Temporary S3 error".to_string(),
                false,
            )
            .await
            .unwrap();

        let retrieved = storage.get_object(id).await.unwrap().unwrap();
        assert_eq!(retrieved.status, SyncStatus::Failed);
        assert_eq!(retrieved.retry_count, 1);
        assert_eq!(retrieved.last_error, Some("Temporary S3 error".to_string()));
        assert_eq!(retrieved.failure_type, Some(FailureType::S3DataRetrieval));
    }
}

#[tokio::test]
async fn record_failure_with_permanent_failure() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let record = create_test_record("test/object.jsonl", Utc::now());
        let id = record.id;

        storage.add_object(record).await.unwrap();

        storage
            .record_failure(
                id,
                FailureType::RecallStorage,
                "Permanent recall error".to_string(),
                true,
            )
            .await
            .unwrap();

        let retrieved = storage.get_object(id).await.unwrap().unwrap();
        assert_eq!(retrieved.status, SyncStatus::FailedPermanently);
        assert_eq!(retrieved.retry_count, 1);
        assert_eq!(
            retrieved.last_error,
            Some("Permanent recall error".to_string())
        );
        assert_eq!(retrieved.failure_type, Some(FailureType::RecallStorage));
    }
}

#[tokio::test]
async fn record_failure_increments_retry_count() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let record = create_test_record("test/object.jsonl", Utc::now());
        let id = record.id;

        storage.add_object(record).await.unwrap();

        storage
            .record_failure(
                id,
                FailureType::S3DataRetrieval,
                "First failure".to_string(),
                false,
            )
            .await
            .unwrap();

        storage
            .record_failure(
                id,
                FailureType::S3DataRetrieval,
                "Second failure".to_string(),
                false,
            )
            .await
            .unwrap();

        storage
            .record_failure(
                id,
                FailureType::RecallStorage,
                "Third failure".to_string(),
                false,
            )
            .await
            .unwrap();

        let retrieved = storage.get_object(id).await.unwrap().unwrap();
        assert_eq!(retrieved.status, SyncStatus::Failed);
        assert_eq!(retrieved.retry_count, 3);
        assert_eq!(retrieved.last_error, Some("Third failure".to_string()));
        assert_eq!(retrieved.failure_type, Some(FailureType::RecallStorage));
    }
}

#[tokio::test]
async fn get_failed_objects_returns_only_failed_records() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let mut records = Vec::new();
        for i in 0..5 {
            let record = create_test_record(&format!("test/object_{}.jsonl", i), Utc::now());
            records.push(record.clone());
            storage.add_object(record).await.unwrap();
        }

        storage
            .set_object_status(records[0].id, SyncStatus::Complete)
            .await
            .unwrap();
        storage
            .set_object_status(records[1].id, SyncStatus::Processing)
            .await
            .unwrap();

        storage
            .record_failure(
                records[2].id,
                FailureType::S3DataRetrieval,
                "Failed".to_string(),
                false,
            )
            .await
            .unwrap();

        storage
            .record_failure(
                records[3].id,
                FailureType::RecallStorage,
                "Failed".to_string(),
                false,
            )
            .await
            .unwrap();

        storage
            .record_failure(
                records[4].id,
                FailureType::Other,
                "Permanent fail".to_string(),
                true,
            )
            .await
            .unwrap();

        let failed_objects = storage.get_failed_objects().await.unwrap();

        assert_eq!(failed_objects.len(), 2);
        let failed_ids: Vec<_> = failed_objects.iter().map(|r| r.id).collect();
        assert!(failed_ids.contains(&records[2].id));
        assert!(failed_ids.contains(&records[3].id));
        assert!(!failed_ids.contains(&records[4].id));
    }
}

#[tokio::test]
async fn get_permanently_failed_objects_returns_only_permanently_failed_records() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let mut records = Vec::new();
        for i in 0..5 {
            let record = create_test_record(&format!("test/object_{}.jsonl", i), Utc::now());
            records.push(record.clone());
            storage.add_object(record).await.unwrap();
        }

        storage
            .set_object_status(records[0].id, SyncStatus::Complete)
            .await
            .unwrap();
        storage
            .set_object_status(records[1].id, SyncStatus::Processing)
            .await
            .unwrap();

        storage
            .record_failure(
                records[2].id,
                FailureType::S3DataRetrieval,
                "Failed".to_string(),
                false,
            )
            .await
            .unwrap();

        storage
            .record_failure(
                records[3].id,
                FailureType::RecallStorage,
                "Permanent fail 1".to_string(),
                true,
            )
            .await
            .unwrap();

        storage
            .record_failure(
                records[4].id,
                FailureType::Other,
                "Permanent fail 2".to_string(),
                true,
            )
            .await
            .unwrap();

        let permanently_failed = storage.get_permanently_failed_objects().await.unwrap();

        assert_eq!(permanently_failed.len(), 2);
        let failed_ids: Vec<_> = permanently_failed.iter().map(|r| r.id).collect();
        assert!(failed_ids.contains(&records[3].id));
        assert!(failed_ids.contains(&records[4].id));
        assert!(!failed_ids.contains(&records[2].id));
    }
}

#[tokio::test]
async fn sync_record_with_object_index_data() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        // Create a record with ObjectIndex data
        let mut record = create_test_record("test/object.jsonl", Utc::now());
        record.object_key = Some("s3://bucket/key".to_string());
        record.size_bytes = Some(1024);
        record.metadata = Some(serde_json::json!({"key": "value"}));
        record.event_timestamp = Some(Utc::now());
        record.data = Some(b"test data".to_vec());

        storage.add_object(record.clone()).await.unwrap();

        let retrieved = storage.get_object(record.id).await.unwrap().unwrap();
        assert_eq!(retrieved.object_key, Some("s3://bucket/key".to_string()));
        assert_eq!(retrieved.size_bytes, Some(1024));
        assert_eq!(
            retrieved.metadata,
            Some(serde_json::json!({"key": "value"}))
        );
        assert_eq!(retrieved.data, Some(b"test data".to_vec()));
        assert!(retrieved.event_timestamp.is_some());
    }
}

#[tokio::test]
async fn sync_record_from_object_index_conversion() {
    use crate::db::ObjectIndex;

    let object_index = ObjectIndex {
        id: Uuid::new_v4(),
        object_key: Some("s3://bucket/key".to_string()),
        competition_id: Some(Uuid::new_v4()),
        agent_id: Some(Uuid::new_v4()),
        data_type: DataType::from_str("trade").unwrap(),
        size_bytes: Some(2048),
        metadata: Some(serde_json::json!({"test": "data"})),
        event_timestamp: Some(Utc::now()),
        created_at: Utc::now(),
        data: Some(b"embedded data".to_vec()),
    };

    let sync_record = SyncRecord::from_object_index(&object_index, SyncStatus::PendingSync);

    assert_eq!(sync_record.id, object_index.id);
    assert_eq!(sync_record.object_key, object_index.object_key);
    assert_eq!(sync_record.competition_id, object_index.competition_id);
    assert_eq!(sync_record.agent_id, object_index.agent_id);
    assert_eq!(sync_record.data_type, object_index.data_type);
    assert_eq!(sync_record.size_bytes, object_index.size_bytes);
    assert_eq!(sync_record.metadata, object_index.metadata);
    assert_eq!(sync_record.event_timestamp, object_index.event_timestamp);
    assert_eq!(sync_record.data, object_index.data);
    assert_eq!(sync_record.status, SyncStatus::PendingSync);
    assert_eq!(sync_record.retry_count, 0);
    assert_eq!(sync_record.last_error, None);
    assert_eq!(sync_record.failure_type, None);
}

#[tokio::test]
async fn sync_record_to_object_index_conversion() {
    let mut sync_record = create_test_record("test/object.jsonl", Utc::now());
    sync_record.object_key = Some("s3://bucket/key".to_string());
    sync_record.size_bytes = Some(1024);
    sync_record.metadata = Some(serde_json::json!({"key": "value"}));
    sync_record.event_timestamp = Some(Utc::now());
    sync_record.data = Some(b"test data".to_vec());

    let object_index = sync_record.to_object_index();

    assert_eq!(object_index.id, sync_record.id);
    assert_eq!(object_index.object_key, sync_record.object_key);
    assert_eq!(object_index.competition_id, sync_record.competition_id);
    assert_eq!(object_index.agent_id, sync_record.agent_id);
    assert_eq!(object_index.data_type, sync_record.data_type);
    assert_eq!(object_index.size_bytes, sync_record.size_bytes);
    assert_eq!(object_index.metadata, sync_record.metadata);
    assert_eq!(object_index.event_timestamp, sync_record.event_timestamp);
    assert_eq!(object_index.data, sync_record.data);
    assert_eq!(object_index.created_at, sync_record.timestamp);
}

#[tokio::test]
async fn record_failure_on_nonexistent_object_fails() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let nonexistent_id = Uuid::new_v4();

        let result = storage
            .record_failure(
                nonexistent_id,
                FailureType::S3DataRetrieval,
                "Error".to_string(),
                false,
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::sync::storage::error::SyncStorageError::ObjectNotFound(ref id) if id == &nonexistent_id.to_string()
        ));
    }
}

#[tokio::test]
async fn retry_workflow_simulation() {
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

        storage
            .record_failure(
                id,
                FailureType::S3DataRetrieval,
                "Temporary network error".to_string(),
                false,
            )
            .await
            .unwrap();

        let failed_objects = storage.get_failed_objects().await.unwrap();
        assert_eq!(failed_objects.len(), 1);
        assert_eq!(failed_objects[0].id, id);
        assert_eq!(failed_objects[0].retry_count, 1);

        storage
            .set_object_status(id, SyncStatus::Processing)
            .await
            .unwrap();

        storage
            .record_failure(
                id,
                FailureType::RecallStorage,
                "Recall service unavailable".to_string(),
                false,
            )
            .await
            .unwrap();

        let failed_objects = storage.get_failed_objects().await.unwrap();
        assert_eq!(failed_objects.len(), 1);
        assert_eq!(failed_objects[0].retry_count, 2);

        storage
            .set_object_status(id, SyncStatus::Processing)
            .await
            .unwrap();

        storage
            .record_failure(
                id,
                FailureType::Other,
                "Data corruption detected".to_string(),
                true,
            )
            .await
            .unwrap();

        let failed_objects = storage.get_failed_objects().await.unwrap();
        assert_eq!(failed_objects.len(), 0);

        let permanently_failed = storage.get_permanently_failed_objects().await.unwrap();
        assert_eq!(permanently_failed.len(), 1);
        assert_eq!(permanently_failed[0].id, id);
        assert_eq!(permanently_failed[0].retry_count, 3);
    }
}

#[tokio::test]
async fn large_data_storage_and_retrieval() {
    for storage_factory in get_test_storages() {
        let storage = storage_factory().await;
        storage.clear_all().await.unwrap();

        let mut record = create_test_record("test/large.jsonl", Utc::now());
        let large_data = vec![0u8; 1024 * 1024]; // 1MB of data
        record.data = Some(large_data.clone());
        record.size_bytes = Some(large_data.len() as i64);

        storage.add_object(record.clone()).await.unwrap();

        let retrieved = storage.get_object(record.id).await.unwrap().unwrap();
        assert_eq!(retrieved.data, Some(large_data));
        assert_eq!(retrieved.size_bytes, Some(1024 * 1024));
    }
}

