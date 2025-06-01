use crate::config::{Config, DatabaseConfig, RecallConfig, S3Config, SyncConfig};
use crate::db::{Database, FakeDatabase};
use crate::recall::fake::FakeRecallStorage;
use crate::s3::{FakeStorage, Storage};
use crate::sync::{
    storage::{FakeSyncStorage, SyncRecord, SyncStatus, SyncStorage},
    synchronizer::Synchronizer,
};
use crate::test_utils::create_test_object_index;
use bytes::Bytes;
use chrono::{Duration, Utc};
use std::sync::Arc;

// Helper to create a test config
fn create_test_config() -> Config {
    Config {
        database: DatabaseConfig {
            url: "postgres://fake:fake@localhost:5432/fake".to_string(),
            max_connections: 5,
        },
        s3: S3Config {
            endpoint: Some("http://localhost:9000".to_string()),
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            access_key_id: Some("test".to_string()),
            secret_access_key: Some("test".to_string()),
        },
        recall: RecallConfig {
            private_key: "fake-key".to_string(),
            network: "localnet".to_string(),
            config_path: Some("networks.toml".to_string()),
            bucket: None,
        },
        sync: SyncConfig {
            interval_seconds: 60,
            batch_size: 10,
            workers: 2,
            retry_limit: 3,
            retry_delay_seconds: 5,
            state_db_path: ":memory:".to_string(),
        },
    }
}

// Setup a test environment with fake implementations
async fn setup_test_env() -> (
    FakeDatabase,
    FakeSyncStorage,
    FakeStorage,
    FakeRecallStorage,
    Config,
) {
    let database = FakeDatabase::new();
    let now = Utc::now();

    let object1 = create_test_object_index("test/object1.jsonl", now);
    let mut object2 = create_test_object_index("test/object2.jsonl", now + Duration::hours(1));
    object2.size_bytes = Some(2048);

    database.add_object(object1).await.unwrap();
    database.add_object(object2).await.unwrap();

    let sync_storage = FakeSyncStorage::new();

    let config = create_test_config();

    let s3_storage = FakeStorage::new();

    s3_storage
        .add_object("test/object1.jsonl", Bytes::from("Test data for object1"))
        .await
        .unwrap();

    s3_storage
        .add_object("test/object2.jsonl", Bytes::from("Test data for object2"))
        .await
        .unwrap();

    let recall_storage = FakeRecallStorage::new();

    (database, sync_storage, s3_storage, recall_storage, config)
}

#[tokio::test]
async fn when_no_filters_applied_all_objects_are_synchronized() {
    let (database, sync_storage, s3_storage, recall_storage, config) = setup_test_env().await;

    let objects = database.get_objects_to_sync(100, None).await.unwrap();
    let num_objects = objects.len();

    let sync_storage = Arc::new(sync_storage);

    let synchronizer = Synchronizer::with_storage(
        Arc::new(database),
        sync_storage.clone(),
        Arc::new(s3_storage),
        Arc::new(recall_storage),
        config,
        false,
    );

    synchronizer.run(None, None).await.unwrap();

    for obj in objects {
        let status = sync_storage.get_object_status(obj.id).await.unwrap();
        assert_eq!(
            status,
            Some(SyncStatus::Complete),
            "Object {} should be marked as complete",
            obj.object_key
        );
    }

    let completed_objects = sync_storage
        .get_objects_with_status(SyncStatus::Complete)
        .await
        .unwrap();
    assert_eq!(
        completed_objects.len(),
        num_objects,
        "All objects should be synchronized"
    );
}

#[tokio::test]
async fn when_competition_id_filter_is_applied_only_matching_objects_are_synchronized() {
    let (database, sync_storage, s3_storage, recall_storage, config) = setup_test_env().await;

    let competition_id = uuid::Uuid::new_v4();
    let now = Utc::now();
    let mut filtered_object = create_test_object_index("test/filtered.jsonl", now);
    filtered_object.competition_id = Some(competition_id);
    database.add_object(filtered_object.clone()).await.unwrap();

    s3_storage
        .add_object("test/filtered.jsonl", Bytes::from("Filtered test data"))
        .await
        .unwrap();

    let database = Arc::new(database);
    let sync_storage = Arc::new(sync_storage);

    let synchronizer = Synchronizer::with_storage(
        database.clone(),
        sync_storage.clone(),
        Arc::new(s3_storage),
        Arc::new(recall_storage),
        config,
        false,
    );

    synchronizer
        .run(Some(competition_id.to_string()), None)
        .await
        .unwrap();

    let status = sync_storage
        .get_object_status(filtered_object.id)
        .await
        .unwrap();
    assert_eq!(
        status,
        Some(SyncStatus::Complete),
        "Filtered object should be marked as complete"
    );

    let all_objects = database.get_objects_to_sync(100, None).await.unwrap();
    for obj in all_objects {
        if obj.competition_id != Some(competition_id) {
            let status = sync_storage.get_object_status(obj.id).await.unwrap();
            assert_eq!(
                status, None,
                "Object {} without matching competition_id should not be synchronized",
                obj.object_key
            );
        }
    }
}

#[tokio::test]
async fn when_timestamp_filter_is_applied_only_newer_objects_are_synchronized() {
    let (database, sync_storage, s3_storage, recall_storage, config) = setup_test_env().await;

    let old_time = Utc::now() - Duration::days(7);
    let old_object = create_test_object_index("test/old.jsonl", old_time);
    database.add_object(old_object.clone()).await.unwrap();

    s3_storage
        .add_object("test/old.jsonl", Bytes::from("Old test data"))
        .await
        .unwrap();

    let database = Arc::new(database);
    let sync_storage = Arc::new(sync_storage);

    let synchronizer = Synchronizer::with_storage(
        database.clone(),
        sync_storage.clone(),
        Arc::new(s3_storage),
        Arc::new(recall_storage),
        config,
        false,
    );

    let filter_time = Utc::now() - Duration::days(2);
    synchronizer
        .run(None, Some(filter_time.to_rfc3339()))
        .await
        .unwrap();

    let status = sync_storage.get_object_status(old_object.id).await.unwrap();
    assert_eq!(
        status, None,
        "Old object should not be synchronized when using timestamp filter"
    );

    let all_objects = database.get_objects_to_sync(100, None).await.unwrap();
    for obj in all_objects {
        if obj.object_last_modified_at > filter_time {
            let status = sync_storage.get_object_status(obj.id).await.unwrap();
            assert_eq!(
                status,
                Some(SyncStatus::Complete),
                "Newer object {} should be synchronized",
                obj.object_key
            );
        }
    }
}

#[tokio::test]
async fn when_object_is_already_being_processed_it_is_skipped() {
    let (database, sync_storage, s3_storage, recall_storage, config) = setup_test_env().await;

    let test_object = create_test_object_index("test/concurrent.jsonl", Utc::now());
    database.add_object(test_object.clone()).await.unwrap();

    s3_storage
        .add_object("test/concurrent.jsonl", Bytes::from("Concurrent test data"))
        .await
        .unwrap();

    let database = Arc::new(database);
    let sync_storage = Arc::new(sync_storage);
    let s3_storage = Arc::new(s3_storage);
    let recall_storage = Arc::new(recall_storage);

    let sync_record = SyncRecord::new(
        test_object.id,
        test_object.object_key.clone(),
        test_object.bucket_name.clone(),
        test_object.object_last_modified_at,
    );
    sync_storage.add_object(sync_record).await.unwrap();

    sync_storage
        .set_object_status(test_object.id, SyncStatus::Processing)
        .await
        .unwrap();

    let synchronizer = Synchronizer::with_storage(
        database.clone(),
        sync_storage.clone(),
        s3_storage.clone(),
        recall_storage.clone(),
        config,
        false,
    );

    synchronizer.run(None, None).await.unwrap();

    // The object should still be in Processing status since we didn't let our synchronizer complete it
    let status = sync_storage
        .get_object_status(test_object.id)
        .await
        .unwrap();
    assert_eq!(status, Some(SyncStatus::Processing));

    // Now mark it as complete and run again
    sync_storage
        .set_object_status(test_object.id, SyncStatus::Complete)
        .await
        .unwrap();
    synchronizer.run(None, None).await.unwrap();

    // The object should still be complete (not processed again)
    let status = sync_storage
        .get_object_status(test_object.id)
        .await
        .unwrap();
    assert_eq!(status, Some(SyncStatus::Complete));
}
