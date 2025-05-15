use crate::config::{Config, DatabaseConfig, RecallConfig, S3Config, SyncConfig};
use crate::db::{Database, FakeDatabase};
use crate::recall::test_utils::FakeRecallConnector;
use crate::s3::test_utils::FakeS3Connector;
use crate::sync::storage::{FakeSyncStorage, SyncStorage};
use crate::sync::Synchronizer;
use crate::test_utils::create_test_object_index;
use bytes::Bytes;
use chrono::{Duration, Utc};
use std::sync::Arc;

// Removed unused imports
// use mockall::predicate::*;
// use mockall::mock;

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
            endpoint: "http://localhost:8080".to_string(),
            private_key: "fake-key".to_string(),
            prefix: Some("test".to_string()),
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
    Arc<FakeS3Connector>,
    Arc<FakeRecallConnector>,
    Config,
) {
    // Create fake database with test data
    let database = FakeDatabase::new();
    let now = Utc::now();

    let object1 = create_test_object_index("test/object1.jsonl", now);
    let mut object2 = create_test_object_index("test/object2.jsonl", now + Duration::hours(1));
    object2.size_bytes = Some(2048); // Customize size if needed

    database.fake_add_object(object1);
    database.fake_add_object(object2);

    // Create fake sync storage
    let sync_storage = FakeSyncStorage::new();

    // Create test config
    let config = create_test_config();

    // Create fake S3 connector
    let s3_connector = Arc::new(FakeS3Connector::new(&config.s3.bucket));

    // Setup test data in S3
    s3_connector
        .fake_add_object("test/object1.jsonl", Bytes::from("Test data for object1"))
        .await;

    s3_connector
        .fake_add_object("test/object2.jsonl", Bytes::from("Test data for object2"))
        .await;

    // Create fake Recall connector
    let recall_connector = Arc::new(FakeRecallConnector::new(config.recall.prefix.clone()));

    (
        database,
        sync_storage,
        s3_connector,
        recall_connector,
        config,
    )
}

#[tokio::test]
async fn test_synchronizer_run_with_fake_implementations() {
    let (database, sync_storage, s3_connector, recall_connector, config) = setup_test_env().await;

    // Get the objects before creating the synchronizer
    let objects = database.get_objects_to_sync(10, None).await.unwrap();
    let object1 = objects
        .iter()
        .find(|o| o.object_key == "test/object1.jsonl")
        .unwrap()
        .clone();
    let object2 = objects
        .iter()
        .find(|o| o.object_key == "test/object2.jsonl")
        .unwrap()
        .clone();

    // Create synchronizer with fake implementations
    let synchronizer = Synchronizer::with_storage(
        database,
        sync_storage,
        (*s3_connector).clone(),
        (*recall_connector).clone(),
        config,
        false,
    );

    // Run synchronization
    let result = synchronizer.run(None, None).await;

    // Check that the run completed successfully
    assert!(result.is_ok(), "Synchronizer run should succeed");

    // Check that objects were synced
    let sync_storage = synchronizer.get_sync_storage();

    let status1 = SyncStorage::get_object_status(&**sync_storage, object1.id)
        .await
        .unwrap();
    let status2 = SyncStorage::get_object_status(&**sync_storage, object2.id)
        .await
        .unwrap();

    assert_eq!(
        status1,
        Some(crate::sync::storage::SyncStatus::Complete),
        "Object 1 should be synced"
    );
    assert_eq!(
        status2,
        Some(crate::sync::storage::SyncStatus::Complete),
        "Object 2 should be synced"
    );

    // Check that we have synced objects
    let last_object = SyncStorage::get_last_object(&**sync_storage).await.unwrap();
    assert!(
        last_object.is_some(),
        "Should have at least one synced object"
    );
}

#[tokio::test]
async fn test_synchronizer_with_timestamp_filter() {
    let (database, sync_storage, s3_connector, recall_connector, config) = setup_test_env().await;

    // Create synchronizer with fake implementations
    let synchronizer = Synchronizer::with_storage(
        database,
        sync_storage,
        (*s3_connector).clone(),
        (*recall_connector).clone(),
        config,
        false,
    );

    // Run synchronization with a future timestamp to filter out all objects
    let future_time = (Utc::now() + Duration::days(1)).to_rfc3339();
    let result = synchronizer.run(None, Some(future_time)).await;

    // Check that the run completed successfully
    assert!(result.is_ok(), "Synchronizer run should succeed");

    // Check that no objects were synced by looking at the completed status
    let sync_storage = synchronizer.get_sync_storage();
    let synced_objects = SyncStorage::get_objects_with_status(
        &**sync_storage,
        crate::sync::storage::SyncStatus::Complete,
    )
    .await
    .unwrap();

    assert!(
        synced_objects.is_empty(),
        "No objects should be synced due to timestamp filter"
    );
}

#[tokio::test]
async fn test_synchronizer_with_competition_id_filter() {
    // This test is a placeholder since filtering by competition_id is not fully
    // implemented in the current version of the synchronizer.
    // When that functionality is implemented, this test should be expanded.

    let (database, sync_storage, s3_connector, recall_connector, config) = setup_test_env().await;

    // Create synchronizer with fake implementations
    let synchronizer = Synchronizer::with_storage(
        database,
        sync_storage,
        (*s3_connector).clone(),
        (*recall_connector).clone(),
        config,
        false,
    );

    // Run synchronization with a competition_id filter
    let result = synchronizer
        .run(Some("test-competition-id".to_string()), None)
        .await;

    // Check that the run completed successfully
    assert!(
        result.is_ok(),
        "Synchronizer run should succeed even with unimplemented filter"
    );
}
