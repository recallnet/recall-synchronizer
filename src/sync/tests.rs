use crate::config::{Config, DatabaseConfig, RecallConfig, S3Config, SyncConfig};
use crate::db::{Database, FakeDatabase};
use crate::recall::FakeRecallStorage;
use crate::s3::FakeStorage;
use crate::s3::Storage;
use crate::sync::storage::{FakeSyncStorage, SyncStorage};
use crate::sync::synchronizer::Synchronizer;
use crate::test_utils::create_test_object_index;
use bytes::Bytes;
use chrono::{Duration, Utc};

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
    FakeStorage,
    FakeRecallStorage,
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

    // Create fake S3 storage
    let s3_storage = FakeStorage::new();

    // Setup test data in S3
    s3_storage
        .add_object("test/object1.jsonl", Bytes::from("Test data for object1"))
        .await
        .unwrap();

    s3_storage
        .add_object("test/object2.jsonl", Bytes::from("Test data for object2"))
        .await
        .unwrap();

    // Create fake Recall storage
    let recall_storage = FakeRecallStorage::new();

    (database, sync_storage, s3_storage, recall_storage, config)
}

#[tokio::test]
async fn test_synchronizer_run_with_fake_implementations() {
    let (database, sync_storage, s3_storage, recall_storage, config) = setup_test_env().await;

    // Get the objects before creating the synchronizer
    let objects = database.get_objects_to_sync(100, None).await.unwrap();
    let num_objects = objects.len();

    let synchronizer = Synchronizer::with_storage(
        database,
        sync_storage,
        s3_storage,
        recall_storage,
        config,
        false,
    );

    // Run the synchronizer without filters
    synchronizer.run(None, None).await.unwrap();

    // Verify that all objects were synchronized
    let sync_storage = synchronizer.get_sync_storage();
    for obj in objects {
        let status = sync_storage.get_object_status(obj.id).await.unwrap();
        assert_eq!(
            status,
            Some(crate::sync::storage::SyncStatus::Complete),
            "Object {} should be marked as complete",
            obj.object_key
        );
    }

    // Verify the number of synchronized objects
    let completed_objects = sync_storage
        .get_objects_with_status(crate::sync::storage::SyncStatus::Complete)
        .await
        .unwrap();
    assert_eq!(
        completed_objects.len(),
        num_objects,
        "All objects should be synchronized"
    );
}

#[tokio::test]
async fn test_synchronizer_with_competition_id_filter() {
    let (database, sync_storage, s3_storage, recall_storage, config) = setup_test_env().await;

    // Create a specific competition ID and add an object with it
    let competition_id = uuid::Uuid::new_v4();
    let now = Utc::now();
    let mut filtered_object = create_test_object_index("test/filtered.jsonl", now);
    filtered_object.competition_id = Some(competition_id);
    database.fake_add_object(filtered_object.clone());

    // Add the filtered object's data to S3
    s3_storage
        .add_object("test/filtered.jsonl", Bytes::from("Filtered test data"))
        .await
        .unwrap();

    // Store clone for later verification
    let database_clone = database.clone();

    let synchronizer = Synchronizer::with_storage(
        database,
        sync_storage,
        s3_storage,
        recall_storage,
        config,
        false,
    );

    // Run the synchronizer with competition_id filter
    synchronizer
        .run(Some(competition_id.to_string()), None)
        .await
        .unwrap();

    // Verify that only the filtered object was synchronized
    let sync_storage = synchronizer.get_sync_storage();
    let status = sync_storage
        .get_object_status(filtered_object.id)
        .await
        .unwrap();
    assert_eq!(
        status,
        Some(crate::sync::storage::SyncStatus::Complete),
        "Filtered object should be marked as complete"
    );

    // Verify that other objects were not synchronized
    let all_objects = database_clone.get_objects_to_sync(100, None).await.unwrap();
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
async fn test_synchronizer_with_timestamp_filter() {
    let (database, sync_storage, s3_storage, recall_storage, config) = setup_test_env().await;

    // Add an older object that should not be synchronized
    let old_time = Utc::now() - Duration::days(7);
    let old_object = create_test_object_index("test/old.jsonl", old_time);
    database.fake_add_object(old_object.clone());

    // Add the old object's data to S3
    s3_storage
        .add_object("test/old.jsonl", Bytes::from("Old test data"))
        .await
        .unwrap();

    // Store clone for later verification
    let database_clone = database.clone();

    let synchronizer = Synchronizer::with_storage(
        database,
        sync_storage,
        s3_storage,
        recall_storage,
        config,
        false,
    );

    // Run the synchronizer with a timestamp filter (2 days ago)
    let filter_time = Utc::now() - Duration::days(2);
    synchronizer
        .run(None, Some(filter_time.to_rfc3339()))
        .await
        .unwrap();

    // Verify that the old object was not synchronized
    let sync_storage = synchronizer.get_sync_storage();
    let status = sync_storage.get_object_status(old_object.id).await.unwrap();
    assert_eq!(
        status, None,
        "Old object should not be synchronized when using timestamp filter"
    );

    // Verify that newer objects were synchronized
    let all_objects = database_clone.get_objects_to_sync(100, None).await.unwrap();
    for obj in all_objects {
        if obj.object_last_modified_at > filter_time {
            let status = sync_storage.get_object_status(obj.id).await.unwrap();
            assert_eq!(
                status,
                Some(crate::sync::storage::SyncStatus::Complete),
                "Newer object {} should be synchronized",
                obj.object_key
            );
        }
    }
}