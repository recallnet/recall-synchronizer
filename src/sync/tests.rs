use crate::config::{Config, DatabaseConfig, RecallConfig, S3Config, SyncConfig};
use crate::db::{Database, FakeDatabase, ObjectIndex};
use crate::recall::fake::FakeRecallStorage;
use crate::recall::Storage as RecallStorage;
use crate::s3::{FakeStorage, Storage as S3Storage};
use crate::sync::{
    storage::{FakeSyncStorage, SyncRecord, SyncStatus, SyncStorage},
    synchronizer::Synchronizer,
};
use crate::test_utils::create_test_object_index;
use bytes::Bytes;
use chrono::{Duration, Utc};
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

/// Extract object keys from a vector of ObjectIndex
fn obj_to_keys(objects: Vec<ObjectIndex>) -> Vec<String> {
    objects.into_iter().map(|obj| obj.object_key).collect()
}

/// Extract object keys from a vector of SyncRecord
fn rec_to_keys(records: Vec<SyncRecord>) -> Vec<String> {
    records.into_iter().map(|rec| rec.object_key).collect()
}

/// Assert that subset is a subset of superset
fn assert_subset(superset: Vec<String>, subset: Vec<String>) {
    let superset_set: HashSet<String> = superset.into_iter().collect();
    let subset_set: HashSet<String> = subset.into_iter().collect();

    let missing: Vec<String> = subset_set.difference(&superset_set).cloned().collect();

    assert!(
        missing.is_empty(),
        "Subset contains elements not in superset: {:?}",
        missing
    );
}

/// Return elements that are in the first vector but not in the second
fn vec_difference(first: Vec<String>, second: Vec<String>) -> Vec<String> {
    let first_set: HashSet<String> = first.into_iter().collect();
    let second_set: HashSet<String> = second.into_iter().collect();

    first_set.difference(&second_set).cloned().collect()
}

/// Test environment that holds all storage implementations and the synchronizer
struct TestEnvironment {
    database: Arc<FakeDatabase>,
    sync_storage: Arc<FakeSyncStorage>,
    s3_storage: Arc<FakeStorage>,
    recall_storage: Arc<FakeRecallStorage>,
    synchronizer: Synchronizer<FakeDatabase, FakeSyncStorage, FakeStorage, FakeRecallStorage>,
}

impl TestEnvironment {
    /// Verify that an object has been properly synchronized
    async fn verify_object_synced(&self, object: &ObjectIndex) -> Result<(), String> {
        let status = self
            .sync_storage
            .get_object_status(object.id)
            .await
            .map_err(|e| format!("Failed to get object status: {}", e))?;

        if status != Some(SyncStatus::Complete) {
            return Err(format!(
                "Object {} is not marked as complete in sync storage. Status: {:?}",
                object.object_key, status
            ));
        }

        let s3_data = self
            .s3_storage
            .get_object(&object.object_key)
            .await
            .map_err(|e| format!("Failed to get object from S3: {}", e))?;

        let recall_data = self
            .recall_storage
            .get_blob(&object.object_key)
            .await
            .map_err(|e| format!("Failed to get blob from Recall: {}", e))?;

        if s3_data.as_ref() != recall_data.as_slice() {
            return Err(format!(
                "Data mismatch for object {}: S3 has {} bytes, Recall has {} bytes",
                object.object_key,
                s3_data.len(),
                recall_data.len()
            ));
        }

        Ok(())
    }

    /// Verify that an object has NOT been synchronized
    async fn verify_object_not_synced(&self, object: &ObjectIndex) -> Result<(), String> {
        self.verify_object_not_synced_by_id(object.id, &object.object_key)
            .await
    }

    /// Verify that an object has NOT been synchronized (by ID and key)
    async fn verify_object_not_synced_by_id(
        &self,
        object_id: Uuid,
        object_key: &str,
    ) -> Result<(), String> {
        // Check sync storage status
        let status = self
            .sync_storage
            .get_object_status(object_id)
            .await
            .map_err(|e| format!("Failed to get object status: {}", e))?;

        // Object should either have no status or not be Complete
        if status == Some(SyncStatus::Complete) {
            return Err(format!(
                "Object {} is unexpectedly marked as complete in sync storage",
                object_key
            ));
        }

        // Verify the blob does not exist in Recall
        let exists_in_recall = self
            .recall_storage
            .has_blob(object_key)
            .await
            .map_err(|e| format!("Failed to check blob existence in Recall: {}", e))?;

        if exists_in_recall {
            return Err(format!(
                "Object {} unexpectedly found in Recall storage",
                object_key
            ));
        }

        Ok(())
    }

    /// Add an object to the database and S3 storage
    async fn add_object_to_db_and_s3(&self, object: ObjectIndex, data: &str) {
        self.database.add_object(object.clone()).await.unwrap();
        self.s3_storage
            .add_object(&object.object_key, Bytes::from(data.to_string()))
            .await
            .unwrap();
    }
}

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
async fn setup() -> TestEnvironment {
    setup_with_config(create_test_config()).await
}

// Setup a test environment with custom config
async fn setup_with_config(config: Config) -> TestEnvironment {
    let database = Arc::new(FakeDatabase::new());
    let sync_storage = Arc::new(FakeSyncStorage::new());
    let s3_storage = Arc::new(FakeStorage::new());
    let recall_storage = Arc::new(FakeRecallStorage::new());

    let synchronizer = Synchronizer::with_storage(
        database.clone(),
        sync_storage.clone(),
        s3_storage.clone(),
        recall_storage.clone(),
        config,
        false,
    );

    let env = TestEnvironment {
        database,
        sync_storage,
        s3_storage,
        recall_storage,
        synchronizer,
    };

    // Add initial test data
    let now = Utc::now();
    let object1 = create_test_object_index("test/object1.jsonl", now);
    let mut object2 = create_test_object_index("test/object2.jsonl", now + Duration::hours(1));
    object2.size_bytes = Some(2048);

    env.add_object_to_db_and_s3(object1, "Test data for object1")
        .await;
    env.add_object_to_db_and_s3(object2, "Test data for object2")
        .await;

    env
}

#[tokio::test]
async fn when_no_filters_applied_all_objects_are_synchronized() {
    let env = setup().await;
    let objects = env.database.get_objects_to_sync(100, None).await.unwrap();
    let num_objects = objects.len();

    env.synchronizer.run(None, None).await.unwrap();

    for obj in &objects {
        env.verify_object_synced(obj)
            .await
            .unwrap_or_else(|e| panic!("Object {} verification failed: {}", obj.object_key, e));
    }

    let completed_objects = env
        .sync_storage
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
    let env = setup().await;

    let competition_id = uuid::Uuid::new_v4();
    let now = Utc::now();
    let mut filtered_object = create_test_object_index("test/filtered.jsonl", now);
    filtered_object.competition_id = Some(competition_id);
    env.add_object_to_db_and_s3(filtered_object.clone(), "Filtered test data")
        .await;

    env.synchronizer
        .run(Some(competition_id.to_string()), None)
        .await
        .unwrap();

    env.verify_object_synced(&filtered_object)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Filtered object {} verification failed: {}",
                filtered_object.object_key, e
            )
        });

    let all_objects = env.database.get_objects_to_sync(100, None).await.unwrap();
    for obj in all_objects {
        if obj.competition_id != Some(competition_id) {
            env.verify_object_not_synced(&obj)
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "Object {} should not be synchronized: {}",
                        obj.object_key, e
                    )
                });
        }
    }
}

#[tokio::test]
async fn when_timestamp_filter_is_applied_only_newer_objects_are_synchronized() {
    let env = setup().await;

    let old_time = Utc::now() - Duration::days(7);
    let old_object = create_test_object_index("test/old.jsonl", old_time);
    env.add_object_to_db_and_s3(old_object.clone(), "Old test data")
        .await;

    let filter_time = Utc::now() - Duration::days(2);
    env.synchronizer.run(None, Some(filter_time)).await.unwrap();

    env.verify_object_not_synced(&old_object)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Old object {} should not be synchronized: {}",
                old_object.object_key, e
            )
        });

    let all_objects = env.database.get_objects_to_sync(100, None).await.unwrap();
    for obj in all_objects {
        if obj.object_last_modified_at > filter_time {
            env.verify_object_synced(&obj).await.unwrap_or_else(|e| {
                panic!("Newer object {} verification failed: {}", obj.object_key, e)
            });
        }
    }
}

#[tokio::test]
async fn when_object_is_already_being_processed_it_is_skipped() {
    let env = setup().await;

    let test_object = create_test_object_index("test/concurrent.jsonl", Utc::now());
    env.add_object_to_db_and_s3(test_object.clone(), "Concurrent test data")
        .await;

    let sync_record = SyncRecord::new(
        test_object.id,
        test_object.object_key.clone(),
        test_object.bucket_name.clone(),
        test_object.object_last_modified_at,
    );
    env.sync_storage.add_object(sync_record).await.unwrap();

    env.sync_storage
        .set_object_status(test_object.id, SyncStatus::Processing)
        .await
        .unwrap();

    env.synchronizer.run(None, None).await.unwrap();

    // The object should still be in Processing status since we didn't let our synchronizer complete it
    let status = env
        .sync_storage
        .get_object_status(test_object.id)
        .await
        .unwrap();
    assert_eq!(status, Some(SyncStatus::Processing));

    // Verify the object was NOT added to Recall (since it was already processing)
    let exists_in_recall = env
        .recall_storage
        .has_blob(&test_object.object_key)
        .await
        .unwrap();
    assert!(
        !exists_in_recall,
        "Object should not be in Recall when skipped due to Processing status"
    );

    // Now mark it as complete and run again
    env.sync_storage
        .set_object_status(test_object.id, SyncStatus::Complete)
        .await
        .unwrap();
    env.synchronizer.run(None, None).await.unwrap();

    // The object should still be complete (not processed again)
    let status = env
        .sync_storage
        .get_object_status(test_object.id)
        .await
        .unwrap();
    assert_eq!(status, Some(SyncStatus::Complete));
}

#[tokio::test]
async fn batch_size_limits_database_fetch() {
    // This test verifies that the synchronizer fetches only batch_size objects
    // from the database at a time, even if more objects are available.

    let mut config = create_test_config();
    config.sync.batch_size = 3;
    let env = setup_with_config(config).await;

    // Clear initial test data
    env.database.clear_data().await.unwrap();
    env.sync_storage.clear_data().await.unwrap();

    // Add 10 objects to the database and S3
    let now = Utc::now();
    for i in 0..10 {
        let object = create_test_object_index(
            &format!("test/batch-{}.jsonl", i),
            now - Duration::minutes(i as i64), // Older objects have lower indices
        );
        env.add_object_to_db_and_s3(object, &format!("Test data {}", i))
            .await;
    }

    // First sync run
    env.synchronizer.run(None, None).await.unwrap();

    // Should have processed exactly 3 objects (batch_size)
    let completed = env
        .sync_storage
        .get_objects_with_status(SyncStatus::Complete)
        .await
        .unwrap();

    assert_eq!(
        completed.len(),
        3,
        "Should process exactly batch_size (3) objects"
    );

    // The 3 newest objects should be synced (test/batch-0.jsonl, test/batch-1.jsonl, test/batch-2.jsonl)
    let synced_keys: Vec<String> = completed.iter().map(|r| r.object_key.clone()).collect();
    assert!(synced_keys.contains(&"test/batch-0.jsonl".to_string()));
    assert!(synced_keys.contains(&"test/batch-1.jsonl".to_string()));
    assert!(synced_keys.contains(&"test/batch-2.jsonl".to_string()));
}

#[tokio::test]
async fn multiple_sync_runs_with_new_objects() {
    // This test demonstrates that the current implementation has an issue with objects
    // that have the same timestamp. It only tracks the last timestamp, not the object ID,
    // which can cause objects to be skipped when they share timestamps.

    let mut config = create_test_config();
    config.sync.batch_size = 3; // Process only 3 objects at a time
    let env = setup_with_config(config).await;

    // Clear initial test data
    env.database.clear_data().await.unwrap();
    env.sync_storage.clear_data().await.unwrap();

    let base_time = Utc::now();

    // Add first batch of 5 objects with the SAME timestamp
    let batch1_time = base_time - Duration::hours(2);
    let mut batch1_objects = Vec::new();
    for i in 0..5 {
        let object = create_test_object_index(&format!("test/batch1-{}.jsonl", i), batch1_time);
        batch1_objects.push(object.clone());
        env.add_object_to_db_and_s3(object, &format!("Batch 1 data {}", i))
            .await;
    }

    // First sync - should process 2 objects (batch_size)

    // Debug: Check what objects are available
    let all_objects_before = env.database.get_objects_to_sync(100, None).await.unwrap();
    println!("All objects before sync:");
    for obj in &all_objects_before {
        println!(
            "  - {} (timestamp: {}, id: {})",
            obj.object_key, obj.object_last_modified_at, obj.id
        );
    }

    env.synchronizer.run(None, None).await.unwrap();

    let completed_run1 = env
        .sync_storage
        .get_objects_with_status(SyncStatus::Complete)
        .await
        .unwrap();
    assert_eq!(completed_run1.len(), 3, "First run should sync 3 objects");

    // Add second batch of 3 objects with a newer but also same timestamp
    let batch2_time = base_time - Duration::hours(1);
    let mut batch2_objects = Vec::new();
    for i in 0..3 {
        let object = create_test_object_index(&format!("test/batch2-{}.jsonl", i), batch2_time);
        batch2_objects.push(object.clone());
        env.add_object_to_db_and_s3(object, &format!("Batch 2 data {}", i))
            .await;
    }

    env.synchronizer.run(None, None).await.unwrap();

    let completed_run2 = env
        .sync_storage
        .get_objects_with_status(SyncStatus::Complete)
        .await
        .unwrap();

    assert_eq!(completed_run2.len(), 6, "Should have 6 synced objects");

    // All 5 objects from batch 1 should be in completed_run2
    assert_subset(
        rec_to_keys(completed_run2.clone()),
        obj_to_keys(batch1_objects.clone()),
    );

    // Get the 6th remaining object that was synced
    let synced_batch2_keys = vec_difference(
        rec_to_keys(completed_run2.clone()),
        obj_to_keys(batch1_objects.clone()),
    );

    // Assert it is from the second batch
    assert_subset(obj_to_keys(batch2_objects.clone()), synced_batch2_keys);
}
