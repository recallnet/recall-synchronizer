use crate::config::{
    Config, DatabaseConfig, RecallConfig, S3Config, SyncConfig, SyncStorageConfig,
};
use crate::db::{Database, FakeDatabase, ObjectIndex};
use crate::recall::fake::FakeRecallStorage;
use crate::recall::Storage as RecallStorage;
use crate::s3::{FakeStorage, Storage as S3Storage};
use crate::sync::{
    storage::{FakeSyncStorage, SyncRecord, SyncStatus, SyncStorage},
    synchronizer::Synchronizer,
};
use crate::test_utils::create_test_object_index_s3;
use bytes::Bytes;
use chrono::{Duration, Utc};
use std::sync::Arc;
use uuid::Uuid;

/// Extract object IDs from a vector of SyncRecord
fn rec_to_ids(records: Vec<SyncRecord>) -> Vec<Uuid> {
    records.into_iter().map(|rec| rec.id).collect()
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
    fn construct_recall_key(object: &ObjectIndex) -> String {
        let mut parts = Vec::new();

        if let Some(competition_id) = &object.competition_id {
            parts.push(competition_id.to_string());
        }

        if let Some(agent_id) = &object.agent_id {
            parts.push(agent_id.to_string());
        }

        parts.push(object.data_type.clone());
        parts.push(object.id.to_string());

        parts.join("/")
    }

    /// Verify that an object has been properly synchronized
    async fn verify_object_synced(&self, object: &ObjectIndex) -> Result<(), String> {
        let record = self
            .sync_storage
            .get_object(object.id)
            .await
            .map_err(|e| format!("Failed to get object: {}", e))?;

        match record {
            Some(r) if r.status == SyncStatus::Complete => {
                // Object is marked as complete, continue verification
            }
            Some(r) => {
                return Err(format!(
                    "Object {} is not marked as complete in sync storage. Status: {:?}",
                    object.id, r.status
                ));
            }
            None => {
                return Err(format!("Object {} not found in sync storage", object.id));
            }
        }

        let s3_data = self
            .s3_storage
            .get_object(object.object_key.as_ref().expect("object_key required"))
            .await
            .map_err(|e| format!("Failed to get object from S3: {}", e))?;

        // Use the correct Recall key format
        let recall_key = Self::construct_recall_key(object);
        let recall_data = self
            .recall_storage
            .get_blob(&recall_key)
            .await
            .map_err(|e| format!("Failed to get blob from Recall: {}", e))?;

        if s3_data.as_ref() != recall_data.as_slice() {
            return Err(format!(
                "Data mismatch for object {}: S3 has {} bytes, Recall has {} bytes",
                object.id,
                s3_data.len(),
                recall_data.len()
            ));
        }

        Ok(())
    }

    /// Verify that an object has NOT been synchronized
    async fn verify_object_not_synced(&self, object: &ObjectIndex) -> Result<(), String> {
        // Check sync storage status
        let record = self
            .sync_storage
            .get_object(object.id)
            .await
            .map_err(|e| format!("Failed to get object: {}", e))?;

        // Object should either not exist or not be Complete
        if let Some(r) = record {
            if r.status == SyncStatus::Complete {
                return Err(format!(
                    "Object {} is unexpectedly marked as complete in sync storage",
                    object.id
                ));
            }
        }

        // Verify the blob does not exist in Recall using the correct key
        let recall_key = Self::construct_recall_key(object);
        let exists_in_recall = self
            .recall_storage
            .has_blob(&recall_key)
            .await
            .map_err(|e| format!("Failed to check blob existence in Recall: {}", e))?;

        if exists_in_recall {
            return Err(format!(
                "Object {} unexpectedly found in Recall storage with key {}",
                object.id, recall_key
            ));
        }

        Ok(())
    }

    /// Add an object to the database and S3 storage
    async fn add_object_to_db_and_s3(&self, object: ObjectIndex, data: &str) {
        self.database.add_object(object.clone()).await.unwrap();
        self.s3_storage
            .add_object(
                object.object_key.as_ref().expect("object_key required"),
                Bytes::from(data.to_string()),
            )
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
        s3: Some(S3Config {
            endpoint: Some("http://localhost:9000".to_string()),
            region: "us-east-1".to_string(),
            bucket: "test-bucket".to_string(),
            access_key_id: Some("test".to_string()),
            secret_access_key: Some("test".to_string()),
        }),
        recall: RecallConfig {
            private_key: "fake-key".to_string(),
            network: "localnet".to_string(),
            config_path: Some("networks.toml".to_string()),
            bucket: None,
        },
        sync: SyncConfig { batch_size: 10 },
        sync_storage: SyncStorageConfig {
            db_path: ":memory:".to_string(),
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
        Some(s3_storage.clone()),
        recall_storage.clone(),
        config.sync,
    );

    TestEnvironment {
        database,
        sync_storage,
        s3_storage,
        recall_storage,
        synchronizer,
    }
}

#[tokio::test]
async fn when_no_filters_applied_all_objects_are_synchronized() {
    let env = setup().await;

    let now = Utc::now();
    let object1 = create_test_object_index_s3("test/object1.jsonl", now);
    let object2 = create_test_object_index_s3("test/object2.jsonl", now + Duration::hours(1));

    env.add_object_to_db_and_s3(object1.clone(), "Test data for object1")
        .await;
    env.add_object_to_db_and_s3(object2.clone(), "Test data for object2")
        .await;

    let objects = vec![object1, object2];

    env.synchronizer.run(None).await.unwrap();

    for obj in &objects {
        env.verify_object_synced(obj)
            .await
            .unwrap_or_else(|e| panic!("Object {} verification failed: {}", obj.id, e));
    }

    let completed_objects = env
        .sync_storage
        .get_objects_with_status(SyncStatus::Complete)
        .await
        .unwrap();
    assert_eq!(
        completed_objects.len(),
        objects.len(),
        "All objects should be synchronized"
    );
}


#[tokio::test]
async fn when_timestamp_filter_is_applied_only_newer_objects_are_synchronized() {
    let env = setup().await;

    let old_time = Utc::now() - Duration::days(7);
    let recent_time = Utc::now() - Duration::hours(1);

    let old_object = create_test_object_index_s3("test/old.jsonl", old_time);
    let new_object = create_test_object_index_s3("test/new.jsonl", recent_time);

    env.add_object_to_db_and_s3(old_object.clone(), "Old test data")
        .await;
    env.add_object_to_db_and_s3(new_object.clone(), "New test data")
        .await;

    let filter_time = Utc::now() - Duration::days(2);
    env.synchronizer.run(Some(filter_time)).await.unwrap();

    env.verify_object_not_synced(&old_object)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Old object {} should not be synchronized: {}",
                old_object.id, e
            )
        });

    env.verify_object_synced(&new_object)
        .await
        .unwrap_or_else(|e| panic!("Newer object {} verification failed: {}", new_object.id, e));
}

#[tokio::test]
async fn when_object_is_already_being_processed_it_is_skipped() {
    let env = setup().await;

    let test_object = create_test_object_index_s3("test/concurrent.jsonl", Utc::now());
    env.add_object_to_db_and_s3(test_object.clone(), "Concurrent test data")
        .await;

    let sync_record = SyncRecord::new(
        test_object.id,
        test_object.competition_id,
        test_object.agent_id,
        test_object.data_type.clone(),
        test_object.created_at,
    );
    env.sync_storage.add_object(sync_record).await.unwrap();

    env.sync_storage
        .set_object_status(test_object.id, SyncStatus::Processing)
        .await
        .unwrap();

    env.synchronizer.run(None).await.unwrap();

    // The object should still be in Processing status since we didn't let our synchronizer complete it
    let record = env.sync_storage.get_object(test_object.id).await.unwrap();
    assert_eq!(record.map(|r| r.status), Some(SyncStatus::Processing));

    let exists_in_recall = env
        .recall_storage
        .has_blob(
            test_object
                .object_key
                .as_ref()
                .expect("object_key required"),
        )
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
    env.synchronizer.run(None).await.unwrap();

    let record = env.sync_storage.get_object(test_object.id).await.unwrap();
    assert_eq!(
        record.map(|r| r.status),
        Some(SyncStatus::Complete),
        "Object should remain complete after re-running synchronizer"
    );
}

#[tokio::test]
async fn with_batch_size_should_limits_database_fetch() {
    let mut config = create_test_config();
    config.sync.batch_size = 3;
    let env = setup_with_config(config).await;

    // Add 10 objects to the database and S3
    let base_time = Utc::now() - Duration::hours(24);
    for i in 0..10 {
        let object = create_test_object_index_s3(
            &format!("test/batch-{}.jsonl", i),
            base_time + Duration::hours(i as i64),
        );
        env.add_object_to_db_and_s3(object, &format!("Test data {}", i))
            .await;
    }

    env.synchronizer.run(None).await.unwrap();

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
}

#[tokio::test]
async fn multiple_sync_runs_with_new_objects() {
    let mut config = create_test_config();
    config.sync.batch_size = 3;
    let env = setup_with_config(config).await;

    let base_time = Utc::now();

    // Add first batch of 5 objects with the SAME timestamp
    let batch1_time = base_time - Duration::hours(2);
    let mut batch1_objects = Vec::new();
    for i in 0..5 {
        let object = create_test_object_index_s3(&format!("test/batch1-{}.jsonl", i), batch1_time);
        batch1_objects.push(object.clone());
        env.add_object_to_db_and_s3(object, &format!("Batch 1 data {}", i))
            .await;
    }

    env.synchronizer.run(None).await.unwrap();

    let synced_records = env
        .sync_storage
        .get_objects_with_status(SyncStatus::Complete)
        .await
        .unwrap();
    assert_eq!(synced_records.len(), 3, "First run should sync 3 objects");

    // Add second batch of 3 objects with a newer but also same timestamp
    let batch2_time = base_time - Duration::hours(1);
    let mut batch2_objects = Vec::new();
    for i in 0..3 {
        let object = create_test_object_index_s3(&format!("test/batch2-{}.jsonl", i), batch2_time);
        batch2_objects.push(object.clone());
        env.add_object_to_db_and_s3(object, &format!("Batch 2 data {}", i))
            .await;
    }

    env.synchronizer.run(None).await.unwrap();

    let synced_records = env
        .sync_storage
        .get_objects_with_status(SyncStatus::Complete)
        .await
        .unwrap();
    assert_eq!(synced_records.len(), 6, "Should have 6 synced objects");

    let synced_ids = rec_to_ids(synced_records.clone());

    let batch1_synced_count = batch1_objects
        .iter()
        .filter(|obj| synced_ids.contains(&obj.id))
        .count();
    let batch2_synced_count = batch2_objects
        .iter()
        .filter(|obj| synced_ids.contains(&obj.id))
        .count();

    assert_eq!(
        batch1_synced_count, 5,
        "5 objects from batch1 should be synced, got {}",
        batch1_synced_count
    );

    assert_eq!(
        batch2_synced_count, 1,
        "1 object from batch2 should be synced, got {}",
        batch2_synced_count
    );
}

#[tokio::test]
async fn when_since_param_includes_already_synced_objects_they_are_skipped() {
    let mut config = create_test_config();
    config.sync.batch_size = 3;
    let env = setup_with_config(config).await;

    let base_time = Utc::now() - Duration::hours(24);
    let mut objects = Vec::new();

    for i in 0..5 {
        let object = create_test_object_index_s3(
            &format!("test/obj-{}.jsonl", i),
            base_time + Duration::hours(i as i64),
        );
        objects.push(object.clone());
        env.add_object_to_db_and_s3(object, &format!("Test data {}", i))
            .await;
    }

    // Sync twice to ensure all objects are processed
    env.synchronizer.run(None).await.unwrap();
    env.synchronizer.run(None).await.unwrap();

    let synced = env
        .sync_storage
        .get_objects_with_status(SyncStatus::Complete)
        .await
        .unwrap();
    assert_eq!(synced.len(), 5, "All 5 objects should be synced");

    for i in 5..10 {
        let object = create_test_object_index_s3(
            &format!("test/obj-{}.jsonl", i),
            base_time + Duration::hours(i as i64),
        );
        objects.push(object.clone());
        env.add_object_to_db_and_s3(object, &format!("Test data {}", i))
            .await;
    }

    // Sync with 'since' that includes already synced objects starting from the 3rd object
    let since_time = base_time + Duration::hours(2) + Duration::minutes(30);
    env.synchronizer.run(Some(since_time)).await.unwrap();

    for (i, object) in objects.iter().enumerate().take(8) {
        env.verify_object_synced(object)
            .await
            .unwrap_or_else(|e| panic!("Object {} should be synced: {}", i, e));
    }

    for (i, object) in objects.iter().enumerate().skip(8).take(2) {
        env.verify_object_not_synced(object)
            .await
            .unwrap_or_else(|e| panic!("Object {} should not be synced: {}", i, e));
    }
}

#[tokio::test]
async fn when_since_param_skips_unsynced_objects_they_remain_unsynced() {
    let mut config = create_test_config();
    config.sync.batch_size = 3;
    let env = setup_with_config(config).await;

    let base_time = Utc::now() - Duration::hours(10);
    let mut objects = Vec::new();

    for i in 0..8 {
        let object = create_test_object_index_s3(
            &format!("test/obj-{}.jsonl", i),
            base_time + Duration::hours(i as i64),
        );
        objects.push(object.clone());
        env.add_object_to_db_and_s3(object, &format!("Test data {}", i))
            .await;
    }

    // Sync with 'since' that skips first 3 objects
    let since_time = base_time + Duration::hours(3) - Duration::minutes(30);
    env.synchronizer.run(Some(since_time)).await.unwrap();

    for (i, object) in objects.iter().enumerate().take(3) {
        env.verify_object_not_synced(object)
            .await
            .unwrap_or_else(|e| panic!("Object {} should not be synced: {}", i, e));
    }

    for (i, object) in objects.iter().enumerate().skip(3).take(3) {
        env.verify_object_synced(object)
            .await
            .unwrap_or_else(|e| panic!("Object {} should be synced: {}", i, e));
    }

    for (i, object) in objects.iter().enumerate().skip(6).take(2) {
        env.verify_object_not_synced(object)
            .await
            .unwrap_or_else(|e| panic!("Object {} should not be synced: {}", i, e));
    }

    env.synchronizer.run(None).await.unwrap();

    for (i, object) in objects.iter().enumerate().take(3) {
        env.verify_object_not_synced(object)
            .await
            .unwrap_or_else(|e| panic!("Object {} should still not be synced: {}", i, e));
    }

    for (i, object) in objects.iter().enumerate().skip(3).take(5) {
        env.verify_object_synced(object)
            .await
            .unwrap_or_else(|e| panic!("Object {} should be synced: {}", i, e));
    }
}


#[tokio::test]
async fn regular_sync_processes_all_unsynced_objects_from_all_competitions() {
    let mut config = create_test_config();
    config.sync.batch_size = 3;
    let env = setup_with_config(config).await;

    let base_time = Utc::now() - Duration::hours(10);
    let comp1_id = Uuid::new_v4();
    let comp2_id = Uuid::new_v4();

    for i in 0..3 {
        let mut object = create_test_object_index_s3(
            &format!("comp1/obj-{}.jsonl", i),
            base_time + Duration::minutes(i as i64),
        );
        object.competition_id = Some(comp1_id);
        env.add_object_to_db_and_s3(object, &format!("Comp1 data {}", i))
            .await;

        let mut object = create_test_object_index_s3(
            &format!("comp2/obj-{}.jsonl", i),
            base_time + Duration::minutes((i + 5) as i64),
        );
        object.competition_id = Some(comp2_id);
        env.add_object_to_db_and_s3(object, &format!("Comp2 data {}", i))
            .await;
    }

    // First sync (batch size 3) - should sync 3 oldest objects regardless of competition
    env.synchronizer.run(None).await.unwrap();

    let synced = env
        .sync_storage
        .get_objects_with_status(SyncStatus::Complete)
        .await
        .unwrap();
    assert_eq!(synced.len(), 3);
    
    // Second sync - should sync remaining 3 objects
    env.synchronizer.run(None).await.unwrap();

    let synced = env
        .sync_storage
        .get_objects_with_status(SyncStatus::Complete)
        .await
        .unwrap();
    assert_eq!(synced.len(), 6, "Should have 6 synced objects");

    let comp1_synced = synced
        .iter()
        .filter(|r| r.competition_id == Some(comp1_id))
        .count();
    let comp2_synced = synced
        .iter()
        .filter(|r| r.competition_id == Some(comp2_id))
        .count();

    assert_eq!(comp1_synced, 3);
    assert_eq!(comp2_synced, 3);
}

#[tokio::test]
async fn reset_clears_sync_state_and_allows_resyncing() {
    let env = setup().await;

    let mut objects = Vec::new();
    for i in 0..3 {
        let object = create_test_object_index_s3(
            &format!("test/reset-{}.jsonl", i),
            Utc::now() - Duration::hours(i as i64),
        );
        objects.push(object.clone());
        env.add_object_to_db_and_s3(object, &format!("Test data {}", i))
            .await;
    }

    env.synchronizer.run(None).await.unwrap();

    for obj in &objects {
        env.verify_object_synced(obj)
            .await
            .unwrap_or_else(|e| panic!("Initial sync failed for {}: {}", obj.id, e));
    }

    // Delete blobs one-by-one to simulate network reset
    for obj in &objects {
        let recall_key = TestEnvironment::construct_recall_key(obj);
        env.recall_storage
            .delete_blob(&recall_key)
            .await
            .unwrap_or_else(|e| panic!("Failed to delete blob {}: {}", recall_key, e));
    }

    // Verify sync doesn't re-upload (sync state still shows complete)
    env.synchronizer.run(None).await.unwrap();
    for obj in &objects {
        let recall_key = TestEnvironment::construct_recall_key(obj);
        let exists = env.recall_storage.has_blob(&recall_key).await.unwrap();
        assert!(
            !exists,
            "Object {} should not be re-synced before reset",
            obj.id
        );
    }

    env.synchronizer.reset().await.unwrap();

    let synced = env
        .sync_storage
        .get_objects_with_status(SyncStatus::Complete)
        .await
        .unwrap();
    assert_eq!(synced.len(), 0, "Sync state should be empty after reset");

    // Run sync again - should re-sync all objects
    env.synchronizer.run(None).await.unwrap();

    for obj in &objects {
        env.verify_object_synced(obj)
            .await
            .unwrap_or_else(|e| panic!("Object {} should be re-synced after reset: {}", obj.id, e));
    }
}

#[tokio::test]
async fn start_synchronizer_runs_at_interval() {
    let mut config = create_test_config();
    config.sync.batch_size = 2;
    let env = setup_with_config(config).await;

    let base_time = Utc::now();

    for i in 0..6 {
        let object = create_test_object_index_s3(
            &format!("test/interval-{}.jsonl", i),
            base_time - Duration::minutes(i as i64),
        );
        env.add_object_to_db_and_s3(object, &format!("Test data {}", i))
            .await;
    }

    // Start the synchronizer in a separate task with 1 second interval
    let sync_task = {
        let sync = env.synchronizer.clone();
        tokio::spawn(async move { sync.start(1, None).await })
    };

    // Check after ~0.5 seconds (first immediate run)
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let blobs_run1 = env.recall_storage.list_blobs("").await.unwrap();
    assert_eq!(
        blobs_run1.len(),
        2,
        "First run should sync 2 objects (batch_size=2)"
    );

    // Check after ~1.5 seconds (second interval run)
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let blobs_run2 = env.recall_storage.list_blobs("").await.unwrap();
    assert_eq!(
        blobs_run2.len(),
        4,
        "Second run should have 4 objects total"
    );

    // Check after ~2.5 seconds (third interval run)
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let blobs_run3 = env.recall_storage.list_blobs("").await.unwrap();
    assert_eq!(blobs_run3.len(), 6, "Third run should have all 6 objects");

    sync_task.abort();
}

#[tokio::test]
async fn recall_key_structure_follows_required_format() {
    let env = setup().await;

    let competition_id = Uuid::new_v4();
    let agent_id = Uuid::new_v4();
    let object_id = Uuid::new_v4();

    // Create an object with an S3 key that doesn't match the required format
    let mut object = create_test_object_index_s3("some/random/s3/key.jsonl", Utc::now());
    object.id = object_id;
    object.competition_id = Some(competition_id);
    object.agent_id = Some(agent_id);
    object.data_type = "CHAIN_OF_THOUGHT".to_string();

    env.add_object_to_db_and_s3(object.clone(), "Test data for recall key format")
        .await;

    env.synchronizer.run(None).await.unwrap();

    let record = env.sync_storage.get_object(object_id).await.unwrap();
    assert_eq!(
        record.map(|r| r.status),
        Some(SyncStatus::Complete),
        "Object should be marked as complete"
    );

    let expected_key = format!(
        "{}/{}/{}/{}",
        competition_id, agent_id, "CHAIN_OF_THOUGHT", object_id
    );

    let exists_with_correct_key = env.recall_storage.has_blob(&expected_key).await.unwrap();

    assert!(
        exists_with_correct_key,
        "Object should be stored in Recall with key: {}",
        expected_key
    );

    let exists_with_s3_key = env
        .recall_storage
        .has_blob(object.object_key.as_ref().expect("object_key required"))
        .await
        .unwrap();

    assert!(
        !exists_with_s3_key,
        "Object should NOT be stored with the original S3 key: {}",
        object.id
    );
}

#[tokio::test]
async fn direct_storage_synchronization_works() {
    use crate::db::FakeDatabase;
    use crate::test_utils::create_test_object_index_direct;

    // Create test setup
    let db = Arc::new(FakeDatabase::new());
    let sync_storage = Arc::new(FakeSyncStorage::new());
    let recall_storage = Arc::new(FakeRecallStorage::new());
    let config = SyncConfig { batch_size: 10 };

    // Create synchronizer without S3 (direct mode)
    let synchronizer = Synchronizer::with_storage(
        db.clone(),
        sync_storage.clone(),
        None::<Arc<FakeStorage>>, // No S3 storage for direct mode
        recall_storage.clone(),
        config,
    );

    // Add some test objects with embedded data
    let test_data = b"This is test data stored directly in the database".to_vec();
    let object1 = create_test_object_index_direct(
        "comp1".to_string(),
        "agent1".to_string(),
        test_data.clone(),
    );
    let object2 = create_test_object_index_direct(
        "comp1".to_string(),
        "agent2".to_string(),
        test_data.clone(),
    );

    db.add_object(object1.clone()).await.unwrap();
    db.add_object(object2.clone()).await.unwrap();

    // Run synchronization
    synchronizer.run(None).await.unwrap();

    // Verify objects were synchronized
    let synced = sync_storage
        .get_objects_with_status(SyncStatus::Complete)
        .await
        .unwrap();
    assert_eq!(synced.len(), 2);

    // Verify data was stored in Recall with proper keys
    let recall_key1 = TestEnvironment::construct_recall_key(&object1);
    let recall_key2 = TestEnvironment::construct_recall_key(&object2);

    assert!(recall_storage.has_blob(&recall_key1).await.unwrap());
    assert!(recall_storage.has_blob(&recall_key2).await.unwrap());

    // Verify the data stored in Recall matches the embedded data
    let stored_data1 = recall_storage.get_blob(&recall_key1).await.unwrap();
    assert_eq!(stored_data1, test_data);
}
