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
use std::sync::Arc;
use uuid::Uuid;

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
        // Check sync storage status
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

        // Verify the blob exists in Recall
        let exists_in_recall = self
            .recall_storage
            .has_blob(&object.object_key)
            .await
            .map_err(|e| format!("Failed to check blob existence in Recall: {}", e))?;

        if !exists_in_recall {
            return Err(format!(
                "Object {} not found in Recall storage",
                object.object_key
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
    let database = Arc::new(FakeDatabase::new());
    let sync_storage = Arc::new(FakeSyncStorage::new());
    let s3_storage = Arc::new(FakeStorage::new());
    let recall_storage = Arc::new(FakeRecallStorage::new());
    let config = create_test_config();

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

    // Verify each object is properly synchronized
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

    // Verify the filtered object is properly synchronized
    env.verify_object_synced(&filtered_object)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Filtered object {} verification failed: {}",
                filtered_object.object_key, e
            )
        });

    // Verify other objects were NOT synchronized
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

    // Verify the old object was NOT synchronized
    env.verify_object_not_synced(&old_object)
        .await
        .unwrap_or_else(|e| {
            panic!(
                "Old object {} should not be synchronized: {}",
                old_object.object_key, e
            )
        });

    // Verify newer objects were synchronized
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
