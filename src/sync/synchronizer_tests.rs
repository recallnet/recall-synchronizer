#[cfg(test)]
mod tests {
    use crate::db::{FakeDatabase, ObjectIndex};
    use crate::sync::storage::{FakeSyncStorage, SyncStorage};
    use crate::sync::Synchronizer;
    use crate::config::{
        Config, DatabaseConfig, S3Config, RecallConfig, SyncConfig
    };
    use crate::s3::test_utils::FakeS3Connector;
    use crate::recall::test_utils::FakeRecallConnector;
    use bytes::Bytes;
    use chrono::{DateTime, Duration, Utc};
    use std::sync::Arc;
    use uuid::Uuid;

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

        let object1 = ObjectIndex {
            id: Uuid::new_v4(),
            object_key: "test/object1.jsonl".to_string(),
            bucket_name: "test-bucket".to_string(),
            competition_id: Some(Uuid::new_v4()),
            agent_id: Some(Uuid::new_v4()),
            data_type: "TEST_DATA".to_string(),
            size_bytes: Some(1024),
            content_hash: Some("hash1".to_string()),
            metadata: None,
            event_timestamp: Some(now),
            object_last_modified_at: now,
            created_at: now,
            updated_at: now,
        };

        let object2 = ObjectIndex {
            id: Uuid::new_v4(),
            object_key: "test/object2.jsonl".to_string(),
            bucket_name: "test-bucket".to_string(),
            competition_id: Some(Uuid::new_v4()),
            agent_id: Some(Uuid::new_v4()),
            data_type: "TEST_DATA".to_string(),
            size_bytes: Some(2048),
            content_hash: Some("hash2".to_string()),
            metadata: None,
            event_timestamp: Some(now + Duration::hours(1)),
            object_last_modified_at: now + Duration::hours(1),
            created_at: now,
            updated_at: now,
        };

        database.fake_add_object(object1);
        database.fake_add_object(object2);

        // Create fake sync storage
        let sync_storage = FakeSyncStorage::new();

        // Create test config
        let config = create_test_config();

        // Create fake S3 connector
        let s3_connector = Arc::new(FakeS3Connector::new(&config.s3.bucket));

        // Setup test data in S3
        s3_connector.fake_add_object(
            "test/object1.jsonl",
            Bytes::from("Test data for object1")
        ).await;

        s3_connector.fake_add_object(
            "test/object2.jsonl",
            Bytes::from("Test data for object2")
        ).await;

        // Create fake Recall connector
        let recall_connector = Arc::new(FakeRecallConnector::new(config.recall.prefix.clone()));

        (database, sync_storage, s3_connector, recall_connector, config)
    }
    
    #[tokio::test]
    async fn test_synchronizer_run_with_fake_implementations() {
        let (database, sync_storage, s3_connector, recall_connector, config) =
            setup_test_env().await;
        
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
        let is_synced1 = SyncStorage::is_object_synced(&**sync_storage, "test/object1.jsonl").await.unwrap();
        let is_synced2 = SyncStorage::is_object_synced(&**sync_storage, "test/object2.jsonl").await.unwrap();

        assert!(is_synced1, "Object 1 should be synced");
        assert!(is_synced2, "Object 2 should be synced");

        // Check that the last sync timestamp was updated
        let last_sync = SyncStorage::get_last_sync_timestamp(&**sync_storage).await.unwrap();
        assert!(last_sync.is_some(), "Last sync timestamp should be set");
    }
    
    #[tokio::test]
    async fn test_synchronizer_with_timestamp_filter() {
        let (database, sync_storage, s3_connector, recall_connector, config) =
            setup_test_env().await;
        
        // Update the last sync timestamp to filter out all objects
        let future_time = Utc::now() + Duration::days(1);
        sync_storage.fake_set_last_sync_timestamp(Some(future_time));
        
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
        
        // Check that no objects were synced (since they're all older than our timestamp)
        let sync_storage = synchronizer.get_sync_storage();
        let is_synced1 = SyncStorage::is_object_synced(&**sync_storage, "test/object1.jsonl").await.unwrap();
        let is_synced2 = SyncStorage::is_object_synced(&**sync_storage, "test/object2.jsonl").await.unwrap();
        
        assert!(!is_synced1, "Object 1 should not be synced due to timestamp filter");
        assert!(!is_synced2, "Object 2 should not be synced due to timestamp filter");
    }
    
    #[tokio::test]
    async fn test_synchronizer_with_competition_id_filter() {
        // This test is a placeholder since filtering by competition_id is not fully
        // implemented in the current version of the synchronizer.
        // When that functionality is implemented, this test should be expanded.
        
        let (database, sync_storage, s3_connector, recall_connector, config) =
            setup_test_env().await;
        
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
        let result = synchronizer.run(Some("test-competition-id".to_string()), None).await;
        
        // Check that the run completed successfully
        assert!(result.is_ok(), "Synchronizer run should succeed even with unimplemented filter");
    }
}