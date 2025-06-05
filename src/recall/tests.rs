use crate::config::RecallConfig;
use crate::recall::error::RecallError;
use crate::recall::fake::FakeRecallStorage;
use crate::recall::{RecallBlockchain, Storage};
use crate::test_utils::{get_next_wallet, load_test_config};
use std::sync::Arc;
use uuid::Uuid;

type StorageFactory =
    Box<dyn Fn() -> futures::future::BoxFuture<'static, Box<dyn Storage + Send + Sync>>>;

fn get_test_storages() -> Vec<(&'static str, StorageFactory)> {
    let mut storages: Vec<(&'static str, StorageFactory)> = vec![];

    // Always add fake storage
    storages.push((
        "fake",
        Box::new(|| {
            Box::pin(async { Box::new(FakeRecallStorage::new()) as Box<dyn Storage + Send + Sync> })
        }),
    ));

    // Conditionally add real Recall
    let config = load_test_config();
    if config.recall.enabled {
        storages.push((
            "real_recall",
            Box::new(|| {
                Box::pin(async {
                    let config = load_test_config();

                    // Get a unique wallet for this test
                    let test_wallet = get_next_wallet();
                    println!("Using test wallet: {}", test_wallet.address);

                    let network = config.recall.network.clone().unwrap_or_else(|| "localnet".to_string());
                    let config_path = config.recall.config_path.clone().unwrap_or_else(|| "networks.toml".to_string());
                    let recall_config = RecallConfig {
                        private_key: test_wallet.private_key.clone(),
                        network,
                        config_path: Some(config_path),
                        bucket: None,
                    };

                    println!("Preparing wallet {} with credits...", test_wallet.address);
                    // Buy 20 RECALL worth of credits
                    match RecallBlockchain::prepare_account(&recall_config, 20).await {
                        Ok(_) => {
                            println!("Successfully prepared wallet {} with credits", test_wallet.address);
                        }
                        Err(e) => {
                            println!("Warning: Failed to prepare wallet {} with credits: {}. Tests may fail if account lacks credits.", test_wallet.address, e);
                        }
                    }

                    match RecallBlockchain::new(&recall_config).await {
                        Ok(blockchain) => {
                            println!("Successfully connected to Recall storage with wallet {}", test_wallet.address);
                            Box::new(Arc::new(blockchain)) as Box<dyn Storage + Send + Sync>
                        },
                        Err(e) => {
                            panic!("Failed to connect to real Recall storage: {}\n\nMake sure the Recall container is running and accessible", e);
                        }
                    }
                })
            }),
        ));
    }

    storages
}

#[tokio::test]
async fn add_blob_and_has_blob_work_correctly() {
    for (name, storage_factory) in get_test_storages() {
        let storage = storage_factory().await;
        let key = format!("test-blob-{}-{}", name, Uuid::new_v4());
        let data = b"test data".to_vec();

        // Initially blob should not exist
        let exists = storage.has_blob(&key).await.unwrap();
        assert!(!exists, "Blob should not exist initially for {}", name);

        // Add blob
        storage.add_blob(&key, data.clone()).await.unwrap();

        // Now blob should exist
        let exists = storage.has_blob(&key).await.unwrap();
        assert!(exists, "Blob should exist after adding for {}", name);

        // Clean up
        //storage.delete_blob(&key).await.unwrap();
    }
}

#[tokio::test]
async fn get_blob_works_correctly() {
    for (name, storage_factory) in get_test_storages() {
        let storage = storage_factory().await;
        let key = format!("test-get-blob-{}-{}", name, Uuid::new_v4());
        let original_data = b"test data for get_blob".to_vec();

        let result = storage.get_blob(&key).await;
        assert!(
            result.is_err(),
            "Getting non-existent blob should fail for {}",
            name
        );

        storage.add_blob(&key, original_data.clone()).await.unwrap();

        let retrieved_data = storage.get_blob(&key).await.unwrap();

        assert_eq!(
            retrieved_data, original_data,
            "Retrieved data should match original for {}",
            name
        );

        storage.delete_blob(&key).await.unwrap();
    }
}

#[tokio::test]
async fn list_blobs_works_correctly() {
    for (name, storage_factory) in get_test_storages() {
        let storage = storage_factory().await;
        let prefix = format!("test-prefix-{}-{}/", name, Uuid::new_v4());

        storage.clear_prefix(&prefix).await.unwrap();

        let blobs = storage.list_blobs(&prefix).await.unwrap();
        assert!(
            blobs.is_empty(),
            "Should be no blobs initially for {}",
            name
        );

        // Add some blobs
        let keys = vec![
            format!("{}file1.txt", prefix),
            format!("{}file2.txt", prefix),
            format!("{}dir/file3.txt", prefix),
        ];

        for key in &keys {
            storage.add_blob(key, b"test data".to_vec()).await.unwrap();
        }

        let blobs = storage.list_blobs(&prefix).await.unwrap();
        assert_eq!(blobs.len(), 3, "Should have 3 blobs for {}", name);

        // Check that all keys are present
        for key in &keys {
            assert!(
                blobs.contains(key),
                "Should contain key {} for {}",
                key,
                name
            );
        }

        // List blobs with more specific prefix
        let dir_prefix = format!("{}dir/", prefix);
        let dir_blobs = storage.list_blobs(&dir_prefix).await.unwrap();
        assert_eq!(dir_blobs.len(), 1, "Should have 1 blob in dir for {}", name);
        assert!(
            dir_blobs.contains(&keys[2]),
            "Should contain dir file for {}",
            name
        );

        storage.clear_prefix(&prefix).await.unwrap();
    }
}

#[tokio::test]
//#[ignore = "Deletion needs to be refined"]
async fn delete_blob_works_correctly() {
    for (name, storage_factory) in get_test_storages() {
        let storage = storage_factory().await;
        let key = format!("test-delete-{}-{}", name, Uuid::new_v4());
        let data = b"test data".to_vec();

        println!("Adding blob with key: {}", key);
        storage.add_blob(&key, data).await.unwrap();

        // Wait for blob to be available with retry
        let max_wait = tokio::time::Duration::from_secs(10);
        let start = tokio::time::Instant::now();
        loop {
            if storage.has_blob(&key).await.unwrap() {
                break;
            }
            if start.elapsed() > max_wait {
                panic!("Blob did not become available within timeout for {}", name);
            }
            println!("Waiting for blob to be available for {}...", name);
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        storage.delete_blob(&key).await.unwrap();

        // Wait for blob to be deleted with retry
        let start = tokio::time::Instant::now();
        let mut blob_deleted = false;
        while start.elapsed() < max_wait {
            if !storage.has_blob(&key).await.unwrap() {
                blob_deleted = true;
                break;
            }
            println!("Waiting for blob to be deleted for {}...", name);
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        assert!(
            blob_deleted,
            "Blob should not exist after deletion for {}",
            name
        );

        let result = storage.delete_blob(&key).await;
        assert!(
            result.is_err(),
            "Deleting non-existent blob should fail for {}",
            name
        );
    }
}

#[tokio::test]
async fn clear_prefix_works_correctly() {
    for (name, storage_factory) in get_test_storages() {
        let storage = storage_factory().await;
        let prefix = format!("test-clear-{}-{}/", name, Uuid::new_v4());
        let other_prefix = format!("test-other-{}-{}/", name, Uuid::new_v4());

        // Add blobs with our prefix
        let our_keys = vec![
            format!("{}file1.txt", prefix),
            format!("{}file2.txt", prefix),
            format!("{}dir/file3.txt", prefix),
        ];

        for key in &our_keys {
            storage.add_blob(key, b"test data".to_vec()).await.unwrap();
        }

        // Add blobs with other prefix
        let other_keys = vec![
            format!("{}file1.txt", other_prefix),
            format!("{}file2.txt", other_prefix),
        ];

        for key in &other_keys {
            storage.add_blob(key, b"test data".to_vec()).await.unwrap();
        }

        storage.clear_prefix(&prefix).await.unwrap();

        let our_blobs = storage.list_blobs(&prefix).await.unwrap();
        assert!(
            our_blobs.is_empty(),
            "Our blobs should be cleared for {}",
            name
        );

        let other_blobs = storage.list_blobs(&other_prefix).await.unwrap();
        assert_eq!(
            other_blobs.len(),
            2,
            "Other blobs should still exist for {}",
            name
        );

        storage.clear_prefix(&other_prefix).await.unwrap();
    }
}

#[tokio::test]
async fn error_handling_works_correctly() {
    // Test with fake storage only as we can control failures
    let storage = FakeRecallStorage::new();
    let key = format!("test-error-{}", Uuid::new_v4());

    storage.fake_fail_blob(&key);

    let add_result = storage.add_blob(&key, b"data".to_vec()).await;
    assert!(add_result.is_err(), "Add should fail for failed blob");

    let has_result = storage.has_blob(&key).await;
    assert!(has_result.is_err(), "Has should fail for failed blob");

    storage.fake_reset_blob(&key);

    let add_result = storage.add_blob(&key, b"data".to_vec()).await;
    assert!(
        add_result.is_ok(),
        "Add should succeed after clearing failure"
    );

    let has_result = storage.has_blob(&key).await;
    assert!(
        has_result.is_ok(),
        "Has should succeed after clearing failure"
    );
}

#[tokio::test]
async fn fake_storage_failure_simulation() {
    let storage = FakeRecallStorage::new();
    let key = "fail-test";
    let data = b"test data".to_vec();

    storage.fake_fail_blob(key);

    // Operations should fail with Operation error
    let add_result = storage.add_blob(key, data.clone()).await;
    assert!(matches!(add_result, Err(RecallError::Operation(_))));

    let has_result = storage.has_blob(key).await;
    assert!(matches!(has_result, Err(RecallError::Operation(_))));

    storage.fake_reset_blob(key);

    // Now operations should succeed
    storage.add_blob(key, data).await.unwrap();

    let exists = storage.has_blob(key).await.unwrap();
    assert!(exists);
}

#[tokio::test]
async fn concurrent_operations() {
    for (name, storage_factory) in get_test_storages() {
        let storage = Arc::new(storage_factory().await);
        let prefix = format!("test-concurrent-{}-{}/", name, Uuid::new_v4());

        // Spawn multiple tasks that add blobs concurrently
        let mut handles = vec![];
        for i in 0..10 {
            let storage_clone = storage.clone();
            let key = format!("{}file{}.txt", prefix, i);
            let handle = tokio::spawn(async move {
                storage_clone
                    .add_blob(&key, format!("data{}", i).into_bytes())
                    .await
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        let blobs = storage.list_blobs(&prefix).await.unwrap();
        assert_eq!(blobs.len(), 10, "Should have 10 blobs for {}", name);

        storage.clear_prefix(&prefix).await.unwrap();
    }
}
