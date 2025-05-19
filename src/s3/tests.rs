#[cfg(test)]
#[allow(clippy::module_inception)]
use crate::s3::error::StorageError;
use crate::s3::fake::FakeStorage;
use crate::s3::real_s3::S3Storage;
use crate::s3::storage::Storage;
use crate::test_utils;
use bytes::Bytes;
use std::sync::Arc;

async fn get_test_storages() -> Vec<(&'static str, Arc<dyn Storage + Send + Sync + 'static>)> {
    let mut storages = vec![];

    // Always include FakeStorage
    let fake_storage = Arc::new(FakeStorage::new());
    fake_storage
        .add_object("test_key", Bytes::from("test data"))
        .await
        .unwrap();
    storages.push((
        "fake",
        fake_storage as Arc<dyn Storage + Send + Sync + 'static>,
    ));

    // Conditionally include real S3 if configured
    let config = test_utils::load_test_config();
    if config.s3.enabled {
        // Convert test config to real S3Config
        let s3_config = crate::config::S3Config {
            endpoint: Some(config.s3.endpoint),
            region: config.s3.region,
            bucket: config.s3.bucket,
            access_key_id: Some(config.s3.access_key_id),
            secret_access_key: Some(config.s3.secret_access_key),
        };
        match S3Storage::new(&s3_config).await {
            Ok(storage) => {
                let real_storage = Arc::new(storage);
                // Pre-populate test data
                real_storage
                    .add_object("test_key", Bytes::from("test data"))
                    .await
                    .unwrap();
                storages.push((
                    "s3",
                    real_storage as Arc<dyn Storage + Send + Sync + 'static>,
                ));
            }
            Err(e) => {
                eprintln!("Failed to create S3 storage for tests: {}", e);
            }
        }
    }

    storages
}

#[tokio::test]
async fn if_object_exists_get_object_returns_data() {
    for (name, storage) in get_test_storages().await {
        let result = storage.get_object("test_key").await;
        assert!(
            result.is_ok(),
            "Failed to get existing object with {}: {:?}",
            name,
            result
        );
        assert_eq!(result.unwrap(), Bytes::from("test data"));
    }
}

#[tokio::test]
async fn if_object_not_exists_get_object_returns_error() {
    for (name, storage) in get_test_storages().await {
        let result = storage.get_object("nonexistent_key").await;
        assert!(
            matches!(result, Err(StorageError::ObjectNotFound(_))),
            "Expected ObjectNotFound error with {}, got: {:?}",
            name,
            result
        );
    }
}

#[tokio::test]
async fn add_and_remove_object_works_correctly() {
    for (name, storage) in get_test_storages().await {
        let key = format!("test_add_remove_{}", name);
        let data = Bytes::from("new test data");

        // Add object
        storage.add_object(&key, data.clone()).await.unwrap();

        // Get and verify data
        let retrieved = storage.get_object(&key).await.unwrap();
        assert_eq!(retrieved, data);

        // Remove object
        storage.remove_object(&key).await.unwrap();

        // Verify it no longer exists
        let result = storage.get_object(&key).await;
        assert!(
            matches!(result, Err(StorageError::ObjectNotFound(_))),
            "Object should not exist after removal with {}",
            name
        );
    }
}

#[tokio::test]
async fn fake_fail_object_causes_object_specific_failures() {
    let storage = FakeStorage::new();
    storage
        .add_object("fail_test_key", Bytes::from("data"))
        .await
        .unwrap();
    storage
        .add_object("fail_test_key2", Bytes::from("data"))
        .await
        .unwrap();

    // Before failing, object should be accessible
    let result = storage.get_object("fail_test_key").await;
    assert!(result.is_ok());

    // Set object to fail
    storage.fake_fail_object("fail_test_key").await;

    // Now get_object should fail with ObjectNotFound
    let result = storage.get_object("fail_test_key").await;
    assert!(matches!(result, Err(StorageError::ObjectNotFound(_))));

    // Other objects should still work
    let result = storage.get_object("fail_test_key2").await;
    assert!(result.is_ok());

    // Not existing object should still fail
    let result = storage.get_object("nonexistent_key").await;
    assert!(matches!(result, Err(StorageError::ObjectNotFound(_))));
}
