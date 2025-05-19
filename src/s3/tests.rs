#[cfg(test)]
use crate::s3::error::StorageError;
use crate::s3::fake::FakeStorage;
use crate::s3::s3::S3Storage;
use crate::s3::storage::Storage;
use crate::test_utils;
use bytes::Bytes;
use std::sync::Arc;

type StorageFactory =
    Box<dyn Fn() -> futures::future::BoxFuture<'static, Arc<dyn Storage + Send + Sync>>>;

// Create test storages using lazy factory pattern
fn get_test_storages() -> Vec<(&'static str, StorageFactory)> {
    let mut storages: Vec<(&'static str, StorageFactory)> = vec![];

    // Always include FakeStorage
    storages.push((
        "fake",
        Box::new(|| {
            Box::pin(async {
                let fake_storage = Arc::new(FakeStorage::new());
                // Pre-populate test data
                fake_storage
                    .add_object("test_key", Bytes::from("test data"))
                    .await
                    .unwrap();
                fake_storage as Arc<dyn Storage + Send + Sync>
            })
        }),
    ));

    // Conditionally include real S3 if configured
    let config = test_utils::load_test_config();
    if config.s3.enabled {
        // Clone the config values we need before moving into the closure
        let endpoint = config.s3.endpoint.clone();
        let region = config.s3.region.clone();
        let bucket = config.s3.bucket.clone();
        let access_key_id = config.s3.access_key_id.clone();
        let secret_access_key = config.s3.secret_access_key.clone();

        storages.push((
            "s3",
            Box::new(move || {
                let endpoint = endpoint.clone();
                let region = region.clone();
                let bucket = bucket.clone();
                let access_key_id = access_key_id.clone();
                let secret_access_key = secret_access_key.clone();

                Box::pin(async move {
                    // Convert test config to real S3Config
                    let s3_config = crate::config::S3Config {
                        endpoint: Some(endpoint),
                        region,
                        bucket,
                        access_key_id: Some(access_key_id),
                        secret_access_key: Some(secret_access_key),
                    };

                    match S3Storage::new(&s3_config).await {
                        Ok(storage) => {
                            let real_storage = Arc::new(storage);
                            // Pre-populate test data
                            real_storage
                                .add_object("test_key", Bytes::from("test data"))
                                .await
                                .unwrap();
                            real_storage as Arc<dyn Storage + Send + Sync>
                        }
                        Err(e) => {
                            panic!("Failed to create S3 storage for tests: {}", e);
                        }
                    }
                })
            }),
        ));
    }

    storages
}

#[tokio::test]
async fn if_object_exists_get_object_returns_data() {
    for (name, storage_factory) in get_test_storages() {
        let storage = storage_factory().await;
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
    for (name, storage_factory) in get_test_storages() {
        let storage = storage_factory().await;
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
    for (name, storage_factory) in get_test_storages() {
        let storage = storage_factory().await;
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

#[tokio::test]
async fn has_bucket_and_create_bucket_work_correctly() {
    for (name, storage_factory) in get_test_storages() {
        let storage = storage_factory().await;
        // Use unique bucket names to avoid conflicts
        let bucket_name = format!("test-bucket-{}-{}", name, uuid::Uuid::new_v4().simple());

        let exists = storage.has_bucket(&bucket_name).await.unwrap();
        assert!(
            !exists,
            "Bucket {} should not exist initially for {}",
            bucket_name, name
        );

        storage
            .create_bucket(&bucket_name)
            .await
            .unwrap_or_else(|_| panic!("Failed to create bucket {} for {}", bucket_name, name));

        let exists = storage.has_bucket(&bucket_name).await.unwrap();
        assert!(
            exists,
            "Bucket {} should exist after creation for {}",
            bucket_name, name
        );

        // Creating bucket again should succeed (idempotent)
        storage
            .create_bucket(&bucket_name)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "Creating bucket {} again should succeed for {}",
                    bucket_name, name
                )
            });

        // Verify bucket still exists
        let exists = storage.has_bucket(&bucket_name).await.unwrap();
        assert!(
            exists,
            "Bucket {} should still exist for {}",
            bucket_name, name
        );
    }
}
