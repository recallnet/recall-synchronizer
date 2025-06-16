#[cfg(test)]
use crate::s3::error::StorageError;
use crate::s3::fake::FakeStorage;
use crate::s3::s3::S3Storage;
use crate::s3::storage::Storage;
use crate::test_utils::{is_s3_enabled, load_test_config};
use bytes::Bytes;
use std::sync::Arc;

type StorageFactory =
    Box<dyn Fn() -> futures::future::BoxFuture<'static, Arc<dyn Storage + Send + Sync>>>;

// Create test storages using lazy factory pattern
fn get_test_storages() -> Vec<(&'static str, StorageFactory)> {
    let mut storages: Vec<(&'static str, StorageFactory)> = vec![];

    storages.push((
        "fake",
        Box::new(|| {
            Box::pin(async {
                let fake_storage = Arc::new(FakeStorage::new());
                fake_storage
                    .add_object("test_key", Bytes::from("test data"))
                    .await
                    .unwrap();
                fake_storage as Arc<dyn Storage + Send + Sync>
            })
        }),
    ));

    if is_s3_enabled() {
        let config = load_test_config().expect("Failed to load test config");

        if let Some(s3_config) = config.s3 {
            let s3_config_clone = s3_config.clone();

            storages.push((
                "s3",
                Box::new(move || {
                    let s3_config = s3_config_clone.clone();

                    Box::pin(async move {
                        match S3Storage::new(&s3_config).await {
                            Ok(storage) => {
                                let real_storage = Arc::new(storage);

                                if !real_storage
                                    .has_bucket(&s3_config.bucket)
                                    .await
                                    .unwrap_or(false)
                                {
                                    if let Err(e) =
                                        real_storage.create_bucket(&s3_config.bucket).await
                                    {
                                        eprintln!("Warning: Failed to create test bucket: {}", e);
                                        // Continue anyway, as bucket might exist but we can't check
                                    }
                                }

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
    }

    storages
}

#[tokio::test]
async fn if_object_exists_get_object_returns_data() {
    for (name, storage_factory) in get_test_storages() {
        let storage = storage_factory().await;

        let result = storage
            .add_object("test_key", Bytes::from("test data"))
            .await;
        assert!(
            result.is_ok(),
            "Failed to add object with {}: {:?}",
            name,
            result
        );

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

        storage.add_object(&key, data.clone()).await.unwrap();

        let retrieved = storage.get_object(&key).await.unwrap();
        assert_eq!(retrieved, data);

        storage.remove_object(&key).await.unwrap();

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

    storage.fake_fail_object("fail_test_key").await;

    let result = storage.get_object("fail_test_key").await;
    assert!(matches!(result, Err(StorageError::ObjectNotFound(_))));

    let result = storage.get_object("fail_test_key2").await;
    assert!(result.is_ok());

    let result = storage.get_object("nonexistent_key").await;
    assert!(matches!(result, Err(StorageError::ObjectNotFound(_))));
}

#[tokio::test]
async fn has_bucket_and_create_bucket_work_correctly() {
    for (name, storage_factory) in get_test_storages() {
        let storage = storage_factory().await;
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

        storage
            .create_bucket(&bucket_name)
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "Creating bucket {} again should succeed for {}",
                    bucket_name, name
                )
            });

        let exists = storage.has_bucket(&bucket_name).await.unwrap();
        assert!(
            exists,
            "Bucket {} should still exist for {}",
            bucket_name, name
        );
    }
}
