use crate::config::S3Config;
use crate::s3::error::StorageError;
use crate::s3::storage::Storage;
use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Region, Client};
use bytes::Bytes;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// Real S3 implementation of the Storage trait
#[derive(Clone)]
pub struct S3Storage {
    client: Client,
    bucket: String,
    cache: Arc<Mutex<lru::LruCache<String, Bytes>>>,
}

impl S3Storage {
    /// Create a new S3Storage instance from configuration
    pub async fn new(config: &S3Config) -> Result<Self, StorageError> {
        info!(
            "Creating S3Storage with config: endpoint={:?}, region={}, bucket={}, access_key={:?}",
            config.endpoint, config.region, config.bucket, config.access_key_id
        );

        // Create a custom S3 client configuration for MinIO
        let mut s3_config_builder = aws_sdk_s3::config::Builder::new()
            .region(Region::new(config.region.clone()))
            .force_path_style(true); // MinIO requires path-style requests

        // Configure credentials if provided
        if let (Some(access_key), Some(secret_key)) =
            (&config.access_key_id, &config.secret_access_key)
        {
            let credentials = Credentials::new(
                access_key,
                secret_key,
                None,
                None,
                "StaticCredentialsProvider",
            );

            s3_config_builder = s3_config_builder.credentials_provider(credentials);
        }

        // Configure endpoint
        if let Some(endpoint) = &config.endpoint {
            info!("Setting custom endpoint: {}", endpoint);
            s3_config_builder = s3_config_builder.endpoint_url(endpoint);
        }

        let s3_config = s3_config_builder.build();
        let client = Client::from_conf(s3_config);

        // Create LRU cache for objects - default to 100 items
        let cache_size = NonZeroUsize::new(100).unwrap();
        let cache = Arc::new(Mutex::new(lru::LruCache::new(cache_size)));

        info!("Created S3 client for region {}", config.region);

        let storage = Self {
            client,
            bucket: config.bucket.clone(),
            cache,
        };

        // For testing, ensure bucket exists
        #[cfg(test)]
        {
            storage.ensure_bucket_exists().await?;
        }

        Ok(storage)
    }

    #[cfg(test)]
    async fn ensure_bucket_exists(&self) -> Result<(), StorageError> {
        // First, try a simple list buckets call to test connectivity
        info!("Testing S3 connectivity by listing buckets...");
        match self.client.list_buckets().send().await {
            Ok(buckets) => {
                info!(
                    "Successfully connected to S3. Found {} buckets",
                    buckets.buckets().map_or(0, |b| b.len())
                );
            }
            Err(e) => {
                eprintln!("Failed to list buckets. Connectivity error: {}", e);
                eprintln!("Error details: {:?}", e);
                return Err(StorageError::Other(anyhow::anyhow!(
                    "Cannot connect to S3 endpoint. Error: {}",
                    e
                )));
            }
        }

        // Check if bucket exists using trait method
        if !self.has_bucket(&self.bucket).await? {
            // Create the bucket using trait method
            self.create_bucket(&self.bucket).await?;
        }

        Ok(())
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn get_object(&self, key: &str) -> Result<Bytes, StorageError> {
        // Check cache first
        {
            let mut cache = self.cache.lock().await;
            if let Some(data) = cache.get(key) {
                debug!("Cache hit for object: {}", key);
                return Ok(data.clone());
            }
        }

        debug!("Fetching object from S3: {}", key);

        // Get from S3 if not in cache
        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                // Check if this is a service error and extract the specific error type
                if let aws_sdk_s3::error::SdkError::ServiceError(ref service_err) = e {
                    let err = service_err.err();
                    let metadata = err.meta();

                    // Check the error code
                    if let Some(code) = metadata.code() {
                        match code {
                            "NoSuchKey" | "KeyNotFound" => {
                                return StorageError::ObjectNotFound(key.to_string());
                            }
                            "AccessDenied" => {
                                return StorageError::AccessDenied(key.to_string(), e.to_string());
                            }
                            _ => {}
                        }
                    }
                }

                // Fallback to string matching for other cases
                let error_str = e.to_string();
                if error_str.contains("NoSuchKey")
                    || error_str.contains("KeyNotFound")
                    || error_str.contains("does not exist")
                    || error_str.contains("404")
                    || error_str.contains("Not Found")
                {
                    StorageError::ObjectNotFound(key.to_string())
                } else if error_str.contains("AccessDenied") {
                    StorageError::AccessDenied(key.to_string(), error_str)
                } else {
                    StorageError::ReadError(key.to_string(), error_str)
                }
            })?;

        let data = response
            .body
            .collect()
            .await
            .map_err(|e| StorageError::ReadError(key.to_string(), e.to_string()))?
            .into_bytes();

        // Store in cache
        {
            let mut cache = self.cache.lock().await;
            cache.put(key.to_string(), data.clone());
        }

        debug!("Successfully fetched object from S3: {}", key);
        Ok(data)
    }

    #[cfg(test)]
    async fn add_object(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(data.clone().into())
            .send()
            .await
            .map_err(|e| {
                let error_msg = format!(
                    "Failed to put object '{}' to bucket '{}': {}",
                    key, self.bucket, e
                );
                if e.to_string().contains("NoSuchBucket") {
                    StorageError::Other(anyhow::anyhow!(
                        "Bucket '{}' does not exist. Please create it first.",
                        self.bucket
                    ))
                } else {
                    StorageError::Other(anyhow::anyhow!(error_msg))
                }
            })?;

        // Also add to cache
        let mut cache = self.cache.lock().await;
        cache.put(key.to_string(), data);

        Ok(())
    }

    #[cfg(test)]
    async fn remove_object(&self, key: &str) -> Result<(), StorageError> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                eprintln!("Error deleting object '{}': {}", key, e);
                StorageError::Other(anyhow::anyhow!("Failed to delete object '{}': {}", key, e))
            })?;

        // Also remove from cache
        let mut cache = self.cache.lock().await;
        cache.pop(key);

        debug!("Successfully removed object: {}", key);
        Ok(())
    }

    #[cfg(test)]
    async fn has_bucket(&self, bucket: &str) -> Result<bool, StorageError> {
        let result = self.client.head_bucket().bucket(bucket).send().await;

        match result {
            Ok(_) => {
                info!("Bucket '{}' exists", bucket);
                Ok(true)
            }
            Err(e) => {
                // Check if this is a service error
                if let aws_sdk_s3::error::SdkError::ServiceError(ref service_err) = e {
                    let err = service_err.err();
                    let metadata = err.meta();

                    // Check the error code
                    if let Some(code) = metadata.code() {
                        debug!("has_bucket error for '{}': code={}", bucket, code);

                        if code == "NoSuchBucket" || code == "NotFound" {
                            info!("Bucket '{}' does not exist", bucket);
                            return Ok(false);
                        }
                    }
                }

                // Default error handling
                let error_str = e.to_string();
                if error_str.contains("NoSuchBucket")
                    || error_str.contains("404")
                    || error_str.contains("NotFound")
                {
                    info!("Bucket '{}' does not exist", bucket);
                    Ok(false)
                } else {
                    eprintln!("Error checking bucket '{}': {:?}", bucket, e);
                    Err(StorageError::Other(anyhow::anyhow!(
                        "Error checking bucket existence: {}",
                        e
                    )))
                }
            }
        }
    }

    #[cfg(test)]
    async fn create_bucket(&self, bucket: &str) -> Result<(), StorageError> {
        info!("Creating bucket '{}'", bucket);

        match self.client.create_bucket().bucket(bucket).send().await {
            Ok(_) => {
                info!("Successfully created bucket '{}'", bucket);
                Ok(())
            }
            Err(create_err) => {
                // Check if this is a service error
                if let aws_sdk_s3::error::SdkError::ServiceError(ref service_err) = create_err {
                    let err = service_err.err();
                    let metadata = err.meta();

                    // Check the error code
                    match metadata.code() {
                        Some("BucketAlreadyExists") | Some("BucketAlreadyOwnedByYou") => {
                            info!("Bucket '{}' already exists", bucket);
                            return Ok(());
                        }
                        _ => {}
                    }
                }

                // Fallback to string matching
                let error_str = create_err.to_string();
                if error_str.contains("BucketAlreadyExists")
                    || error_str.contains("BucketAlreadyOwnedByYou")
                    || error_str.contains("already exists")
                {
                    info!("Bucket '{}' already exists", bucket);
                    Ok(())
                } else {
                    eprintln!("Failed to create bucket '{}': {:?}", bucket, create_err);
                    Err(StorageError::Other(anyhow::anyhow!(
                        "Failed to create bucket '{}': {}",
                        bucket,
                        create_err
                    )))
                }
            }
        }
    }
}
