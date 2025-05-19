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
        // Set up AWS SDK config
        let config_loader = aws_config::from_env().region(Region::new(config.region.clone()));

        // If access key and secret are provided, use them for credentials
        let aws_config = if let (Some(access_key), Some(secret_key)) =
            (&config.access_key_id, &config.secret_access_key)
        {
            let credentials = Credentials::new(
                access_key,
                secret_key,
                None,
                None,
                "StaticCredentialsProvider",
            );

            config_loader.credentials_provider(credentials).load().await
        } else {
            config_loader.load().await
        };

        // Create S3 client with endpoint override if provided
        let mut client_builder = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(endpoint) = &config.endpoint {
            client_builder = client_builder.endpoint_url(endpoint);
        }

        let s3_config = client_builder.build();
        let client = Client::from_conf(s3_config);

        // Create LRU cache for objects - default to 100 items
        let cache_size = NonZeroUsize::new(100).unwrap();
        let cache = Arc::new(Mutex::new(lru::LruCache::new(cache_size)));

        info!("Connected to S3 in region {}", config.region);

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
            cache,
        })
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
            .map_err(|e| match e {
                _ if e.to_string().contains("NoSuchKey") => {
                    StorageError::ObjectNotFound(key.to_string())
                }
                _ if e.to_string().contains("AccessDenied") => {
                    StorageError::AccessDenied(key.to_string(), e.to_string())
                }
                _ => StorageError::ReadError(key.to_string(), e.to_string()),
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
            .map_err(|e| StorageError::Other(anyhow::anyhow!("Failed to put object: {}", e)))?;

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
            .map_err(|e| StorageError::Other(anyhow::anyhow!("Failed to delete object: {}", e)))?;

        // Also remove from cache
        let mut cache = self.cache.lock().await;
        cache.pop(key);

        Ok(())
    }
}
