// src/s3/mod.rs
use anyhow::{Context, Result};
use aws_credential_types::Credentials;
use aws_sdk_s3::{config::Region, Client};
use bytes::Bytes;
#[cfg(test)]
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::config::S3Config;

#[cfg(test)]
pub mod test_utils {
    use super::*;

    #[derive(Clone)]
    pub struct FakeS3Connector {
        #[allow(dead_code)]
        bucket: String,
        objects: Arc<Mutex<HashMap<String, Bytes>>>,
    }

    impl FakeS3Connector {
        pub fn new(bucket: &str) -> Self {
            FakeS3Connector {
                bucket: bucket.to_string(),
                objects: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        pub async fn fake_add_object(&self, key: &str, data: Bytes) {
            let mut objects = self.objects.lock().await;
            objects.insert(key.to_string(), data);
        }
    }

    impl FakeS3Connector {
        pub async fn get_object(&self, key: &str) -> Result<Bytes> {
            debug!("[FAKE] Fetching object from S3: {}", key);

            let objects = self.objects.lock().await;
            match objects.get(key) {
                Some(data) => {
                    debug!("[FAKE] Found object in fake storage: {}", key);
                    Ok(data.clone())
                }
                None => {
                    let data = Bytes::from(format!("Fake data for {}", key));
                    debug!("[FAKE] Generated fake data for: {}", key);
                    Ok(data)
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct S3Connector {
    client: Client,
    bucket: String,
    cache: Arc<Mutex<lru::LruCache<String, Bytes>>>,
}

impl S3Connector {
    pub async fn new(config: &S3Config) -> Result<Self> {
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

    pub async fn get_object(&self, key: &str) -> Result<Bytes> {
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
            .context(format!("Failed to get object from S3: {}", key))?;

        let data = response
            .body
            .collect()
            .await
            .context(format!("Failed to read object body: {}", key))?
            .into_bytes();

        // Store in cache
        {
            let mut cache = self.cache.lock().await;
            cache.put(key.to_string(), data.clone());
        }

        debug!("Successfully fetched object from S3: {}", key);
        Ok(data)
    }
}
