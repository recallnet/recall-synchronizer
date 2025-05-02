// src/s3/mod.rs
use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    config::{Credentials, Region},
    Client,
};
use tracing::{debug, error, info};

use crate::config::S3Config;

pub struct S3Connector {
    client: Client,
    bucket: String,
}

impl S3Connector {
    pub async fn new(config: &S3Config) -> Result<Self> {
        let s3_config_builder = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(config.region.clone()));

        let s3_config_builder = if let (Some(access_key), Some(secret_key)) =
            (&config.access_key_id, &config.secret_access_key)
        {
            s3_config_builder
                .credentials_provider(Credentials::from_keys(access_key, secret_key, None))
        } else {
            s3_config_builder
        };

        let s3_config = if let Some(endpoint) = &config.endpoint {
            s3_config_builder.endpoint_url(endpoint).build().await
        } else {
            s3_config_builder.build().await
        };

        let client = Client::new(&s3_config);

        // Test connection
        client
            .list_buckets()
            .send()
            .await
            .context("Failed to connect to S3 service")?;

        info!("Connected to S3 service");
        Ok(Self {
            client,
            bucket: config.bucket.clone(),
        })
    }

    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>> {
        debug!("Fetching object with key: {}", key);

        let response = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .context(format!("Failed to retrieve object with key: {}", key))?;

        let data = response.body.collect().await?.into_bytes().to_vec();
        debug!(
            "Successfully retrieved object {} ({} bytes)",
            key,
            data.len()
        );

        Ok(data)
    }
}
