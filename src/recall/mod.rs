use anyhow::{Context, Result};
use recall_rs::{
    client::{Client, ClientBuilder},
    BucketInfo,
};
use std::str::FromStr;
use tracing::{debug, error, info};

use crate::config::RecallConfig;

pub struct RecallConnector {
    client: Client,
    prefix: Option<String>,
}

impl RecallConnector {
    pub async fn new(config: &RecallConfig) -> Result<Self> {
        let client = ClientBuilder::default()
            .with_address(&config.endpoint)
            .with_private_key_hex(&config.private_key)
            .build()
            .await
            .context("Failed to initialize Recall client")?;

        info!("Connected to Recall network at {}", &config.endpoint);

        Ok(Self {
            client,
            prefix: config.prefix.clone(),
        })
    }

    pub async fn store_object(&self, key: &str, data: &[u8]) -> Result<String> {
        let full_key = if let Some(prefix) = &self.prefix {
            format!("{}/{}", prefix, key)
        } else {
            key.to_string()
        };

        debug!("Storing object to Recall: {}", full_key);

        let cid = self
            .client
            .write_object(&full_key, data)
            .await
            .context(format!("Failed to store object to Recall: {}", full_key))?;

        debug!("Successfully stored object {} with CID: {}", full_key, cid);
        Ok(cid.to_string())
    }

    pub async fn ensure_bucket_exists(&self, bucket_name: &str) -> Result<()> {
        // Check if bucket exists
        let buckets = self
            .client
            .list_buckets()
            .await
            .context("Failed to list buckets")?;

        // If bucket doesn't exist, create it
        if !buckets.iter().any(|b| b.name == bucket_name) {
            info!("Creating bucket: {}", bucket_name);
            self.client
                .create_bucket(bucket_name)
                .await
                .context(format!("Failed to create bucket: {}", bucket_name))?;
        }

        Ok(())
    }
}
