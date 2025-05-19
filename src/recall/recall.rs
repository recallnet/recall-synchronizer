use crate::config::RecallConfig;
use crate::recall::error::RecallError;
use crate::recall::storage::RecallStorage;
use async_trait::async_trait;
use hex;
use recall_provider::json_rpc::{JsonRpcProvider, Url};
use recall_sdk::machine::bucket::{AddOptions, Bucket, QueryOptions};
use recall_sdk::machine::Machine;
use recall_signer::{key::SecretKey, AccountKind, Signer, Wallet};
use std::sync::Arc;
use tempfile;
use tokio::fs;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// Real Recall blockchain implementation of the RecallStorage trait
pub struct RecallBlockchain {
    endpoint: String,
    prefix: Option<String>,
    provider: Arc<JsonRpcProvider>,
    signer: Arc<Wallet>,
    bucket: Arc<Bucket>,
    cache: Arc<Mutex<lru::LruCache<String, String>>>,
}

impl Clone for RecallBlockchain {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            prefix: self.prefix.clone(),
            provider: self.provider.clone(),
            signer: self.signer.clone(),
            bucket: self.bucket.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl RecallBlockchain {
    /// Create a new RecallBlockchain instance from configuration
    pub async fn new(config: &RecallConfig) -> Result<Self, RecallError> {
        info!(
            "Creating RecallBlockchain with endpoint {}",
            config.endpoint
        );

        // Parse the endpoint URL
        let url = config
            .endpoint
            .parse::<Url>()
            .map_err(|e| RecallError::Configuration(format!("Invalid endpoint URL: {}", e)))?;

        // Parse chain ID from config (defaulting to 1 for mainnet)
        let chain_id = 1234; // Use the appropriate chain ID for Recall

        // Create provider
        let provider = JsonRpcProvider::new_http(url, chain_id.into(), None, None)
            .map_err(|e| RecallError::Connection(format!("Failed to create provider: {}", e)))?;

        // Create secret key from hex string
        let secret_key_bytes = hex::decode(&config.private_key)
            .map_err(|e| RecallError::Configuration(format!("Invalid private key hex: {}", e)))?;

        // Use libsecp256k1 instead of secp256k1
        let libsecp_secret_key = libsecp256k1::SecretKey::parse_slice(&secret_key_bytes)
            .map_err(|e| RecallError::Configuration(format!("Invalid secret key: {}", e)))?;

        let secret_key = SecretKey::from(libsecp_secret_key);

        // Create wallet with the secret key
        let subnet_id = "recall"
            .parse()
            .map_err(|e| RecallError::Configuration(format!("Invalid subnet ID: {}", e)))?;

        let mut wallet = Wallet::new_secp256k1(secret_key, AccountKind::Ethereum, subnet_id)
            .map_err(|e| RecallError::Configuration(format!("Failed to create wallet: {}", e)))?;

        // Initialize signer sequence
        wallet.init_sequence(&provider).await.map_err(|e| {
            RecallError::Connection(format!("Failed to init signer sequence: {}", e))
        })?;

        // Get or create a bucket
        let address = wallet.address();

        // Try to list existing buckets first
        let existing_buckets = Bucket::list(&provider, &wallet, Default::default())
            .await
            .map_err(|e| RecallError::Connection(format!("Failed to list buckets: {}", e)))?;

        let bucket = if let Some(existing_meta) = existing_buckets.first() {
            // Use existing bucket
            info!(
                "Using existing bucket at address: {}",
                existing_meta.address
            );
            Bucket::attach(existing_meta.address).await.map_err(|e| {
                RecallError::Connection(format!("Failed to attach to bucket: {}", e))
            })?
        } else {
            // Create a new bucket
            info!("Creating new bucket");
            let (bucket, _) = Bucket::new(
                &provider,
                &mut wallet,
                Some(address),                    // owner
                std::collections::HashMap::new(), // metadata
                Default::default(),               // gas options
            )
            .await
            .map_err(|e| RecallError::Connection(format!("Failed to create bucket: {}", e)))?;
            bucket
        };

        // Create LRU cache for CIDs - default to 100 items
        let cache_size = std::num::NonZeroUsize::new(100).unwrap();
        let cache = Arc::new(Mutex::new(lru::LruCache::new(cache_size)));

        Ok(Self {
            endpoint: config.endpoint.clone(),
            prefix: config.prefix.clone(),
            provider: Arc::new(provider),
            signer: Arc::new(wallet),
            bucket: Arc::new(bucket),
            cache,
        })
    }

    /// Build the full key with optional prefix
    fn build_full_key(&self, key: &str) -> String {
        if let Some(prefix) = &self.prefix {
            format!("{}/{}", prefix, key)
        } else {
            key.to_string()
        }
    }
}

#[async_trait]
impl RecallStorage for RecallBlockchain {
    async fn add_blob(&self, key: &str, data: Vec<u8>) -> Result<String, RecallError> {
        let full_key = self.build_full_key(key);
        debug!("Storing blob to Recall: {}", full_key);

        // Check cache first
        {
            let mut cache = self.cache.lock().await;
            if let Some(cid) = cache.get(&full_key) {
                debug!("Cache hit for blob: {}, CID: {}", full_key, cid);
                return Ok(cid.clone());
            }
        }

        // Create a temporary file with the data
        let temp_file = tempfile::NamedTempFile::new().map_err(|e| {
            RecallError::Write(
                full_key.clone(),
                format!("Failed to create temp file: {}", e),
            )
        })?;

        fs::write(temp_file.path(), &data).await.map_err(|e| {
            RecallError::Write(
                full_key.clone(),
                format!("Failed to write temp file: {}", e),
            )
        })?;

        // Add to bucket
        let provider = self.provider.clone();
        let mut signer = (*self.signer).clone();
        let from_address = signer.address();

        let add_options = AddOptions {
            overwrite: false,
            metadata: std::collections::HashMap::new(),
            ttl: None,
            token_amount: None,
            gas_params: Default::default(),
            broadcast_mode: Default::default(),
            show_progress: false,
        };

        let _receipt = self
            .bucket
            .add_from_path(
                &*provider,
                &mut signer,
                from_address,
                &full_key,
                temp_file.path(),
                add_options,
            )
            .await
            .map_err(|e| {
                RecallError::Write(full_key.clone(), format!("Failed to add to bucket: {}", e))
            })?;

        // The receipt contains the transaction, we'll use a formatted CID
        let cid = format!("recall:{}", full_key);

        // Cache the CID
        let mut cache = self.cache.lock().await;
        cache.put(full_key.clone(), cid.clone());

        debug!("Successfully stored blob {} with CID: {}", full_key, cid);
        Ok(cid)
    }

    async fn has_blob(&self, key: &str) -> Result<bool, RecallError> {
        let full_key = self.build_full_key(key);

        // Check cache first
        {
            let cache = self.cache.lock().await;
            if cache.contains(&full_key) {
                debug!("Cache hit for blob: {}", full_key);
                return Ok(true);
            }
        }

        debug!("Checking if blob exists: {}", full_key);

        // Query the bucket for the object
        let query_options = QueryOptions {
            prefix: full_key.clone(),
            delimiter: String::new(),
            start_key: None,
            limit: 1,
            height: Default::default(),
        };

        let provider = self.provider.clone();
        let result = self
            .bucket
            .query(&*provider, query_options)
            .await
            .map_err(|e| {
                RecallError::Read(full_key.clone(), format!("Failed to query bucket: {}", e))
            })?;

        // Check if we found the exact key
        Ok(result
            .objects
            .iter()
            .any(|(key_bytes, _)| String::from_utf8_lossy(key_bytes) == full_key))
    }

    async fn list_blobs(&self, prefix: &str) -> Result<Vec<String>, RecallError> {
        let full_prefix = self.build_full_key(prefix);
        debug!("Listing blobs with prefix: {}", full_prefix);

        let mut all_blobs = Vec::new();
        let mut start_key = None;

        loop {
            let query_options = QueryOptions {
                prefix: full_prefix.clone(),
                delimiter: String::new(),
                start_key: start_key.clone(),
                limit: 100, // Query in batches
                height: Default::default(),
            };

            let provider = self.provider.clone();
            let result = self
                .bucket
                .query(&*provider, query_options)
                .await
                .map_err(|e| {
                    RecallError::Read(
                        full_prefix.clone(),
                        format!("Failed to query bucket: {}", e),
                    )
                })?;

            let batch_size = result.objects.len();
            for (key_bytes, _) in &result.objects {
                let key = String::from_utf8_lossy(key_bytes).to_string();
                all_blobs.push(key);
            }

            // Update start key for next batch
            if let Some((last_key_bytes, _)) = result.objects.last() {
                start_key = Some(last_key_bytes.clone());
            }

            // Check if we got a full batch (meaning there might be more)
            if batch_size < 100 {
                break;
            }
        }

        debug!(
            "Found {} blobs with prefix: {}",
            all_blobs.len(),
            full_prefix
        );
        Ok(all_blobs)
    }

    #[cfg(test)]
    async fn delete_blob(&self, key: &str) -> Result<(), RecallError> {
        let full_key = self.build_full_key(key);
        debug!("Deleting blob: {}", full_key);

        // Check if the blob exists first
        if !self.has_blob(key).await? {
            return Err(RecallError::BlobNotFound(full_key.clone()));
        }

        // Delete from bucket
        let delete_options = recall_sdk::machine::bucket::DeleteOptions::default();

        let provider = self.provider.clone();
        let mut signer = (*self.signer).clone();
        let from_address = signer.address();

        let _receipt = self
            .bucket
            .delete(
                &*provider,
                &mut signer,
                from_address,
                &full_key,
                delete_options,
            )
            .await
            .map_err(|e| {
                RecallError::Delete(
                    full_key.clone(),
                    format!("Failed to delete from bucket: {}", e),
                )
            })?;

        // The transaction is submitted
        debug!("Delete transaction submitted");

        // Remove from cache
        let mut cache = self.cache.lock().await;
        cache.pop(&full_key);

        debug!("Successfully deleted blob: {}", full_key);
        Ok(())
    }

    #[cfg(test)]
    async fn clear_prefix(&self, prefix: &str) -> Result<(), RecallError> {
        let full_prefix = self.build_full_key(prefix);
        debug!("Clearing all blobs with prefix: {}", full_prefix);

        // List all blobs with the prefix
        let blobs = self.list_blobs(prefix).await?;

        // Delete each blob
        for blob_key in &blobs {
            // Extract the relative key from the full path
            let relative_key =
                if let Some(stripped) = blob_key.strip_prefix(&self.build_full_key("")) {
                    stripped
                } else {
                    blob_key.as_str()
                };

            self.delete_blob(relative_key).await?;
        }

        debug!(
            "Successfully cleared {} blobs with prefix: {}",
            blobs.len(),
            full_prefix
        );
        Ok(())
    }
}
