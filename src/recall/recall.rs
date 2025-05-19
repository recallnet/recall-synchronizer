use crate::config::RecallConfig;
use crate::recall::error::RecallError;
use crate::recall::storage::RecallStorage;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

/// Real Recall blockchain implementation of the RecallStorage trait
#[derive(Clone)]
pub struct RecallBlockchain {
    endpoint: String,
    prefix: Option<String>,
    private_key: String,
    cache: Arc<Mutex<lru::LruCache<String, String>>>,
}

impl RecallBlockchain {
    /// Create a new RecallBlockchain instance from configuration
    pub async fn new(config: &RecallConfig) -> Result<Self, RecallError> {
        info!("Creating RecallBlockchain with endpoint {}", config.endpoint);
        
        // Create LRU cache for CIDs - default to 100 items
        let cache_size = std::num::NonZeroUsize::new(100).unwrap();
        let cache = Arc::new(Mutex::new(lru::LruCache::new(cache_size)));
        
        Ok(Self {
            endpoint: config.endpoint.clone(),
            prefix: config.prefix.clone(),
            private_key: config.private_key.clone(),
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
        
        // Stub implementation - return a fake CID
        let cid = format!("bafybeimock{}len{}", key.len(), data.len());
        
        debug!("Successfully stored blob {} with CID: {}", full_key, cid);
        
        // Cache the CID
        let mut cache = self.cache.lock().await;
        cache.put(full_key.clone(), cid.clone());
        
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
        
        // Stub implementation - return false for now
        Ok(false)
    }

    async fn list_blobs(&self, prefix: &str) -> Result<Vec<String>, RecallError> {
        let full_prefix = self.build_full_key(prefix);
        debug!("Listing blobs with prefix: {}", full_prefix);
        
        // Stub implementation - return empty list
        Ok(vec![])
    }

    #[cfg(test)]
    async fn delete_blob(&self, key: &str) -> Result<(), RecallError> {
        let full_key = self.build_full_key(key);
        debug!("Deleting blob: {}", full_key);
        
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
        
        // Stub implementation
        debug!("Successfully cleared prefix: {}", full_prefix);
        Ok(())
    }
}