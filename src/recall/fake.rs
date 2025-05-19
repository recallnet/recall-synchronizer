use crate::recall::error::RecallError;
use crate::recall::storage::RecallStorage;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// `FakeRecallStorage` is an in-memory implementation of the `RecallStorage` trait for testing purposes.
/// It allows simulating various storage scenarios, including successful operations and failures.
#[derive(Clone)]
pub struct FakeRecallStorage {
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    cids: Arc<Mutex<HashMap<String, String>>>,
    fail_blobs: Arc<Mutex<HashSet<String>>>,
    #[cfg_attr(not(test), allow(dead_code))]
    prefixes: Arc<Mutex<HashSet<String>>>,
}

#[allow(dead_code)]
impl FakeRecallStorage {
    /// Create a new empty FakeRecallStorage instance
    pub fn new() -> Self {
        FakeRecallStorage {
            data: Arc::new(Mutex::new(HashMap::new())),
            cids: Arc::new(Mutex::new(HashMap::new())),
            fail_blobs: Arc::new(Mutex::new(HashSet::new())),
            prefixes: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Simulate a failure for a specific blob
    /// After calling this, has_blob and add_blob will return errors for this key
    pub fn fake_fail_blob(&self, key: &str) {
        let mut fail_blobs = self.fail_blobs.lock().unwrap();
        fail_blobs.insert(key.to_string());
    }

    /// Reset failure for a specific blob
    pub fn fake_reset_blob(&self, key: &str) {
        let mut fail_blobs = self.fail_blobs.lock().unwrap();
        fail_blobs.remove(key);
    }

    /// Test-only helper to mark a blob as failed (synchronous version)
    #[cfg(test)]
    pub fn mark_blob_failed(&self, key: &str) {
        self.fake_fail_blob(key);
    }

    /// Test-only helper to clear a blob failure (synchronous version)
    #[cfg(test)]
    pub fn clear_blob_failure(&self, key: &str) {
        self.fake_reset_blob(key);
    }

    /// Test-only helper to add a prefix
    #[cfg(test)]
    pub fn add_prefix(&self, prefix: &str) {
        let mut prefixes = self.prefixes.lock().unwrap();
        prefixes.insert(prefix.to_string());
    }

    /// Test-only helper to check if a prefix exists
    #[cfg(test)]
    pub fn has_prefix(&self, prefix: &str) -> bool {
        let prefixes = self.prefixes.lock().unwrap();
        prefixes.contains(prefix)
    }

    /// Test-only helper to clear all prefixes
    #[cfg(test)]
    pub fn clear_prefixes(&self) {
        let mut prefixes = self.prefixes.lock().unwrap();
        prefixes.clear();
    }
}

#[async_trait]
impl RecallStorage for FakeRecallStorage {
    async fn add_blob(&self, key: &str, data: Vec<u8>) -> Result<String, RecallError> {
        let fail_blobs = self.fail_blobs.lock().unwrap();
        if fail_blobs.contains(key) {
            return Err(RecallError::Operation(format!(
                "Simulated failure for blob: {}",
                key
            )));
        }
        drop(fail_blobs);

        // Generate a fake CID based on key and data length
        let fake_cid = format!("bafybeifake{}len{}", key.len(), data.len());

        let mut storage_data = self.data.lock().unwrap();
        let mut cids = self.cids.lock().unwrap();

        storage_data.insert(key.to_string(), data);
        cids.insert(key.to_string(), fake_cid.clone());

        // Track prefix
        let mut prefixes = self.prefixes.lock().unwrap();
        if let Some(pos) = key.rfind('/') {
            prefixes.insert(key[..=pos].to_string());
        }

        Ok(fake_cid)
    }

    async fn has_blob(&self, key: &str) -> Result<bool, RecallError> {
        let fail_blobs = self.fail_blobs.lock().unwrap();
        if fail_blobs.contains(key) {
            return Err(RecallError::Operation(format!(
                "Simulated failure for blob: {}",
                key
            )));
        }
        drop(fail_blobs);

        let data = self.data.lock().unwrap();
        Ok(data.contains_key(key))
    }

    async fn list_blobs(&self, prefix: &str) -> Result<Vec<String>, RecallError> {
        let data = self.data.lock().unwrap();
        let keys: Vec<String> = data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        Ok(keys)
    }

    #[cfg(test)]
    async fn delete_blob(&self, key: &str) -> Result<(), RecallError> {
        let mut storage_data = self.data.lock().unwrap();
        let mut cids = self.cids.lock().unwrap();

        if storage_data.remove(key).is_some() {
            cids.remove(key);
            Ok(())
        } else {
            Err(RecallError::BlobNotFound(key.to_string()))
        }
    }

    #[cfg(test)]
    async fn clear_prefix(&self, prefix: &str) -> Result<(), RecallError> {
        let mut storage_data = self.data.lock().unwrap();
        let mut cids = self.cids.lock().unwrap();

        let keys_to_remove: Vec<String> = storage_data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();

        for key in keys_to_remove {
            storage_data.remove(&key);
            cids.remove(&key);
        }

        Ok(())
    }
}

#[cfg(test)]
impl Default for FakeRecallStorage {
    fn default() -> Self {
        Self::new()
    }
}
