use crate::recall::error::RecallError;
use crate::recall::storage::Storage;
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// `FakeRecallStorage` is an in-memory implementation of the `RecallStorage` trait for testing purposes.
/// It allows simulating various storage scenarios, including successful operations and failures.
#[derive(Clone)]
pub struct FakeRecallStorage {
    data: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    fail_blobs: Arc<Mutex<HashSet<String>>>,
}

impl FakeRecallStorage {
    /// Create a new empty FakeRecallStorage instance
    pub fn new() -> Self {
        FakeRecallStorage {
            data: Arc::new(Mutex::new(HashMap::new())),
            fail_blobs: Arc::new(Mutex::new(HashSet::new())),
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

}

#[async_trait]
impl Storage for FakeRecallStorage {
    async fn add_blob(&self, key: &str, data: Vec<u8>) -> Result<(), RecallError> {
        let fail_blobs = self.fail_blobs.lock().unwrap();
        if fail_blobs.contains(key) {
            return Err(RecallError::Operation(format!(
                "Simulated failure for blob: {}",
                key
            )));
        }
        drop(fail_blobs);

        let mut storage_data = self.data.lock().unwrap();
        storage_data.insert(key.to_string(), data);

        Ok(())
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

        if storage_data.remove(key).is_some() {
            Ok(())
        } else {
            Err(RecallError::BlobNotFound(key.to_string()))
        }
    }

    #[cfg(test)]
    async fn clear_prefix(&self, prefix: &str) -> Result<(), RecallError> {
        let mut storage_data = self.data.lock().unwrap();

        let keys_to_remove: Vec<String> = storage_data
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();

        for key in keys_to_remove {
            storage_data.remove(&key);
        }

        Ok(())
    }

    #[cfg(test)]
    async fn get_blob(&self, key: &str) -> Result<Vec<u8>, RecallError> {
        let fail_blobs = self.fail_blobs.lock().unwrap();
        if fail_blobs.contains(key) {
            return Err(RecallError::Operation(format!(
                "Simulated failure for blob: {}",
                key
            )));
        }
        drop(fail_blobs);

        let data = self.data.lock().unwrap();
        match data.get(key) {
            Some(blob_data) => Ok(blob_data.clone()),
            None => Err(RecallError::BlobNotFound(key.to_string())),
        }
    }
}

#[cfg(test)]
impl Default for FakeRecallStorage {
    fn default() -> Self {
        Self::new()
    }
}
