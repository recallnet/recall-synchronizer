use crate::s3::error::StorageError;
use crate::s3::storage::Storage;
use async_trait::async_trait;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// `FakeStorage` is an in-memory implementation of the `Storage` trait for testing purposes.
/// It allows simulating various storage scenarios, including successful operations and failures.
#[derive(Clone)]
pub struct FakeStorage {
    data: Arc<Mutex<HashMap<String, Bytes>>>,
    fail_objects: Arc<Mutex<HashMap<String, bool>>>,
}

#[allow(dead_code)]
impl FakeStorage {
    /// Create a new empty FakeStorage instance
    pub fn new() -> Self {
        FakeStorage {
            data: Arc::new(Mutex::new(HashMap::new())),
            fail_objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Simulate a failure for a specific object
    /// After calling this, get_object and exists will return errors for this key
    pub async fn fake_fail_object(&self, key: &str) {
        let mut fail_objects = self.fail_objects.lock().await;
        fail_objects.insert(key.to_string(), true);
    }
}

#[async_trait]
impl Storage for FakeStorage {
    async fn get_object(&self, key: &str) -> Result<Bytes, StorageError> {
        let fail_objects = self.fail_objects.lock().await;
        if fail_objects.get(key).copied().unwrap_or(false) {
            return Err(StorageError::ObjectNotFound(key.to_string()));
        }

        let data = self.data.lock().await;
        match data.get(key) {
            Some(bytes) => Ok(bytes.clone()),
            None => Err(StorageError::ObjectNotFound(key.to_string())),
        }
    }

    #[cfg(test)]
    async fn add_object(&self, key: &str, data: Bytes) -> Result<(), StorageError> {
        let mut storage_data = self.data.lock().await;
        storage_data.insert(key.to_string(), data);
        Ok(())
    }

    #[cfg(test)]
    async fn remove_object(&self, key: &str) -> Result<(), StorageError> {
        let mut storage_data = self.data.lock().await;
        if storage_data.remove(key).is_some() {
            Ok(())
        } else {
            Err(StorageError::ObjectNotFound(key.to_string()))
        }
    }
}

#[cfg(test)]
impl Default for FakeStorage {
    fn default() -> Self {
        Self::new()
    }
}
