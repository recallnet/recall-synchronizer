use crate::db::database::Database;
use crate::db::error::DatabaseError;
use crate::db::models::ObjectIndex;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// A fake in-memory implementation of the Database trait for testing
pub struct FakeDatabase {
    objects: Arc<RwLock<HashMap<String, ObjectIndex>>>,
}

impl FakeDatabase {
    /// Create a new empty FakeDatabase
    pub fn new() -> Self {
        FakeDatabase {
            objects: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a test object to the database
    pub fn fake_add_object(&self, object: ObjectIndex) {
        let mut objects = self.objects.write().unwrap();
        objects.insert(object.object_key.clone(), object);
    }

    /// Clear all objects from the database
    pub fn fake_clear_objects(&self) {
        let mut objects = self.objects.write().unwrap();
        objects.clear();
    }
}

#[async_trait]
impl Database for FakeDatabase {
    async fn get_objects_to_sync(&self, limit: u32, since: Option<DateTime<Utc>>) 
        -> Result<Vec<ObjectIndex>, DatabaseError> {
        let objects = self.objects.read().unwrap();
        
        let mut filtered: Vec<ObjectIndex> = objects.values()
            .filter(|obj| match since {
                Some(ts) => obj.object_last_modified_at > ts,
                None => true,
            })
            .take(limit as usize)
            .cloned()
            .collect();
        
        // Sort by last_modified_at to ensure consistent results
        filtered.sort_by(|a, b| a.object_last_modified_at.cmp(&b.object_last_modified_at));
            
        Ok(filtered)
    }

    async fn get_object_by_key(&self, object_key: &str) 
        -> Result<ObjectIndex, DatabaseError> {
        let objects = self.objects.read().unwrap();
        
        objects.get(object_key)
            .cloned()
            .ok_or_else(|| DatabaseError::ObjectNotFound(object_key.to_string()))
    }
}