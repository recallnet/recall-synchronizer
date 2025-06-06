use crate::db::database::Database;
use crate::db::error::DatabaseError;
use crate::db::models::ObjectIndex;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

/// A fake in-memory implementation of the Database trait for testing
#[derive(Clone)]
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
}

#[async_trait]
impl Database for FakeDatabase {
    async fn get_objects(
        &self,
        limit: u32,
        since: Option<DateTime<Utc>>,
        after_id: Option<Uuid>,
        competition_id: Option<Uuid>,
    ) -> Result<Vec<ObjectIndex>, DatabaseError> {
        let objects = self.objects.read().unwrap();

        let mut filtered: Vec<ObjectIndex> = objects
            .values()
            .filter(|obj| {
                if let Some(comp_id) = competition_id {
                    if obj.competition_id != Some(comp_id) {
                        return false;
                    }
                }

                match (since, after_id) {
                    (Some(ts), Some(id)) => {
                        // For objects with timestamp > since OR (timestamp == since AND id > after_id)
                        obj.object_last_modified_at > ts
                            || (obj.object_last_modified_at == ts && obj.id > id)
                    }
                    (Some(ts), None) => obj.object_last_modified_at > ts,
                    (None, Some(_)) => true, // If only after_id is provided, include all
                    (None, None) => true,
                }
            })
            .cloned()
            .collect();

        // Sort by last_modified_at in ascending order (oldest first)
        // For same timestamps, sort by ID ascending
        // This ensures we complete all objects with the same timestamp before moving to newer ones
        filtered.sort_by(
            |a, b| match a.object_last_modified_at.cmp(&b.object_last_modified_at) {
                std::cmp::Ordering::Equal => a.id.cmp(&b.id),
                other => other,
            },
        );

        // Apply limit after sorting
        filtered.truncate(limit as usize);

        Ok(filtered)
    }

    #[cfg(test)]
    async fn add_object(&self, object: ObjectIndex) -> Result<(), DatabaseError> {
        let mut objects = self.objects.write().unwrap();
        objects.insert(object.object_key.clone(), object);
        Ok(())
    }

    #[cfg(test)]
    async fn clear_data(&self) -> Result<(), DatabaseError> {
        let mut objects = self.objects.write().unwrap();
        objects.clear();
        Ok(())
    }
}
