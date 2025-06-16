use crate::db::database::Database;
use crate::db::error::DatabaseError;
use crate::db::syncable::SyncableObject;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

/// A fake in-memory implementation of the Database trait for testing
#[derive(Clone)]
pub struct FakeDatabase<T> {
    objects: Arc<RwLock<HashMap<Uuid, T>>>,
    _phantom: PhantomData<T>,
}

impl<T> FakeDatabase<T> {
    /// Create a new empty FakeDatabase
    pub fn new() -> Self {
        FakeDatabase {
            objects: Arc::new(RwLock::new(HashMap::new())),
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T> Database for FakeDatabase<T>
where
    T: SyncableObject + 'static,
{
    type Object = T;

    async fn get_objects(
        &self,
        limit: u32,
        since: Option<DateTime<Utc>>,
        after_id: Option<Uuid>,
        competition_id: Option<Uuid>,
    ) -> Result<Vec<T>, DatabaseError> {
        let objects = self.objects.read().unwrap();

        let mut filtered: Vec<T> = objects
            .values()
            .filter(|obj| {
                if let Some(comp_id) = competition_id {
                    if obj.competition_id() != comp_id {
                        return false;
                    }
                }

                match (since, after_id) {
                    (Some(ts), Some(id)) => {
                        // For objects with timestamp > since OR (timestamp == since AND id > after_id)
                        obj.created_at() > ts || (obj.created_at() == ts && obj.id() > id)
                    }
                    (Some(ts), None) => obj.created_at() > ts,
                    (None, Some(_)) => true, // If only after_id is provided, include all
                    (None, None) => true,
                }
            })
            .cloned()
            .collect();

        // Sort by created_at in ascending order (oldest first)
        // For same timestamps, sort by ID ascending
        filtered.sort_by(|a, b| match a.created_at().cmp(&b.created_at()) {
            std::cmp::Ordering::Equal => a.id().cmp(&b.id()),
            other => other,
        });

        // Apply limit after sorting
        filtered.truncate(limit as usize);

        Ok(filtered)
    }

    #[cfg(test)]
    async fn add_object(&self, object: T) -> Result<(), DatabaseError> {
        let mut objects = self.objects.write().unwrap();
        objects.insert(object.id(), object);
        Ok(())
    }
}
