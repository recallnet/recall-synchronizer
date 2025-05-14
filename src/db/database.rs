use crate::db::error::DatabaseError;
use crate::db::models::ObjectIndex;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;

/// Database trait defining the interface for reading object metadata
#[async_trait]
pub trait Database: Send + Sync + 'static {
    /// Query for objects that need to be synchronized
    ///
    /// * `limit` - Maximum number of objects to return
    /// * `since` - Only return objects modified after this timestamp
    async fn get_objects_to_sync(
        &self,
        limit: u32,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<ObjectIndex>, DatabaseError>;

    /// Get a specific object by its key
    #[allow(dead_code)]
    async fn get_object_by_key(&self, object_key: &str) -> Result<ObjectIndex, DatabaseError>;

    /// Add an object to the database (test-only)
    #[cfg(test)]
    async fn add_object(&self, object: ObjectIndex) -> Result<(), DatabaseError>;
}

/// Implementation of Database trait for Arc<T> where T implements Database
///
/// This allows sharing database instances across threads and components efficiently.
/// The Arc wrapper provides thread-safe reference counting, enabling multiple
/// parts of the application to share the same database instance.
#[async_trait]
impl<T: Database + ?Sized> Database for Arc<T> {
    async fn get_objects_to_sync(
        &self,
        limit: u32,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<ObjectIndex>, DatabaseError> {
        (**self).get_objects_to_sync(limit, since).await
    }

    async fn get_object_by_key(&self, object_key: &str) -> Result<ObjectIndex, DatabaseError> {
        (**self).get_object_by_key(object_key).await
    }

    #[cfg(test)]
    async fn add_object(&self, object: ObjectIndex) -> Result<(), DatabaseError> {
        (**self).add_object(object).await
    }
}
