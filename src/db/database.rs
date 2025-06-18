use crate::db::error::DatabaseError;
use crate::db::models::ObjectIndex;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::Arc;
use uuid::Uuid;

/// Database trait for ObjectIndex storage
#[async_trait]
pub trait Database: Send + Sync + 'static {
    /// Query for objects that need to be synchronized with filtering options
    ///
    /// * `limit` - Maximum number of objects to return
    /// * `since` - Only return objects modified after this timestamp
    /// * `after_id` - For objects with the same timestamp as `since`, only return those with ID > after_id
    /// * `competition_id` - Filter objects by competition ID if provided
    async fn get_objects(
        &self,
        limit: u32,
        since: Option<DateTime<Utc>>,
        after_id: Option<Uuid>,
        competition_id: Option<Uuid>,
    ) -> Result<Vec<ObjectIndex>, DatabaseError>;

    /// Add an object to the database (test-only)
    #[cfg(test)]
    async fn add_object(&self, object: ObjectIndex) -> Result<(), DatabaseError>;
}

/// Implementation of Database trait for Arc<T> where T implements Database
#[async_trait]
impl<T: Database + ?Sized> Database for Arc<T> {
    async fn get_objects(
        &self,
        limit: u32,
        since: Option<DateTime<Utc>>,
        after_id: Option<Uuid>,
        competition_id: Option<Uuid>,
    ) -> Result<Vec<ObjectIndex>, DatabaseError> {
        (**self)
            .get_objects(limit, since, after_id, competition_id)
            .await
    }

    #[cfg(test)]
    async fn add_object(&self, object: ObjectIndex) -> Result<(), DatabaseError> {
        (**self).add_object(object).await
    }
}
