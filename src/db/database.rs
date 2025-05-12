use crate::db::error::DatabaseError;
use crate::db::models::ObjectIndex;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

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
}
