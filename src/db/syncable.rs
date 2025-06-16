use crate::db::models::{ObjectIndex, ObjectIndexDirect};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Trait for objects that can be synchronized
pub trait SyncableObject: Send + Sync + Clone {
    /// Get the unique identifier for this object
    fn id(&self) -> Uuid;

    /// Get the competition ID
    fn competition_id(&self) -> Uuid;

    /// Get the agent ID
    fn agent_id(&self) -> Uuid;

    /// Get the data type
    fn data_type(&self) -> &str;

    /// Get the creation timestamp
    fn created_at(&self) -> DateTime<Utc>;


    /// Get the data to sync
    /// Returns None if data needs to be fetched from external storage (S3)
    /// Returns Some(data) if data is embedded in the object
    fn embedded_data(&self) -> Option<&[u8]>;

    /// Get the S3 key if this object uses S3 storage
    fn s3_key(&self) -> Option<&str>;
}

impl SyncableObject for ObjectIndex {
    fn id(&self) -> Uuid {
        self.id
    }

    fn competition_id(&self) -> Uuid {
        self.competition_id
    }

    fn agent_id(&self) -> Uuid {
        self.agent_id
    }

    fn data_type(&self) -> &str {
        &self.data_type
    }

    fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }


    fn embedded_data(&self) -> Option<&[u8]> {
        // ObjectIndex doesn't have embedded data
        None
    }

    fn s3_key(&self) -> Option<&str> {
        // ObjectIndex stores data in S3
        Some(&self.object_key)
    }
}

impl SyncableObject for ObjectIndexDirect {
    fn id(&self) -> Uuid {
        self.id
    }

    fn competition_id(&self) -> Uuid {
        self.competition_id
    }

    fn agent_id(&self) -> Uuid {
        self.agent_id
    }

    fn data_type(&self) -> &str {
        &self.data_type
    }

    fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }


    fn embedded_data(&self) -> Option<&[u8]> {
        // ObjectIndexDirect has embedded data
        Some(&self.data)
    }

    fn s3_key(&self) -> Option<&str> {
        // ObjectIndexDirect doesn't use S3
        None
    }
}
