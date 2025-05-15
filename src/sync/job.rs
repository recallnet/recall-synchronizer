use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::db::models::ObjectIndex;
use crate::db::Database;
use crate::recall::RecallConnector;
use crate::s3::S3Connector;
use crate::sync::storage::{SyncRecord, SyncStatus, SyncStorage};

#[allow(clippy::enum_variant_names, dead_code)]
#[derive(Debug, Error)]
pub enum JobError {
    #[error("Failed to fetch object from S3: {0}")]
    S3Error(#[from] anyhow::Error),

    #[error("Failed to store object to Recall: {0}")]
    RecallError(String),

    #[error("Failed to update sync storage: {0}")]
    StorageError(String),

    #[error("Database error: {0}")]
    DatabaseError(String),
}

#[allow(dead_code)]
pub struct SyncJob {
    pub id: String,
    pub object: ObjectIndex,
    pub attempts: u32,
}

#[allow(dead_code)]
impl SyncJob {
    pub fn new(object: ObjectIndex) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            object,
            attempts: 0,
        }
    }

    pub async fn execute<D: Database, S: SyncStorage>(
        &mut self,
        _database: &Arc<D>,
        s3: &Arc<S3Connector>,
        recall: &Arc<RecallConnector>,
        storage: &Arc<S>,
    ) -> Result<(), JobError> {
        self.attempts += 1;

        // Check if already synced
        if let Some(status) = storage
            .get_object_status(self.object.id)
            .await
            .map_err(|e| JobError::StorageError(e.to_string()))?
        {
            if status == SyncStatus::Complete {
                debug!(
                    "[Job {}] Object already synced: {}",
                    self.id, self.object.object_key
                );
                return Ok(());
            }
        } else {
            // Add object as pending if not already in storage
            let record = SyncRecord {
                id: self.object.id,
                object_key: self.object.object_key.clone(),
                bucket_name: self.object.bucket_name.clone(),
                timestamp: self.object.object_last_modified_at,
                status: SyncStatus::PendingSync,
            };
            storage
                .add_object(record)
                .await
                .map_err(|e| JobError::StorageError(e.to_string()))?;
        }

        // Mark as processing
        storage
            .set_object_status(self.object.id, SyncStatus::Processing)
            .await
            .map_err(|e| JobError::StorageError(e.to_string()))?;

        // Get object data from S3
        debug!(
            "[Job {}] Fetching object from S3: {}",
            self.id, self.object.object_key
        );
        let data = match s3.get_object(&self.object.object_key).await {
            Ok(data) => data,
            Err(e) => {
                error!("[Job {}] Failed to fetch object from S3: {}", self.id, e);
                return Err(JobError::S3Error(e));
            }
        };

        // Store to Recall
        debug!(
            "[Job {}] Storing object to Recall: {}",
            self.id, self.object.object_key
        );
        let cid = match recall.store_object(&self.object.object_key, &data).await {
            Ok(cid) => cid,
            Err(e) => {
                error!("[Job {}] Failed to store object to Recall: {}", self.id, e);
                return Err(JobError::RecallError(e.to_string()));
            }
        };

        // Mark as complete
        debug!(
            "[Job {}] Marking object as complete: {}",
            self.id, self.object.object_key
        );
        match storage
            .set_object_status(self.object.id, SyncStatus::Complete)
            .await
        {
            Ok(_) => {
                info!(
                    "[Job {}] Successfully synced object: {} -> {}",
                    self.id, self.object.object_key, cid
                );
                Ok(())
            }
            Err(e) => {
                error!("[Job {}] Failed to mark object as complete: {}", self.id, e);
                Err(JobError::StorageError(e.to_string()))
            }
        }
    }

    /// Create a batch of jobs from database objects
    pub async fn create_batch<D: Database>(
        database: &Arc<D>,
        limit: u32,
        since_timestamp: Option<chrono::DateTime<Utc>>,
    ) -> Result<Vec<Self>, JobError> {
        let objects = database
            .get_objects_to_sync(limit, since_timestamp)
            .await
            .map_err(|e| JobError::DatabaseError(e.to_string()))?;

        let jobs = objects.into_iter().map(Self::new).collect();

        Ok(jobs)
    }
}
