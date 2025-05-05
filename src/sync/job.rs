use anyhow::Result;
use sqlx::types::chrono::{DateTime, Utc};
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::state::SyncState;
use crate::db::models::ObjectInfo;
use crate::db::DbConnector;
use crate::recall::RecallConnector;
use crate::s3::S3Connector;

#[derive(Debug, Error)]
pub enum JobError {
    #[error("Failed to fetch object from S3: {0}")]
    S3Error(#[from] anyhow::Error),

    #[error("Failed to store object to Recall: {0}")]
    RecallError(String),

    #[error("Failed to update state: {0}")]
    StateError(String),
}

pub struct SyncJob {
    pub id: String,
    pub object: ObjectInfo,
    pub attempts: u32,
}

impl SyncJob {
    pub fn new(object: ObjectInfo) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            object,
            attempts: 0,
        }
    }

    pub async fn execute(
        &mut self,
        db: &Arc<DbConnector>,
        s3: &Arc<S3Connector>,
        recall: &Arc<RecallConnector>,
        state: &Arc<SyncState>,
    ) -> Result<(), JobError> {
        self.attempts += 1;

        // Check if already processed
        if state
            .is_processed(&self.object.key)
            .await
            .map_err(|e| JobError::StateError(e.to_string()))?
        {
            debug!(
                "[Job {}] Object already processed: {}",
                self.id, self.object.key
            );
            return Ok(());
        }

        // Get object data from S3
        debug!(
            "[Job {}] Fetching object from S3: {}",
            self.id, self.object.key
        );
        let data = match s3.get_object(&self.object.key).await {
            Ok(data) => data,
            Err(e) => {
                error!("[Job {}] Failed to fetch object from S3: {}", self.id, e);
                return Err(JobError::S3Error(e));
            }
        };

        // Store to Recall
        debug!(
            "[Job {}] Storing object to Recall: {}",
            self.id, self.object.key
        );
        let cid = match recall.store_object(&self.object.key, &data).await {
            Ok(cid) => cid,
            Err(e) => {
                error!("[Job {}] Failed to store object to Recall: {}", self.id, e);
                return Err(JobError::RecallError(e.to_string()));
            }
        };

        // Mark as processed
        debug!(
            "[Job {}] Marking object as processed: {}",
            self.id, self.object.key
        );
        match state
            .mark_processed(&self.object.key, &cid, Utc::now())
            .await
        {
            Ok(_) => {
                info!(
                    "[Job {}] Successfully synced object: {} -> {}",
                    self.id, self.object.key, cid
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "[Job {}] Failed to mark object as processed: {}",
                    self.id, e
                );
                Err(JobError::StateError(e.to_string()))
            }
        }
    }
}
