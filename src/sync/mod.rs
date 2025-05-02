use anyhow::Result;
use sqlx::types::chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};
use tokio::time;
use tracing::{info, debug, warn, error};

use crate::config::Config;
use crate::db::{DbConnector, ObjectInfo};
use crate::recall::RecallConnector;
use crate::s3::S3Connector;

mod state;
use state::SyncState;

pub struct Synchronizer {
    db: Arc<DbConnector>,
    s3: Arc<S3Connector>,
    recall: Arc<RecallConnector>,
    state: Arc<SyncState>,
    config: Config,
}

impl Synchronizer {
    pub async fn new(config: Config, reset: bool) -> Result<Self> {
        let db = Arc::new(DbConnector::new(&config.database).await?);
        let s3 = Arc::new(S3Connector::new(&config.s3).await?);
        let recall = Arc::new(RecallConnector::new(&config.recall).await?);
        let state = Arc::new(SyncState::new(&config.sync.state_db_path, reset).await?);
        
        Ok(Self {
            db,
            s3,
            recall,
            state,
            config,
        })
    }
    
    pub async fn run(
        &self,
        competition_id: Option<String>,
        since_str: Option<String>,
    ) -> Result<()> {
        let since = if let Some(s) = since_str {
            // Parse the since string to DateTime
            Some(DateTime::parse_from_rfc3339(&s)?.with_timezone(&Utc))
        } else {
            // Get last sync time from state
            self.state.get_last_sync_time().await?
        };
        
        info!(
            "Starting synchronization{}{}",
            since.map_or("".to_string(), |t| format!(" since {}", t)),
            competition_id.map_or("".to_string(), |id| format!(" for competition {}", id))
        );
        
        let mut interval = time::interval(Duration::from_secs(self.config.sync.interval_seconds));
        
        loop {
            interval.tick().await;
            
            // Get updated objects
            let objects = self.db.get_updated_objects(
                since,
                competition_id.clone(),
                self.config.sync.batch_size
            ).await?;
            
            if objects.is_empty() {
                debug!("No new objects to sync");
                continue;
            }
            
            info!("Processing {} objects", objects.len());
            
            // Process objects in parallel with worker pool
            let (tx, mut rx) = mpsc::channel(objects.len());
            let semaphore = Arc::new(Semaphore::new(self.config.sync.workers));
            
            for object in objects {
                let tx = tx.clone();
                let db = self.db.clone();
                let s3 = self.s3.clone();
                let recall = self.recall.clone();
                let state = self.state.clone();
                let sem = semaphore.clone();
                
                tokio::spawn(async move {
                    let _permit = sem.acquire().await.unwrap();
                    let result = Self::process_object(object, &db, &s3, &recall, &state).await;
                    let _ = tx.send(result).await;
                });
            }
            
            drop(tx); // Drop sender to close channel when all workers are done
            
            // Wait for all workers to complete
            let mut success_count = 0;
            let mut latest_timestamp: Option<DateTime<Utc>> = None;
            
            while let Some(result) = rx.recv().await {
                match result {
                    Ok((key, timestamp)) => {
                        success_count += 1;
                        // Keep track of the latest timestamp
                        if let Some(current) = latest_timestamp {
                            if timestamp > current {
                                latest_timestamp = Some(timestamp);
                            }
                        } else {
                            latest_timestamp = Some(timestamp);
                        }
                        debug!("Successfully synced: {}", key);
                    }
                    Err(e) => {
                        error!("Failed to sync object: {}", e);
                    }
                }
            }
            
            info!("Completed batch: {} objects synced successfully", success_count);
            
            // Update the last sync time
            if let Some(timestamp) = latest_timestamp {
                self.state.update_last_sync_time(timestamp).await?;
            }
        }
    }
    
    async fn process_object(
        object: ObjectInfo,
        db: &DbConnector,
        s3: &S3Connector,
        recall: &RecallConnector,
        state: &SyncState,
    ) -> Result<(String, DateTime<Utc>)> {
        // Check if already processed
        if state.is_processed(&object.key).await? {
            debug!("Object already processed: {}", object.key);
            return Ok((object.key, object.updated_at));
        }
        
        // Get object data from S3
        let data = s3.get_object(&object.key).await?;
        
        // Store to Recall
        let cid = recall.store_object(&object.key, &data).await?;
        
        // Mark as processed
        state.mark_processed(&object.key, &cid).await?;
        
        Ok((object.key, object.updated_at))
    }
}