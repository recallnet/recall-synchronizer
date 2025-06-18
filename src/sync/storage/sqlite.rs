use crate::sync::storage::error::SyncStorageError;
use crate::sync::storage::models::{SyncRecord, SyncStatus};
use crate::sync::storage::sync_storage::SyncStorage;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::task;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// A SQLite implementation of the SyncStorage trait
pub struct SqliteSyncStorage {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteSyncStorage {
    /// Create a new SqliteSyncStorage with the given database path
    pub fn new(db_path: &str) -> Result<Self, SyncStorageError> {
        info!("Creating SQLite sync storage at path: {}", db_path);

        if let Some(parent) = Path::new(db_path).parent() {
            if !parent.exists() {
                debug!("Creating parent directory: {:?}", parent);
                fs::create_dir_all(parent).map_err(|e| {
                    error!("Failed to create directory {:?}: {}", parent, e);
                    SyncStorageError::OpenError(format!("Failed to create directory: {}", e))
                })?;
            }
        }

        let connection = Connection::open(db_path).map_err(|e| {
            error!("Failed to open SQLite database at {}: {}", db_path, e);
            SyncStorageError::OpenError(format!("Failed to open SQLite database: {}", e))
        })?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS sync_records (
                    id TEXT PRIMARY KEY,
                    competition_id TEXT,
                    agent_id TEXT,
                    data_type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    status TEXT NOT NULL
                )",
                [],
            )
            .map_err(|e| {
                error!("Failed to create sync_records table: {}", e);
                SyncStorageError::OpenError(format!("Failed to create sync_records table: {}", e))
            })?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS sync_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )",
                [],
            )
            .map_err(|e| {
                error!("Failed to create sync_state table: {}", e);
                SyncStorageError::OpenError(format!("Failed to create sync_state table: {}", e))
            })?;

        // Create indexes for efficient querying
        connection
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_sync_records_status ON sync_records(status)",
                [],
            )
            .map_err(|e| {
                error!("Failed to create status index: {}", e);
                SyncStorageError::OpenError(format!("Failed to create status index: {}", e))
            })?;

        connection
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_sync_records_timestamp ON sync_records(timestamp)",
                [],
            )
            .map_err(|e| {
                error!("Failed to create timestamp index: {}", e);
                SyncStorageError::OpenError(format!("Failed to create timestamp index: {}", e))
            })?;

        info!(
            "SQLite sync storage initialized successfully at: {}",
            db_path
        );
        Ok(SqliteSyncStorage {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    // Helper function to convert between DateTime<Utc> and ISO 8601 strings
    fn datetime_to_string(dt: DateTime<Utc>) -> String {
        dt.to_rfc3339()
    }

    // Helper function to parse ISO 8601 strings to DateTime<Utc>
    fn string_to_datetime(s: &str) -> Result<DateTime<Utc>, SyncStorageError> {
        DateTime::parse_from_rfc3339(s)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| {
                SyncStorageError::OperationError(format!("Failed to parse datetime: {}", e))
            })
    }

    // Helper function to convert SyncStatus to/from string
    fn status_to_string(status: SyncStatus) -> &'static str {
        match status {
            SyncStatus::PendingSync => "PENDING_SYNC",
            SyncStatus::Processing => "PROCESSING",
            SyncStatus::Complete => "COMPLETE",
        }
    }

    fn string_to_status(s: &str) -> Result<SyncStatus, SyncStorageError> {
        match s {
            "PENDING_SYNC" => Ok(SyncStatus::PendingSync),
            "PROCESSING" => Ok(SyncStatus::Processing),
            "COMPLETE" => Ok(SyncStatus::Complete),
            _ => Err(SyncStorageError::OperationError(format!(
                "Invalid status: {}",
                s
            ))),
        }
    }
}

#[async_trait]
impl SyncStorage for SqliteSyncStorage {
    async fn add_object(&self, record: SyncRecord) -> Result<(), SyncStorageError> {
        debug!(
            "Adding sync record: id={}, competition_id={:?}, agent_id={:?}, data_type={}, status={:?}",
            record.id, record.competition_id, record.agent_id, record.data_type, record.status
        );

        let connection = Arc::clone(&self.connection);
        let id_str = record.id.to_string();
        let competition_id_str = record.competition_id.map(|id| id.to_string());
        let agent_id_str = record.agent_id.map(|id| id.to_string());
        let timestamp_str = Self::datetime_to_string(record.timestamp);
        let status_str = Self::status_to_string(record.status);

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => {
                    error!("Failed to acquire database lock");
                    return Err(SyncStorageError::Locked);
                }
            };

            conn.execute(
                "INSERT OR REPLACE INTO sync_records (id, competition_id, agent_id, data_type, timestamp, status) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    id_str,
                    competition_id_str,
                    agent_id_str,
                    record.data_type,
                    timestamp_str,
                    status_str
                ],
            )
            .map_err(|e| {
                error!("Failed to insert sync record: {}", e);
                SyncStorageError::OperationError(format!("Failed to insert record: {}", e))
            })?;

            debug!("Successfully added sync record for id: {}", id_str);
            Ok(())
        })
        .await
        .map_err(|e| {
            error!("Task panic while adding object: {}", e);
            SyncStorageError::OperationError(format!("Task panic: {}", e))
        })?
    }

    async fn set_object_status(
        &self,
        id: Uuid,
        status: SyncStatus,
    ) -> Result<(), SyncStorageError> {
        debug!("Updating object status: id={}, status={:?}", id, status);

        let connection = Arc::clone(&self.connection);
        let id_str = id.to_string();
        let status_str = Self::status_to_string(status);

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => {
                    error!("Failed to acquire database lock");
                    return Err(SyncStorageError::Locked);
                }
            };

            let rows_affected = conn
                .execute(
                    "UPDATE sync_records SET status = ?1 WHERE id = ?2",
                    params![status_str, id_str],
                )
                .map_err(|e| {
                    error!("Failed to update object status: {}", e);
                    SyncStorageError::OperationError(format!("Failed to update status: {}", e))
                })?;

            if rows_affected == 0 {
                warn!("Object not found for status update: id={}", id_str);
                return Err(SyncStorageError::ObjectNotFound(id_str));
            }

            debug!(
                "Successfully updated status to {} for object: {}",
                status_str, id_str
            );
            Ok(())
        })
        .await
        .map_err(|e| {
            error!("Task panic while updating object status: {}", e);
            SyncStorageError::OperationError(format!("Task panic: {}", e))
        })?
    }

    async fn get_object(&self, id: Uuid) -> Result<Option<SyncRecord>, SyncStorageError> {
        debug!("Getting sync record by id: {}", id);

        let connection = Arc::clone(&self.connection);
        let id_str = id.to_string();

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => {
                    error!("Failed to acquire database lock");
                    return Err(SyncStorageError::Locked);
                }
            };

            let result: Option<(String, Option<String>, Option<String>, String, String, String)> = conn
                .query_row(
                    "SELECT id, competition_id, agent_id, data_type, timestamp, status 
                     FROM sync_records WHERE id = ?1",
                    params![id_str],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    },
                )
                .optional()
                .map_err(|e| {
                    error!("Failed to query sync record: {}", e);
                    SyncStorageError::OperationError(format!("Failed to query record: {}", e))
                })?;

            match result {
                Some((
                    id_str,
                    competition_id_str,
                    agent_id_str,
                    data_type,
                    timestamp_str,
                    status_str,
                )) => {
                    let id = Uuid::parse_str(&id_str).map_err(|e| {
                        error!("Failed to parse UUID from database: {}", e);
                        SyncStorageError::OperationError(format!("Failed to parse UUID: {}", e))
                    })?;
                    let competition_id = competition_id_str
                        .map(|s| Uuid::parse_str(&s))
                        .transpose()
                        .map_err(|e| {
                            error!("Failed to parse competition UUID from database: {}", e);
                            SyncStorageError::OperationError(format!(
                                "Failed to parse competition UUID: {}",
                                e
                            ))
                        })?;
                    let agent_id = agent_id_str
                        .map(|s| Uuid::parse_str(&s))
                        .transpose()
                        .map_err(|e| {
                            error!("Failed to parse agent UUID from database: {}", e);
                            SyncStorageError::OperationError(format!(
                                "Failed to parse agent UUID: {}",
                                e
                            ))
                        })?;
                    let timestamp = Self::string_to_datetime(&timestamp_str)?;
                    let status = Self::string_to_status(&status_str)?;

                    debug!("Found sync record: id={}, status={:?}", id_str, status);
                    Ok(Some(SyncRecord::with_status(
                        id,
                        competition_id,
                        agent_id,
                        data_type,
                        timestamp,
                        status,
                    )))
                }
                None => {
                    debug!("No sync record found for id: {}", id_str);
                    Ok(None)
                }
            }
        })
        .await
        .map_err(|e| {
            error!("Task panic while getting object: {}", e);
            SyncStorageError::OperationError(format!("Task panic: {}", e))
        })?
    }

    async fn get_objects_with_status(
        &self,
        status: SyncStatus,
    ) -> Result<Vec<SyncRecord>, SyncStorageError> {
        debug!("Getting sync records with status: {:?}", status);

        let connection = Arc::clone(&self.connection);
        let status_str = Self::status_to_string(status);

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => {
                    error!("Failed to acquire database lock");
                    return Err(SyncStorageError::Locked);
                }
            };

            let mut stmt = conn
                .prepare(
                    "SELECT id, competition_id, agent_id, data_type, timestamp, status 
                     FROM sync_records 
                     WHERE status = ?1 
                     ORDER BY timestamp",
                )
                .map_err(|e| {
                    error!("Failed to prepare statement for status query: {}", e);
                    SyncStorageError::OperationError(format!("Failed to prepare statement: {}", e))
                })?;

            let records: Result<Vec<SyncRecord>, SyncStorageError> = stmt
                .query_map(params![status_str], |row| {
                    let id_str: String = row.get(0)?;
                    let competition_id_str: Option<String> = row.get(1)?;
                    let agent_id_str: Option<String> = row.get(2)?;
                    let data_type: String = row.get(3)?;
                    let timestamp_str: String = row.get(4)?;
                    let status_str: String = row.get(5)?;

                    Ok((
                        id_str,
                        competition_id_str,
                        agent_id_str,
                        data_type,
                        timestamp_str,
                        status_str,
                    ))
                })
                .map_err(|e| {
                    error!("Failed to query records by status: {}", e);
                    SyncStorageError::OperationError(format!("Failed to query records: {}", e))
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    error!("Failed to collect records: {}", e);
                    SyncStorageError::OperationError(format!("Failed to collect records: {}", e))
                })?
                .into_iter()
                .map(
                    |(
                        id_str,
                        competition_id_str,
                        agent_id_str,
                        data_type,
                        timestamp_str,
                        status_str,
                    )| {
                        let id = Uuid::parse_str(&id_str).map_err(|e| {
                            error!("Failed to parse UUID: {}", e);
                            SyncStorageError::OperationError(format!("Failed to parse UUID: {}", e))
                        })?;
                        let competition_id = competition_id_str
                            .map(|s| Uuid::parse_str(&s))
                            .transpose()
                            .map_err(|e| {
                                error!("Failed to parse competition UUID: {}", e);
                                SyncStorageError::OperationError(format!(
                                    "Failed to parse competition UUID: {}",
                                    e
                                ))
                            })?;
                        let agent_id = agent_id_str
                            .map(|s| Uuid::parse_str(&s))
                            .transpose()
                            .map_err(|e| {
                                error!("Failed to parse agent UUID: {}", e);
                                SyncStorageError::OperationError(format!(
                                    "Failed to parse agent UUID: {}",
                                    e
                                ))
                            })?;
                        let timestamp = Self::string_to_datetime(&timestamp_str)?;
                        let status = Self::string_to_status(&status_str)?;

                        Ok(SyncRecord::with_status(
                            id,
                            competition_id,
                            agent_id,
                            data_type,
                            timestamp,
                            status,
                        ))
                    },
                )
                .collect();

            match &records {
                Ok(recs) => {
                    info!(
                        "Found {} sync records with status {:?}",
                        recs.len(),
                        status_str
                    );
                }
                Err(e) => {
                    error!("Error collecting records with status: {}", e);
                }
            }

            records
        })
        .await
        .map_err(|e| {
            error!("Task panic while getting objects with status: {}", e);
            SyncStorageError::OperationError(format!("Task panic: {}", e))
        })?
    }

    async fn get_last_object(&self) -> Result<Option<SyncRecord>, SyncStorageError> {
        debug!("Getting last sync record by timestamp");

        let connection = Arc::clone(&self.connection);

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => {
                    error!("Failed to acquire database lock");
                    return Err(SyncStorageError::Locked);
                }
            };

            // First try the simple query without unwrapping the tuple
            let result = conn
                .query_row(
                    "SELECT id, competition_id, agent_id, data_type, timestamp, status 
                     FROM sync_records 
                     ORDER BY timestamp DESC 
                     LIMIT 1",
                    [],
                    |row| {
                        let id_str: String = row.get(0)?;
                        let competition_id_str: Option<String> = row.get(1)?;
                        let agent_id_str: Option<String> = row.get(2)?;
                        let data_type: String = row.get(3)?;
                        let timestamp_str: String = row.get(4)?;
                        let status_str: String = row.get(5)?;
                        Ok((
                            id_str,
                            competition_id_str,
                            agent_id_str,
                            data_type,
                            timestamp_str,
                            status_str,
                        ))
                    },
                )
                .optional()
                .map_err(|e| {
                    error!("Failed to query last object: {}", e);
                    SyncStorageError::OperationError(format!("Failed to query last object: {}", e))
                })?;

            match result {
                Some((
                    id_str,
                    competition_id_str,
                    agent_id_str,
                    data_type,
                    timestamp_str,
                    status_str,
                )) => {
                    let id = Uuid::parse_str(&id_str).map_err(|e| {
                        error!("Failed to parse UUID: {}", e);
                        SyncStorageError::OperationError(format!("Failed to parse UUID: {}", e))
                    })?;
                    let competition_id = competition_id_str
                        .map(|s| Uuid::parse_str(&s))
                        .transpose()
                        .map_err(|e| {
                            error!("Failed to parse competition UUID: {}", e);
                            SyncStorageError::OperationError(format!(
                                "Failed to parse competition UUID: {}",
                                e
                            ))
                        })?;
                    let agent_id = agent_id_str
                        .map(|s| Uuid::parse_str(&s))
                        .transpose()
                        .map_err(|e| {
                            error!("Failed to parse agent UUID: {}", e);
                            SyncStorageError::OperationError(format!(
                                "Failed to parse agent UUID: {}",
                                e
                            ))
                        })?;
                    let timestamp = Self::string_to_datetime(&timestamp_str)?;
                    let status = Self::string_to_status(&status_str)?;

                    debug!(
                        "Found last sync record: id={}, timestamp={}",
                        id_str, timestamp_str
                    );
                    Ok(Some(SyncRecord::with_status(
                        id,
                        competition_id,
                        agent_id,
                        data_type,
                        timestamp,
                        status,
                    )))
                }
                None => {
                    debug!("No sync records found in database");
                    Ok(None)
                }
            }
        })
        .await
        .map_err(|e| {
            error!("Task panic while getting last object: {}", e);
            SyncStorageError::OperationError(format!("Task panic: {}", e))
        })?
    }

    async fn get_last_synced_object_id(&self) -> Result<Option<Uuid>, SyncStorageError> {
        debug!("Getting last synced object ID");

        let connection = Arc::clone(&self.connection);
        let key = "last_synced_object_id".to_string();

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => {
                    error!("Failed to acquire database lock");
                    return Err(SyncStorageError::Locked);
                }
            };

            let result: Option<String> = conn
                .query_row(
                    "SELECT value FROM sync_state WHERE key = ?1",
                    params![key],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    error!("Failed to query last synced ID: {}", e);
                    SyncStorageError::OperationError(format!(
                        "Failed to query last synced ID: {}",
                        e
                    ))
                })?;

            match result {
                Some(id_str) => {
                    let id = Uuid::parse_str(&id_str).map_err(|e| {
                        error!("Failed to parse UUID: {}", e);
                        SyncStorageError::OperationError(format!("Failed to parse UUID: {}", e))
                    })?;
                    debug!("Found last synced object ID: {} for key: {}", id, key);
                    Ok(Some(id))
                }
                None => {
                    debug!("No last synced object ID found for key: {}", key);
                    Ok(None)
                }
            }
        })
        .await
        .map_err(|e| {
            error!("Task panic while getting last synced object ID: {}", e);
            SyncStorageError::OperationError(format!("Task panic: {}", e))
        })?
    }

    async fn set_last_synced_object_id(&self, id: Uuid) -> Result<(), SyncStorageError> {
        info!("Setting last synced object ID: {}", id);

        let connection = Arc::clone(&self.connection);
        let id_str = id.to_string();
        let key = "last_synced_object_id".to_string();

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => {
                    error!("Failed to acquire database lock");
                    return Err(SyncStorageError::Locked);
                }
            };

            conn.execute(
                "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?1, ?2)",
                params![key, id_str],
            )
            .map_err(|e| {
                error!("Failed to set last synced ID: {}", e);
                SyncStorageError::OperationError(format!("Failed to set last synced ID: {}", e))
            })?;

            debug!(
                "Successfully set last synced ID {} for key: {}",
                id_str, key
            );
            Ok(())
        })
        .await
        .map_err(|e| {
            error!("Task panic while setting last synced object ID: {}", e);
            SyncStorageError::OperationError(format!("Task panic: {}", e))
        })?
    }

    async fn clear_all(&self) -> Result<(), SyncStorageError> {
        info!("Clearing all sync storage data");

        let connection = Arc::clone(&self.connection);

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => {
                    error!("Failed to acquire database lock");
                    return Err(SyncStorageError::Locked);
                }
            };

            let records_deleted = conn.execute("DELETE FROM sync_records", []).map_err(|e| {
                error!("Failed to clear sync records: {}", e);
                SyncStorageError::OperationError(format!("Failed to clear records: {}", e))
            })?;

            debug!("Deleted {} sync records", records_deleted);

            let state_deleted = conn.execute("DELETE FROM sync_state", []).map_err(|e| {
                error!("Failed to clear sync state: {}", e);
                SyncStorageError::OperationError(format!("Failed to clear sync state: {}", e))
            })?;

            debug!("Deleted {} sync state entries", state_deleted);

            info!("Successfully cleared all sync storage data");
            Ok(())
        })
        .await
        .map_err(|e| {
            error!("Task panic while clearing all data: {}", e);
            SyncStorageError::OperationError(format!("Task panic: {}", e))
        })?
    }
}
