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
use uuid::Uuid;

/// A SQLite implementation of the SyncStorage trait
pub struct SqliteSyncStorage {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteSyncStorage {
    /// Create a new SqliteSyncStorage with the given database path
    pub fn new(db_path: &str) -> Result<Self, SyncStorageError> {
        // Ensure the directory exists
        if let Some(parent) = Path::new(db_path).parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(|e| {
                    SyncStorageError::OpenError(format!("Failed to create directory: {}", e))
                })?;
            }
        }

        // Open or create the SQLite database
        let connection = Connection::open(db_path).map_err(|e| {
            SyncStorageError::OpenError(format!("Failed to open SQLite database: {}", e))
        })?;

        // Create the new sync_records table
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS sync_records (
                    id TEXT PRIMARY KEY,
                    object_key TEXT NOT NULL,
                    bucket_name TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    status TEXT NOT NULL
                )",
                [],
            )
            .map_err(|e| {
                SyncStorageError::OpenError(format!("Failed to create sync_records table: {}", e))
            })?;

        // Create a table to track the last synced object ID
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS sync_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )",
                [],
            )
            .map_err(|e| {
                SyncStorageError::OpenError(format!("Failed to create sync_state table: {}", e))
            })?;

        // Create indexes for efficient querying
        connection
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_sync_records_status ON sync_records(status)",
                [],
            )
            .map_err(|e| {
                SyncStorageError::OpenError(format!("Failed to create status index: {}", e))
            })?;

        connection
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_sync_records_timestamp ON sync_records(timestamp)",
                [],
            )
            .map_err(|e| {
                SyncStorageError::OpenError(format!("Failed to create timestamp index: {}", e))
            })?;

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
        let connection = Arc::clone(&self.connection);
        let id_str = record.id.to_string();
        let timestamp_str = Self::datetime_to_string(record.timestamp);
        let status_str = Self::status_to_string(record.status);

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => return Err(SyncStorageError::Locked),
            };

            conn.execute(
                "INSERT OR REPLACE INTO sync_records (id, object_key, bucket_name, timestamp, status) 
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    id_str,
                    record.object_key,
                    record.bucket_name,
                    timestamp_str,
                    status_str
                ],
            )
            .map_err(|e| {
                SyncStorageError::OperationError(format!("Failed to insert record: {}", e))
            })?;

            Ok(())
        })
        .await
        .map_err(|e| SyncStorageError::OperationError(format!("Task panic: {}", e)))?
    }

    async fn set_object_status(
        &self,
        id: Uuid,
        status: SyncStatus,
    ) -> Result<(), SyncStorageError> {
        let connection = Arc::clone(&self.connection);
        let id_str = id.to_string();
        let status_str = Self::status_to_string(status);

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => return Err(SyncStorageError::Locked),
            };

            let rows_affected = conn
                .execute(
                    "UPDATE sync_records SET status = ?1 WHERE id = ?2",
                    params![status_str, id_str],
                )
                .map_err(|e| {
                    SyncStorageError::OperationError(format!("Failed to update status: {}", e))
                })?;

            if rows_affected == 0 {
                return Err(SyncStorageError::ObjectNotFound(id_str));
            }

            Ok(())
        })
        .await
        .map_err(|e| SyncStorageError::OperationError(format!("Task panic: {}", e)))?
    }

    async fn get_object(&self, id: Uuid) -> Result<Option<SyncRecord>, SyncStorageError> {
        let connection = Arc::clone(&self.connection);
        let id_str = id.to_string();

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => return Err(SyncStorageError::Locked),
            };

            let result: Option<(String, String, String, String, String)> = conn
                .query_row(
                    "SELECT id, object_key, bucket_name, timestamp, status 
                     FROM sync_records WHERE id = ?1",
                    params![id_str],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .optional()
                .map_err(|e| {
                    SyncStorageError::OperationError(format!("Failed to query record: {}", e))
                })?;

            match result {
                Some((id_str, object_key, bucket_name, timestamp_str, status_str)) => {
                    let id = Uuid::parse_str(&id_str).map_err(|e| {
                        SyncStorageError::OperationError(format!("Failed to parse UUID: {}", e))
                    })?;
                    let timestamp = Self::string_to_datetime(&timestamp_str)?;
                    let status = Self::string_to_status(&status_str)?;

                    Ok(Some(SyncRecord::with_status(
                        id,
                        object_key,
                        bucket_name,
                        timestamp,
                        status,
                    )))
                }
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| SyncStorageError::OperationError(format!("Task panic: {}", e)))?
    }

    async fn get_objects_with_status(
        &self,
        status: SyncStatus,
    ) -> Result<Vec<SyncRecord>, SyncStorageError> {
        let connection = Arc::clone(&self.connection);
        let status_str = Self::status_to_string(status);

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => return Err(SyncStorageError::Locked),
            };

            let mut stmt = conn
                .prepare(
                    "SELECT id, object_key, bucket_name, timestamp, status 
                     FROM sync_records 
                     WHERE status = ?1 
                     ORDER BY timestamp",
                )
                .map_err(|e| {
                    SyncStorageError::OperationError(format!("Failed to prepare statement: {}", e))
                })?;

            let records: Result<Vec<SyncRecord>, SyncStorageError> = stmt
                .query_map(params![status_str], |row| {
                    let id_str: String = row.get(0)?;
                    let object_key: String = row.get(1)?;
                    let bucket_name: String = row.get(2)?;
                    let timestamp_str: String = row.get(3)?;
                    let status_str: String = row.get(4)?;

                    Ok((id_str, object_key, bucket_name, timestamp_str, status_str))
                })
                .map_err(|e| {
                    SyncStorageError::OperationError(format!("Failed to query records: {}", e))
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    SyncStorageError::OperationError(format!("Failed to collect records: {}", e))
                })?
                .into_iter()
                .map(
                    |(id_str, object_key, bucket_name, timestamp_str, status_str)| {
                        let id = Uuid::parse_str(&id_str).map_err(|e| {
                            SyncStorageError::OperationError(format!("Failed to parse UUID: {}", e))
                        })?;
                        let timestamp = Self::string_to_datetime(&timestamp_str)?;
                        let status = Self::string_to_status(&status_str)?;

                        Ok(SyncRecord::with_status(
                            id,
                            object_key,
                            bucket_name,
                            timestamp,
                            status,
                        ))
                    },
                )
                .collect();

            records
        })
        .await
        .map_err(|e| SyncStorageError::OperationError(format!("Task panic: {}", e)))?
    }

    async fn get_last_object(&self) -> Result<Option<SyncRecord>, SyncStorageError> {
        let connection = Arc::clone(&self.connection);

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => return Err(SyncStorageError::Locked),
            };

            // First try the simple query without unwrapping the tuple
            let result = conn
                .query_row(
                    "SELECT id, object_key, bucket_name, timestamp, status 
                     FROM sync_records 
                     ORDER BY timestamp DESC 
                     LIMIT 1",
                    [],
                    |row| {
                        let id_str: String = row.get(0)?;
                        let object_key: String = row.get(1)?;
                        let bucket_name: String = row.get(2)?;
                        let timestamp_str: String = row.get(3)?;
                        let status_str: String = row.get(4)?;
                        Ok((id_str, object_key, bucket_name, timestamp_str, status_str))
                    },
                )
                .optional()
                .map_err(|e| {
                    SyncStorageError::OperationError(format!("Failed to query last object: {}", e))
                })?;

            match result {
                Some((id_str, object_key, bucket_name, timestamp_str, status_str)) => {
                    let id = Uuid::parse_str(&id_str).map_err(|e| {
                        SyncStorageError::OperationError(format!("Failed to parse UUID: {}", e))
                    })?;
                    let timestamp = Self::string_to_datetime(&timestamp_str)?;
                    let status = Self::string_to_status(&status_str)?;

                    Ok(Some(SyncRecord::with_status(
                        id,
                        object_key,
                        bucket_name,
                        timestamp,
                        status,
                    )))
                }
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| SyncStorageError::OperationError(format!("Task panic: {}", e)))?
    }

    async fn get_last_synced_object_id(
        &self,
        competition_id: Option<Uuid>,
    ) -> Result<Option<Uuid>, SyncStorageError> {
        let connection = Arc::clone(&self.connection);
        let key = match competition_id {
            None => "last_synced_object_id".to_string(),
            Some(comp_id) => format!("last_synced_object_id:{}", comp_id),
        };

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => return Err(SyncStorageError::Locked),
            };

            let result: Option<String> = conn
                .query_row(
                    "SELECT value FROM sync_state WHERE key = ?1",
                    params![key],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| {
                    SyncStorageError::OperationError(format!(
                        "Failed to query last synced ID: {}",
                        e
                    ))
                })?;

            match result {
                Some(id_str) => {
                    let id = Uuid::parse_str(&id_str).map_err(|e| {
                        SyncStorageError::OperationError(format!("Failed to parse UUID: {}", e))
                    })?;
                    Ok(Some(id))
                }
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| SyncStorageError::OperationError(format!("Task panic: {}", e)))?
    }

    async fn set_last_synced_object_id(
        &self,
        id: Uuid,
        competition_id: Option<Uuid>,
    ) -> Result<(), SyncStorageError> {
        let connection = Arc::clone(&self.connection);
        let id_str = id.to_string();
        let key = match competition_id {
            None => "last_synced_object_id".to_string(),
            Some(comp_id) => format!("last_synced_object_id:{}", comp_id),
        };

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => return Err(SyncStorageError::Locked),
            };

            conn.execute(
                "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?1, ?2)",
                params![key, id_str],
            )
            .map_err(|e| {
                SyncStorageError::OperationError(format!("Failed to set last synced ID: {}", e))
            })?;

            Ok(())
        })
        .await
        .map_err(|e| SyncStorageError::OperationError(format!("Task panic: {}", e)))?
    }

    async fn clear_all(&self) -> Result<(), SyncStorageError> {
        let connection = Arc::clone(&self.connection);

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => return Err(SyncStorageError::Locked),
            };

            conn.execute("DELETE FROM sync_records", []).map_err(|e| {
                SyncStorageError::OperationError(format!("Failed to clear records: {}", e))
            })?;

            conn.execute("DELETE FROM sync_state", []).map_err(|e| {
                SyncStorageError::OperationError(format!("Failed to clear sync state: {}", e))
            })?;

            Ok(())
        })
        .await
        .map_err(|e| SyncStorageError::OperationError(format!("Task panic: {}", e)))?
    }
}
