use crate::sync::storage::error::SyncStorageError;
use crate::sync::storage::sync_storage::SyncStorage;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::task;

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

        // Create tables if they don't exist
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS synced_objects (
                object_key TEXT PRIMARY KEY,
                synced_at TEXT NOT NULL
            )",
                [],
            )
            .map_err(|e| {
                SyncStorageError::OpenError(format!("Failed to create synced_objects table: {}", e))
            })?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS sync_state (
                id INTEGER PRIMARY KEY CHECK (id = 1), -- Ensure only one row
                last_sync_timestamp TEXT
            )",
                [],
            )
            .map_err(|e| {
                SyncStorageError::OpenError(format!("Failed to create sync_state table: {}", e))
            })?;

        // Insert default sync state if it doesn't exist
        connection
            .execute(
                "INSERT OR IGNORE INTO sync_state (id, last_sync_timestamp) VALUES (1, NULL)",
                [],
            )
            .map_err(|e| {
                SyncStorageError::OpenError(format!("Failed to initialize sync state: {}", e))
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
}

#[async_trait]
impl SyncStorage for SqliteSyncStorage {
    async fn mark_object_synced(
        &self,
        object_key: &str,
        sync_timestamp: DateTime<Utc>,
    ) -> Result<(), SyncStorageError> {
        let connection = Arc::clone(&self.connection);
        let object_key = object_key.to_string();
        let sync_timestamp_str = Self::datetime_to_string(sync_timestamp);

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => return Err(SyncStorageError::Locked),
            };

            conn.execute(
                "INSERT OR REPLACE INTO synced_objects (object_key, synced_at) VALUES (?, ?)",
                params![object_key, sync_timestamp_str],
            )
            .map_err(|e| {
                SyncStorageError::OperationError(format!("Failed to mark object as synced: {}", e))
            })?;

            Ok(())
        })
        .await
        .map_err(|e| SyncStorageError::OperationError(format!("Task panic: {}", e)))?
    }

    async fn is_object_synced(&self, object_key: &str) -> Result<bool, SyncStorageError> {
        let connection = Arc::clone(&self.connection);
        let object_key = object_key.to_string();

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => return Err(SyncStorageError::Locked),
            };

            let exists: Option<bool> = conn
                .query_row(
                    "SELECT 1 FROM synced_objects WHERE object_key = ?",
                    params![object_key],
                    |_| Ok(true),
                )
                .optional()
                .map_err(|e| {
                    SyncStorageError::OperationError(format!(
                        "Failed to check if object is synced: {}",
                        e
                    ))
                })?;

            Ok(exists.is_some())
        })
        .await
        .map_err(|e| SyncStorageError::OperationError(format!("Task panic: {}", e)))?
    }

    async fn update_last_sync_timestamp(
        &self,
        timestamp: DateTime<Utc>,
    ) -> Result<(), SyncStorageError> {
        let connection = Arc::clone(&self.connection);
        let timestamp_str = Self::datetime_to_string(timestamp);

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => return Err(SyncStorageError::Locked),
            };

            conn.execute(
                "UPDATE sync_state SET last_sync_timestamp = ? WHERE id = 1",
                params![timestamp_str],
            )
            .map_err(|e| {
                SyncStorageError::OperationError(format!(
                    "Failed to update last sync timestamp: {}",
                    e
                ))
            })?;

            Ok(())
        })
        .await
        .map_err(|e| SyncStorageError::OperationError(format!("Task panic: {}", e)))?
    }

    async fn get_last_sync_timestamp(&self) -> Result<Option<DateTime<Utc>>, SyncStorageError> {
        let connection = Arc::clone(&self.connection);

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => return Err(SyncStorageError::Locked),
            };

            // First check if the table is initialized
            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM sync_state", [], |row| row.get(0))
                .map_err(|e| {
                    SyncStorageError::OperationError(format!(
                        "Failed to check sync_state table: {}",
                        e
                    ))
                })?;

            // If no rows, initialize the table
            if count == 0 {
                conn.execute(
                    "INSERT INTO sync_state (id, last_sync_timestamp) VALUES (1, NULL)",
                    [],
                )
                .map_err(|e| {
                    SyncStorageError::OperationError(format!(
                        "Failed to initialize sync state: {}",
                        e
                    ))
                })?;

                return Ok(None);
            }

            // Get the timestamp
            let timestamp_result: rusqlite::Result<Option<String>> = conn.query_row(
                "SELECT last_sync_timestamp FROM sync_state WHERE id = 1",
                [],
                |row| row.get(0),
            );

            match timestamp_result {
                Ok(Some(ts)) if !ts.is_empty() => {
                    let dt = Self::string_to_datetime(&ts)?;
                    Ok(Some(dt))
                }
                Ok(None) | Ok(Some(_)) => Ok(None),
                Err(e) => {
                    if let rusqlite::Error::InvalidColumnType(..) = e {
                        // If the column is NULL
                        Ok(None)
                    } else {
                        Err(SyncStorageError::OperationError(format!(
                            "Failed to get last sync timestamp: {}",
                            e
                        )))
                    }
                }
            }
        })
        .await
        .map_err(|e| SyncStorageError::OperationError(format!("Task panic: {}", e)))?
    }
}
