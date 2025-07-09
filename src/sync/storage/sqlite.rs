use crate::db::data_type::DataType;
use crate::sync::storage::error::SyncStorageError;
use crate::sync::storage::models::{FailureType, SyncRecord, SyncStatus};
use crate::sync::storage::sync_storage::SyncStorage;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::fs;
use std::path::Path;
use std::str::FromStr;
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
        info!("Creating SQLite sync storage at path: {db_path}");

        if let Some(parent) = Path::new(db_path).parent() {
            if !parent.exists() {
                debug!("Creating parent directory: {:?}", parent);
                fs::create_dir_all(parent).map_err(|e| {
                    error!("Failed to create directory {parent:?}: {e}");
                    SyncStorageError::OpenError(format!("Failed to create directory: {e}"))
                })?;
            }
        }

        let connection = Connection::open(db_path).map_err(|e| {
            error!("Failed to open SQLite database at {db_path}: {e}");
            SyncStorageError::OpenError(format!("Failed to open SQLite database: {e}"))
        })?;

        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS sync_records (
                    id TEXT PRIMARY KEY,
                    competition_id TEXT,
                    agent_id TEXT,
                    data_type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    status TEXT NOT NULL,
                    retry_count INTEGER DEFAULT 0,
                    last_error TEXT,
                    failure_type TEXT,
                    first_attempt_at TEXT NOT NULL DEFAULT (datetime('now')),
                    last_attempt_at TEXT NOT NULL DEFAULT (datetime('now')),
                    object_key TEXT,
                    size_bytes INTEGER,
                    metadata TEXT,
                    event_timestamp TEXT,
                    data BLOB
                )",
                [],
            )
            .map_err(|e| {
                error!("Failed to create sync_records table: {e}");
                SyncStorageError::OpenError(format!("Failed to create sync_records table: {e}"))
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
                error!("Failed to create sync_state table: {e}");
                SyncStorageError::OpenError(format!("Failed to create sync_state table: {e}"))
            })?;

        // Create indexes for efficient querying
        connection
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_sync_records_status ON sync_records(status)",
                [],
            )
            .map_err(|e| {
                error!("Failed to create status index: {e}");
                SyncStorageError::OpenError(format!("Failed to create status index: {e}"))
            })?;

        connection
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_sync_records_timestamp ON sync_records(timestamp)",
                [],
            )
            .map_err(|e| {
                error!("Failed to create timestamp index: {e}");
                SyncStorageError::OpenError(format!("Failed to create timestamp index: {e}"))
            })?;

        connection
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_sync_records_failed ON sync_records(status) WHERE status = 'FAILED'",
                [],
            )
            .map_err(|e| {
                error!("Failed to create failed index: {e}");
                SyncStorageError::OpenError(format!("Failed to create failed index: {e}"))
            })?;

        connection
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_sync_records_failure_type ON sync_records(failure_type)",
                [],
            )
            .map_err(|e| {
                error!("Failed to create failure type index: {e}");
                SyncStorageError::OpenError(format!("Failed to create failure type index: {e}"))
            })?;

        info!("SQLite sync storage initialized successfully at: {db_path}");
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
            .map_err(|e| SyncStorageError::OperationError(format!("Failed to parse datetime: {e}")))
    }

    // Helper function to convert SyncStatus to/from string
    fn status_to_string(status: SyncStatus) -> &'static str {
        match status {
            SyncStatus::PendingSync => "PENDING_SYNC",
            SyncStatus::Processing => "PROCESSING",
            SyncStatus::Complete => "COMPLETE",
            SyncStatus::Failed => "FAILED",
            SyncStatus::FailedPermanently => "FAILED_PERMANENTLY",
        }
    }

    fn string_to_status(s: &str) -> Result<SyncStatus, SyncStorageError> {
        match s {
            "PENDING_SYNC" => Ok(SyncStatus::PendingSync),
            "PROCESSING" => Ok(SyncStatus::Processing),
            "COMPLETE" => Ok(SyncStatus::Complete),
            "FAILED" => Ok(SyncStatus::Failed),
            "FAILED_PERMANENTLY" => Ok(SyncStatus::FailedPermanently),
            _ => Err(SyncStorageError::OperationError(format!(
                "Invalid status: {s}"
            ))),
        }
    }

    // Helper function to convert FailureType to/from string
    fn failure_type_to_string(failure_type: FailureType) -> &'static str {
        match failure_type {
            FailureType::S3DataRetrieval => "S3_DATA_RETRIEVAL",
            FailureType::RecallStorage => "RECALL_STORAGE",
            FailureType::Other => "OTHER",
        }
    }

    fn string_to_failure_type(s: &str) -> Result<FailureType, SyncStorageError> {
        match s {
            "S3_DATA_RETRIEVAL" => Ok(FailureType::S3DataRetrieval),
            "RECALL_STORAGE" => Ok(FailureType::RecallStorage),
            "OTHER" => Ok(FailureType::Other),
            _ => Err(SyncStorageError::OperationError(format!(
                "Invalid failure type: {s}"
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
        let data_type_str = record.data_type.to_string();
        let timestamp_str = Self::datetime_to_string(record.timestamp);
        let status_str = Self::status_to_string(record.status);
        let retry_count = record.retry_count;
        let last_error = record.last_error;
        let failure_type_str = record
            .failure_type
            .map(|ft| Self::failure_type_to_string(ft).to_string());
        let first_attempt_at_str = Self::datetime_to_string(record.first_attempt_at);
        let last_attempt_at_str = Self::datetime_to_string(record.last_attempt_at);
        let object_key = record.object_key;
        let size_bytes = record.size_bytes;
        let metadata = record.metadata.map(|m| m.to_string());
        let event_timestamp = record
            .event_timestamp
            .map(|dt| Self::datetime_to_string(dt));
        let data = record.data;

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => {
                    error!("Failed to acquire database lock");
                    return Err(SyncStorageError::Locked);
                }
            };

            conn.execute(
                "INSERT OR REPLACE INTO sync_records (id, competition_id, agent_id, data_type, timestamp, status, retry_count, last_error, failure_type, first_attempt_at, last_attempt_at, object_key, size_bytes, metadata, event_timestamp, data) 
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                params![
                    id_str,
                    competition_id_str,
                    agent_id_str,
                    data_type_str,
                    timestamp_str,
                    status_str,
                    retry_count,
                    last_error,
                    failure_type_str,
                    first_attempt_at_str,
                    last_attempt_at_str,
                    object_key,
                    size_bytes,
                    metadata,
                    event_timestamp,
                    data
                ],
            )
            .map_err(|e| {
                error!("Failed to insert sync record: {e}");
                SyncStorageError::OperationError(format!("Failed to insert record: {e}"))
            })?;

            debug!("Successfully added sync record for id: {id_str}");
            Ok(())
        })
        .await
        .map_err(|e| {
            error!("Task panic while adding object: {e}");
            SyncStorageError::OperationError(format!("Task panic: {e}"))
        })?
    }

    async fn upsert_object(&self, record: SyncRecord) -> Result<(), SyncStorageError> {
        debug!(
            "Upserting sync record: id={}, competition_id={:?}, agent_id={:?}, data_type={}, status={:?}",
            record.id, record.competition_id, record.agent_id, record.data_type, record.status
        );

        let connection = Arc::clone(&self.connection);
        let id_str = record.id.to_string();
        let competition_id_str = record.competition_id.map(|id| id.to_string());
        let agent_id_str = record.agent_id.map(|id| id.to_string());
        let data_type_str = record.data_type.to_string();
        let timestamp_str = Self::datetime_to_string(record.timestamp);
        let status_str = Self::status_to_string(record.status);
        let retry_count = record.retry_count;
        let last_error = record.last_error;
        let failure_type_str = record
            .failure_type
            .map(|ft| Self::failure_type_to_string(ft).to_string());
        let first_attempt_at_str = Self::datetime_to_string(record.first_attempt_at);
        let last_attempt_at_str = Self::datetime_to_string(record.last_attempt_at);
        let object_key = record.object_key;
        let size_bytes = record.size_bytes;
        let metadata_str = record.metadata.map(|m| m.to_string());
        let event_timestamp_str = record.event_timestamp.map(|ts| Self::datetime_to_string(ts));
        let data = record.data;

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => {
                    error!("Failed to acquire database lock");
                    return Err(SyncStorageError::Locked);
                }
            };

            conn.execute(
                "INSERT OR REPLACE INTO sync_records (id, competition_id, agent_id, data_type, timestamp, status, retry_count, last_error, failure_type, first_attempt_at, last_attempt_at, object_key, size_bytes, metadata, event_timestamp, data)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
                params![
                    id_str,
                    competition_id_str,
                    agent_id_str,
                    data_type_str,
                    timestamp_str,
                    status_str,
                    retry_count,
                    last_error,
                    failure_type_str,
                    first_attempt_at_str,
                    last_attempt_at_str,
                    object_key,
                    size_bytes,
                    metadata_str,
                    event_timestamp_str,
                    data
                ],
            )
            .map_err(|e| {
                error!("Failed to upsert sync record: {e}");
                SyncStorageError::OperationError(format!("Failed to upsert sync record: {e}"))
            })?;

            debug!("Successfully upserted sync record: {id_str}");
            Ok(())
        })
        .await
        .map_err(|e| {
            error!("Task panic while upserting object: {e}");
            SyncStorageError::OperationError(format!("Task panic: {e}"))
        })?
    }

    async fn set_object_status(
        &self,
        id: Uuid,
        status: SyncStatus,
    ) -> Result<(), SyncStorageError> {
        debug!("Updating object status: id={id}, status={status:?}");

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
                    error!("Failed to update object status: {e}");
                    SyncStorageError::OperationError(format!("Failed to update status: {e}"))
                })?;

            if rows_affected == 0 {
                warn!("Object not found for status update: id={id_str}");
                return Err(SyncStorageError::ObjectNotFound(id_str));
            }

            debug!("Successfully updated status to {status_str} for object: {id_str}");
            Ok(())
        })
        .await
        .map_err(|e| {
            error!("Task panic while updating object status: {e}");
            SyncStorageError::OperationError(format!("Task panic: {e}"))
        })?
    }

    async fn get_object(&self, id: Uuid) -> Result<Option<SyncRecord>, SyncStorageError> {
        debug!("Getting sync record by id: {id}");

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

            let result: Option<(
                String,
                Option<String>,
                Option<String>,
                String,
                String,
                String,
                u32,
                Option<String>,
                Option<String>,
                String,
                String,
                Option<String>,
                Option<i64>,
                Option<String>,
                Option<String>,
                Option<Vec<u8>>,
            )> = conn
                .query_row(
                    "SELECT id, competition_id, agent_id, data_type, timestamp, status, retry_count, last_error, failure_type, first_attempt_at, last_attempt_at, object_key, size_bytes, metadata, event_timestamp, data 
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
                            row.get(6)?,
                            row.get(7)?,
                            row.get(8)?,
                            row.get(9)?,
                            row.get(10)?,
                            row.get(11)?,
                            row.get(12)?,
                            row.get(13)?,
                            row.get(14)?,
                            row.get(15)?,
                        ))
                    },
                )
                .optional()
                .map_err(|e| {
                    error!("Failed to query sync record: {e}");
                    SyncStorageError::OperationError(format!("Failed to query record: {e}"))
                })?;

            match result {
                Some((
                    id_str,
                    competition_id_str,
                    agent_id_str,
                    data_type,
                    timestamp_str,
                    status_str,
                    retry_count,
                    last_error,
                    failure_type_str,
                    first_attempt_at_str,
                    last_attempt_at_str,
                    object_key,
                    size_bytes,
                    metadata_str,
                    event_timestamp_str,
                    data,
                )) => {
                    let id = Uuid::parse_str(&id_str).map_err(|e| {
                        error!("Failed to parse UUID from database: {e}");
                        SyncStorageError::OperationError(format!("Failed to parse UUID: {e}"))
                    })?;
                    let competition_id = competition_id_str
                        .map(|s| Uuid::parse_str(&s))
                        .transpose()
                        .map_err(|e| {
                            error!("Failed to parse competition UUID from database: {e}");
                            SyncStorageError::OperationError(format!(
                                "Failed to parse competition UUID: {e}"
                            ))
                        })?;
                    let agent_id = agent_id_str
                        .map(|s| Uuid::parse_str(&s))
                        .transpose()
                        .map_err(|e| {
                            error!("Failed to parse agent UUID from database: {e}");
                            SyncStorageError::OperationError(format!(
                                "Failed to parse agent UUID: {e}"
                            ))
                        })?;
                    let timestamp = Self::string_to_datetime(&timestamp_str)?;
                    let status = Self::string_to_status(&status_str)?;
                    let data_type = DataType::from_str(&data_type).map_err(|e| {
                        error!("Failed to parse data type from database: {e}");
                        SyncStorageError::OperationError(format!("Failed to parse data type: {e}"))
                    })?;
                    let failure_type = failure_type_str
                        .map(|s| Self::string_to_failure_type(&s))
                        .transpose()?;
                    let first_attempt_at = Self::string_to_datetime(&first_attempt_at_str)?;
                    let last_attempt_at = Self::string_to_datetime(&last_attempt_at_str)?;
                    let metadata = metadata_str
                        .map(|s| serde_json::from_str(&s))
                        .transpose()
                        .map_err(|e| {
                            error!("Failed to parse metadata from database: {e}");
                            SyncStorageError::OperationError(format!("Failed to parse metadata: {e}"))
                        })?;
                    let event_timestamp = event_timestamp_str
                        .map(|s| Self::string_to_datetime(&s))
                        .transpose()?;

                    debug!("Found sync record: id={id_str}, status={status:?}");
                    Ok(Some(SyncRecord {
                        id,
                        competition_id,
                        agent_id,
                        data_type,
                        timestamp,
                        status,
                        retry_count,
                        last_error,
                        failure_type,
                        first_attempt_at,
                        last_attempt_at,
                        object_key,
                        size_bytes,
                        metadata,
                        event_timestamp,
                        data,
                    }))
                }
                None => {
                    debug!("No sync record found for id: {id_str}");
                    Ok(None)
                }
            }
        })
        .await
        .map_err(|e| {
            error!("Task panic while getting object: {e}");
            SyncStorageError::OperationError(format!("Task panic: {e}"))
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
                    "SELECT id, competition_id, agent_id, data_type, timestamp, status, retry_count, last_error, failure_type, first_attempt_at, last_attempt_at, object_key, size_bytes, metadata, event_timestamp, data
                     FROM sync_records 
                     WHERE status = ?1 
                     ORDER BY timestamp",
                )
                .map_err(|e| {
                    error!("Failed to prepare statement for status query: {e}");
                    SyncStorageError::OperationError(format!("Failed to prepare statement: {e}"))
                })?;

            let records: Result<Vec<SyncRecord>, SyncStorageError> = stmt
                .query_map(params![status_str], |row| {
                    let id_str: String = row.get(0)?;
                    let competition_id_str: Option<String> = row.get(1)?;
                    let agent_id_str: Option<String> = row.get(2)?;
                    let data_type: String = row.get(3)?;
                    let timestamp_str: String = row.get(4)?;
                    let status_str: String = row.get(5)?;
                    let retry_count: u32 = row.get(6)?;
                    let last_error: Option<String> = row.get(7)?;
                    let failure_type_str: Option<String> = row.get(8)?;
                    let first_attempt_at_str: String = row.get(9)?;
                    let last_attempt_at_str: String = row.get(10)?;
                    let object_key: Option<String> = row.get(11)?;
                    let size_bytes: Option<i64> = row.get(12)?;
                    let metadata_str: Option<String> = row.get(13)?;
                    let event_timestamp_str: Option<String> = row.get(14)?;
                    let data: Option<Vec<u8>> = row.get(15)?;

                    Ok((
                        id_str,
                        competition_id_str,
                        agent_id_str,
                        data_type,
                        timestamp_str,
                        status_str,
                        retry_count,
                        last_error,
                        failure_type_str,
                        first_attempt_at_str,
                        last_attempt_at_str,
                        object_key,
                        size_bytes,
                        metadata_str,
                        event_timestamp_str,
                        data,
                    ))
                })
                .map_err(|e| {
                    error!("Failed to query records by status: {e}");
                    SyncStorageError::OperationError(format!("Failed to query records: {e}"))
                })?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    error!("Failed to collect records: {e}");
                    SyncStorageError::OperationError(format!("Failed to collect records: {e}"))
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
                        retry_count,
                        last_error,
                        failure_type_str,
                        first_attempt_at_str,
                        last_attempt_at_str,
                        object_key,
                        size_bytes,
                        metadata_str,
                        event_timestamp_str,
                        data,
                    )| {
                        let id = Uuid::parse_str(&id_str).map_err(|e| {
                            error!("Failed to parse UUID: {e}");
                            SyncStorageError::OperationError(format!("Failed to parse UUID: {e}"))
                        })?;
                        let competition_id = competition_id_str
                            .map(|s| Uuid::parse_str(&s))
                            .transpose()
                            .map_err(|e| {
                                error!("Failed to parse competition UUID: {e}");
                                SyncStorageError::OperationError(format!(
                                    "Failed to parse competition UUID: {e}"
                                ))
                            })?;
                        let agent_id = agent_id_str
                            .map(|s| Uuid::parse_str(&s))
                            .transpose()
                            .map_err(|e| {
                                error!("Failed to parse agent UUID: {e}");
                                SyncStorageError::OperationError(format!(
                                    "Failed to parse agent UUID: {e}"
                                ))
                            })?;
                        let timestamp = Self::string_to_datetime(&timestamp_str)?;
                        let status = Self::string_to_status(&status_str)?;
                        let data_type = DataType::from_str(&data_type).map_err(|e| {
                            error!("Failed to parse data type: {e}");
                            SyncStorageError::OperationError(format!(
                                "Failed to parse data type: {e}"
                            ))
                        })?;
                        let failure_type = failure_type_str
                            .map(|s| Self::string_to_failure_type(&s))
                            .transpose()?;
                        let first_attempt_at = Self::string_to_datetime(&first_attempt_at_str)?;
                        let last_attempt_at = Self::string_to_datetime(&last_attempt_at_str)?;
                        let metadata = metadata_str
                            .map(|s| serde_json::from_str(&s))
                            .transpose()
                            .map_err(|e| {
                                error!("Failed to parse metadata JSON: {e}");
                                SyncStorageError::OperationError(format!(
                                    "Failed to parse metadata JSON: {e}"
                                ))
                            })?;
                        let event_timestamp = event_timestamp_str
                            .map(|s| Self::string_to_datetime(&s))
                            .transpose()?;

                        Ok(SyncRecord {
                            id,
                            competition_id,
                            agent_id,
                            data_type,
                            timestamp,
                            status,
                            retry_count,
                            last_error,
                            failure_type,
                            first_attempt_at,
                            last_attempt_at,
                            object_key,
                            size_bytes,
                            metadata,
                            event_timestamp,
                            data,
                        })
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
                    error!("Error collecting records with status: {e}");
                }
            }

            records
        })
        .await
        .map_err(|e| {
            error!("Task panic while getting objects with status: {e}");
            SyncStorageError::OperationError(format!("Task panic: {e}"))
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
                    "SELECT id, competition_id, agent_id, data_type, timestamp, status, retry_count, last_error, failure_type, first_attempt_at, last_attempt_at, object_key, size_bytes, metadata, event_timestamp, data
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
                        let retry_count: u32 = row.get(6)?;
                        let last_error: Option<String> = row.get(7)?;
                        let failure_type_str: Option<String> = row.get(8)?;
                        let first_attempt_at_str: String = row.get(9)?;
                        let last_attempt_at_str: String = row.get(10)?;
                        let object_key: Option<String> = row.get(11)?;
                        let size_bytes: Option<i64> = row.get(12)?;
                        let metadata_str: Option<String> = row.get(13)?;
                        let event_timestamp_str: Option<String> = row.get(14)?;
                        let data: Option<Vec<u8>> = row.get(15)?;
                        Ok((
                            id_str,
                            competition_id_str,
                            agent_id_str,
                            data_type,
                            timestamp_str,
                            status_str,
                            retry_count,
                            last_error,
                            failure_type_str,
                            first_attempt_at_str,
                            last_attempt_at_str,
                            object_key,
                            size_bytes,
                            metadata_str,
                            event_timestamp_str,
                            data,
                        ))
                    },
                )
                .optional()
                .map_err(|e| {
                    error!("Failed to query last object: {e}");
                    SyncStorageError::OperationError(format!("Failed to query last object: {e}"))
                })?;

            match result {
                Some((
                    id_str,
                    competition_id_str,
                    agent_id_str,
                    data_type,
                    timestamp_str,
                    status_str,
                    retry_count,
                    last_error,
                    failure_type_str,
                    first_attempt_at_str,
                    last_attempt_at_str,
                    object_key,
                    size_bytes,
                    metadata_str,
                    event_timestamp_str,
                    data,
                )) => {
                    let id = Uuid::parse_str(&id_str).map_err(|e| {
                        error!("Failed to parse UUID: {e}");
                        SyncStorageError::OperationError(format!("Failed to parse UUID: {e}"))
                    })?;
                    let competition_id = competition_id_str
                        .map(|s| Uuid::parse_str(&s))
                        .transpose()
                        .map_err(|e| {
                            error!("Failed to parse competition UUID: {e}");
                            SyncStorageError::OperationError(format!(
                                "Failed to parse competition UUID: {e}"
                            ))
                        })?;
                    let agent_id = agent_id_str
                        .map(|s| Uuid::parse_str(&s))
                        .transpose()
                        .map_err(|e| {
                            error!("Failed to parse agent UUID: {e}");
                            SyncStorageError::OperationError(format!(
                                "Failed to parse agent UUID: {e}"
                            ))
                        })?;
                    let timestamp = Self::string_to_datetime(&timestamp_str)?;
                    let status = Self::string_to_status(&status_str)?;
                    let data_type = DataType::from_str(&data_type).map_err(|e| {
                        error!("Failed to parse data type: {e}");
                        SyncStorageError::OperationError(format!("Failed to parse data type: {e}"))
                    })?;
                    let failure_type = failure_type_str
                        .map(|s| Self::string_to_failure_type(&s))
                        .transpose()?;
                    let first_attempt_at = Self::string_to_datetime(&first_attempt_at_str)?;
                    let last_attempt_at = Self::string_to_datetime(&last_attempt_at_str)?;
                    let metadata = metadata_str
                        .map(|s| serde_json::from_str(&s))
                        .transpose()
                        .map_err(|e| {
                            error!("Failed to parse metadata JSON: {e}");
                            SyncStorageError::OperationError(format!(
                                "Failed to parse metadata JSON: {e}"
                            ))
                        })?;
                    let event_timestamp = event_timestamp_str
                        .map(|s| Self::string_to_datetime(&s))
                        .transpose()?;

                    debug!("Found last sync record: id={id_str}, timestamp={timestamp_str}");
                    Ok(Some(SyncRecord {
                        id,
                        competition_id,
                        agent_id,
                        data_type,
                        timestamp,
                        status,
                        retry_count,
                        last_error,
                        failure_type,
                        first_attempt_at,
                        last_attempt_at,
                        object_key,
                        size_bytes,
                        metadata,
                        event_timestamp,
                        data,
                    }))
                }
                None => {
                    debug!("No sync records found in database");
                    Ok(None)
                }
            }
        })
        .await
        .map_err(|e| {
            error!("Task panic while getting last object: {e}");
            SyncStorageError::OperationError(format!("Task panic: {e}"))
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
                    error!("Failed to query last synced ID: {e}");
                    SyncStorageError::OperationError(format!("Failed to query last synced ID: {e}"))
                })?;

            match result {
                Some(id_str) => {
                    let id = Uuid::parse_str(&id_str).map_err(|e| {
                        error!("Failed to parse UUID: {e}");
                        SyncStorageError::OperationError(format!("Failed to parse UUID: {e}"))
                    })?;
                    debug!("Found last synced object ID: {id} for key: {key}");
                    Ok(Some(id))
                }
                None => {
                    debug!("No last synced object ID found for key: {key}");
                    Ok(None)
                }
            }
        })
        .await
        .map_err(|e| {
            error!("Task panic while getting last synced object ID: {e}");
            SyncStorageError::OperationError(format!("Task panic: {e}"))
        })?
    }

    async fn set_last_synced_object_id(&self, id: Uuid) -> Result<(), SyncStorageError> {
        info!("Setting last synced object ID: {id}");

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
                error!("Failed to set last synced ID: {e}");
                SyncStorageError::OperationError(format!("Failed to set last synced ID: {e}"))
            })?;

            debug!("Successfully set last synced ID {id_str} for key: {key}");
            Ok(())
        })
        .await
        .map_err(|e| {
            error!("Task panic while setting last synced object ID: {e}");
            SyncStorageError::OperationError(format!("Task panic: {e}"))
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
                error!("Failed to clear sync records: {e}");
                SyncStorageError::OperationError(format!("Failed to clear records: {e}"))
            })?;

            debug!("Deleted {records_deleted} sync records");

            let state_deleted = conn.execute("DELETE FROM sync_state", []).map_err(|e| {
                error!("Failed to clear sync state: {e}");
                SyncStorageError::OperationError(format!("Failed to clear sync state: {e}"))
            })?;

            debug!("Deleted {state_deleted} sync state entries");

            info!("Successfully cleared all sync storage data");
            Ok(())
        })
        .await
        .map_err(|e| {
            error!("Task panic while clearing all data: {e}");
            SyncStorageError::OperationError(format!("Task panic: {e}"))
        })?
    }

    async fn record_failure(
        &self,
        id: Uuid,
        failure_type: FailureType,
        error_message: String,
        is_permanent: bool,
    ) -> Result<(), SyncStorageError> {
        debug!("Recording failure for object: id={id}, failure_type={failure_type:?}, error={error_message}");

        let connection = Arc::clone(&self.connection);
        let id_str = id.to_string();
        let failure_type_str = Self::failure_type_to_string(failure_type);
        let now_str = Self::datetime_to_string(Utc::now());

        task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => {
                    error!("Failed to acquire database lock");
                    return Err(SyncStorageError::Locked);
                }
            };

            // Get current retry count
            let current_retry_count: u32 = conn
                .query_row(
                    "SELECT retry_count FROM sync_records WHERE id = ?1",
                    params![id_str],
                    |row| row.get(0),
                )
                .unwrap_or(0);

            let new_retry_count = current_retry_count + 1;
            let new_status = if is_permanent {
                Self::status_to_string(SyncStatus::FailedPermanently)
            } else {
                Self::status_to_string(SyncStatus::Failed)
            };

            let rows_affected = conn
                .execute(
                    "UPDATE sync_records SET 
                     status = ?1, 
                     retry_count = ?2, 
                     last_error = ?3, 
                     failure_type = ?4, 
                     last_attempt_at = ?5 
                     WHERE id = ?6",
                    params![
                        new_status,
                        new_retry_count,
                        error_message,
                        failure_type_str,
                        now_str,
                        id_str
                    ],
                )
                .map_err(|e| {
                    error!("Failed to record failure: {e}");
                    SyncStorageError::OperationError(format!("Failed to record failure: {e}"))
                })?;

            if rows_affected == 0 {
                warn!("Object not found for failure recording: id={id_str}");
                return Err(SyncStorageError::ObjectNotFound(id_str));
            }

            debug!("Successfully recorded failure for object: {id_str}, new_retry_count={new_retry_count}, status={new_status}");
            Ok(())
        })
        .await
        .map_err(|e| {
            error!("Task panic while recording failure: {e}");
            SyncStorageError::OperationError(format!("Task panic: {e}"))
        })?
    }

    async fn get_failed_objects(&self) -> Result<Vec<SyncRecord>, SyncStorageError> {
        self.get_objects_with_status(SyncStatus::Failed).await
    }

    async fn get_permanently_failed_objects(&self) -> Result<Vec<SyncRecord>, SyncStorageError> {
        self.get_objects_with_status(SyncStatus::FailedPermanently)
            .await
    }

    async fn clear_object_data(&self, id: Uuid) -> Result<(), SyncStorageError> {
        let connection = Arc::clone(&self.connection);
        let id_str = id.to_string();

        let rows_affected = tokio::task::spawn_blocking(move || {
            let conn = match connection.lock() {
                Ok(conn) => conn,
                Err(_) => return Err(SyncStorageError::Locked),
            };

            conn.execute(
                "UPDATE sync_records SET data = NULL WHERE id = ?1",
                params![id_str],
            )
            .map_err(|e| {
                error!("Failed to clear object data: {e}");
                SyncStorageError::OperationError(format!("Failed to clear object data: {e}"))
            })
        })
        .await
        .map_err(|e| {
            error!("Task join error: {e}");
            SyncStorageError::OperationError(format!("Task join error: {e}"))
        })??;

        if rows_affected == 0 {
            return Err(SyncStorageError::ObjectNotFound(id.to_string()));
        }

        Ok(())
    }
}
