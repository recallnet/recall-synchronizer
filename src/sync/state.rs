use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};
use sqlx::types::chrono::{DateTime, Utc};
use std::path::Path;
use tokio::sync::Mutex;
use tracing::{debug, info};

pub struct SyncState {
    conn: Mutex<Connection>,
}

impl SyncState {
    pub async fn new(path: &str, reset: bool) -> Result<Self> {
        let path = Path::new(path);

        if reset && path.exists() {
            std::fs::remove_file(path)?;
            info!("Reset state database");
        }

        let conn = Connection::open(path)?;

        // Initialize schema
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY,
                cid TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS sync_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        )?;

        let state = Self {
            conn: Mutex::new(conn),
        };

        info!("Initialized state database at {}", path.display());
        Ok(state)
    }

    pub async fn is_processed(&self, key: &str) -> Result<bool> {
        let conn = self.conn.lock().await;

        let exists: Option<bool> = conn
            .query_row(
                "SELECT 1 FROM sync_state WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()?;

        Ok(exists.is_some())
    }

    pub async fn mark_processed(&self, key: &str, cid: &str) -> Result<()> {
        let conn = self.conn.lock().await;

        conn.execute(
            "INSERT OR REPLACE INTO sync_state (key, cid, timestamp) VALUES (?1, ?2, ?3)",
            params![key, cid, Utc::now().to_rfc3339()],
        )?;

        Ok(())
    }

    pub async fn get_last_sync_time(&self) -> Result<Option<DateTime<Utc>>> {
        let conn = self.conn.lock().await;

        let timestamp: Option<String> = conn
            .query_row(
                "SELECT value FROM sync_metadata WHERE key = 'last_sync_time'",
                [],
                |row| row.get(0),
            )
            .optional()?;

        match timestamp {
            Some(ts) => {
                let datetime = DateTime::parse_from_rfc3339(&ts)?.with_timezone(&Utc);
                Ok(Some(datetime))
            }
            None => Ok(None),
        }
    }

    pub async fn update_last_sync_time(&self, timestamp: DateTime<Utc>) -> Result<()> {
        let conn = self.conn.lock().await;

        conn.execute(
            "INSERT OR REPLACE INTO sync_metadata (key, value) VALUES ('last_sync_time', ?1)",
            params![timestamp.to_rfc3339()],
        )?;

        debug!("Updated last sync time to {}", timestamp);
        Ok(())
    }

    pub async fn get_stats(&self) -> Result<(usize, Option<DateTime<Utc>>)> {
        let conn = self.conn.lock().await;

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM sync_state", [], |row| row.get(0))?;

        let last_sync = self.get_last_sync_time().await?;

        Ok((count as usize, last_sync))
    }
}
