use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use sqlx::types::chrono::{DateTime, Utc};
use std::path::Path;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

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

        let conn = Connection::open(path).context(format!(
            "Failed to open state database at {}",
            path.display()
        ))?;

        // Initialize schema
        conn.execute(
            "CREATE TABLE IF NOT EXISTS sync_state (
                key TEXT PRIMARY KEY,
                cid TEXT NOT NULL,
                synced_at TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 1
            )",
            [],
        )
        .context("Failed to create sync_state table")?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS sync_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        )
        .context("Failed to create sync_metadata table")?;

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
            .optional()
            .context(format!("Failed to check if key is processed: {}", key))?;

        Ok(exists.is_some())
    }

    pub async fn mark_processed(
        &self,
        key: &str,
        cid: &str,
        timestamp: DateTime<Utc>,
    ) -> Result<()> {
        let conn = self.conn.lock().await;

        conn.execute(
            "INSERT OR REPLACE INTO sync_state (key, cid, synced_at, attempts) 
             VALUES (?1, ?2, ?3, COALESCE((SELECT attempts + 1 FROM sync_state WHERE key = ?1), 1))",
            params![key, cid, timestamp.to_rfc3339()],
        ).context(format!("Failed to mark key as processed: {}", key))?;

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
            .optional()
            .context("Failed to get last sync time")?;

        match timestamp {
            Some(ts) => {
                let datetime = DateTime::parse_from_rfc3339(&ts)
                    .context(format!("Failed to parse timestamp: {}", ts))?
                    .with_timezone(&Utc);
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
        )
        .context("Failed to update last sync time")?;

        debug!("Updated last sync time to {}", timestamp);
        Ok(())
    }

    pub async fn get_stats(&self) -> Result<(usize, Option<DateTime<Utc>>)> {
        let conn = self.conn.lock().await;

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sync_state", [], |row| row.get(0))
            .context("Failed to get sync state count")?;

        let last_sync = self.get_last_sync_time().await?;

        Ok((count as usize, last_sync))
    }

    pub async fn get_failed_jobs(&self, limit: usize) -> Result<Vec<String>> {
        let conn = self.conn.lock().await;

        let mut stmt = conn
            .prepare(
                "SELECT key FROM sync_state WHERE attempts > 1 ORDER BY attempts DESC LIMIT ?1",
            )
            .context("Failed to prepare query for failed jobs")?;

        let rows = stmt
            .query_map(params![limit as i64], |row| {
                let key: String = row.get(0)?;
                Ok(key)
            })
            .context("Failed to query failed jobs")?;

        let mut keys = Vec::new();
        for row in rows {
            keys.push(row.context("Failed to read failed job key")?);
        }

        Ok(keys)
    }
}
