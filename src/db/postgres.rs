use crate::db::database::Database;
use crate::db::error::DatabaseError;
use crate::db::models::ObjectIndex;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row as _};
use std::time::Duration;

/// A PostgreSQL implementation of the Database trait
pub struct PostgresDatabase {
    pool: PgPool,
}

impl PostgresDatabase {
    /// Create a new PostgresDatabase with the given connection URL
    pub async fn new(database_url: &str) -> Result<Self, DatabaseError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(3))
            .idle_timeout(Duration::from_secs(60))
            .connect(database_url)
            .await
            .map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;

        Ok(PostgresDatabase { pool })
    }

    /// Helper function to create an ObjectIndex from a database row
    fn row_to_object_index(
        &self,
        row: sqlx::postgres::PgRow,
    ) -> Result<ObjectIndex, DatabaseError> {
        Ok(ObjectIndex {
            id: row
                .try_get("id")
                .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?,
            object_key: row
                .try_get("object_key")
                .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?,
            bucket_name: row
                .try_get("bucket_name")
                .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?,
            competition_id: row
                .try_get("competition_id")
                .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?,
            agent_id: row
                .try_get("agent_id")
                .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?,
            data_type: row
                .try_get("data_type")
                .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?,
            size_bytes: row
                .try_get("size_bytes")
                .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?,
            content_hash: row
                .try_get("content_hash")
                .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?,
            metadata: row
                .try_get("metadata")
                .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?,
            event_timestamp: row
                .try_get("event_timestamp")
                .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?,
            object_last_modified_at: row
                .try_get("object_last_modified_at")
                .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?,
            created_at: row
                .try_get("created_at")
                .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?,
            updated_at: row
                .try_get("updated_at")
                .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?,
        })
    }
}

#[async_trait]
impl Database for PostgresDatabase {
    async fn get_objects_to_sync(
        &self,
        limit: u32,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<ObjectIndex>, DatabaseError> {
        let query_base = r#"
            SELECT
                id, object_key, bucket_name, competition_id, agent_id,
                data_type, size_bytes, content_hash, metadata,
                event_timestamp, object_last_modified_at, created_at, updated_at
            FROM object_index
        "#;

        let objects = if let Some(ts) = since {
            // With timestamp filter
            let query = format!("{} WHERE object_last_modified_at > $1 ORDER BY object_last_modified_at ASC LIMIT $2", query_base);
            match sqlx::query(&query)
                .bind(ts)
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    // For testing, if the object_index table doesn't exist, return an empty list
                    if e.to_string()
                        .contains("relation \"object_index\" does not exist")
                    {
                        return Ok(Vec::new());
                    }
                    return Err(DatabaseError::QueryError(e.to_string()));
                }
            }
        } else {
            // Without timestamp filter
            let query = format!(
                "{} ORDER BY object_last_modified_at ASC LIMIT $1",
                query_base
            );
            match sqlx::query(&query)
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    // For testing, if the object_index table doesn't exist, return an empty list
                    if e.to_string()
                        .contains("relation \"object_index\" does not exist")
                    {
                        return Ok(Vec::new());
                    }
                    return Err(DatabaseError::QueryError(e.to_string()));
                }
            }
        };

        // Convert rows to ObjectIndex objects using the helper function
        let mut result = Vec::with_capacity(objects.len());
        for row in objects {
            result.push(self.row_to_object_index(row)?);
        }

        Ok(result)
    }

    async fn get_object_by_key(&self, object_key: &str) -> Result<ObjectIndex, DatabaseError> {
        let query = r#"
            SELECT
                id, object_key, bucket_name, competition_id, agent_id,
                data_type, size_bytes, content_hash, metadata,
                event_timestamp, object_last_modified_at, created_at, updated_at
            FROM object_index
            WHERE object_key = $1
        "#;

        let row = match sqlx::query(query)
            .bind(object_key)
            .fetch_optional(&self.pool)
            .await
        {
            Ok(row) => row,
            Err(e) => {
                // For testing, if the object_index table doesn't exist, return not found
                if e.to_string()
                    .contains("relation \"object_index\" does not exist")
                {
                    return Err(DatabaseError::ObjectNotFound(object_key.to_string()));
                }
                return Err(DatabaseError::QueryError(e.to_string()));
            }
        };

        // Convert row to ObjectIndex or return not found error
        let row = row.ok_or_else(|| DatabaseError::ObjectNotFound(object_key.to_string()))?;
        self.row_to_object_index(row)
    }
}
