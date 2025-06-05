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
    schema: Option<String>,
}

impl PostgresDatabase {
    /// Create a new PostgresDatabase with the given connection URL
    pub async fn new(database_url: &str) -> Result<Self, DatabaseError> {
        Self::new_with_schema(database_url, None).await
    }

    /// Create a new PostgresDatabase with a specific schema
    pub async fn new_with_schema(
        database_url: &str,
        schema: Option<String>,
    ) -> Result<Self, DatabaseError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .acquire_timeout(Duration::from_secs(10))
            .idle_timeout(Duration::from_secs(60))
            .connect_lazy(database_url)
            .map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;

        if let Err(e) = sqlx::query("SELECT 1").execute(&pool).await {
            return Err(DatabaseError::ConnectionError(format!(
                "Database is not accessible: {}",
                e
            )));
        };

        let db = PostgresDatabase { pool, schema };

        // If a schema is specified, create it and the tables
        if let Some(ref schema_name) = db.schema {
            db.initialize_schema(schema_name).await?;
        }

        Ok(db)
    }

    /// Initialize a schema with the required tables
    async fn initialize_schema(&self, schema_name: &str) -> Result<(), DatabaseError> {
        // Create schema if it doesn't exist
        let create_schema_query = format!("CREATE SCHEMA IF NOT EXISTS {}", schema_name);
        sqlx::query(&create_schema_query)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to create schema: {}", e)))?;

        // Create the object_index table in the schema
        let create_table_query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.object_index (
                id UUID PRIMARY KEY,
                object_key TEXT UNIQUE NOT NULL,
                bucket_name VARCHAR(100) NOT NULL,
                competition_id UUID,
                agent_id UUID,
                data_type VARCHAR(50) NOT NULL,
                size_bytes BIGINT,
                content_hash VARCHAR(128),
                metadata JSONB,
                event_timestamp TIMESTAMPTZ,
                object_last_modified_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            "#,
            schema_name
        );

        sqlx::query(&create_table_query)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to create table: {}", e)))?;

        // Create index
        let create_index_query = format!(
            "CREATE INDEX IF NOT EXISTS object_index_last_modified_idx ON {}.object_index (object_last_modified_at)",
            schema_name
        );

        sqlx::query(&create_index_query)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(format!("Failed to create index: {}", e)))?;

        Ok(())
    }

    /// Get the table name with schema prefix if applicable
    fn table_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.object_index", schema),
            None => "object_index".to_string(),
        }
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
    async fn get_objects(
        &self,
        limit: u32,
        since: Option<DateTime<Utc>>,
        after_id: Option<uuid::Uuid>,
        competition_id: Option<uuid::Uuid>,
    ) -> Result<Vec<ObjectIndex>, DatabaseError> {
        let query_base = format!(
            r#"
            SELECT
                id, object_key, bucket_name, competition_id, agent_id,
                data_type, size_bytes, content_hash, metadata,
                event_timestamp, object_last_modified_at, created_at, updated_at
            FROM {}
            "#,
            self.table_name()
        );

        // Build WHERE clause dynamically based on provided filters
        let mut where_clauses = Vec::new();
        let mut bind_params: Vec<String> = Vec::new();
        let mut param_count = 1;

        // Competition ID filter
        if competition_id.is_some() {
            where_clauses.push(format!("competition_id = ${}", param_count));
            bind_params.push("competition_id".to_string());
            param_count += 1;
        }

        // Timestamp and after_id filters
        match (since, after_id) {
            (Some(_), Some(_)) => {
                where_clauses.push(format!(
                    "(object_last_modified_at > ${} OR (object_last_modified_at = ${} AND id > ${}))",
                    param_count,
                    param_count,
                    param_count + 1
                ));
                bind_params.push("since".to_string());
                bind_params.push("after_id".to_string());
                param_count += 2;
            }
            (Some(_), None) => {
                where_clauses.push(format!("object_last_modified_at > ${}", param_count));
                bind_params.push("since".to_string());
                param_count += 1;
            }
            _ => {}
        }

        // Build final query
        let query = if where_clauses.is_empty() {
            format!(
                "{} ORDER BY object_last_modified_at ASC, id ASC LIMIT ${}",
                query_base, param_count
            )
        } else {
            format!(
                "{} WHERE {} ORDER BY object_last_modified_at ASC, id ASC LIMIT ${}",
                query_base,
                where_clauses.join(" AND "),
                param_count
            )
        };

        // Execute query with appropriate bindings
        let mut query_builder = sqlx::query(&query);

        // Bind parameters in the correct order
        for param in &bind_params {
            match param.as_str() {
                "competition_id" => {
                    query_builder = query_builder.bind(competition_id.unwrap());
                }
                "since" => {
                    query_builder = query_builder.bind(since.unwrap());
                }
                "after_id" => {
                    query_builder = query_builder.bind(after_id.unwrap());
                }
                _ => {}
            }
        }

        query_builder = query_builder.bind(i64::from(limit));

        let objects = match query_builder.fetch_all(&self.pool).await {
            Ok(rows) => rows,
            Err(e) => {
                if e.to_string().contains("does not exist") {
                    return Ok(Vec::new());
                }
                return Err(DatabaseError::QueryError(e.to_string()));
            }
        };

        // Convert rows to ObjectIndex objects
        let mut result = Vec::with_capacity(objects.len());
        for row in objects {
            result.push(self.row_to_object_index(row)?);
        }

        Ok(result)
    }

    #[cfg(test)]
    async fn get_object_by_key(&self, object_key: &str) -> Result<ObjectIndex, DatabaseError> {
        let query = format!(
            r#"
            SELECT
                id, object_key, bucket_name, competition_id, agent_id,
                data_type, size_bytes, content_hash, metadata,
                event_timestamp, object_last_modified_at, created_at, updated_at
            FROM {}
            WHERE object_key = $1
            "#,
            self.table_name()
        );

        let row = match sqlx::query(&query)
            .bind(object_key)
            .fetch_optional(&self.pool)
            .await
        {
            Ok(row) => row,
            Err(e) => {
                // For testing, if the table doesn't exist, return not found
                if e.to_string().contains("does not exist") {
                    return Err(DatabaseError::ObjectNotFound(object_key.to_string()));
                }
                return Err(DatabaseError::QueryError(e.to_string()));
            }
        };

        // Convert row to ObjectIndex or return not found error
        let row = row.ok_or_else(|| DatabaseError::ObjectNotFound(object_key.to_string()))?;
        self.row_to_object_index(row)
    }

    #[cfg(test)]
    async fn add_object(&self, object: ObjectIndex) -> Result<(), DatabaseError> {
        let query = format!(
            r#"
            INSERT INTO {} (
                id, object_key, bucket_name, competition_id, agent_id,
                data_type, size_bytes, content_hash, metadata,
                event_timestamp, object_last_modified_at, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (object_key) DO UPDATE SET
                bucket_name = EXCLUDED.bucket_name,
                competition_id = EXCLUDED.competition_id,
                agent_id = EXCLUDED.agent_id,
                data_type = EXCLUDED.data_type,
                size_bytes = EXCLUDED.size_bytes,
                content_hash = EXCLUDED.content_hash,
                metadata = EXCLUDED.metadata,
                event_timestamp = EXCLUDED.event_timestamp,
                object_last_modified_at = EXCLUDED.object_last_modified_at,
                updated_at = EXCLUDED.updated_at
            "#,
            self.table_name()
        );

        sqlx::query(&query)
            .bind(object.id)
            .bind(&object.object_key)
            .bind(&object.bucket_name)
            .bind(object.competition_id)
            .bind(object.agent_id)
            .bind(&object.data_type)
            .bind(object.size_bytes)
            .bind(&object.content_hash)
            .bind(&object.metadata)
            .bind(object.event_timestamp)
            .bind(object.object_last_modified_at)
            .bind(object.created_at)
            .bind(object.updated_at)
            .execute(&self.pool)
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(())
    }

    #[cfg(test)]
    async fn clear_data(&self) -> Result<(), DatabaseError> {
        let query = format!("DELETE FROM {}", self.table_name());
        sqlx::query(&query).execute(&self.pool).await.map_err(|e| {
            if e.to_string().contains("does not exist") {
                // Table doesn't exist yet, which is fine for tests
                return DatabaseError::Other(anyhow::anyhow!("Table not initialized"));
            }
            DatabaseError::Other(e.into())
        })?;

        Ok(())
    }
}
