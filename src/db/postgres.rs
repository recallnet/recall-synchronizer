use crate::db::database::Database;
use crate::db::error::DatabaseError;
use crate::db::models::ObjectIndex;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row as _};
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Macro to extract a field from a database row with error handling
macro_rules! get_field {
    ($row:expr, $field:expr) => {
        $row.try_get($field)
            .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?
    };
}

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
            .map_err(|e| {
                error!("Failed to create connection pool: {}", e);
                DatabaseError::ConnectionError(e.to_string())
            })?;

        if let Err(e) = sqlx::query("SELECT 1").execute(&pool).await {
            error!("Database connectivity test failed: {}", e);
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

        info!("PostgreSQL database connection established successfully");
        Ok(db)
    }

    /// Initialize a schema with the required tables
    async fn initialize_schema(&self, schema_name: &str) -> Result<(), DatabaseError> {
        info!("Initializing schema: {}", schema_name);

        // Create schema if it doesn't exist
        let create_schema_query = format!("CREATE SCHEMA IF NOT EXISTS {}", schema_name);
        debug!("Executing: {}", create_schema_query);
        sqlx::query(&create_schema_query)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("Failed to create schema '{}': {}", schema_name, e);
                DatabaseError::QueryError(format!("Failed to create schema: {}", e))
            })?;

        // Create the object_index table in the schema
        let create_table_query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.object_index (
                id UUID PRIMARY KEY,
                object_key TEXT UNIQUE NOT NULL,
                bucket_name VARCHAR(100) NOT NULL,
                competition_id UUID NOT NULL,
                agent_id UUID NOT NULL,
                data_type VARCHAR(50) NOT NULL,
                size_bytes BIGINT,
                metadata JSONB,
                event_timestamp TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL
            )
            "#,
            schema_name
        );

        debug!("Creating object_index table in schema '{}'", schema_name);
        sqlx::query(&create_table_query)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("Failed to create object_index table: {}", e);
                DatabaseError::QueryError(format!("Failed to create table: {}", e))
            })?;

        // Create index
        let create_index_query = format!(
            "CREATE INDEX IF NOT EXISTS object_index_created_at_idx ON {}.object_index (created_at)",
            schema_name
        );

        debug!("Creating index on created_at");
        sqlx::query(&create_index_query)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("Failed to create index: {}", e);
                DatabaseError::QueryError(format!("Failed to create index: {}", e))
            })?;

        info!(
            "Schema '{}' initialization completed successfully",
            schema_name
        );
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
        debug!("Converting database row to ObjectIndex");
        Ok(ObjectIndex {
            id: get_field!(row, "id"),
            object_key: get_field!(row, "object_key"),
            bucket_name: get_field!(row, "bucket_name"),
            competition_id: get_field!(row, "competition_id"),
            agent_id: get_field!(row, "agent_id"),
            data_type: get_field!(row, "data_type"),
            size_bytes: get_field!(row, "size_bytes"),
            metadata: get_field!(row, "metadata"),
            event_timestamp: get_field!(row, "event_timestamp"),
            created_at: get_field!(row, "created_at"),
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
        debug!(
            "Querying objects with limit={}, since={:?}, after_id={:?}, competition_id={:?}",
            limit, since, after_id, competition_id
        );

        let query_base = format!(
            r#"
            SELECT
                id, object_key, bucket_name, competition_id, agent_id,
                data_type, size_bytes, metadata,
                event_timestamp, created_at
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
                    "(created_at > ${} OR (created_at = ${} AND id > ${}))",
                    param_count,
                    param_count,
                    param_count + 1
                ));
                bind_params.push("since".to_string());
                bind_params.push("after_id".to_string());
                param_count += 2;
            }
            (Some(_), None) => {
                where_clauses.push(format!("created_at > ${}", param_count));
                bind_params.push("since".to_string());
                param_count += 1;
            }
            _ => {}
        }

        // Build final query
        let query = if where_clauses.is_empty() {
            format!(
                "{} ORDER BY created_at ASC, id ASC LIMIT ${}",
                query_base, param_count
            )
        } else {
            format!(
                "{} WHERE {} ORDER BY created_at ASC, id ASC LIMIT ${}",
                query_base,
                where_clauses.join(" AND "),
                param_count
            )
        };

        debug!("Executing query: {}", query);

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
            Ok(rows) => {
                debug!("Query returned {} rows", rows.len());
                rows
            }
            Err(e) => {
                if e.to_string().contains("does not exist") {
                    warn!("Table does not exist, returning empty result");
                    return Ok(Vec::new());
                }
                error!("Database query failed: {}", e);
                return Err(DatabaseError::QueryError(e.to_string()));
            }
        };

        // Convert rows to ObjectIndex objects
        let mut result = Vec::with_capacity(objects.len());
        for row in objects {
            result.push(self.row_to_object_index(row)?);
        }

        info!("Retrieved {} objects from database", result.len());
        Ok(result)
    }

    #[cfg(test)]
    async fn add_object(&self, object: ObjectIndex) -> Result<(), DatabaseError> {
        debug!(
            "Adding object to database: id={}, key={}",
            object.id, object.object_key
        );

        let query = format!(
            r#"
            INSERT INTO {} (
                id, object_key, bucket_name, competition_id, agent_id,
                data_type, size_bytes, metadata,
                event_timestamp, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (object_key) DO UPDATE SET
                bucket_name = EXCLUDED.bucket_name,
                competition_id = EXCLUDED.competition_id,
                agent_id = EXCLUDED.agent_id,
                data_type = EXCLUDED.data_type,
                size_bytes = EXCLUDED.size_bytes,
                metadata = EXCLUDED.metadata,
                event_timestamp = EXCLUDED.event_timestamp,
                created_at = EXCLUDED.created_at
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
            .bind(&object.metadata)
            .bind(object.event_timestamp)
            .bind(object.created_at)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("Failed to insert object: {}", e);
                DatabaseError::QueryError(e.to_string())
            })?;

        info!(
            "Successfully added object: id={}, key={}",
            object.id, object.object_key
        );
        Ok(())
    }
}
