use crate::db::database::Database;
use crate::db::error::DatabaseError;
use crate::db::models::ObjectIndex;
use crate::db::pg_schema::SchemaMode;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// A PostgreSQL implementation of the Database trait
pub struct PostgresDatabase {
    pool: PgPool,
    schema: Option<String>,
    mode: SchemaMode,
}

impl PostgresDatabase {
    /// Create a new PostgresDatabase with the given connection URL and mode
    pub async fn new(database_url: &str, mode: SchemaMode) -> Result<Self, DatabaseError> {
        Self::new_with_schema(database_url, None, mode).await
    }

    /// Create a new PostgresDatabase with a specific schema
    pub async fn new_with_schema(
        database_url: &str,
        schema: Option<String>,
        mode: SchemaMode,
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

        let db = PostgresDatabase { pool, schema, mode };

        // If a schema is specified, create it and the tables
        if let Some(ref schema_name) = db.schema {
            db.initialize_schema(schema_name).await?;
        } else {
            // Create enum in public schema if no specific schema is provided
            db.ensure_enum_exists().await?;
        }

        info!("PostgreSQL database connection established successfully");
        Ok(db)
    }

    /// Ensure the enum type exists in the public schema
    async fn ensure_enum_exists(&self) -> Result<(), DatabaseError> {
        let create_enum_query = r#"
            DO $$ BEGIN
                CREATE TYPE sync_data_type AS ENUM (
                    'trade',
                    'agent_rank_history',
                    'agent_rank',
                    'competitions_leaderboard',
                    'portfolio_snapshot'
                );
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
        "#;

        debug!("Ensuring sync_data_type enum exists in public schema");
        sqlx::query(create_enum_query)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("Failed to create sync_data_type enum: {}", e);
                DatabaseError::QueryError(format!("Failed to create enum type: {}", e))
            })?;

        Ok(())
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

        // Create the enum type in the schema if it doesn't exist
        let create_enum_query = format!(
            r#"
            DO $$ BEGIN
                CREATE TYPE {}.sync_data_type AS ENUM (
                    'trade',
                    'agent_rank_history',
                    'agent_rank',
                    'competitions_leaderboard',
                    'portfolio_snapshot'
                );
            EXCEPTION
                WHEN duplicate_object THEN null;
            END $$;
            "#,
            schema_name
        );

        debug!("Creating sync_data_type enum");
        sqlx::query(&create_enum_query)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("Failed to create sync_data_type enum: {}", e);
                DatabaseError::QueryError(format!("Failed to create enum type: {}", e))
            })?;

        // Create the object_index table in the schema
        let table_definition = self.mode.table_definition(Some(schema_name));

        let create_table_query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.object_index ({})
            "#,
            schema_name,
            table_definition
        );

        debug!("Creating object_index table in schema '{}'", schema_name);
        sqlx::query(&create_table_query)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("Failed to create object_index table: {}", e);
                DatabaseError::QueryError(format!("Failed to create table: {}", e))
            })?;

        // Create index on created_at
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
}

#[async_trait]
impl Database for PostgresDatabase {
    async fn get_objects(
        &self,
        limit: u32,
        since: Option<DateTime<Utc>>,
        after_id: Option<uuid::Uuid>,
    ) -> Result<Vec<ObjectIndex>, DatabaseError> {
        debug!(
            "Querying objects with limit={}, since={:?}, after_id={:?}",
            limit, since, after_id
        );

        let query_base = format!(
            r#"
            SELECT
                {}
            FROM {}
            "#,
            self.mode.select_columns(),
            self.table_name()
        );

        // Build WHERE clause dynamically based on provided filters
        let mut where_clauses = Vec::new();
        let mut bind_params: Vec<String> = Vec::new();
        let mut param_count = 1;

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

        let rows = match query_builder.fetch_all(&self.pool).await {
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

        // Convert rows to objects
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            result.push(self.mode.object_from_row(row)?);
        }

        info!("Retrieved {} objects from database", result.len());
        Ok(result)
    }

    #[cfg(test)]
    async fn add_object(&self, object: ObjectIndex) -> Result<(), DatabaseError> {
        debug!("Adding object to database");

        let query = self.mode.new_insert_query(&object, &self.table_name());

        query.execute(&self.pool).await.map_err(|e| {
            error!("Failed to insert object: {}", e);
            DatabaseError::QueryError(e.to_string())
        })?;

        info!("Successfully added object");
        Ok(())
    }
}
