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

    /// Create enum type with default values if it doesn't exist
    async fn create_enum_type(&self, schema_prefix: Option<&str>) -> Result<(), DatabaseError> {
        let (enum_name, debug_name) = match schema_prefix {
            Some(schema) => (
                format!("{schema}.sync_data_type"),
                format!("{schema}.sync_data_type"),
            ),
            None => ("sync_data_type".to_string(), "sync_data_type".to_string()),
        };

        let default_values = [
            "trade",
            "agent_score_history",
            "agent_score",
            "competitions_leaderboard",
            "portfolio_snapshot",
        ];

        let enum_values = default_values
            .iter()
            .map(|v| format!("'{v}'"))
            .collect::<Vec<_>>()
            .join(",\n        ");

        let create_enum_query =
            format!("CREATE TYPE {enum_name} AS ENUM (\n        {enum_values}\n    )");

        debug!("Creating {debug_name} enum with default values");

        match sqlx::query(&create_enum_query).execute(&self.pool).await {
            Ok(_) => {
                debug!("Successfully created {debug_name} enum");
                Ok(())
            }
            Err(e) => {
                let error_str = e.to_string();
                if error_str.contains("already exists")
                    || error_str.contains("duplicate key value")
                    || error_str.contains("pg_type_typname_nsp_index")
                {
                    debug!("{debug_name} enum already exists");
                    Ok(())
                } else {
                    error!("Failed to create {debug_name} enum: {e}");
                    Err(DatabaseError::QueryError(format!(
                        "Failed to create enum type: {e}"
                    )))
                }
            }
        }
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
                error!("Failed to create connection pool: {e}");
                DatabaseError::ConnectionError(e.to_string())
            })?;

        if let Err(e) = sqlx::query("SELECT 1").execute(&pool).await {
            error!("Database connectivity test failed: {e}");
            return Err(DatabaseError::ConnectionError(format!(
                "Database is not accessible: {e}"
            )));
        };

        let db = PostgresDatabase { pool, schema, mode };

        if let Some(ref schema_name) = db.schema {
            db.initialize_schema(schema_name).await?;
        }

        info!("PostgreSQL database connection established successfully");
        Ok(db)
    }

    /// Initialize a schema with the required tables
    async fn initialize_schema(&self, schema_name: &str) -> Result<(), DatabaseError> {
        info!("Initializing schema: {schema_name}");

        // First ensure the enum exists in public schema (needed for type casting)
        self.create_enum_type(None).await?;

        // Create schema if it doesn't exist
        let create_schema_query = format!("CREATE SCHEMA IF NOT EXISTS {schema_name}");
        debug!("Executing: {create_schema_query}");
        sqlx::query(&create_schema_query)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("Failed to create schema '{schema_name}': {e}");
                DatabaseError::QueryError(format!("Failed to create schema: {e}"))
            })?;

        // Create the enum type in the schema if it doesn't exist
        self.create_enum_type(Some(schema_name)).await?;

        // Create the object_index table in the schema
        let table_definition = self.mode.table_definition(Some(schema_name));

        let create_table_query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema_name}.object_index ({table_definition})
            "#
        );

        debug!("Creating object_index table in schema '{schema_name}'");
        sqlx::query(&create_table_query)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("Failed to create object_index table: {e}");
                DatabaseError::QueryError(format!("Failed to create table: {e}"))
            })?;

        // Create index on created_at
        let create_index_query = format!(
            "CREATE INDEX IF NOT EXISTS object_index_created_at_idx ON {schema_name}.object_index (created_at)"
        );

        debug!("Creating index on created_at");
        sqlx::query(&create_index_query)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!("Failed to create index: {e}");
                DatabaseError::QueryError(format!("Failed to create index: {e}"))
            })?;

        info!("Schema '{schema_name}' initialization completed successfully");
        Ok(())
    }

    /// Get the table name with schema prefix if applicable
    fn table_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{schema}.object_index"),
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
        debug!("Querying objects with limit={limit}, since={since:?}, after_id={after_id:?}");

        let select_columns = self.mode.select_columns();
        let table_name = self.table_name();
        let query_base = format!(
            r#"
            SELECT
                {select_columns}
            FROM {table_name}
            "#
        );

        // Build WHERE clause dynamically based on provided filters
        let mut where_clauses = Vec::new();
        let mut bind_params: Vec<String> = Vec::new();
        let mut param_count = 1;

        // Timestamp and after_id filters
        match (since, after_id) {
            (Some(_), Some(_)) => {
                let next_param = param_count + 1;
                where_clauses.push(format!(
                    "(created_at > ${param_count} OR (created_at = ${param_count} AND id > ${next_param}))"
                ));
                bind_params.push("since".to_string());
                bind_params.push("after_id".to_string());
                param_count += 2;
            }
            (Some(_), None) => {
                where_clauses.push(format!("created_at > ${param_count}"));
                bind_params.push("since".to_string());
                param_count += 1;
            }
            _ => {}
        }

        // Build final query
        let query = if where_clauses.is_empty() {
            format!("{query_base} ORDER BY created_at ASC, id ASC LIMIT ${param_count}")
        } else {
            let where_clause = where_clauses.join(" AND ");
            format!(
                "{query_base} WHERE {where_clause} ORDER BY created_at ASC, id ASC LIMIT ${param_count}"
            )
        };

        debug!("Executing query: {query}");

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
                let row_count = rows.len();
                debug!("Query returned {row_count} rows");
                rows
            }
            Err(e) => {
                if e.to_string().contains("does not exist") {
                    warn!("Table does not exist, returning empty result");
                    return Ok(Vec::new());
                }
                error!("Database query failed: {e}");
                return Err(DatabaseError::QueryError(e.to_string()));
            }
        };

        // Convert rows to objects
        let mut result = Vec::with_capacity(rows.len());
        for row in rows {
            result.push(self.mode.object_from_row(row)?);
        }

        let object_count = result.len();
        info!("Retrieved {object_count} objects from database");
        Ok(result)
    }

    #[cfg(test)]
    async fn add_object(&self, object: ObjectIndex) -> Result<(), DatabaseError> {
        debug!("Adding object to database");

        let query = self.mode.new_insert_query(&object, &self.table_name());

        query.execute(&self.pool).await.map_err(|e| {
            error!("Failed to insert object: {e}");
            DatabaseError::QueryError(e.to_string())
        })?;

        info!("Successfully added object");
        Ok(())
    }
}
