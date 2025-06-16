use crate::db::error::DatabaseError;
use sqlx::postgres::PgRow;
use sqlx::Row as _;

/// Trait for objects that can be mapped to/from PostgreSQL rows
pub trait PgSchema: Sized + Send + Sync + Clone {
    /// Get the schema definition for this object type
    fn schema_definition() -> &'static str;

    /// Get the column names for SELECT queries
    fn select_columns() -> &'static str;

    /// Convert a PostgreSQL row to this object type
    fn from_row(row: PgRow) -> Result<Self, DatabaseError>;

    /// Create a new INSERT query with bindings for test usage
    /// Returns a query with all values bound
    #[cfg(test)]
    fn new_insert_query(
        &self,
        table_name: &str,
    ) -> sqlx::query::Query<'static, sqlx::Postgres, sqlx::postgres::PgArguments>;
}

/// Helper macro to extract a field from a database row with error handling
#[macro_export]
macro_rules! pg_get_field {
    ($row:expr, $field:expr) => {
        $row.try_get($field)
            .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?
    };
}

/// Implementation for ObjectIndex (S3 storage)
impl PgSchema for crate::db::ObjectIndex {
    fn schema_definition() -> &'static str {
        r#"
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
        "#
    }

    fn select_columns() -> &'static str {
        "id, object_key, bucket_name, competition_id, agent_id, data_type, size_bytes, metadata, event_timestamp, created_at"
    }

    fn from_row(row: PgRow) -> Result<Self, DatabaseError> {
        Ok(Self {
            id: pg_get_field!(row, "id"),
            object_key: pg_get_field!(row, "object_key"),
            bucket_name: pg_get_field!(row, "bucket_name"),
            competition_id: pg_get_field!(row, "competition_id"),
            agent_id: pg_get_field!(row, "agent_id"),
            data_type: pg_get_field!(row, "data_type"),
            size_bytes: pg_get_field!(row, "size_bytes"),
            metadata: pg_get_field!(row, "metadata"),
            event_timestamp: pg_get_field!(row, "event_timestamp"),
            created_at: pg_get_field!(row, "created_at"),
        })
    }

    #[cfg(test)]
    fn new_insert_query(
        &self,
        table_name: &str,
    ) -> sqlx::query::Query<'static, sqlx::Postgres, sqlx::postgres::PgArguments> {
        let query_string = format!(
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
            table_name
        );

        // Clone the values we need to move into the query
        let id = self.id;
        let object_key = self.object_key.clone();
        let bucket_name = self.bucket_name.clone();
        let competition_id = self.competition_id;
        let agent_id = self.agent_id;
        let data_type = self.data_type.clone();
        let size_bytes = self.size_bytes;
        let metadata = self.metadata.clone();
        let event_timestamp = self.event_timestamp;
        let created_at = self.created_at;

        sqlx::query(&*Box::leak(query_string.into_boxed_str()))
            .bind(id)
            .bind(object_key)
            .bind(bucket_name)
            .bind(competition_id)
            .bind(agent_id)
            .bind(data_type)
            .bind(size_bytes)
            .bind(metadata)
            .bind(event_timestamp)
            .bind(created_at)
    }
}

/// Implementation for ObjectIndexDirect (direct database storage)
impl PgSchema for crate::db::ObjectIndexDirect {
    fn schema_definition() -> &'static str {
        r#"
        id UUID PRIMARY KEY,
        competition_id UUID NOT NULL,
        agent_id UUID NOT NULL,
        data_type VARCHAR(50) NOT NULL,
        size_bytes BIGINT,
        metadata JSONB,
        event_timestamp TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL,
        data BYTEA NOT NULL
        "#
    }

    fn select_columns() -> &'static str {
        "id, competition_id, agent_id, data_type, size_bytes, metadata, event_timestamp, created_at, data"
    }

    fn from_row(row: PgRow) -> Result<Self, DatabaseError> {
        Ok(Self {
            id: pg_get_field!(row, "id"),
            competition_id: pg_get_field!(row, "competition_id"),
            agent_id: pg_get_field!(row, "agent_id"),
            data_type: pg_get_field!(row, "data_type"),
            size_bytes: pg_get_field!(row, "size_bytes"),
            metadata: pg_get_field!(row, "metadata"),
            event_timestamp: pg_get_field!(row, "event_timestamp"),
            created_at: pg_get_field!(row, "created_at"),
            data: pg_get_field!(row, "data"),
        })
    }

    #[cfg(test)]
    fn new_insert_query(
        &self,
        table_name: &str,
    ) -> sqlx::query::Query<'static, sqlx::Postgres, sqlx::postgres::PgArguments> {
        let query_string = format!(
            r#"
            INSERT INTO {} (
                id, competition_id, agent_id,
                data_type, size_bytes, metadata,
                event_timestamp, created_at, data
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (id) DO UPDATE SET
                competition_id = EXCLUDED.competition_id,
                agent_id = EXCLUDED.agent_id,
                data_type = EXCLUDED.data_type,
                size_bytes = EXCLUDED.size_bytes,
                metadata = EXCLUDED.metadata,
                event_timestamp = EXCLUDED.event_timestamp,
                created_at = EXCLUDED.created_at,
                data = EXCLUDED.data
            "#,
            table_name
        );

        // Clone the values we need to move into the query
        let id = self.id;
        let competition_id = self.competition_id;
        let agent_id = self.agent_id;
        let data_type = self.data_type.clone();
        let size_bytes = self.size_bytes;
        let metadata = self.metadata.clone();
        let event_timestamp = self.event_timestamp;
        let created_at = self.created_at;
        let data = self.data.clone();

        sqlx::query(&*Box::leak(query_string.into_boxed_str()))
            .bind(id)
            .bind(competition_id)
            .bind(agent_id)
            .bind(data_type)
            .bind(size_bytes)
            .bind(metadata)
            .bind(event_timestamp)
            .bind(created_at)
            .bind(data)
    }
}
