use crate::db::error::DatabaseError;
use crate::db::models::ObjectIndex;
use sqlx::postgres::PgRow;
use sqlx::Row as _;

/// Helper macro to extract a field from a database row with error handling
#[macro_export]
macro_rules! pg_get_field {
    ($row:expr, $field:expr) => {
        $row.try_get($field)
            .map_err(|e| DatabaseError::DeserializationError(e.to_string()))?
    };
}

/// Helper macro to extract an optional field from a database row
#[macro_export]
macro_rules! pg_get_optional_field {
    ($row:expr, $field:expr) => {
        match $row.try_get($field) {
            Ok(val) => Ok(Some(val)),
            Err(sqlx::Error::ColumnNotFound(_)) => Ok(None),
            Err(e) => Err(DatabaseError::DeserializationError(e.to_string())),
        }?
    };
}

/// Helper macro to extract a field that might be TEXT or BYTEA
/// Tries to read as TEXT first and converts to bytes, falls back to BYTEA
#[macro_export]
macro_rules! pg_get_text_or_bytea_field {
    ($row:expr, $field:expr) => {
        match $row.try_get::<String, _>($field) {
            Ok(text) => Ok(Some(text.into_bytes())),
            Err(_) => match $row.try_get::<Vec<u8>, _>($field) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(sqlx::Error::ColumnNotFound(_)) => Ok(None),
                Err(e) => Err(DatabaseError::DeserializationError(format!(
                    "Failed to read {} column: {}",
                    $field, e
                ))),
            },
        }?
    };
}

/// Configuration for which schema to use
pub enum SchemaMode {
    S3,
    Direct,
}

// Internal schema definitions
struct S3Schema;
struct DirectSchema;

impl S3Schema {
    fn select_columns() -> &'static str {
        "id, object_key, competition_id, agent_id, data_type, size_bytes, metadata, event_timestamp, created_at"
    }
}

impl DirectSchema {
    fn select_columns() -> &'static str {
        "id, competition_id, agent_id, data_type, size_bytes, metadata, event_timestamp, created_at, data"
    }
}

impl SchemaMode {
    pub fn select_columns(&self) -> &'static str {
        match self {
            SchemaMode::S3 => S3Schema::select_columns(),
            SchemaMode::Direct => DirectSchema::select_columns(),
        }
    }

    /// Generate the table definition for the object_index table
    pub fn table_definition(&self, schema_name: Option<&str>) -> String {
        let enum_type = match schema_name {
            Some(schema) => format!("{}.sync_data_type", schema),
            None => "sync_data_type".to_string(),
        };

        match self {
            SchemaMode::S3 => format!(
                r#"
                id UUID PRIMARY KEY,
                object_key TEXT UNIQUE NOT NULL,
                competition_id UUID,
                agent_id UUID,
                data_type {} NOT NULL,
                size_bytes BIGINT,
                metadata JSONB,
                event_timestamp TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL
                "#,
                enum_type
            ),
            SchemaMode::Direct => format!(
                r#"
                id UUID PRIMARY KEY,
                competition_id UUID,
                agent_id UUID,
                data_type {} NOT NULL,
                size_bytes BIGINT,
                metadata JSONB,
                event_timestamp TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL,
                data BYTEA NOT NULL
                "#,
                enum_type
            ),
        }
    }

    pub fn object_from_row(&self, row: PgRow) -> Result<ObjectIndex, DatabaseError> {
        match self {
            SchemaMode::S3 => Ok(ObjectIndex {
                id: pg_get_field!(row, "id"),
                object_key: Some(pg_get_field!(row, "object_key")),
                competition_id: pg_get_optional_field!(row, "competition_id"),
                agent_id: pg_get_optional_field!(row, "agent_id"),
                data_type: pg_get_field!(row, "data_type"),
                size_bytes: pg_get_field!(row, "size_bytes"),
                metadata: pg_get_field!(row, "metadata"),
                event_timestamp: pg_get_field!(row, "event_timestamp"),
                created_at: pg_get_field!(row, "created_at"),
                data: None,
            }),
            SchemaMode::Direct => Ok(ObjectIndex {
                id: pg_get_field!(row, "id"),
                competition_id: pg_get_optional_field!(row, "competition_id"),
                agent_id: pg_get_optional_field!(row, "agent_id"),
                data_type: pg_get_field!(row, "data_type"),
                size_bytes: pg_get_field!(row, "size_bytes"),
                metadata: pg_get_field!(row, "metadata"),
                event_timestamp: pg_get_field!(row, "event_timestamp"),
                created_at: pg_get_field!(row, "created_at"),
                data: pg_get_text_or_bytea_field!(row, "data"),
                object_key: None,
            }),
        }
    }

    #[cfg(test)]
    pub fn new_insert_query(
        &self,
        object: &ObjectIndex,
        table_name: &str,
    ) -> sqlx::query::Query<'static, sqlx::Postgres, sqlx::postgres::PgArguments> {
        // Extract schema from table name if present
        let enum_cast = if table_name.contains('.') {
            let parts: Vec<&str> = table_name.split('.').collect();
            format!("{}.sync_data_type", parts[0])
        } else {
            "sync_data_type".to_string()
        };

        match self {
            SchemaMode::S3 => {
                let query_string = format!(
                    r#"
                    INSERT INTO {} (
                        id, object_key, competition_id, agent_id,
                        data_type, size_bytes, metadata,
                        event_timestamp, created_at
                    ) VALUES ($1, $2, $3, $4, $5::text::{}, $6, $7, $8, $9)
                    ON CONFLICT (object_key) DO UPDATE SET
                        competition_id = EXCLUDED.competition_id,
                        agent_id = EXCLUDED.agent_id,
                        data_type = EXCLUDED.data_type,
                        size_bytes = EXCLUDED.size_bytes,
                        metadata = EXCLUDED.metadata,
                        event_timestamp = EXCLUDED.event_timestamp,
                        created_at = EXCLUDED.created_at
                    "#,
                    table_name, enum_cast
                );

                let id = object.id;
                let object_key = object
                    .object_key
                    .clone()
                    .expect("object_key required for S3 mode");
                let competition_id = object.competition_id;
                let agent_id = object.agent_id;
                let data_type = object.data_type;
                let size_bytes = object.size_bytes;
                let metadata = object.metadata.clone();
                let event_timestamp = object.event_timestamp;
                let created_at = object.created_at;

                sqlx::query(&*Box::leak(query_string.into_boxed_str()))
                    .bind(id)
                    .bind(object_key)
                    .bind(competition_id)
                    .bind(agent_id)
                    .bind(data_type)
                    .bind(size_bytes)
                    .bind(metadata)
                    .bind(event_timestamp)
                    .bind(created_at)
            }
            SchemaMode::Direct => {
                let query_string = format!(
                    r#"
                    INSERT INTO {} (
                        id, competition_id, agent_id,
                        data_type, size_bytes, metadata,
                        event_timestamp, created_at, data
                    ) VALUES ($1, $2, $3, $4::text::{}, $5, $6, $7, $8, $9)
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
                    table_name, enum_cast
                );

                let id = object.id;
                let competition_id = object.competition_id;
                let agent_id = object.agent_id;
                let data_type = object.data_type;
                let size_bytes = object.size_bytes;
                let metadata = object.metadata.clone();
                let event_timestamp = object.event_timestamp;
                let created_at = object.created_at;
                let data = object.data.clone().expect("data required for direct mode");

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
    }
}
