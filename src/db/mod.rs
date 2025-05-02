use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

use self::models::ObjectInfo;
use crate::config::DatabaseConfig;

pub mod models;

pub struct DbConnector {
    pool: PgPool,
}

impl DbConnector {
    pub async fn new(config: &DatabaseConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.url)
            .await
            .context("Failed to connect to PostgreSQL database")?;

        // Test connection
        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .context("Failed to execute test query on PostgreSQL database")?;

        info!("Connected to PostgreSQL database");
        Ok(Self { pool })
    }

    pub async fn get_updated_objects(
        &self,
        since: Option<DateTime<Utc>>,
        competition_id: Option<String>,
        limit: usize,
    ) -> Result<Vec<ObjectInfo>> {
        let mut query = sqlx::QueryBuilder::new(
            "SELECT key, updated_at, competition_id, metadata, size_bytes FROM object_store_index WHERE 1=1 "
        );

        if let Some(since_time) = since {
            query.push(" AND updated_at > ");
            query.push_bind(since_time);
        }

        if let Some(comp_id) = &competition_id {
            query.push(" AND competition_id = ");
            query.push_bind(comp_id);
        }

        query.push(" ORDER BY updated_at ASC LIMIT ");
        query.push_bind(limit as i64);

        let query = query.build_query_as();
        let objects = query
            .fetch_all(&self.pool)
            .await
            .context("Failed to fetch updated objects from database")?;

        debug!("Found {} objects to sync", objects.len());
        Ok(objects)
    }

    pub async fn get_latest_updated_timestamp(&self) -> Result<Option<DateTime<Utc>>> {
        let record = sqlx::query!("SELECT MAX(updated_at) as latest FROM object_store_index")
            .fetch_one(&self.pool)
            .await
            .context("Failed to fetch latest updated timestamp")?;

        Ok(record.latest)
    }
}
