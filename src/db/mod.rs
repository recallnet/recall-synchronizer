use anyhow::Result;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::types::chrono::{DateTime, Utc};
use tracing::{debug, info};

use crate::config::DatabaseConfig;

pub struct DbConnector {
    pool: PgPool,
}

#[derive(Debug)]
pub struct ObjectInfo {
    pub key: String,
    pub updated_at: DateTime<Utc>,
    pub competition_id: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

impl DbConnector {
    pub async fn new(config: &DatabaseConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.url)
            .await?;

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
            "SELECT key, updated_at, competition_id, metadata FROM object_store_index WHERE 1=1 ",
        );

        if let Some(since_time) = since {
            query.push(" AND updated_at > ");
            query.push_bind(since_time);
        }

        if let Some(comp_id) = competition_id {
            query.push(" AND competition_id = ");
            query.push_bind(comp_id);
        }

        query.push(" ORDER BY updated_at ASC LIMIT ");
        query.push_bind(limit as i64);

        let query = query.build_query_as::<ObjectInfo>();
        let objects = query.fetch_all(&self.pool).await?;

        debug!("Found {} objects to sync", objects.len());
        Ok(objects)
    }

    pub async fn get_latest_updated_timestamp(&self) -> Result<Option<DateTime<Utc>>> {
        let record = sqlx::query!("SELECT MAX(updated_at) as latest FROM object_store_index")
            .fetch_one(&self.pool)
            .await?;

        Ok(record.latest)
    }
}
