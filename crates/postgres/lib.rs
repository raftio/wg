//! PostgreSQL backend for wg job queue.
//!
//! This crate provides a PostgreSQL-based storage backend for the wg job queue.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use wg_postgres::PostgresBackend;
//! use wg_core::Client;
//!
//! #[tokio::main]
//! async fn main() -> wg_core::Result<()> {
//!     let backend = PostgresBackend::new("postgres://localhost/mydb", "myapp").await?;
//!     let client = Client::new(backend);
//!     Ok(())
//! }
//! ```

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;
use wg_core::{Backend, Result, WgError};

/// PostgreSQL backend for job queue storage.
#[derive(Clone)]
pub struct PostgresBackend {
    pool: PgPool,
    namespace: String,
}

impl PostgresBackend {
    /// Create a new PostgreSQL backend.
    pub async fn new(database_url: &str, namespace: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to connect to PostgreSQL: {}", e)))?;

        let backend = Self {
            pool,
            namespace: namespace.to_string(),
        };

        // Initialize tables
        backend.init_tables().await?;

        Ok(backend)
    }

    /// Initialize the required tables.
    async fn init_tables(&self) -> Result<()> {
        let jobs_table = format!("{}_jobs", self.namespace);
        let scheduled_table = format!("{}_scheduled", self.namespace);
        let retry_table = format!("{}_retry", self.namespace);
        let dead_table = format!("{}_dead", self.namespace);

        // Create jobs table (FIFO queue)
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                job_json TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            "#,
            jobs_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create jobs table: {}", e)))?;

        // Create scheduled table
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                job_json TEXT NOT NULL,
                run_at BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            "#,
            scheduled_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create scheduled table: {}", e)))?;

        // Create index on run_at
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_run_at ON {} (run_at)",
            self.namespace, scheduled_table
        ))
        .execute(&self.pool)
        .await
        .ok(); // Ignore if already exists

        // Create retry table
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                job_json TEXT NOT NULL,
                retry_at BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            "#,
            retry_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create retry table: {}", e)))?;

        // Create index on retry_at
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_retry_at ON {} (retry_at)",
            self.namespace, retry_table
        ))
        .execute(&self.pool)
        .await
        .ok();

        // Create dead table
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                job_json TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            "#,
            dead_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create dead table: {}", e)))?;

        Ok(())
    }

    fn jobs_table(&self) -> String {
        format!("{}_jobs", self.namespace)
    }

    fn scheduled_table(&self) -> String {
        format!("{}_scheduled", self.namespace)
    }

    fn retry_table(&self) -> String {
        format!("{}_retry", self.namespace)
    }

    fn dead_table(&self) -> String {
        format!("{}_dead", self.namespace)
    }
}

#[async_trait]
impl Backend for PostgresBackend {
    async fn push_job(&self, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {} (job_json) VALUES ($1)",
            self.jobs_table()
        ))
        .bind(job_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to push job: {}", e)))?;
        Ok(())
    }

    async fn pop_job(&self, timeout: Duration) -> Result<Option<String>> {
        let deadline = tokio::time::Instant::now() + timeout;
        let poll_interval = Duration::from_millis(100);

        loop {
            // Try to pop a job using DELETE ... RETURNING
            let result: Option<(String,)> = sqlx::query_as(&format!(
                r#"
                DELETE FROM {} WHERE id = (
                    SELECT id FROM {} ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED
                ) RETURNING job_json
                "#,
                self.jobs_table(),
                self.jobs_table()
            ))
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to pop job: {}", e)))?;

            if let Some((job_json,)) = result {
                return Ok(Some(job_json));
            }

            // Check timeout
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }

            // Wait before polling again
            tokio::time::sleep(poll_interval).await;
        }
    }

    async fn schedule_job(&self, job_json: &str, run_at: i64) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {} (job_json, run_at) VALUES ($1, $2)",
            self.scheduled_table()
        ))
        .bind(job_json)
        .bind(run_at)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to schedule job: {}", e)))?;
        Ok(())
    }

    async fn get_due_scheduled(&self, now: i64, limit: usize) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(&format!(
            "SELECT job_json FROM {} WHERE run_at <= $1 ORDER BY run_at LIMIT $2",
            self.scheduled_table()
        ))
        .bind(now)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to get scheduled jobs: {}", e)))?;

        Ok(rows.into_iter().map(|(json,)| json).collect())
    }

    async fn remove_scheduled(&self, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "DELETE FROM {} WHERE job_json = $1",
            self.scheduled_table()
        ))
        .bind(job_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to remove scheduled job: {}", e)))?;
        Ok(())
    }

    async fn retry_job(&self, job_json: &str, retry_at: i64) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {} (job_json, retry_at) VALUES ($1, $2)",
            self.retry_table()
        ))
        .bind(job_json)
        .bind(retry_at)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to retry job: {}", e)))?;
        Ok(())
    }

    async fn get_due_retries(&self, now: i64, limit: usize) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(&format!(
            "SELECT job_json FROM {} WHERE retry_at <= $1 ORDER BY retry_at LIMIT $2",
            self.retry_table()
        ))
        .bind(now)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to get retry jobs: {}", e)))?;

        Ok(rows.into_iter().map(|(json,)| json).collect())
    }

    async fn remove_retry(&self, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "DELETE FROM {} WHERE job_json = $1",
            self.retry_table()
        ))
        .bind(job_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to remove retry job: {}", e)))?;
        Ok(())
    }

    async fn push_dead(&self, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {} (job_json) VALUES ($1)",
            self.dead_table()
        ))
        .bind(job_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to push dead job: {}", e)))?;
        Ok(())
    }

    async fn queue_len(&self) -> Result<usize> {
        let row: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", self.jobs_table()))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to get queue length: {}", e)))?;
        Ok(row.0 as usize)
    }

    async fn schedule_len(&self) -> Result<usize> {
        let row: (i64,) =
            sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", self.scheduled_table()))
                .fetch_one(&self.pool)
                .await
                .map_err(|e| WgError::Backend(format!("Failed to get schedule length: {}", e)))?;
        Ok(row.0 as usize)
    }

    async fn retry_len(&self) -> Result<usize> {
        let row: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", self.retry_table()))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to get retry length: {}", e)))?;
        Ok(row.0 as usize)
    }

    async fn dead_len(&self) -> Result<usize> {
        let row: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", self.dead_table()))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to get dead length: {}", e)))?;
        Ok(row.0 as usize)
    }
}
