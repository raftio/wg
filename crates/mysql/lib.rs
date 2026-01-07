//! MySQL backend for wg job queue.
//!
//! This crate provides a MySQL-based storage backend for the wg job queue.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use wg_mysql::MySqlBackend;
//! use wg_core::Client;
//!
//! #[tokio::main]
//! async fn main() -> wg_core::Result<()> {
//!     let backend = MySqlBackend::new("mysql://root@localhost/mydb", "myapp").await?;
//!     let client = Client::new(backend);
//!     Ok(())
//! }
//! ```

use async_trait::async_trait;
use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use std::time::Duration;
use wg_core::{Backend, Result, WgError, WorkerPoolInfo};

/// MySQL backend for job queue storage.
#[derive(Clone)]
pub struct MySqlBackend {
    pool: MySqlPool,
    namespace: String,
}

impl MySqlBackend {
    /// Create a new MySQL backend.
    pub async fn new(database_url: &str, namespace: &str) -> Result<Self> {
        let pool = MySqlPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to connect to MySQL: {}", e)))?;

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
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                job_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                job_json TEXT NOT NULL,
                run_at BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_run_at (run_at)
            )
            "#,
            scheduled_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create scheduled table: {}", e)))?;

        // Create retry table
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                job_json TEXT NOT NULL,
                retry_at BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_retry_at (retry_at)
            )
            "#,
            retry_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create retry table: {}", e)))?;

        // Create dead table
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                job_json TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            "#,
            dead_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create dead table: {}", e)))?;

        // Create worker pools table for heartbeat monitoring
        let worker_pools_table = format!("{}_worker_pools", self.namespace);
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                pool_id VARCHAR(255) PRIMARY KEY,
                heartbeat_at BIGINT NOT NULL,
                started_at BIGINT NOT NULL,
                concurrency INT NOT NULL,
                host VARCHAR(255) NOT NULL,
                pid INT NOT NULL,
                job_names TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_heartbeat_at (heartbeat_at)
            )
            "#,
            worker_pools_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create worker_pools table: {}", e)))?;

        // Create in_progress table for tracking jobs being processed
        let in_progress_table = format!("{}_in_progress", self.namespace);
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                pool_id VARCHAR(255) NOT NULL,
                job_json TEXT NOT NULL,
                started_at BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_pool_id (pool_id)
            )
            "#,
            in_progress_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create in_progress table: {}", e)))?;

        // Create concurrency table for job-level concurrency control
        let concurrency_table = format!("{}_concurrency", self.namespace);
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                job_name VARCHAR(255) PRIMARY KEY,
                max_concurrency INT NOT NULL DEFAULT 0,
                inflight INT NOT NULL DEFAULT 0
            )
            "#,
            concurrency_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create concurrency table: {}", e)))?;

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

    fn worker_pools_table(&self) -> String {
        format!("{}_worker_pools", self.namespace)
    }

    fn in_progress_table(&self) -> String {
        format!("{}_in_progress", self.namespace)
    }

    fn concurrency_table(&self) -> String {
        format!("{}_concurrency", self.namespace)
    }
}

#[async_trait]
impl Backend for MySqlBackend {
    async fn push_job(&self, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {} (job_json) VALUES (?)",
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
            // MySQL doesn't support SKIP LOCKED in older versions, use transaction
            let mut tx = self
                .pool
                .begin()
                .await
                .map_err(|e| WgError::Backend(format!("Failed to begin transaction: {}", e)))?;

            // Select the oldest job
            let result: Option<(i64, String)> = sqlx::query_as(&format!(
                "SELECT id, job_json FROM {} ORDER BY id LIMIT 1 FOR UPDATE",
                self.jobs_table()
            ))
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to select job: {}", e)))?;

            if let Some((id, job_json)) = result {
                // Delete the job
                sqlx::query(&format!("DELETE FROM {} WHERE id = ?", self.jobs_table()))
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| WgError::Backend(format!("Failed to delete job: {}", e)))?;

                tx.commit()
                    .await
                    .map_err(|e| WgError::Backend(format!("Failed to commit: {}", e)))?;

                return Ok(Some(job_json));
            }

            tx.rollback()
                .await
                .map_err(|e| WgError::Backend(format!("Failed to rollback: {}", e)))?;

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
            "INSERT INTO {} (job_json, run_at) VALUES (?, ?)",
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
            "SELECT job_json FROM {} WHERE run_at <= ? ORDER BY run_at LIMIT ?",
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
            "DELETE FROM {} WHERE job_json = ? LIMIT 1",
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
            "INSERT INTO {} (job_json, retry_at) VALUES (?, ?)",
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
            "SELECT job_json FROM {} WHERE retry_at <= ? ORDER BY retry_at LIMIT ?",
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
            "DELETE FROM {} WHERE job_json = ? LIMIT 1",
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
            "INSERT INTO {} (job_json) VALUES (?)",
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

    async fn list_dead(&self, limit: usize, offset: usize) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(&format!(
            "SELECT job_json FROM {} ORDER BY id DESC LIMIT ? OFFSET ?",
            self.dead_table()
        ))
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to list dead jobs: {}", e)))?;

        Ok(rows.into_iter().map(|(json,)| json).collect())
    }

    async fn get_dead_by_id(&self, job_id: &str) -> Result<Option<String>> {
        // Search for the job by ID in the JSON
        let pattern = format!("%{}%", job_id);
        let row: Option<(String,)> = sqlx::query_as(&format!(
            "SELECT job_json FROM {} WHERE job_json LIKE ? LIMIT 1",
            self.dead_table()
        ))
        .bind(&pattern)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to get dead job: {}", e)))?;

        Ok(row.map(|(json,)| json))
    }

    async fn remove_dead(&self, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "DELETE FROM {} WHERE job_json = ? LIMIT 1",
            self.dead_table()
        ))
        .bind(job_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to remove dead job: {}", e)))?;
        Ok(())
    }

    // ========== Heartbeat Operations ==========

    async fn heartbeat(&self, info: &WorkerPoolInfo) -> Result<()> {
        let job_names_json =
            serde_json::to_string(&info.job_names).unwrap_or_else(|_| "[]".to_string());

        sqlx::query(&format!(
            r#"
            INSERT INTO {} (pool_id, heartbeat_at, started_at, concurrency, host, pid, job_names)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                heartbeat_at = VALUES(heartbeat_at),
                concurrency = VALUES(concurrency),
                host = VALUES(host),
                pid = VALUES(pid),
                job_names = VALUES(job_names)
            "#,
            self.worker_pools_table()
        ))
        .bind(&info.pool_id)
        .bind(info.heartbeat_at)
        .bind(info.started_at)
        .bind(info.concurrency as i32)
        .bind(&info.host)
        .bind(info.pid as i32)
        .bind(&job_names_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to send heartbeat: {}", e)))?;
        Ok(())
    }

    async fn remove_heartbeat(&self, pool_id: &str) -> Result<()> {
        // Remove from worker_pools table
        sqlx::query(&format!(
            "DELETE FROM {} WHERE pool_id = ?",
            self.worker_pools_table()
        ))
        .bind(pool_id)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to remove heartbeat: {}", e)))?;

        // Also clean up in_progress jobs
        sqlx::query(&format!(
            "DELETE FROM {} WHERE pool_id = ?",
            self.in_progress_table()
        ))
        .bind(pool_id)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to clean up in_progress: {}", e)))?;

        Ok(())
    }

    async fn list_worker_pools(&self) -> Result<Vec<WorkerPoolInfo>> {
        let rows: Vec<(String, i64, i64, i32, String, i32, String)> = sqlx::query_as(&format!(
            "SELECT pool_id, heartbeat_at, started_at, concurrency, host, pid, job_names FROM {}",
            self.worker_pools_table()
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to list worker pools: {}", e)))?;

        let pools = rows
            .into_iter()
            .map(
                |(pool_id, heartbeat_at, started_at, concurrency, host, pid, job_names)| {
                    WorkerPoolInfo {
                        pool_id,
                        heartbeat_at,
                        started_at,
                        concurrency: concurrency as usize,
                        host,
                        pid: pid as u32,
                        job_names: serde_json::from_str(&job_names).unwrap_or_default(),
                    }
                },
            )
            .collect();

        Ok(pools)
    }

    async fn get_stale_pools(&self, threshold_secs: u64) -> Result<Vec<String>> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let stale_threshold = now - threshold_secs as i64;

        let rows: Vec<(String,)> = sqlx::query_as(&format!(
            "SELECT pool_id FROM {} WHERE heartbeat_at < ?",
            self.worker_pools_table()
        ))
        .bind(stale_threshold)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to get stale pools: {}", e)))?;

        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    // ========== In-Progress Job Tracking ==========

    async fn mark_in_progress(&self, pool_id: &str, job_json: &str) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        sqlx::query(&format!(
            "INSERT INTO {} (pool_id, job_json, started_at) VALUES (?, ?, ?)",
            self.in_progress_table()
        ))
        .bind(pool_id)
        .bind(job_json)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to mark in_progress: {}", e)))?;
        Ok(())
    }

    async fn complete_in_progress(&self, pool_id: &str, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "DELETE FROM {} WHERE pool_id = ? AND job_json = ? LIMIT 1",
            self.in_progress_table()
        ))
        .bind(pool_id)
        .bind(job_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to complete in_progress: {}", e)))?;
        Ok(())
    }

    async fn get_in_progress_jobs(&self, pool_id: &str) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(&format!(
            "SELECT job_json FROM {} WHERE pool_id = ?",
            self.in_progress_table()
        ))
        .bind(pool_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to get in_progress jobs: {}", e)))?;

        Ok(rows.into_iter().map(|(json,)| json).collect())
    }

    async fn cleanup_pool(&self, pool_id: &str) -> Result<Vec<String>> {
        // First get all in-progress jobs
        let jobs = self.get_in_progress_jobs(pool_id).await?;

        // Then remove the heartbeat (which also cleans up in_progress)
        self.remove_heartbeat(pool_id).await?;

        Ok(jobs)
    }

    // ========== Concurrency Control ==========

    async fn set_job_concurrency(&self, job_name: &str, max: usize) -> Result<()> {
        sqlx::query(&format!(
            r#"
            INSERT INTO {} (job_name, max_concurrency, inflight)
            VALUES (?, ?, 0)
            ON DUPLICATE KEY UPDATE max_concurrency = VALUES(max_concurrency)
            "#,
            self.concurrency_table()
        ))
        .bind(job_name)
        .bind(max as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to set job concurrency: {}", e)))?;
        Ok(())
    }

    async fn try_acquire_concurrency(&self, job_name: &str) -> Result<bool> {
        // Use a transaction to atomically check and increment
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| WgError::Backend(format!("Failed to begin transaction: {}", e)))?;

        // Try to update if under limit
        let result = sqlx::query(&format!(
            r#"
            UPDATE {} 
            SET inflight = inflight + 1 
            WHERE job_name = ? AND (max_concurrency = 0 OR inflight < max_concurrency)
            "#,
            self.concurrency_table()
        ))
        .bind(job_name)
        .execute(&mut *tx)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to acquire concurrency: {}", e)))?;

        if result.rows_affected() > 0 {
            tx.commit()
                .await
                .map_err(|e| WgError::Backend(format!("Failed to commit: {}", e)))?;
            return Ok(true);
        }

        // Check if the job_name exists - if not, insert with inflight=1
        let exists: Option<(i64,)> = sqlx::query_as(&format!(
            "SELECT 1 FROM {} WHERE job_name = ?",
            self.concurrency_table()
        ))
        .bind(job_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to check concurrency: {}", e)))?;

        if exists.is_none() {
            // No concurrency limit set for this job type, insert with inflight=1 and max=0 (unlimited)
            sqlx::query(&format!(
                "INSERT IGNORE INTO {} (job_name, max_concurrency, inflight) VALUES (?, 0, 1)",
                self.concurrency_table()
            ))
            .bind(job_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to insert concurrency: {}", e)))?;

            tx.commit()
                .await
                .map_err(|e| WgError::Backend(format!("Failed to commit: {}", e)))?;
            return Ok(true);
        }

        tx.rollback()
            .await
            .map_err(|e| WgError::Backend(format!("Failed to rollback: {}", e)))?;

        // Row exists but we couldn't acquire - at capacity
        Ok(false)
    }

    async fn release_concurrency(&self, job_name: &str) -> Result<()> {
        sqlx::query(&format!(
            "UPDATE {} SET inflight = GREATEST(0, inflight - 1) WHERE job_name = ?",
            self.concurrency_table()
        ))
        .bind(job_name)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to release concurrency: {}", e)))?;
        Ok(())
    }

    async fn push_job_front(&self, job_json: &str) -> Result<()> {
        // For MySQL, insert with a lower id to make it come first in FIFO
        sqlx::query(&format!(
            "INSERT INTO {} (id, job_json) VALUES ((SELECT COALESCE(MIN(t.id), 0) - 1 FROM (SELECT id FROM {}) t), ?)",
            self.jobs_table(),
            self.jobs_table()
        ))
        .bind(job_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to push job to front: {}", e)))?;
        Ok(())
    }
}
