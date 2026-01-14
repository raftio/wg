//! PostgreSQL backend for wg job queue.
//!
//! This crate provides a PostgreSQL-based storage backend for the wg job queue.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use wg_postgres::PostgresBackend;
//! use wg_core::Client;
//!
//! #[tokio::main]
//! async fn main() -> wg_core::Result<()> {
//!     let backend = PostgresBackend::new("postgres://localhost/mydb").await?;
//!     let client = Client::new(backend, "myapp");
//!     Ok(())
//! }
//! ```

use async_trait::async_trait;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;
use wg_core::{Backend, Result, WgError, WorkerPoolInfo};

/// Table prefix for all wg-created tables.
const WG_TABLE_PREFIX: &str = "_wg_tb_";

/// PostgreSQL backend for job queue storage.
#[derive(Clone)]
pub struct PostgresBackend {
    pool: PgPool,
}

impl PostgresBackend {
    /// Create a new PostgreSQL backend.
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to connect to PostgreSQL: {}", e)))?;

        let backend = Self { pool };

        // Initialize global tables
        backend.init_global_tables().await?;

        Ok(backend)
    }

    /// Initialize the global tables (not namespace-specific).
    async fn init_global_tables(&self) -> Result<()> {
        // Create worker pools table for heartbeat monitoring (global)
        let worker_pools_table = format!("{}worker_pools", WG_TABLE_PREFIX);
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                pool_id TEXT PRIMARY KEY,
                heartbeat_at BIGINT NOT NULL,
                started_at BIGINT NOT NULL,
                concurrency INTEGER NOT NULL,
                host TEXT NOT NULL,
                pid INTEGER NOT NULL,
                job_names TEXT NOT NULL,
                namespace TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            "#,
            worker_pools_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create worker_pools table: {}", e)))?;

        // Create index on heartbeat_at for stale detection
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS idx_worker_pools_heartbeat_at ON {} (heartbeat_at)",
            worker_pools_table
        ))
        .execute(&self.pool)
        .await
        .ok();

        Ok(())
    }

    /// Initialize the required tables for a namespace.
    pub async fn init_namespace(&self, namespace: &str) -> Result<()> {
        let jobs_table = format!("{}{}_jobs", WG_TABLE_PREFIX, namespace);
        let scheduled_table = format!("{}{}_scheduled", WG_TABLE_PREFIX, namespace);
        let retry_table = format!("{}{}_retry", WG_TABLE_PREFIX, namespace);
        let dead_table = format!("{}{}_dead", WG_TABLE_PREFIX, namespace);

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
            namespace, scheduled_table
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
            namespace, retry_table
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

        // Create in_progress table for tracking jobs being processed
        let in_progress_table = format!("{}{}_in_progress", WG_TABLE_PREFIX, namespace);
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id BIGSERIAL PRIMARY KEY,
                pool_id TEXT NOT NULL,
                job_json TEXT NOT NULL,
                started_at BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            "#,
            in_progress_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create in_progress table: {}", e)))?;

        // Create index on pool_id
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_in_progress_pool ON {} (pool_id)",
            namespace, in_progress_table
        ))
        .execute(&self.pool)
        .await
        .ok();

        // Create concurrency table for job-level concurrency control
        let concurrency_table = format!("{}{}_concurrency", WG_TABLE_PREFIX, namespace);
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                job_name TEXT PRIMARY KEY,
                max_concurrency INTEGER NOT NULL DEFAULT 0,
                inflight INTEGER NOT NULL DEFAULT 0
            )
            "#,
            concurrency_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create concurrency table: {}", e)))?;

        Ok(())
    }

    fn jobs_table(ns: &str) -> String {
        format!("{}{}_jobs", WG_TABLE_PREFIX, ns)
    }

    fn scheduled_table(ns: &str) -> String {
        format!("{}{}_scheduled", WG_TABLE_PREFIX, ns)
    }

    fn retry_table(ns: &str) -> String {
        format!("{}{}_retry", WG_TABLE_PREFIX, ns)
    }

    fn dead_table(ns: &str) -> String {
        format!("{}{}_dead", WG_TABLE_PREFIX, ns)
    }

    fn worker_pools_table() -> String {
        format!("{}worker_pools", WG_TABLE_PREFIX)
    }

    fn in_progress_table(ns: &str) -> String {
        format!("{}{}_in_progress", WG_TABLE_PREFIX, ns)
    }

    fn concurrency_table(ns: &str) -> String {
        format!("{}{}_concurrency", WG_TABLE_PREFIX, ns)
    }
}

#[async_trait]
impl Backend for PostgresBackend {
    async fn push_job(&self, ns: &str, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {} (job_json) VALUES ($1)",
            Self::jobs_table(ns)
        ))
        .bind(job_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to push job: {}", e)))?;
        Ok(())
    }

    async fn pop_job(&self, ns: &str, timeout: Duration) -> Result<Option<String>> {
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
                Self::jobs_table(ns),
                Self::jobs_table(ns)
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

    async fn push_job_front(&self, ns: &str, job_json: &str) -> Result<()> {
        // For PostgreSQL, we insert with a lower id by using a sequence manipulation
        // or simply insert and update the id. Simpler: use negative ids
        sqlx::query(&format!(
            "INSERT INTO {} (id, job_json) VALUES ((SELECT COALESCE(MIN(id), 0) - 1 FROM {}), $1)",
            Self::jobs_table(ns),
            Self::jobs_table(ns)
        ))
        .bind(job_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to push job to front: {}", e)))?;
        Ok(())
    }

    async fn queue_len(&self, ns: &str) -> Result<usize> {
        let row: (i64,) =
            sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", Self::jobs_table(ns)))
                .fetch_one(&self.pool)
                .await
                .map_err(|e| WgError::Backend(format!("Failed to get queue length: {}", e)))?;
        Ok(row.0 as usize)
    }

    async fn schedule_job(&self, ns: &str, job_json: &str, run_at: i64) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {} (job_json, run_at) VALUES ($1, $2)",
            Self::scheduled_table(ns)
        ))
        .bind(job_json)
        .bind(run_at)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to schedule job: {}", e)))?;
        Ok(())
    }

    async fn get_due_scheduled(&self, ns: &str, now: i64, limit: usize) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(&format!(
            "SELECT job_json FROM {} WHERE run_at <= $1 ORDER BY run_at LIMIT $2",
            Self::scheduled_table(ns)
        ))
        .bind(now)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to get scheduled jobs: {}", e)))?;

        Ok(rows.into_iter().map(|(json,)| json).collect())
    }

    async fn remove_scheduled(&self, ns: &str, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "DELETE FROM {} WHERE job_json = $1",
            Self::scheduled_table(ns)
        ))
        .bind(job_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to remove scheduled job: {}", e)))?;
        Ok(())
    }

    async fn schedule_len(&self, ns: &str) -> Result<usize> {
        let row: (i64,) =
            sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", Self::scheduled_table(ns)))
                .fetch_one(&self.pool)
                .await
                .map_err(|e| WgError::Backend(format!("Failed to get schedule length: {}", e)))?;
        Ok(row.0 as usize)
    }

    async fn retry_job(&self, ns: &str, job_json: &str, retry_at: i64) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {} (job_json, retry_at) VALUES ($1, $2)",
            Self::retry_table(ns)
        ))
        .bind(job_json)
        .bind(retry_at)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to retry job: {}", e)))?;
        Ok(())
    }

    async fn get_due_retries(&self, ns: &str, now: i64, limit: usize) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(&format!(
            "SELECT job_json FROM {} WHERE retry_at <= $1 ORDER BY retry_at LIMIT $2",
            Self::retry_table(ns)
        ))
        .bind(now)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to get retry jobs: {}", e)))?;

        Ok(rows.into_iter().map(|(json,)| json).collect())
    }

    async fn remove_retry(&self, ns: &str, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "DELETE FROM {} WHERE job_json = $1",
            Self::retry_table(ns)
        ))
        .bind(job_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to remove retry job: {}", e)))?;
        Ok(())
    }

    async fn retry_len(&self, ns: &str) -> Result<usize> {
        let row: (i64,) =
            sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", Self::retry_table(ns)))
                .fetch_one(&self.pool)
                .await
                .map_err(|e| WgError::Backend(format!("Failed to get retry length: {}", e)))?;
        Ok(row.0 as usize)
    }

    async fn push_dead(&self, ns: &str, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {} (job_json) VALUES ($1)",
            Self::dead_table(ns)
        ))
        .bind(job_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to push dead job: {}", e)))?;
        Ok(())
    }

    async fn dead_len(&self, ns: &str) -> Result<usize> {
        let row: (i64,) =
            sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", Self::dead_table(ns)))
                .fetch_one(&self.pool)
                .await
                .map_err(|e| WgError::Backend(format!("Failed to get dead length: {}", e)))?;
        Ok(row.0 as usize)
    }

    async fn list_dead(&self, ns: &str, limit: usize, offset: usize) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(&format!(
            "SELECT job_json FROM {} ORDER BY id DESC LIMIT $1 OFFSET $2",
            Self::dead_table(ns)
        ))
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to list dead jobs: {}", e)))?;

        Ok(rows.into_iter().map(|(json,)| json).collect())
    }

    async fn get_dead_by_id(&self, ns: &str, job_id: &str) -> Result<Option<String>> {
        // Search for the job by ID in the JSON
        let pattern = format!("%{}%", job_id);
        let row: Option<(String,)> = sqlx::query_as(&format!(
            "SELECT job_json FROM {} WHERE job_json LIKE $1 LIMIT 1",
            Self::dead_table(ns)
        ))
        .bind(&pattern)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to get dead job: {}", e)))?;

        Ok(row.map(|(json,)| json))
    }

    async fn remove_dead(&self, ns: &str, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "DELETE FROM {} WHERE job_json = $1",
            Self::dead_table(ns)
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
            INSERT INTO {} (pool_id, heartbeat_at, started_at, concurrency, host, pid, job_names, namespace)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (pool_id) DO UPDATE SET
                heartbeat_at = EXCLUDED.heartbeat_at,
                concurrency = EXCLUDED.concurrency,
                host = EXCLUDED.host,
                pid = EXCLUDED.pid,
                job_names = EXCLUDED.job_names,
                namespace = EXCLUDED.namespace
            "#,
            Self::worker_pools_table()
        ))
        .bind(&info.pool_id)
        .bind(info.heartbeat_at)
        .bind(info.started_at)
        .bind(info.concurrency as i32)
        .bind(&info.host)
        .bind(info.pid as i32)
        .bind(&job_names_json)
        .bind(&info.namespace)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to send heartbeat: {}", e)))?;
        Ok(())
    }

    async fn remove_heartbeat(&self, pool_id: &str) -> Result<()> {
        // Remove from worker_pools table
        sqlx::query(&format!(
            "DELETE FROM {} WHERE pool_id = $1",
            Self::worker_pools_table()
        ))
        .bind(pool_id)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to remove heartbeat: {}", e)))?;

        Ok(())
    }

    async fn list_worker_pools(&self) -> Result<Vec<WorkerPoolInfo>> {
        let rows: Vec<(String, i64, i64, i32, String, i32, String, String)> = sqlx::query_as(
            &format!(
            "SELECT pool_id, heartbeat_at, started_at, concurrency, host, pid, job_names, namespace FROM {}",
            Self::worker_pools_table()
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to list worker pools: {}", e)))?;

        let pools = rows
            .into_iter()
            .map(
                |(pool_id, heartbeat_at, started_at, concurrency, host, pid, job_names, namespace)| {
                    WorkerPoolInfo {
                        pool_id,
                        heartbeat_at,
                        started_at,
                        concurrency: concurrency as usize,
                        host,
                        pid: pid as u32,
                        job_names: serde_json::from_str(&job_names).unwrap_or_default(),
                        namespace,
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
            "SELECT pool_id FROM {} WHERE heartbeat_at < $1",
            Self::worker_pools_table()
        ))
        .bind(stale_threshold)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to get stale pools: {}", e)))?;

        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    // ========== In-Progress Job Tracking ==========

    async fn mark_in_progress(&self, ns: &str, pool_id: &str, job_json: &str) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        sqlx::query(&format!(
            "INSERT INTO {} (pool_id, job_json, started_at) VALUES ($1, $2, $3)",
            Self::in_progress_table(ns)
        ))
        .bind(pool_id)
        .bind(job_json)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to mark in_progress: {}", e)))?;
        Ok(())
    }

    async fn complete_in_progress(&self, ns: &str, pool_id: &str, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "DELETE FROM {} WHERE pool_id = $1 AND job_json = $2",
            Self::in_progress_table(ns)
        ))
        .bind(pool_id)
        .bind(job_json)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to complete in_progress: {}", e)))?;
        Ok(())
    }

    async fn get_in_progress_jobs(&self, ns: &str, pool_id: &str) -> Result<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(&format!(
            "SELECT job_json FROM {} WHERE pool_id = $1",
            Self::in_progress_table(ns)
        ))
        .bind(pool_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to get in_progress jobs: {}", e)))?;

        Ok(rows.into_iter().map(|(json,)| json).collect())
    }

    async fn cleanup_pool(&self, pool_id: &str) -> Result<Vec<(String, String)>> {
        // First get the namespace from the worker pool
        let row: Option<(String,)> = sqlx::query_as(&format!(
            "SELECT namespace FROM {} WHERE pool_id = $1",
            Self::worker_pools_table()
        ))
        .bind(pool_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to get pool namespace: {}", e)))?;

        let mut jobs = Vec::new();

        if let Some((namespace,)) = row {
            // Get all in-progress jobs
            let in_progress_jobs = self.get_in_progress_jobs(&namespace, pool_id).await?;
            for job_json in in_progress_jobs {
                jobs.push((namespace.clone(), job_json));
            }

            // Clean up in_progress table
            sqlx::query(&format!(
                "DELETE FROM {} WHERE pool_id = $1",
                Self::in_progress_table(&namespace)
            ))
            .bind(pool_id)
            .execute(&self.pool)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to clean up in_progress: {}", e)))?;
        }

        // Remove the heartbeat
        self.remove_heartbeat(pool_id).await?;

        Ok(jobs)
    }

    // ========== Namespace Discovery ==========

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        // Query information_schema to find all tables with our prefix
        let pattern = format!("{}%", WG_TABLE_PREFIX);
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE $1"
        )
        .bind(&pattern)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to list namespaces: {}", e)))?;

        let mut namespaces = std::collections::HashSet::new();
        let prefix_len = WG_TABLE_PREFIX.len();

        for (table_name,) in rows {
            // Tables are like: _wg_tb_{namespace}_jobs, _wg_tb_{namespace}_scheduled, etc.
            // Skip global tables like _wg_tb_worker_pools
            if let Some(rest) = table_name.get(prefix_len..) {
                // Skip global table
                if rest == "worker_pools" {
                    continue;
                }
                // Extract namespace from pattern like "namespace_jobs"
                // Find the last underscore that separates namespace from table type
                for suffix in ["_jobs", "_scheduled", "_retry", "_dead", "_in_progress", "_concurrency"] {
                    if let Some(ns) = rest.strip_suffix(suffix) {
                        if !ns.is_empty() {
                            namespaces.insert(ns.to_string());
                        }
                        break;
                    }
                }
            }
        }

        let mut result: Vec<String> = namespaces.into_iter().collect();
        result.sort();
        Ok(result)
    }

    // ========== Concurrency Control ==========

    async fn set_job_concurrency(&self, ns: &str, job_name: &str, max: usize) -> Result<()> {
        sqlx::query(&format!(
            r#"
            INSERT INTO {} (job_name, max_concurrency, inflight)
            VALUES ($1, $2, 0)
            ON CONFLICT (job_name) DO UPDATE SET max_concurrency = EXCLUDED.max_concurrency
            "#,
            Self::concurrency_table(ns)
        ))
        .bind(job_name)
        .bind(max as i32)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to set job concurrency: {}", e)))?;
        Ok(())
    }

    async fn try_acquire_concurrency(&self, ns: &str, job_name: &str) -> Result<bool> {
        // Use a CTE to atomically check and increment
        // Returns 1 row if acquired, 0 rows if at limit
        let result: Option<(i32,)> = sqlx::query_as(&format!(
            r#"
            UPDATE {} 
            SET inflight = inflight + 1 
            WHERE job_name = $1 AND (max_concurrency = 0 OR inflight < max_concurrency)
            RETURNING inflight
            "#,
            Self::concurrency_table(ns)
        ))
        .bind(job_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to acquire concurrency: {}", e)))?;

        if result.is_some() {
            return Ok(true);
        }

        // Check if the job_name exists - if not, insert with inflight=1
        let exists: Option<(i32,)> = sqlx::query_as(&format!(
            "SELECT 1 FROM {} WHERE job_name = $1",
            Self::concurrency_table(ns)
        ))
        .bind(job_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to check concurrency: {}", e)))?;

        if exists.is_none() {
            // No concurrency limit set for this job type, insert with inflight=1 and max=0 (unlimited)
            sqlx::query(&format!(
                "INSERT INTO {} (job_name, max_concurrency, inflight) VALUES ($1, 0, 1) ON CONFLICT DO NOTHING",
                Self::concurrency_table(ns)
            ))
            .bind(job_name)
            .execute(&self.pool)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to insert concurrency: {}", e)))?;
            return Ok(true);
        }

        // Row exists but we couldn't acquire - at capacity
        Ok(false)
    }

    async fn release_concurrency(&self, ns: &str, job_name: &str) -> Result<()> {
        sqlx::query(&format!(
            "UPDATE {} SET inflight = GREATEST(0, inflight - 1) WHERE job_name = $1",
            Self::concurrency_table(ns)
        ))
        .bind(job_name)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to release concurrency: {}", e)))?;
        Ok(())
    }
}
