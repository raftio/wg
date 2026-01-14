//! SQLite backend for wg job queue.
//!
//! This crate provides a SQLite-based storage backend for the wg job queue.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use wg_sqlite::SqliteBackend;
//! use wg_core::Client;
//!
//! #[tokio::main]
//! async fn main() -> wg_core::Result<()> {
//!     let backend = SqliteBackend::new("sqlite::memory:").await?;
//!     let client = Client::new(backend, "myapp");
//!     Ok(())
//! }
//! ```

use async_trait::async_trait;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use std::time::Duration;
use wg_core::{Backend, Result, WgError, WorkerPoolInfo};

/// Table prefix for all wg-created tables.
const WG_TABLE_PREFIX: &str = "_wg_tb_";

/// SQLite backend for job queue storage.
#[derive(Clone)]
pub struct SqliteBackend {
    pool: SqlitePool,
}

impl SqliteBackend {
    /// Create a new SQLite backend.
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1) // SQLite works best with single connection
            .connect(database_url)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to connect to SQLite: {}", e)))?;

        let backend = Self { pool };

        // Initialize global tables
        backend.init_global_tables().await?;

        Ok(backend)
    }

    /// Create an in-memory SQLite backend and initialize tables for the given namespace.
    ///
    /// This is useful for testing and examples. The database is ephemeral
    /// and will be lost when the connection is closed.
    pub async fn in_memory(namespace: &str) -> Result<Self> {
        let backend = Self::new("sqlite::memory:").await?;
        backend.init_namespace(namespace).await?;
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
                heartbeat_at INTEGER NOT NULL,
                started_at INTEGER NOT NULL,
                concurrency INTEGER NOT NULL,
                host TEXT NOT NULL,
                pid INTEGER NOT NULL,
                job_names TEXT NOT NULL,
                namespace TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_json TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_json TEXT NOT NULL,
                run_at INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
        .ok();

        // Create retry table
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_json TEXT NOT NULL,
                retry_at INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_json TEXT NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pool_id TEXT NOT NULL,
                job_json TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
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
impl Backend for SqliteBackend {
    async fn push_job(&self, ns: &str, job_json: &str) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {} (job_json) VALUES (?)",
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
            // SQLite doesn't have SKIP LOCKED, use simple transaction
            let result: Option<(i64, String)> = sqlx::query_as(&format!(
                "SELECT id, job_json FROM {} ORDER BY id LIMIT 1",
                Self::jobs_table(ns)
            ))
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to select job: {}", e)))?;

            if let Some((id, job_json)) = result {
                // Delete the job
                sqlx::query(&format!("DELETE FROM {} WHERE id = ?", Self::jobs_table(ns)))
                    .bind(id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| WgError::Backend(format!("Failed to delete job: {}", e)))?;

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
        // For SQLite, insert with a lower id to make it come first in FIFO
        sqlx::query(&format!(
            "INSERT INTO {} (id, job_json) VALUES ((SELECT COALESCE(MIN(id), 0) - 1 FROM {}), ?)",
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
            "INSERT INTO {} (job_json, run_at) VALUES (?, ?)",
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
            "SELECT job_json FROM {} WHERE run_at <= ? ORDER BY run_at LIMIT ?",
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
            "DELETE FROM {} WHERE job_json = ?",
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
            "INSERT INTO {} (job_json, retry_at) VALUES (?, ?)",
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
            "SELECT job_json FROM {} WHERE retry_at <= ? ORDER BY retry_at LIMIT ?",
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
            "DELETE FROM {} WHERE job_json = ?",
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
            "INSERT INTO {} (job_json) VALUES (?)",
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
            "SELECT job_json FROM {} ORDER BY id DESC LIMIT ? OFFSET ?",
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
            "SELECT job_json FROM {} WHERE job_json LIKE ? LIMIT 1",
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
            "DELETE FROM {} WHERE job_json = ?",
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pool_id) DO UPDATE SET
                heartbeat_at = excluded.heartbeat_at,
                concurrency = excluded.concurrency,
                host = excluded.host,
                pid = excluded.pid,
                job_names = excluded.job_names,
                namespace = excluded.namespace
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
            "DELETE FROM {} WHERE pool_id = ?",
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
            "SELECT pool_id FROM {} WHERE heartbeat_at < ?",
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
            "INSERT INTO {} (pool_id, job_json, started_at) VALUES (?, ?, ?)",
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
            "DELETE FROM {} WHERE pool_id = ? AND job_json = ?",
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
            "SELECT job_json FROM {} WHERE pool_id = ?",
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
            "SELECT namespace FROM {} WHERE pool_id = ?",
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
                "DELETE FROM {} WHERE pool_id = ?",
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
        // Query sqlite_master to find all tables with our prefix
        let pattern = format!("{}%", WG_TABLE_PREFIX);
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE ?"
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

    async fn init_namespace(&self, namespace: &str) -> Result<()> {
        // Delegate to the struct's init_namespace method
        <SqliteBackend>::init_namespace(self, namespace).await
    }

    // ========== Concurrency Control ==========

    async fn set_job_concurrency(&self, ns: &str, job_name: &str, max: usize) -> Result<()> {
        sqlx::query(&format!(
            r#"
            INSERT INTO {} (job_name, max_concurrency, inflight)
            VALUES (?, ?, 0)
            ON CONFLICT(job_name) DO UPDATE SET max_concurrency = excluded.max_concurrency
            "#,
            Self::concurrency_table(ns)
        ))
        .bind(job_name)
        .bind(max as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to set job concurrency: {}", e)))?;
        Ok(())
    }

    async fn try_acquire_concurrency(&self, ns: &str, job_name: &str) -> Result<bool> {
        // SQLite doesn't support RETURNING in UPDATE, so we need a transaction
        // Try to update if under limit
        let result = sqlx::query(&format!(
            r#"
            UPDATE {} 
            SET inflight = inflight + 1 
            WHERE job_name = ? AND (max_concurrency = 0 OR inflight < max_concurrency)
            "#,
            Self::concurrency_table(ns)
        ))
        .bind(job_name)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to acquire concurrency: {}", e)))?;

        if result.rows_affected() > 0 {
            return Ok(true);
        }

        // Check if the job_name exists - if not, insert with inflight=1
        let exists: Option<(i64,)> = sqlx::query_as(&format!(
            "SELECT 1 FROM {} WHERE job_name = ?",
            Self::concurrency_table(ns)
        ))
        .bind(job_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to check concurrency: {}", e)))?;

        if exists.is_none() {
            // No concurrency limit set for this job type, insert with inflight=1 and max=0 (unlimited)
            sqlx::query(&format!(
                "INSERT OR IGNORE INTO {} (job_name, max_concurrency, inflight) VALUES (?, 0, 1)",
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
            "UPDATE {} SET inflight = MAX(0, inflight - 1) WHERE job_name = ?",
            Self::concurrency_table(ns)
        ))
        .bind(job_name)
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to release concurrency: {}", e)))?;
        Ok(())
    }
}
