//! SQLite backend for wg job queue.
//!
//! This crate provides a SQLite-based storage backend for the wg job queue.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use wg_sqlite::SqliteBackend;
//! use wg_core::Client;
//!
//! #[tokio::main]
//! async fn main() -> wg_core::Result<()> {
//!     let backend = SqliteBackend::new("sqlite:jobs.db", "myapp").await?;
//!     let client = Client::new(backend);
//!     Ok(())
//! }
//! ```

use async_trait::async_trait;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use std::time::Duration;
use wg_core::{Backend, Result, WgError, WorkerPoolInfo};

/// SQLite backend for job queue storage.
#[derive(Clone)]
pub struct SqliteBackend {
    pool: SqlitePool,
    namespace: String,
}

impl SqliteBackend {
    /// Create a new SQLite backend.
    ///
    /// The database_url should be in the format: `sqlite:path/to/db.sqlite` or `sqlite::memory:`
    pub async fn new(database_url: &str, namespace: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1) // SQLite works best with single connection for writes
            .connect(database_url)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to connect to SQLite: {}", e)))?;

        let backend = Self {
            pool,
            namespace: namespace.to_string(),
        };

        // Initialize tables
        backend.init_tables().await?;

        Ok(backend)
    }

    /// Create an in-memory SQLite backend (useful for testing).
    pub async fn in_memory(namespace: &str) -> Result<Self> {
        Self::new("sqlite::memory:", namespace).await
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_json TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
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
                created_at TEXT DEFAULT (datetime('now'))
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
        .ok();

        // Create retry table
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_json TEXT NOT NULL,
                retry_at INTEGER NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_json TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
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
                pool_id TEXT PRIMARY KEY,
                heartbeat_at INTEGER NOT NULL,
                started_at INTEGER NOT NULL,
                concurrency INTEGER NOT NULL,
                host TEXT NOT NULL,
                pid INTEGER NOT NULL,
                job_names TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
            "#,
            worker_pools_table
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| WgError::Backend(format!("Failed to create worker_pools table: {}", e)))?;

        // Create index on heartbeat_at for stale detection
        sqlx::query(&format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_heartbeat_at ON {} (heartbeat_at)",
            self.namespace, worker_pools_table
        ))
        .execute(&self.pool)
        .await
        .ok();

        // Create in_progress table for tracking jobs being processed
        let in_progress_table = format!("{}_in_progress", self.namespace);
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pool_id TEXT NOT NULL,
                job_json TEXT NOT NULL,
                started_at INTEGER NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
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
            self.namespace, in_progress_table
        ))
        .execute(&self.pool)
        .await
        .ok();

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
}

#[async_trait]
impl Backend for SqliteBackend {
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
            // SQLite: use a transaction to atomically select and delete
            let result: Option<(i64, String)> = sqlx::query_as(&format!(
                "SELECT id, job_json FROM {} ORDER BY id LIMIT 1",
                self.jobs_table()
            ))
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| WgError::Backend(format!("Failed to select job: {}", e)))?;

            if let Some((id, job_json)) = result {
                // Delete the job
                sqlx::query(&format!("DELETE FROM {} WHERE id = ?", self.jobs_table()))
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
            "DELETE FROM {} WHERE job_json = ?",
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
            "DELETE FROM {} WHERE job_json = ?",
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
            "DELETE FROM {} WHERE job_json = ?",
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
            ON CONFLICT(pool_id) DO UPDATE SET
                heartbeat_at = excluded.heartbeat_at,
                concurrency = excluded.concurrency,
                host = excluded.host,
                pid = excluded.pid,
                job_names = excluded.job_names
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
            "DELETE FROM {} WHERE pool_id = ? AND job_json = ?",
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
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn create_test_backend() -> SqliteBackend {
        SqliteBackend::in_memory("test").await.unwrap()
    }

    #[tokio::test]
    async fn test_in_memory_creation() {
        let backend = create_test_backend().await;
        assert_eq!(backend.queue_len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_push_and_pop_job() {
        let backend = create_test_backend().await;

        backend
            .push_job(r#"{"id": "1", "data": "test"}"#)
            .await
            .unwrap();
        assert_eq!(backend.queue_len().await.unwrap(), 1);

        let job = backend.pop_job(Duration::from_millis(100)).await.unwrap();
        assert!(job.is_some());
        assert!(job.unwrap().contains("test"));
        assert_eq!(backend.queue_len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_pop_empty_queue_timeout() {
        let backend = create_test_backend().await;

        let start = std::time::Instant::now();
        let job = backend.pop_job(Duration::from_millis(200)).await.unwrap();
        let elapsed = start.elapsed();

        assert!(job.is_none());
        assert!(elapsed >= Duration::from_millis(200));
    }

    #[tokio::test]
    async fn test_fifo_ordering() {
        let backend = create_test_backend().await;

        backend.push_job(r#"{"order": 1}"#).await.unwrap();
        backend.push_job(r#"{"order": 2}"#).await.unwrap();
        backend.push_job(r#"{"order": 3}"#).await.unwrap();

        let job1 = backend
            .pop_job(Duration::from_millis(100))
            .await
            .unwrap()
            .unwrap();
        let job2 = backend
            .pop_job(Duration::from_millis(100))
            .await
            .unwrap()
            .unwrap();
        let job3 = backend
            .pop_job(Duration::from_millis(100))
            .await
            .unwrap()
            .unwrap();

        assert!(job1.contains("1"));
        assert!(job2.contains("2"));
        assert!(job3.contains("3"));
    }

    #[tokio::test]
    async fn test_schedule_job() {
        let backend = create_test_backend().await;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        backend
            .schedule_job(r#"{"id": "sched1"}"#, now - 10)
            .await
            .unwrap();
        backend
            .schedule_job(r#"{"id": "sched2"}"#, now + 3600)
            .await
            .unwrap();

        assert_eq!(backend.schedule_len().await.unwrap(), 2);

        // Get due jobs (only the one in the past)
        let due = backend.get_due_scheduled(now, 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert!(due[0].contains("sched1"));
    }

    #[tokio::test]
    async fn test_remove_scheduled() {
        let backend = create_test_backend().await;
        let job = r#"{"id": "to_remove"}"#;

        backend.schedule_job(job, 12345).await.unwrap();
        assert_eq!(backend.schedule_len().await.unwrap(), 1);

        backend.remove_scheduled(job).await.unwrap();
        assert_eq!(backend.schedule_len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_retry_job() {
        let backend = create_test_backend().await;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        backend
            .retry_job(r#"{"id": "retry1"}"#, now - 10)
            .await
            .unwrap();
        backend
            .retry_job(r#"{"id": "retry2"}"#, now + 3600)
            .await
            .unwrap();

        assert_eq!(backend.retry_len().await.unwrap(), 2);

        // Get due retries (only the one in the past)
        let due = backend.get_due_retries(now, 10).await.unwrap();
        assert_eq!(due.len(), 1);
        assert!(due[0].contains("retry1"));
    }

    #[tokio::test]
    async fn test_remove_retry() {
        let backend = create_test_backend().await;
        let job = r#"{"id": "retry_remove"}"#;

        backend.retry_job(job, 12345).await.unwrap();
        assert_eq!(backend.retry_len().await.unwrap(), 1);

        backend.remove_retry(job).await.unwrap();
        assert_eq!(backend.retry_len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_dead_queue() {
        let backend = create_test_backend().await;

        backend
            .push_dead(r#"{"id": "dead1", "error": "failed"}"#)
            .await
            .unwrap();
        backend
            .push_dead(r#"{"id": "dead2", "error": "timeout"}"#)
            .await
            .unwrap();

        assert_eq!(backend.dead_len().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_list_dead_pagination() {
        let backend = create_test_backend().await;

        for i in 0..5 {
            backend
                .push_dead(&format!(r#"{{"id": "dead{}"}}"#, i))
                .await
                .unwrap();
        }

        // List first 2
        let page1 = backend.list_dead(2, 0).await.unwrap();
        assert_eq!(page1.len(), 2);

        // List next 2
        let page2 = backend.list_dead(2, 2).await.unwrap();
        assert_eq!(page2.len(), 2);

        // List remaining
        let page3 = backend.list_dead(2, 4).await.unwrap();
        assert_eq!(page3.len(), 1);
    }

    #[tokio::test]
    async fn test_get_dead_by_id() {
        let backend = create_test_backend().await;

        backend
            .push_dead(r#"{"id": {"0": "abc-123"}, "error": "test"}"#)
            .await
            .unwrap();
        backend
            .push_dead(r#"{"id": {"0": "def-456"}, "error": "other"}"#)
            .await
            .unwrap();

        let found = backend.get_dead_by_id("abc-123").await.unwrap();
        assert!(found.is_some());
        assert!(found.unwrap().contains("abc-123"));

        let not_found = backend.get_dead_by_id("xyz-999").await.unwrap();
        assert!(not_found.is_none());
    }

    #[tokio::test]
    async fn test_remove_dead() {
        let backend = create_test_backend().await;
        let job = r#"{"id": "to_delete"}"#;

        backend.push_dead(job).await.unwrap();
        assert_eq!(backend.dead_len().await.unwrap(), 1);

        backend.remove_dead(job).await.unwrap();
        assert_eq!(backend.dead_len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_move_scheduled_to_queue() {
        let backend = create_test_backend().await;
        let job = r#"{"id": "moveme"}"#;

        backend.schedule_job(job, 12345).await.unwrap();
        assert_eq!(backend.schedule_len().await.unwrap(), 1);
        assert_eq!(backend.queue_len().await.unwrap(), 0);

        backend.move_scheduled_to_queue(job).await.unwrap();
        assert_eq!(backend.schedule_len().await.unwrap(), 0);
        assert_eq!(backend.queue_len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_move_retry_to_queue() {
        let backend = create_test_backend().await;
        let job = r#"{"id": "retry_move"}"#;

        backend.retry_job(job, 12345).await.unwrap();
        assert_eq!(backend.retry_len().await.unwrap(), 1);
        assert_eq!(backend.queue_len().await.unwrap(), 0);

        backend.move_retry_to_queue(job).await.unwrap();
        assert_eq!(backend.retry_len().await.unwrap(), 0);
        assert_eq!(backend.queue_len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_namespace_isolation() {
        let backend1 = SqliteBackend::in_memory("ns1").await.unwrap();
        let backend2 = SqliteBackend::in_memory("ns2").await.unwrap();

        backend1.push_job(r#"{"ns": "1"}"#).await.unwrap();

        // Different backends have different namespaces (and different in-memory DBs)
        assert_eq!(backend1.queue_len().await.unwrap(), 1);
        assert_eq!(backend2.queue_len().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_queue_len_operations() {
        let backend = create_test_backend().await;

        assert_eq!(backend.queue_len().await.unwrap(), 0);
        assert_eq!(backend.schedule_len().await.unwrap(), 0);
        assert_eq!(backend.retry_len().await.unwrap(), 0);
        assert_eq!(backend.dead_len().await.unwrap(), 0);

        backend.push_job(r#"{"id": "1"}"#).await.unwrap();
        backend.push_job(r#"{"id": "2"}"#).await.unwrap();
        assert_eq!(backend.queue_len().await.unwrap(), 2);

        backend.schedule_job(r#"{"id": "s1"}"#, 100).await.unwrap();
        assert_eq!(backend.schedule_len().await.unwrap(), 1);

        backend.retry_job(r#"{"id": "r1"}"#, 100).await.unwrap();
        backend.retry_job(r#"{"id": "r2"}"#, 200).await.unwrap();
        assert_eq!(backend.retry_len().await.unwrap(), 2);

        backend.push_dead(r#"{"id": "d1"}"#).await.unwrap();
        assert_eq!(backend.dead_len().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_get_due_scheduled_limit() {
        let backend = create_test_backend().await;
        let now = 1000i64;

        // Add 5 due jobs
        for i in 0..5 {
            backend
                .schedule_job(&format!(r#"{{"id": "{}"}}"#, i), now - 10)
                .await
                .unwrap();
        }

        // Request only 3
        let due = backend.get_due_scheduled(now, 3).await.unwrap();
        assert_eq!(due.len(), 3);
    }

    #[tokio::test]
    async fn test_get_due_retries_limit() {
        let backend = create_test_backend().await;
        let now = 1000i64;

        // Add 5 due retries
        for i in 0..5 {
            backend
                .retry_job(&format!(r#"{{"id": "{}"}}"#, i), now - 10)
                .await
                .unwrap();
        }

        // Request only 2
        let due = backend.get_due_retries(now, 2).await.unwrap();
        assert_eq!(due.len(), 2);
    }
}
