//! # wg-client - SDK Client for Job Queue Management
//!
//! This crate provides an `AdminClient` for managing and monitoring the wg job queue.
//!
//! ## Features
//!
//! - **Statistics**: Get queue lengths and overall stats
//! - **Worker Pools**: List active workers, detect stale pools
//! - **Dead Jobs**: List, retry, or delete failed jobs
//! - **Job Management**: Cancel scheduled/retry jobs, unlock in-progress jobs
//!
//! ## Usage
//!
//! ```rust,ignore
//! use wg_client::AdminClient;
//! use wg_sqlite::SqliteBackend;
//!
//! #[tokio::main]
//! async fn main() -> wg_core::Result<()> {
//!     let backend = SqliteBackend::new("jobs.db").await?;
//!     let admin = AdminClient::new(backend);
//!
//!     // List all namespaces
//!     let namespaces = admin.list_namespaces().await?;
//!
//!     // Get queue statistics for all namespaces
//!     let all_stats = admin.all_stats().await?;
//!     for stats in all_stats {
//!         println!("{}: queue={}, dead={}", stats.namespace, stats.queue_len, stats.dead_len);
//!     }
//!
//!     // Get stats for a specific namespace
//!     let stats = admin.stats("myapp").await?;
//!     println!("Queue: {}, Dead: {}", stats.queue_len, stats.dead_len);
//!
//!     // List worker pools
//!     let workers = admin.list_workers().await?;
//!     println!("Active workers: {}", workers.len());
//!
//!     // Retry a dead job in a specific namespace
//!     admin.retry_dead("myapp", "job-id-123").await?;
//!
//!     Ok(())
//! }
//! ```

use serde::{Deserialize, Serialize};
use wg_core::{Backend, SharedBackend, WorkerPoolInfo};

pub use wg_core::{Job, JobId, JobOptions, JobStatus, Result, WgError};

/// Queue statistics for a single namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    /// Number of jobs in the immediate queue.
    pub queue_len: usize,
    /// Number of jobs scheduled for future execution.
    pub schedule_len: usize,
    /// Number of jobs waiting for retry.
    pub retry_len: usize,
    /// Number of dead (failed) jobs.
    pub dead_len: usize,
}

/// Queue statistics with namespace info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceQueueStats {
    /// The namespace name.
    pub namespace: String,
    /// Number of jobs in the immediate queue.
    pub queue_len: usize,
    /// Number of jobs scheduled for future execution.
    pub schedule_len: usize,
    /// Number of jobs waiting for retry.
    pub retry_len: usize,
    /// Number of dead (failed) jobs.
    pub dead_len: usize,
}

/// A parsed dead job with its metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadJob {
    /// Job ID.
    pub id: String,
    /// Job name/type.
    pub job_name: String,
    /// Job payload as JSON value.
    pub payload: serde_json::Value,
    /// When the job was created (Unix timestamp).
    pub created_at: i64,
    /// Last error message.
    pub last_error: Option<String>,
    /// Number of retry attempts made.
    pub retry_count: u32,
}

/// Admin client for managing and monitoring the job queue across all namespaces.
///
/// This client provides methods for:
/// - Listing all namespaces
/// - Getting queue statistics (per-namespace or all)
/// - Listing and managing worker pools
/// - Managing dead jobs (retry, delete)
/// - Canceling scheduled or retry jobs
/// - Unlocking stuck in-progress jobs
#[derive(Clone)]
pub struct AdminClient<B: Backend + Clone = SharedBackend> {
    backend: B,
}

impl AdminClient<SharedBackend> {
    /// Create a new admin client with a shared backend.
    pub fn new(backend: impl Backend + 'static) -> Self {
        Self {
            backend: SharedBackend::new(backend),
        }
    }
}

impl<B: Backend + Clone> AdminClient<B> {
    /// Create a new admin client with a specific backend.
    pub fn with_backend(backend: B) -> Self {
        Self { backend }
    }

    /// Get a reference to the underlying backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    // ========== Namespace Discovery ==========

    /// List all namespaces that have any data in the backend.
    ///
    /// This is useful for discovering all active namespaces across the system.
    pub async fn list_namespaces(&self) -> Result<Vec<String>> {
        self.backend.list_namespaces().await
    }

    /// Get statistics for all namespaces.
    ///
    /// Returns stats for every namespace that has data in the backend.
    pub async fn all_stats(&self) -> Result<Vec<NamespaceQueueStats>> {
        let namespaces = self.list_namespaces().await?;
        let mut all_stats = Vec::with_capacity(namespaces.len());

        for ns in namespaces {
            let (queue_len, schedule_len, retry_len, dead_len) = tokio::try_join!(
                self.backend.queue_len(&ns),
                self.backend.schedule_len(&ns),
                self.backend.retry_len(&ns),
                self.backend.dead_len(&ns),
            )?;

            all_stats.push(NamespaceQueueStats {
                namespace: ns,
                queue_len,
                schedule_len,
                retry_len,
                dead_len,
            });
        }

        Ok(all_stats)
    }

    // ========== Statistics ==========

    /// Get all queue statistics for a namespace.
    pub async fn stats(&self, ns: &str) -> Result<QueueStats> {
        let (queue_len, schedule_len, retry_len, dead_len) = tokio::try_join!(
            self.backend.queue_len(ns),
            self.backend.schedule_len(ns),
            self.backend.retry_len(ns),
            self.backend.dead_len(ns),
        )?;

        Ok(QueueStats {
            queue_len,
            schedule_len,
            retry_len,
            dead_len,
        })
    }

    /// Get the number of jobs in the immediate queue for a namespace.
    pub async fn queue_len(&self, ns: &str) -> Result<usize> {
        self.backend.queue_len(ns).await
    }

    /// Get the number of scheduled jobs for a namespace.
    pub async fn schedule_len(&self, ns: &str) -> Result<usize> {
        self.backend.schedule_len(ns).await
    }

    /// Get the number of jobs waiting for retry in a namespace.
    pub async fn retry_len(&self, ns: &str) -> Result<usize> {
        self.backend.retry_len(ns).await
    }

    /// Get the number of dead jobs in a namespace.
    pub async fn dead_len(&self, ns: &str) -> Result<usize> {
        self.backend.dead_len(ns).await
    }

    // ========== Worker Pools ==========

    /// List all registered worker pools.
    pub async fn list_workers(&self) -> Result<Vec<WorkerPoolInfo>> {
        self.backend.list_worker_pools().await
    }

    /// Get worker pools with stale heartbeats.
    ///
    /// Returns pool IDs of workers that haven't sent a heartbeat
    /// within the specified threshold.
    pub async fn get_stale_pools(&self, threshold_secs: u64) -> Result<Vec<String>> {
        self.backend.get_stale_pools(threshold_secs).await
    }

    /// Clean up a dead worker pool and recover its in-progress jobs.
    ///
    /// This removes the worker's heartbeat and returns all jobs that were
    /// in-progress as (namespace, job_json) tuples so they can be re-enqueued.
    pub async fn cleanup_pool(&self, pool_id: &str) -> Result<Vec<(String, String)>> {
        self.backend.cleanup_pool(pool_id).await
    }

    // ========== Dead Jobs ==========

    /// List dead jobs with pagination for a namespace.
    pub async fn list_dead(&self, ns: &str, limit: usize, offset: usize) -> Result<Vec<DeadJob>> {
        let jobs_json = self.backend.list_dead(ns, limit, offset).await?;

        let jobs = jobs_json
            .into_iter()
            .filter_map(|json| parse_dead_job(&json))
            .collect();

        Ok(jobs)
    }

    /// Get a dead job by its ID in a namespace.
    pub async fn get_dead(&self, ns: &str, job_id: &str) -> Result<Option<DeadJob>> {
        let job_json = self.backend.get_dead_by_id(ns, job_id).await?;

        Ok(job_json.and_then(|json| parse_dead_job(&json)))
    }

    /// Retry a dead job by moving it back to the main queue.
    ///
    /// The job's status will be reset to Pending and retry count to 0.
    pub async fn retry_dead(&self, ns: &str, job_id: &str) -> Result<()> {
        let job_json = self
            .backend
            .get_dead_by_id(ns, job_id)
            .await?
            .ok_or_else(|| WgError::JobNotFound(format!("Dead job {} not found", job_id)))?;

        // Parse and update the job
        let mut job: serde_json::Value = serde_json::from_str(&job_json)?;

        job["status"] = serde_json::json!("Pending");
        job["options"]["retry_count"] = serde_json::json!(0);
        job["last_error"] = serde_json::Value::Null;

        let updated_json = job.to_string();

        // Remove from dead queue and push to main queue
        self.backend.remove_dead(ns, &job_json).await?;
        self.backend.push_job(ns, &updated_json).await?;

        Ok(())
    }

    /// Delete a dead job permanently.
    pub async fn delete_dead(&self, ns: &str, job_id: &str) -> Result<()> {
        let job_json = self
            .backend
            .get_dead_by_id(ns, job_id)
            .await?
            .ok_or_else(|| WgError::JobNotFound(format!("Dead job {} not found", job_id)))?;

        self.backend.remove_dead(ns, &job_json).await
    }

    // ========== Job Management ==========

    /// Cancel a scheduled job by removing it from the schedule queue.
    ///
    /// You need to provide the full job JSON string.
    pub async fn cancel_scheduled(&self, ns: &str, job_json: &str) -> Result<()> {
        self.backend.remove_scheduled(ns, job_json).await
    }

    /// Cancel a job waiting for retry by removing it from the retry queue.
    ///
    /// You need to provide the full job JSON string.
    pub async fn cancel_retry(&self, ns: &str, job_json: &str) -> Result<()> {
        self.backend.remove_retry(ns, job_json).await
    }

    /// Unlock a job that is stuck in-progress.
    ///
    /// This removes the job from the in-progress tracking for a worker pool.
    /// Use this when a job is incorrectly marked as in-progress.
    pub async fn unlock_job(&self, ns: &str, pool_id: &str, job_json: &str) -> Result<()> {
        self.backend.complete_in_progress(ns, pool_id, job_json).await
    }

    /// Get all in-progress jobs for a specific worker pool in a namespace.
    pub async fn get_in_progress_jobs(&self, ns: &str, pool_id: &str) -> Result<Vec<String>> {
        self.backend.get_in_progress_jobs(ns, pool_id).await
    }

    // ========== Concurrency Control ==========

    /// Set the maximum concurrency for a job type in a namespace.
    ///
    /// Set to 0 for unlimited concurrency.
    pub async fn set_job_concurrency(&self, ns: &str, job_name: &str, max: usize) -> Result<()> {
        self.backend.set_job_concurrency(ns, job_name, max).await
    }
}

/// Parse a dead job JSON string into a DeadJob struct.
fn parse_dead_job(json: &str) -> Option<DeadJob> {
    let parsed: serde_json::Value = serde_json::from_str(json).ok()?;

    Some(DeadJob {
        id: parsed["id"]["0"].as_str()?.to_string(),
        job_name: parsed["job_name"].as_str().unwrap_or("unknown").to_string(),
        payload: parsed["payload"].clone(),
        created_at: parsed["created_at"].as_i64().unwrap_or(0),
        last_error: parsed["last_error"].as_str().map(String::from),
        retry_count: parsed["options"]["retry_count"].as_u64().unwrap_or(0) as u32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_stats_default() {
        let stats = QueueStats {
            queue_len: 10,
            schedule_len: 5,
            retry_len: 2,
            dead_len: 1,
        };

        assert_eq!(stats.queue_len, 10);
        assert_eq!(stats.schedule_len, 5);
        assert_eq!(stats.retry_len, 2);
        assert_eq!(stats.dead_len, 1);
    }

    #[test]
    fn test_queue_stats_serialization() {
        let stats = QueueStats {
            queue_len: 10,
            schedule_len: 5,
            retry_len: 2,
            dead_len: 1,
        };

        let json = serde_json::to_string(&stats).unwrap();
        let parsed: QueueStats = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.queue_len, stats.queue_len);
        assert_eq!(parsed.schedule_len, stats.schedule_len);
        assert_eq!(parsed.retry_len, stats.retry_len);
        assert_eq!(parsed.dead_len, stats.dead_len);
    }

    #[test]
    fn test_dead_job_serialization() {
        let dead_job = DeadJob {
            id: "abc-123".to_string(),
            job_name: "send_email".to_string(),
            payload: serde_json::json!({"to": "test@example.com"}),
            created_at: 1704067200,
            last_error: Some("timeout".to_string()),
            retry_count: 3,
        };

        let json = serde_json::to_string(&dead_job).unwrap();
        let parsed: DeadJob = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.id, "abc-123");
        assert_eq!(parsed.job_name, "send_email");
        assert_eq!(parsed.retry_count, 3);
    }

    #[test]
    fn test_parse_dead_job() {
        let json = r#"{
            "id": {"0": "test-job-id"},
            "job_name": "process_order",
            "payload": {"order_id": 123},
            "options": {"max_retries": 3, "retry_count": 2},
            "status": "Dead",
            "created_at": 1704067200,
            "last_error": "connection refused"
        }"#;

        let dead_job = parse_dead_job(json).unwrap();

        assert_eq!(dead_job.id, "test-job-id");
        assert_eq!(dead_job.job_name, "process_order");
        assert_eq!(dead_job.created_at, 1704067200);
        assert_eq!(dead_job.last_error, Some("connection refused".to_string()));
        assert_eq!(dead_job.retry_count, 2);
    }

    #[test]
    fn test_parse_dead_job_missing_fields() {
        let json = r#"{
            "id": {"0": "minimal-job"},
            "payload": {}
        }"#;

        let dead_job = parse_dead_job(json).unwrap();

        assert_eq!(dead_job.id, "minimal-job");
        assert_eq!(dead_job.job_name, "unknown");
        assert_eq!(dead_job.created_at, 0);
        assert!(dead_job.last_error.is_none());
        assert_eq!(dead_job.retry_count, 0);
    }

    #[test]
    fn test_parse_dead_job_invalid_json() {
        let json = "not valid json";
        assert!(parse_dead_job(json).is_none());
    }

    #[test]
    fn test_parse_dead_job_missing_id() {
        let json = r#"{"payload": {}}"#;
        assert!(parse_dead_job(json).is_none());
    }
}
