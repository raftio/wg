//! Backend abstraction for job queue storage.
//!
//! This module provides a trait-based abstraction that allows the job queue
//! to work with different storage backends (Redis, PostgreSQL, MySQL, SQLite).

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

use crate::error::Result;

/// Information about a worker pool for heartbeat monitoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPoolInfo {
    /// Unique identifier for this worker pool.
    pub pool_id: String,
    /// Last heartbeat timestamp (Unix epoch seconds).
    pub heartbeat_at: i64,
    /// When the worker pool started (Unix epoch seconds).
    pub started_at: i64,
    /// Number of concurrent workers.
    pub concurrency: usize,
    /// Hostname of the machine running this pool.
    pub host: String,
    /// Process ID.
    pub pid: u32,
    /// List of job type names this pool handles.
    pub job_names: Vec<String>,
    /// Namespace this pool is processing.
    pub namespace: String,
}

/// Backend trait for job queue storage operations.
///
/// This trait defines all the operations needed for a job queue backend.
/// Implementations must be thread-safe (Send + Sync).
///
/// All job-related operations require a `namespace` parameter to enable
/// multi-tenant job queues using a single backend connection.
#[async_trait]
pub trait Backend: Send + Sync {
    // ========== Queue Operations ==========

    /// Push a job to the immediate processing queue.
    async fn push_job(&self, ns: &str, job_json: &str) -> Result<()>;

    /// Pop a job from the immediate processing queue.
    ///
    /// Returns `None` if no job is available within the timeout period.
    async fn pop_job(&self, ns: &str, timeout: Duration) -> Result<Option<String>>;

    /// Push a job to the front of the immediate processing queue.
    ///
    /// Used when a job couldn't acquire a concurrency slot and needs
    /// to be re-queued for later processing.
    async fn push_job_front(&self, ns: &str, job_json: &str) -> Result<()>;

    /// Get the number of jobs in the immediate queue.
    async fn queue_len(&self, ns: &str) -> Result<usize>;

    // ========== Schedule Operations ==========

    /// Schedule a job to run at a specific timestamp.
    async fn schedule_job(&self, ns: &str, job_json: &str, run_at: i64) -> Result<()>;

    /// Get jobs that are due to be scheduled (run_at <= now).
    async fn get_due_scheduled(&self, ns: &str, now: i64, limit: usize) -> Result<Vec<String>>;

    /// Remove a job from the schedule queue.
    async fn remove_scheduled(&self, ns: &str, job_json: &str) -> Result<()>;

    /// Get the number of jobs in the schedule queue.
    async fn schedule_len(&self, ns: &str) -> Result<usize>;

    /// Move a job from scheduled to immediate queue atomically.
    async fn move_scheduled_to_queue(&self, ns: &str, job_json: &str) -> Result<()> {
        self.remove_scheduled(ns, job_json).await?;
        self.push_job(ns, job_json).await
    }

    // ========== Retry Operations ==========

    /// Add a job to the retry queue with a retry timestamp.
    async fn retry_job(&self, ns: &str, job_json: &str, retry_at: i64) -> Result<()>;

    /// Get jobs that are due for retry (retry_at <= now).
    async fn get_due_retries(&self, ns: &str, now: i64, limit: usize) -> Result<Vec<String>>;

    /// Remove a job from the retry queue.
    async fn remove_retry(&self, ns: &str, job_json: &str) -> Result<()>;

    /// Get the number of jobs in the retry queue.
    async fn retry_len(&self, ns: &str) -> Result<usize>;

    /// Move a job from retry to immediate queue atomically.
    async fn move_retry_to_queue(&self, ns: &str, job_json: &str) -> Result<()> {
        self.remove_retry(ns, job_json).await?;
        self.push_job(ns, job_json).await
    }

    // ========== Dead Letter Queue ==========

    /// Push a job to the dead letter queue.
    async fn push_dead(&self, ns: &str, job_json: &str) -> Result<()>;

    /// Get the number of jobs in the dead letter queue.
    async fn dead_len(&self, ns: &str) -> Result<usize>;

    /// List jobs in the dead letter queue with pagination.
    async fn list_dead(&self, ns: &str, limit: usize, offset: usize) -> Result<Vec<String>>;

    /// Get a dead job by its job ID.
    async fn get_dead_by_id(&self, ns: &str, job_id: &str) -> Result<Option<String>>;

    /// Remove a job from the dead letter queue.
    async fn remove_dead(&self, ns: &str, job_json: &str) -> Result<()>;

    // ========== Heartbeat Operations (Global) ==========

    /// Send a heartbeat for a worker pool.
    ///
    /// This registers/updates the worker pool's presence and status.
    async fn heartbeat(&self, info: &WorkerPoolInfo) -> Result<()>;

    /// Remove a worker pool's heartbeat (called on graceful shutdown).
    async fn remove_heartbeat(&self, pool_id: &str) -> Result<()>;

    /// List all registered worker pools.
    async fn list_worker_pools(&self) -> Result<Vec<WorkerPoolInfo>>;

    /// Get worker pools with stale heartbeats (heartbeat_at older than threshold).
    ///
    /// Returns the pool IDs of stale pools.
    async fn get_stale_pools(&self, threshold_secs: u64) -> Result<Vec<String>>;

    // ========== In-Progress Job Tracking ==========

    /// Mark a job as in-progress for a specific worker pool.
    ///
    /// This is used for stale job recovery - if a worker dies, its in-progress
    /// jobs can be recovered and re-enqueued.
    async fn mark_in_progress(&self, ns: &str, pool_id: &str, job_json: &str) -> Result<()>;

    /// Remove a job from the in-progress set (job completed or failed).
    async fn complete_in_progress(&self, ns: &str, pool_id: &str, job_json: &str) -> Result<()>;

    /// Get all in-progress jobs for a specific worker pool in a namespace.
    async fn get_in_progress_jobs(&self, ns: &str, pool_id: &str) -> Result<Vec<String>>;

    /// Clean up a dead worker pool and return its in-progress jobs for recovery.
    ///
    /// This removes the heartbeat and returns all in-progress jobs so they
    /// can be re-enqueued. Returns Vec<(namespace, job_json)>.
    async fn cleanup_pool(&self, pool_id: &str) -> Result<Vec<(String, String)>>;

    // ========== Namespace Discovery ==========

    /// List all namespaces that have any data (jobs in any queue).
    ///
    /// This is useful for admin/monitoring tools to discover all active namespaces.
    async fn list_namespaces(&self) -> Result<Vec<String>>;

    // ========== Concurrency Control ==========

    /// Set the maximum concurrency for a job type in a namespace.
    ///
    /// This stores the limit in the backend so all worker pools can coordinate.
    /// Set to 0 for unlimited concurrency.
    async fn set_job_concurrency(&self, ns: &str, job_name: &str, max: usize) -> Result<()>;

    /// Try to acquire a concurrency slot for a job type.
    ///
    /// Returns `true` if a slot was acquired, `false` if at capacity.
    /// This operation must be atomic to prevent race conditions.
    async fn try_acquire_concurrency(&self, ns: &str, job_name: &str) -> Result<bool>;

    /// Release a concurrency slot for a job type.
    ///
    /// Called when a job completes (success or failure) to free up a slot.
    async fn release_concurrency(&self, ns: &str, job_name: &str) -> Result<()>;
}

/// A type-erased backend that can be shared across threads.
pub type DynBackend = Arc<dyn Backend>;

/// Wrapper around Arc<dyn Backend> for convenience.
#[derive(Clone)]
pub struct SharedBackend {
    inner: DynBackend,
}

impl SharedBackend {
    /// Create a new SharedBackend from any Backend implementation.
    pub fn new<B: Backend + 'static>(backend: B) -> Self {
        Self {
            inner: Arc::new(backend),
        }
    }

    /// Get a reference to the inner backend.
    pub fn inner(&self) -> &DynBackend {
        &self.inner
    }
}

#[async_trait]
impl Backend for SharedBackend {
    async fn push_job(&self, ns: &str, job_json: &str) -> Result<()> {
        self.inner.push_job(ns, job_json).await
    }

    async fn pop_job(&self, ns: &str, timeout: Duration) -> Result<Option<String>> {
        self.inner.pop_job(ns, timeout).await
    }

    async fn push_job_front(&self, ns: &str, job_json: &str) -> Result<()> {
        self.inner.push_job_front(ns, job_json).await
    }

    async fn queue_len(&self, ns: &str) -> Result<usize> {
        self.inner.queue_len(ns).await
    }

    async fn schedule_job(&self, ns: &str, job_json: &str, run_at: i64) -> Result<()> {
        self.inner.schedule_job(ns, job_json, run_at).await
    }

    async fn get_due_scheduled(&self, ns: &str, now: i64, limit: usize) -> Result<Vec<String>> {
        self.inner.get_due_scheduled(ns, now, limit).await
    }

    async fn remove_scheduled(&self, ns: &str, job_json: &str) -> Result<()> {
        self.inner.remove_scheduled(ns, job_json).await
    }

    async fn schedule_len(&self, ns: &str) -> Result<usize> {
        self.inner.schedule_len(ns).await
    }

    async fn move_scheduled_to_queue(&self, ns: &str, job_json: &str) -> Result<()> {
        self.inner.move_scheduled_to_queue(ns, job_json).await
    }

    async fn retry_job(&self, ns: &str, job_json: &str, retry_at: i64) -> Result<()> {
        self.inner.retry_job(ns, job_json, retry_at).await
    }

    async fn get_due_retries(&self, ns: &str, now: i64, limit: usize) -> Result<Vec<String>> {
        self.inner.get_due_retries(ns, now, limit).await
    }

    async fn remove_retry(&self, ns: &str, job_json: &str) -> Result<()> {
        self.inner.remove_retry(ns, job_json).await
    }

    async fn retry_len(&self, ns: &str) -> Result<usize> {
        self.inner.retry_len(ns).await
    }

    async fn move_retry_to_queue(&self, ns: &str, job_json: &str) -> Result<()> {
        self.inner.move_retry_to_queue(ns, job_json).await
    }

    async fn push_dead(&self, ns: &str, job_json: &str) -> Result<()> {
        self.inner.push_dead(ns, job_json).await
    }

    async fn dead_len(&self, ns: &str) -> Result<usize> {
        self.inner.dead_len(ns).await
    }

    async fn list_dead(&self, ns: &str, limit: usize, offset: usize) -> Result<Vec<String>> {
        self.inner.list_dead(ns, limit, offset).await
    }

    async fn get_dead_by_id(&self, ns: &str, job_id: &str) -> Result<Option<String>> {
        self.inner.get_dead_by_id(ns, job_id).await
    }

    async fn remove_dead(&self, ns: &str, job_json: &str) -> Result<()> {
        self.inner.remove_dead(ns, job_json).await
    }

    async fn heartbeat(&self, info: &WorkerPoolInfo) -> Result<()> {
        self.inner.heartbeat(info).await
    }

    async fn remove_heartbeat(&self, pool_id: &str) -> Result<()> {
        self.inner.remove_heartbeat(pool_id).await
    }

    async fn list_worker_pools(&self) -> Result<Vec<WorkerPoolInfo>> {
        self.inner.list_worker_pools().await
    }

    async fn get_stale_pools(&self, threshold_secs: u64) -> Result<Vec<String>> {
        self.inner.get_stale_pools(threshold_secs).await
    }

    async fn mark_in_progress(&self, ns: &str, pool_id: &str, job_json: &str) -> Result<()> {
        self.inner.mark_in_progress(ns, pool_id, job_json).await
    }

    async fn complete_in_progress(&self, ns: &str, pool_id: &str, job_json: &str) -> Result<()> {
        self.inner.complete_in_progress(ns, pool_id, job_json).await
    }

    async fn get_in_progress_jobs(&self, ns: &str, pool_id: &str) -> Result<Vec<String>> {
        self.inner.get_in_progress_jobs(ns, pool_id).await
    }

    async fn cleanup_pool(&self, pool_id: &str) -> Result<Vec<(String, String)>> {
        self.inner.cleanup_pool(pool_id).await
    }

    async fn list_namespaces(&self) -> Result<Vec<String>> {
        self.inner.list_namespaces().await
    }

    async fn set_job_concurrency(&self, ns: &str, job_name: &str, max: usize) -> Result<()> {
        self.inner.set_job_concurrency(ns, job_name, max).await
    }

    async fn try_acquire_concurrency(&self, ns: &str, job_name: &str) -> Result<bool> {
        self.inner.try_acquire_concurrency(ns, job_name).await
    }

    async fn release_concurrency(&self, ns: &str, job_name: &str) -> Result<()> {
        self.inner.release_concurrency(ns, job_name).await
    }
}
