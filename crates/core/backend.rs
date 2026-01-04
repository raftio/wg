//! Backend abstraction for job queue storage.
//!
//! This module provides a trait-based abstraction that allows the job queue
//! to work with different storage backends (Redis, PostgreSQL, MySQL, SQLite).

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

use crate::error::Result;

/// Backend trait for job queue storage operations.
///
/// This trait defines all the operations needed for a job queue backend.
/// Implementations must be thread-safe (Send + Sync).
#[async_trait]
pub trait Backend: Send + Sync {
    /// Push a job to the immediate processing queue.
    async fn push_job(&self, job_json: &str) -> Result<()>;

    /// Pop a job from the immediate processing queue.
    ///
    /// Returns `None` if no job is available within the timeout period.
    async fn pop_job(&self, timeout: Duration) -> Result<Option<String>>;

    /// Schedule a job to run at a specific timestamp.
    async fn schedule_job(&self, job_json: &str, run_at: i64) -> Result<()>;

    /// Get jobs that are due to be scheduled (run_at <= now).
    async fn get_due_scheduled(&self, now: i64, limit: usize) -> Result<Vec<String>>;

    /// Remove a job from the schedule queue.
    async fn remove_scheduled(&self, job_json: &str) -> Result<()>;

    /// Add a job to the retry queue with a retry timestamp.
    async fn retry_job(&self, job_json: &str, retry_at: i64) -> Result<()>;

    /// Get jobs that are due for retry (retry_at <= now).
    async fn get_due_retries(&self, now: i64, limit: usize) -> Result<Vec<String>>;

    /// Remove a job from the retry queue.
    async fn remove_retry(&self, job_json: &str) -> Result<()>;

    /// Push a job to the dead letter queue.
    async fn push_dead(&self, job_json: &str) -> Result<()>;

    /// Get the number of jobs in the immediate queue.
    async fn queue_len(&self) -> Result<usize>;

    /// Get the number of jobs in the schedule queue.
    async fn schedule_len(&self) -> Result<usize>;

    /// Get the number of jobs in the retry queue.
    async fn retry_len(&self) -> Result<usize>;

    /// Get the number of jobs in the dead letter queue.
    async fn dead_len(&self) -> Result<usize>;

    /// Move a job from scheduled to immediate queue atomically.
    async fn move_scheduled_to_queue(&self, job_json: &str) -> Result<()> {
        self.remove_scheduled(job_json).await?;
        self.push_job(job_json).await
    }

    /// Move a job from retry to immediate queue atomically.
    async fn move_retry_to_queue(&self, job_json: &str) -> Result<()> {
        self.remove_retry(job_json).await?;
        self.push_job(job_json).await
    }
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
    async fn push_job(&self, job_json: &str) -> Result<()> {
        self.inner.push_job(job_json).await
    }

    async fn pop_job(&self, timeout: Duration) -> Result<Option<String>> {
        self.inner.pop_job(timeout).await
    }

    async fn schedule_job(&self, job_json: &str, run_at: i64) -> Result<()> {
        self.inner.schedule_job(job_json, run_at).await
    }

    async fn get_due_scheduled(&self, now: i64, limit: usize) -> Result<Vec<String>> {
        self.inner.get_due_scheduled(now, limit).await
    }

    async fn remove_scheduled(&self, job_json: &str) -> Result<()> {
        self.inner.remove_scheduled(job_json).await
    }

    async fn retry_job(&self, job_json: &str, retry_at: i64) -> Result<()> {
        self.inner.retry_job(job_json, retry_at).await
    }

    async fn get_due_retries(&self, now: i64, limit: usize) -> Result<Vec<String>> {
        self.inner.get_due_retries(now, limit).await
    }

    async fn remove_retry(&self, job_json: &str) -> Result<()> {
        self.inner.remove_retry(job_json).await
    }

    async fn push_dead(&self, job_json: &str) -> Result<()> {
        self.inner.push_dead(job_json).await
    }

    async fn queue_len(&self) -> Result<usize> {
        self.inner.queue_len().await
    }

    async fn schedule_len(&self) -> Result<usize> {
        self.inner.schedule_len().await
    }

    async fn retry_len(&self) -> Result<usize> {
        self.inner.retry_len().await
    }

    async fn dead_len(&self) -> Result<usize> {
        self.inner.dead_len().await
    }

    async fn move_scheduled_to_queue(&self, job_json: &str) -> Result<()> {
        self.inner.move_scheduled_to_queue(job_json).await
    }

    async fn move_retry_to_queue(&self, job_json: &str) -> Result<()> {
        self.inner.move_retry_to_queue(job_json).await
    }
}
