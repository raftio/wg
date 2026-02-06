//! Worker pool for processing jobs.

use serde::{Deserialize, Serialize};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinSet;

use crate::backend::{Backend, SharedBackend};
use crate::config::WorkerConfig;
use crate::error::{Result, WgError};
use crate::heartbeat::{generate_pool_id, Heartbeater};
use crate::job::{Job, JobStatus};
use crate::reaper::Reaper;
use crate::retrier::Retrier;
use crate::scheduler::Scheduler;

/// Result type for job handlers.
pub type JobResult = std::result::Result<(), JobError>;

/// Error returned from job handlers.
#[derive(Debug)]
pub struct JobError {
    /// Error message.
    pub message: String,
    /// Whether the job should be retried.
    pub retryable: bool,
}

impl JobError {
    /// Create a new retryable error.
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }

    /// Create a new non-retryable error (job goes to dead queue).
    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }
}

impl<E: std::error::Error> From<E> for JobError {
    fn from(err: E) -> Self {
        Self::retryable(err.to_string())
    }
}

/// Individual worker that processes jobs.
pub struct Worker<T, F, Fut, B>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
    B: Backend + Clone + 'static,
{
    id: usize,
    pool_id: String,
    namespace: String,
    #[allow(dead_code)] // Used for logging context; concurrency uses job.job_name
    job_name: String,
    max_concurrency: usize,
    backend: B,
    handler: F,
    fetch_timeout: std::time::Duration,
    running: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    in_progress: Arc<AtomicUsize>,
    drain_notify: Arc<Notify>,
    _phantom: PhantomData<T>,
}

impl<T, F, Fut, B> Worker<T, F, Fut, B>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
    B: Backend + Clone + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: usize,
        pool_id: String,
        namespace: String,
        job_name: String,
        max_concurrency: usize,
        backend: B,
        handler: F,
        fetch_timeout: std::time::Duration,
        running: Arc<AtomicBool>,
        draining: Arc<AtomicBool>,
        in_progress: Arc<AtomicUsize>,
        drain_notify: Arc<Notify>,
    ) -> Self {
        Self {
            id,
            pool_id,
            namespace,
            job_name,
            max_concurrency,
            backend,
            handler,
            fetch_timeout,
            running,
            draining,
            in_progress,
            drain_notify,
            _phantom: PhantomData,
        }
    }

    pub async fn run(&self) -> Result<()> {
        tracing::debug!(worker_id = self.id, "Worker started");

        while self.running.load(Ordering::SeqCst) {
            // If draining, don't fetch new jobs
            if self.draining.load(Ordering::SeqCst) {
                tracing::debug!(worker_id = self.id, "Worker draining, stopping fetch");
                break;
            }

            match self.fetch_and_process().await {
                Ok(true) => {
                    // Job processed, continue
                }
                Ok(false) => {
                    // No job available, continue waiting
                }
                Err(e) => {
                    tracing::error!(worker_id = self.id, error = %e, "Worker error");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }

        tracing::debug!(worker_id = self.id, "Worker stopped");
        Ok(())
    }

    async fn fetch_and_process(&self) -> Result<bool> {
        // Pop job with timeout
        let job_json = match self
            .backend
            .pop_job(&self.namespace, self.fetch_timeout)
            .await?
        {
            Some(json) => json,
            None => return Ok(false), // Timeout, no job available
        };

        // Parse the job
        let mut job: Job<T> = match serde_json::from_str(&job_json) {
            Ok(job) => job,
            Err(e) => {
                tracing::error!(error = %e, "Failed to parse job, moving to dead queue");
                self.backend.push_dead(&self.namespace, &job_json).await?;
                return Ok(true);
            }
        };

        // Try to acquire concurrency slot (if concurrency control is enabled)
        let acquired_slot = if self.max_concurrency > 0 {
            match self
                .backend
                .try_acquire_concurrency(&self.namespace, &job.job_name)
                .await
            {
                Ok(true) => true,
                Ok(false) => {
                    // At capacity - push job back to front of queue and wait
                    tracing::debug!(
                        worker_id = self.id,
                        job_name = %job.job_name,
                        "Concurrency limit reached, re-queuing job"
                    );
                    self.backend
                        .push_job_front(&self.namespace, &job_json)
                        .await?;
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    return Ok(false);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to acquire concurrency slot, proceeding anyway");
                    false // Don't release a slot we didn't acquire
                }
            }
        } else {
            false // No concurrency control
        };

        // Track in-progress (both locally and in backend for recovery)
        self.in_progress.fetch_add(1, Ordering::SeqCst);
        if let Err(e) = self
            .backend
            .mark_in_progress(&self.namespace, &self.pool_id, &job_json)
            .await
        {
            tracing::warn!(error = %e, "Failed to mark job in-progress in backend");
        }

        tracing::debug!(
            worker_id = self.id,
            job_id = %job.id,
            job_name = %job.job_name,
            "Processing job"
        );

        // Process the job
        let result = (self.handler)(job.payload.clone()).await;

        // Handle result
        match result {
            Ok(()) => {
                tracing::debug!(
                    worker_id = self.id,
                    job_id = %job.id,
                    "Job completed successfully"
                );
                // Job done, nothing more to do
            }
            Err(err) => {
                job.last_error = Some(err.message.clone());

                if err.retryable && job.options.can_retry() {
                    // Retry the job
                    job.options = job.options.increment_retry();
                    job.status = JobStatus::Retry;

                    let retry_delay = job.options.next_retry_delay();
                    let retry_at = current_timestamp() + retry_delay.as_secs() as i64;

                    let updated_job_json = job.to_json()?;
                    self.backend
                        .retry_job(&self.namespace, &updated_job_json, retry_at)
                        .await?;

                    tracing::debug!(
                        worker_id = self.id,
                        job_id = %job.id,
                        retry_count = job.options.retry_count,
                        retry_at = retry_at,
                        "Job scheduled for retry"
                    );
                } else {
                    // Move to dead queue
                    job.status = JobStatus::Dead;
                    let updated_job_json = job.to_json()?;
                    self.backend
                        .push_dead(&self.namespace, &updated_job_json)
                        .await?;

                    tracing::warn!(
                        worker_id = self.id,
                        job_id = %job.id,
                        error = %err.message,
                        "Job moved to dead queue"
                    );
                }
            }
        }

        // Release concurrency slot if we acquired one
        if acquired_slot {
            if let Err(e) = self
                .backend
                .release_concurrency(&self.namespace, &job.job_name)
                .await
            {
                tracing::warn!(error = %e, "Failed to release concurrency slot");
            }
        }

        // Done processing - remove from in-progress tracking
        if let Err(e) = self
            .backend
            .complete_in_progress(&self.namespace, &self.pool_id, &job_json)
            .await
        {
            tracing::warn!(error = %e, "Failed to complete job in-progress in backend");
        }
        self.in_progress.fetch_sub(1, Ordering::SeqCst);
        self.drain_notify.notify_one();

        Ok(true)
    }
}

/// Get current Unix timestamp in seconds.
fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
