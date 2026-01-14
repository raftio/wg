//! Client for enqueueing jobs.

use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::backend::{Backend, SharedBackend};
use crate::error::{Result, WgError};
use crate::job::{Job, JobId, JobOptions};

/// Client for enqueueing jobs to the queue.
#[derive(Clone)]
pub struct Client<B: Backend + Clone = SharedBackend> {
    backend: B,
    namespace: String,
}

impl Client<SharedBackend> {
    /// Create a new client with a shared backend.
    pub fn new(backend: impl Backend + 'static, namespace: impl Into<String>) -> Self {
        Self {
            backend: SharedBackend::new(backend),
            namespace: namespace.into(),
        }
    }
}

impl<B: Backend + Clone> Client<B> {
    /// Create a new client with a specific backend.
    pub fn with_backend(backend: B, namespace: impl Into<String>) -> Self {
        Self {
            backend,
            namespace: namespace.into(),
        }
    }

    /// Get the namespace this client is using.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Enqueue a job for immediate processing with a specific job name.
    ///
    /// The job_name is used for concurrency control - multiple worker pools
    /// can coordinate to limit how many jobs of a specific type run concurrently.
    ///
    /// The job will be pushed to the jobs queue and processed as soon as
    /// a worker is available.
    pub async fn enqueue<T>(&self, job_name: &str, payload: T) -> Result<JobId>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let job = Job::new(job_name, payload);
        self.enqueue_job(job).await
    }

    /// Enqueue a job with custom options.
    pub async fn enqueue_with_options<T>(
        &self,
        job_name: &str,
        payload: T,
        options: JobOptions,
    ) -> Result<JobId>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let job = Job::with_options(job_name, payload, options);
        self.enqueue_job(job).await
    }

    /// Enqueue a pre-built job.
    pub async fn enqueue_job<T>(&self, job: Job<T>) -> Result<JobId>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let job_id = job.id.clone();
        let json = job.to_json()?;

        self.backend.push_job(&self.namespace, &json).await?;

        tracing::debug!(job_id = %job_id, namespace = %self.namespace, "Job enqueued");
        Ok(job_id)
    }

    /// Schedule a job to run after a delay.
    ///
    /// The job will be added to the schedule queue and moved to the jobs
    /// queue when the delay expires.
    pub async fn schedule<T>(&self, job_name: &str, payload: T, delay: Duration) -> Result<JobId>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let job = Job::new(job_name, payload).schedule_in(delay);
        self.schedule_job(job).await
    }

    /// Schedule a job with custom options.
    pub async fn schedule_with_options<T>(
        &self,
        job_name: &str,
        payload: T,
        delay: Duration,
        options: JobOptions,
    ) -> Result<JobId>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let job = Job::with_options(job_name, payload, options).schedule_in(delay);
        self.schedule_job(job).await
    }

    /// Schedule a job to run at a specific Unix timestamp.
    pub async fn schedule_at<T>(&self, job_name: &str, payload: T, run_at: i64) -> Result<JobId>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let job = Job::new(job_name, payload).schedule_at(run_at);
        self.schedule_job(job).await
    }

    /// Schedule a pre-built job.
    pub async fn schedule_job<T>(&self, job: Job<T>) -> Result<JobId>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let job_id = job.id.clone();
        let run_at = job.scheduled_at.ok_or_else(|| {
            WgError::Config("Job must have scheduled_at set for scheduling".to_string())
        })?;
        let json = job.to_json()?;

        self.backend
            .schedule_job(&self.namespace, &json, run_at)
            .await?;

        tracing::debug!(job_id = %job_id, run_at = run_at, namespace = %self.namespace, "Job scheduled");
        Ok(job_id)
    }

    /// Get the number of jobs in the immediate queue.
    pub async fn queue_len(&self) -> Result<usize> {
        self.backend.queue_len(&self.namespace).await
    }

    /// Get the number of jobs in the schedule queue.
    pub async fn schedule_len(&self) -> Result<usize> {
        self.backend.schedule_len(&self.namespace).await
    }

    /// Get the number of jobs in the retry queue.
    pub async fn retry_len(&self) -> Result<usize> {
        self.backend.retry_len(&self.namespace).await
    }

    /// Get the number of jobs in the dead letter queue.
    pub async fn dead_len(&self) -> Result<usize> {
        self.backend.dead_len(&self.namespace).await
    }
}
