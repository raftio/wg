//! Client for enqueueing jobs.

use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::config::ClientConfig;
use crate::error::{Result, WgError};
use crate::job::{Job, JobId};
use crate::redis_keys::RedisKeys;

/// Client for enqueueing jobs to the queue.
#[derive(Clone)]
pub struct Client {
    conn: ConnectionManager,
    keys: RedisKeys,
}

impl Client {
    /// Create a new client with the given configuration.
    pub async fn new(config: ClientConfig) -> Result<Self> {
        let client = redis::Client::open(config.redis_url)?;
        let conn = ConnectionManager::new(client).await?;
        let keys = RedisKeys::new(config.namespace);
        Ok(Self { conn, keys })
    }

    /// Create a new client with Redis URL and namespace.
    pub async fn connect(redis_url: &str, namespace: &str) -> Result<Self> {
        Self::new(ClientConfig::new(redis_url, namespace)).await
    }

    /// Enqueue a job for immediate processing.
    ///
    /// The job will be pushed to the jobs queue and processed as soon as
    /// a worker is available.
    pub async fn enqueue<T>(&self, payload: T) -> Result<JobId>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let job = Job::new(payload);
        self.enqueue_job(job).await
    }

    /// Enqueue a job with custom options.
    pub async fn enqueue_with_options<T>(
        &self,
        payload: T,
        options: crate::job::JobOptions,
    ) -> Result<JobId>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let job = Job::with_options(payload, options);
        self.enqueue_job(job).await
    }

    /// Enqueue a pre-built job.
    pub async fn enqueue_job<T>(&self, job: Job<T>) -> Result<JobId>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let job_id = job.id.clone();
        let json = job.to_json()?;
        
        let mut conn = self.conn.clone();
        conn.lpush::<_, _, ()>(&self.keys.jobs(), &json).await?;
        
        tracing::debug!(job_id = %job_id, "Job enqueued");
        Ok(job_id)
    }

    /// Schedule a job to run after a delay.
    ///
    /// The job will be added to the schedule queue and moved to the jobs
    /// queue when the delay expires.
    pub async fn schedule<T>(&self, payload: T, delay: Duration) -> Result<JobId>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let job = Job::new(payload).schedule_in(delay);
        self.schedule_job(job).await
    }

    /// Schedule a job with custom options.
    pub async fn schedule_with_options<T>(
        &self,
        payload: T,
        delay: Duration,
        options: crate::job::JobOptions,
    ) -> Result<JobId>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let job = Job::with_options(payload, options).schedule_in(delay);
        self.schedule_job(job).await
    }

    /// Schedule a job to run at a specific Unix timestamp.
    pub async fn schedule_at<T>(&self, payload: T, run_at: i64) -> Result<JobId>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let job = Job::new(payload).schedule_at(run_at);
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
        
        let mut conn = self.conn.clone();
        conn.zadd::<_, _, _, ()>(&self.keys.schedule(), &json, run_at).await?;
        
        tracing::debug!(job_id = %job_id, run_at = run_at, "Job scheduled");
        Ok(job_id)
    }

    /// Get the number of jobs in the immediate queue.
    pub async fn queue_len(&self) -> Result<usize> {
        let mut conn = self.conn.clone();
        let len: usize = conn.llen(self.keys.jobs()).await?;
        Ok(len)
    }

    /// Get the number of jobs in the schedule queue.
    pub async fn schedule_len(&self) -> Result<usize> {
        let mut conn = self.conn.clone();
        let len: usize = conn.zcard(self.keys.schedule()).await?;
        Ok(len)
    }

    /// Get the number of jobs in the retry queue.
    pub async fn retry_len(&self) -> Result<usize> {
        let mut conn = self.conn.clone();
        let len: usize = conn.zcard(self.keys.retry()).await?;
        Ok(len)
    }

    /// Get the number of jobs in the dead letter queue.
    pub async fn dead_len(&self) -> Result<usize> {
        let mut conn = self.conn.clone();
        let len: usize = conn.llen(self.keys.dead()).await?;
        Ok(len)
    }
}

