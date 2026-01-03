//! Worker pool for processing jobs.

use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinSet;

use crate::config::WorkerConfig;
use crate::error::{Result, WgError};
use crate::job::{Job, JobStatus};
use crate::redis_keys::RedisKeys;
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

/// Worker pool for processing jobs.
pub struct WorkerPool<T, F, Fut>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    config: WorkerConfig,
    handler: F,
    running: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    in_progress: Arc<AtomicUsize>,
    drain_notify: Arc<Notify>,
    _phantom: PhantomData<T>,
}

impl<T, F, Fut> WorkerPool<T, F, Fut>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    /// Create a new worker pool with the given configuration and handler.
    pub fn new(config: WorkerConfig, handler: F) -> Self {
        Self {
            config,
            handler,
            running: Arc::new(AtomicBool::new(false)),
            draining: Arc::new(AtomicBool::new(false)),
            in_progress: Arc::new(AtomicUsize::new(0)),
            drain_notify: Arc::new(Notify::new()),
            _phantom: PhantomData,
        }
    }

    /// Create a new builder for WorkerPool.
    pub fn builder() -> WorkerPoolBuilder<T, F, Fut> {
        WorkerPoolBuilder::new()
    }

    /// Run the worker pool.
    ///
    /// This will spawn worker tasks, scheduler, and retrier, and block until
    /// shutdown is requested and draining is complete.
    pub async fn run(&self) -> Result<()> {
        self.running.store(true, Ordering::SeqCst);
        
        let client = redis::Client::open(self.config.redis_url.as_str())?;
        let conn = ConnectionManager::new(client).await?;
        let keys = RedisKeys::new(&self.config.namespace);
        
        let mut tasks = JoinSet::new();
        
        // Spawn scheduler
        let scheduler = Scheduler::new(
            conn.clone(),
            keys.clone(),
            self.config.scheduler_interval,
            self.config.batch_size,
            self.running.clone(),
        );
        tasks.spawn(async move {
            scheduler.run().await
        });
        
        // Spawn retrier
        let retrier = Retrier::new(
            conn.clone(),
            keys.clone(),
            self.config.retrier_interval,
            self.config.batch_size,
            self.running.clone(),
        );
        tasks.spawn(async move {
            retrier.run().await
        });
        
        // Spawn workers
        for worker_id in 0..self.config.num_workers {
            let worker = Worker::new(
                worker_id,
                conn.clone(),
                keys.clone(),
                self.handler.clone(),
                self.config.fetch_timeout,
                self.running.clone(),
                self.draining.clone(),
                self.in_progress.clone(),
                self.drain_notify.clone(),
            );
            tasks.spawn(async move {
                worker.run().await
            });
        }
        
        tracing::info!(
            workers = self.config.num_workers,
            namespace = %self.config.namespace,
            "Worker pool started"
        );
        
        // Wait for shutdown signal
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Shutdown signal received, draining...");
        
        self.shutdown().await;
        
        // Wait for all tasks to complete
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                tracing::error!(error = %e, "Task panicked");
            }
        }
        
        tracing::info!("Worker pool stopped");
        Ok(())
    }

    /// Initiate graceful shutdown.
    ///
    /// This will stop accepting new jobs and wait for in-progress jobs to complete.
    pub async fn shutdown(&self) {
        // Enter draining mode - stop fetching new jobs
        self.draining.store(true, Ordering::SeqCst);
        
        // Wait for in-progress jobs to complete
        let deadline = tokio::time::Instant::now() + self.config.shutdown_timeout;
        
        while self.in_progress.load(Ordering::SeqCst) > 0 {
            if tokio::time::Instant::now() >= deadline {
                tracing::warn!(
                    in_progress = self.in_progress.load(Ordering::SeqCst),
                    "Shutdown timeout reached, forcing stop"
                );
                break;
            }
            
            // Wait for notification or timeout
            tokio::select! {
                _ = self.drain_notify.notified() => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
            }
        }
        
        // Stop all loops
        self.running.store(false, Ordering::SeqCst);
    }

    /// Get the number of in-progress jobs.
    pub fn in_progress_count(&self) -> usize {
        self.in_progress.load(Ordering::SeqCst)
    }

    /// Check if the pool is draining.
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }
}

/// Builder for WorkerPool.
pub struct WorkerPoolBuilder<T, F, Fut>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    config: WorkerConfig,
    handler: Option<F>,
    _phantom: PhantomData<T>,
}

impl<T, F, Fut> WorkerPoolBuilder<T, F, Fut>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            config: WorkerConfig::default(),
            handler: None,
            _phantom: PhantomData,
        }
    }

    /// Set the Redis URL.
    pub fn redis_url(mut self, url: impl Into<String>) -> Self {
        self.config.redis_url = url.into();
        self
    }

    /// Set the namespace.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.config.namespace = namespace.into();
        self
    }

    /// Set the number of workers.
    pub fn workers(mut self, num: usize) -> Self {
        self.config.num_workers = num;
        self
    }

    /// Set the job handler.
    pub fn handler(mut self, handler: F) -> Self {
        self.handler = Some(handler);
        self
    }

    /// Set the fetch timeout.
    pub fn fetch_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.config.fetch_timeout = timeout;
        self
    }

    /// Set the shutdown timeout.
    pub fn shutdown_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.config.shutdown_timeout = timeout;
        self
    }

    /// Build the WorkerPool.
    pub fn build(self) -> Result<WorkerPool<T, F, Fut>> {
        let handler = self.handler.ok_or_else(|| {
            WgError::Config("Handler is required".to_string())
        })?;
        
        Ok(WorkerPool::new(self.config, handler))
    }
}

impl<T, F, Fut> Default for WorkerPoolBuilder<T, F, Fut>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Individual worker that processes jobs.
struct Worker<T, F, Fut>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    id: usize,
    conn: ConnectionManager,
    keys: RedisKeys,
    handler: F,
    fetch_timeout: std::time::Duration,
    running: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    in_progress: Arc<AtomicUsize>,
    drain_notify: Arc<Notify>,
    _phantom: PhantomData<T>,
}

impl<T, F, Fut> Worker<T, F, Fut>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        id: usize,
        conn: ConnectionManager,
        keys: RedisKeys,
        handler: F,
        fetch_timeout: std::time::Duration,
        running: Arc<AtomicBool>,
        draining: Arc<AtomicBool>,
        in_progress: Arc<AtomicUsize>,
        drain_notify: Arc<Notify>,
    ) -> Self {
        Self {
            id,
            conn,
            keys,
            handler,
            fetch_timeout,
            running,
            draining,
            in_progress,
            drain_notify,
            _phantom: PhantomData,
        }
    }

    async fn run(&self) -> Result<()> {
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
        let mut conn = self.conn.clone();
        
        // BRPOP with timeout
        let result: Option<(String, String)> = conn
            .brpop(self.keys.jobs(), self.fetch_timeout.as_secs() as f64)
            .await?;
        
        let job_json = match result {
            Some((_, json)) => json,
            None => return Ok(false), // Timeout, no job available
        };
        
        // Parse the job
        let mut job: Job<T> = match serde_json::from_str(&job_json) {
            Ok(job) => job,
            Err(e) => {
                tracing::error!(error = %e, "Failed to parse job, moving to dead queue");
                conn.lpush::<_, _, ()>(&self.keys.dead(), &job_json).await?;
                return Ok(true);
            }
        };
        
        // Track in-progress
        self.in_progress.fetch_add(1, Ordering::SeqCst);
        
        tracing::debug!(
            worker_id = self.id,
            job_id = %job.id,
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
                    
                    let job_json = job.to_json()?;
                    conn.zadd::<_, _, _, ()>(&self.keys.retry(), &job_json, retry_at).await?;
                    
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
                    let job_json = job.to_json()?;
                    conn.lpush::<_, _, ()>(&self.keys.dead(), &job_json).await?;
                    
                    tracing::warn!(
                        worker_id = self.id,
                        job_id = %job.id,
                        error = %err.message,
                        "Job moved to dead queue"
                    );
                }
            }
        }
        
        // Done processing
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

