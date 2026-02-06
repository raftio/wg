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
use crate::worker::{JobResult, Worker};

/// Builder for WorkerPool.
pub struct WorkerPoolBuilder<T, F, Fut>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    config: WorkerConfig,
    handler: Option<F>,
    backend: Option<SharedBackend>,
    _phantom: PhantomData<T>,
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
            backend: None,
            _phantom: PhantomData,
        }
    }

    /// Set the backend.
    pub fn backend(mut self, backend: impl Backend + 'static) -> Self {
        self.backend = Some(SharedBackend::new(backend));
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

    /// Set the job type name for this worker pool.
    ///
    /// This is used for concurrency control - workers will only acquire/release
    /// concurrency slots for jobs matching this name.
    pub fn job_name(mut self, name: impl Into<String>) -> Self {
        self.config.job_name = name.into();
        self
    }

    /// Set the maximum concurrent jobs of this type.
    ///
    /// This limit is coordinated across all worker pools processing the same job type.
    /// Set to 0 (default) for unlimited concurrency.
    pub fn max_concurrency(mut self, max: usize) -> Self {
        self.config.max_concurrency = max;
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

    /// Build the WorkerPool with the configured backend.
    pub fn build(self) -> Result<WorkerPool<T, F, Fut, SharedBackend>> {
        let handler = self
            .handler
            .ok_or_else(|| WgError::Config("Handler is required".to_string()))?;

        let backend = self
            .backend
            .ok_or_else(|| WgError::Config("Backend is required".to_string()))?;

        Ok(WorkerPool::new(self.config, handler, backend))
    }

    /// Build the WorkerPool with a custom backend.
    pub fn build_with_backend<B: Backend + Clone + 'static>(
        self,
        backend: B,
    ) -> Result<WorkerPool<T, F, Fut, B>> {
        let handler = self
            .handler
            .ok_or_else(|| WgError::Config("Handler is required".to_string()))?;

        Ok(WorkerPool::new(self.config, handler, backend))
    }
}

/// Worker pool for processing jobs.
pub struct WorkerPool<T, F, Fut, B = SharedBackend>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
    B: Backend + Clone + 'static,
{
    config: WorkerConfig,
    handler: F,
    backend: Option<B>,
    pool_id: String,
    running: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    in_progress: Arc<AtomicUsize>,
    drain_notify: Arc<Notify>,
    _phantom: PhantomData<T>,
}

impl<T, F, Fut> WorkerPool<T, F, Fut, SharedBackend>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    /// Create a new builder for WorkerPool.
    pub fn builder() -> WorkerPoolBuilder<T, F, Fut> {
        WorkerPoolBuilder::new()
    }
}

impl<T, F, Fut, B> WorkerPool<T, F, Fut, B>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
    B: Backend + Clone + 'static,
{
    /// Create a new worker pool with the given configuration, handler, and backend.
    pub fn new(config: WorkerConfig, handler: F, backend: B) -> Self {
        let pool_id = config.pool_id.clone().unwrap_or_else(generate_pool_id);

        Self {
            config,
            handler,
            backend: Some(backend),
            pool_id,
            running: Arc::new(AtomicBool::new(false)),
            draining: Arc::new(AtomicBool::new(false)),
            in_progress: Arc::new(AtomicUsize::new(0)),
            drain_notify: Arc::new(Notify::new()),
            _phantom: PhantomData,
        }
    }

    /// Get the pool ID.
    pub fn pool_id(&self) -> &str {
        &self.pool_id
    }

    /// Run the worker pool.
    ///
    /// This will spawn worker tasks, scheduler, retrier, heartbeater, and reaper,
    /// and block until shutdown is requested and draining is complete.
    pub async fn run(&mut self) -> Result<()> {
        self.run_until(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await
    }

    /// Run the worker pool until the provided shutdown future completes.
    ///
    /// This will spawn worker tasks, scheduler, retrier, heartbeater, and reaper,
    /// and block until `shutdown` resolves, then initiate graceful shutdown and
    /// wait for draining to complete.
    pub async fn run_until<S>(&mut self, shutdown: S) -> Result<()>
    where
        S: Future<Output = ()> + Send,
    {
        let backend = self
            .backend
            .take()
            .ok_or_else(|| WgError::Config("Backend not configured".to_string()))?;

        // Initialize namespace if needed (idempotent for SQL backends)
        if let Err(e) = backend.init_namespace(&self.config.namespace).await {
            tracing::warn!(
                error = %e,
                namespace = %self.config.namespace,
                "Failed to initialize namespace, continuing anyway"
            );
            // Continue anyway - might already be initialized or backend doesn't need it
        }

        self.running.store(true, Ordering::SeqCst);

        let mut tasks = JoinSet::new();

        // Spawn heartbeater
        let heartbeater = Heartbeater::new(
            backend.clone(),
            self.pool_id.clone(),
            self.config.namespace.clone(),
            self.config.num_workers,
            self.config.job_names.clone(),
            self.config.heartbeat_interval,
            self.running.clone(),
        );
        tasks.spawn(async move { heartbeater.run().await });

        // Spawn reaper (if enabled)
        if self.config.enable_reaper {
            let reaper = Reaper::new(
                backend.clone(),
                self.config.reaper_interval,
                self.config.stale_threshold,
                self.running.clone(),
            );
            tasks.spawn(async move { reaper.run().await });
        }

        // Spawn scheduler
        let scheduler = Scheduler::new(
            backend.clone(),
            self.config.namespace.clone(),
            self.config.scheduler_interval,
            self.config.batch_size,
            self.running.clone(),
        );
        tasks.spawn(async move { scheduler.run().await });

        // Spawn retrier
        let retrier = Retrier::new(
            backend.clone(),
            self.config.namespace.clone(),
            self.config.retrier_interval,
            self.config.batch_size,
            self.running.clone(),
        );
        tasks.spawn(async move { retrier.run().await });

        // Register max concurrency for this job type (if enabled)
        if self.config.max_concurrency > 0 {
            if let Err(e) = backend
                .set_job_concurrency(
                    &self.config.namespace,
                    &self.config.job_name,
                    self.config.max_concurrency,
                )
                .await
            {
                tracing::warn!(error = %e, "Failed to set job concurrency in backend");
            } else {
                tracing::info!(
                    job_name = %self.config.job_name,
                    max_concurrency = self.config.max_concurrency,
                    namespace = %self.config.namespace,
                    "Registered job concurrency limit"
                );
            }
        }

        // Spawn workers
        for worker_id in 0..self.config.num_workers {
            let worker = Worker::new(
                worker_id,
                self.pool_id.clone(),
                self.config.namespace.clone(),
                self.config.job_name.clone(),
                self.config.max_concurrency,
                backend.clone(),
                self.handler.clone(),
                self.config.fetch_timeout,
                self.running.clone(),
                self.draining.clone(),
                self.in_progress.clone(),
                self.drain_notify.clone(),
            );
            tasks.spawn(async move { worker.run().await });
        }

        tracing::info!(
            workers = self.config.num_workers,
            namespace = %self.config.namespace,
            pool_id = %self.pool_id,
            "Worker pool started"
        );

        // Wait for shutdown signal/future
        shutdown.await;
        tracing::info!("Shutdown requested, draining...");

        self.shutdown().await;

        // Wait for all tasks to complete
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                tracing::error!(error = %e, "Task panicked");
            }
        }

        tracing::info!(pool_id = %self.pool_id, "Worker pool stopped");
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
