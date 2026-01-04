//! Server implementation that runs API and workers concurrently.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use actix_web::{web, App, HttpServer};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use wg_core::{Backend, JobResult, SharedBackend, WorkerConfig, WorkerPool};

use crate::api::{self, AppState};
use crate::config::ServerConfig;

/// The wg server that runs both the API and worker pool.
pub struct Server<T, F, Fut>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    config: ServerConfig,
    backend: SharedBackend,
    handler: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F, Fut> Server<T, F, Fut>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    /// Create a new server with the given configuration, backend, and handler.
    pub fn new(config: ServerConfig, backend: impl Backend + 'static, handler: F) -> Self {
        Self {
            config,
            backend: SharedBackend::new(backend),
            handler,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Run the server.
    ///
    /// This starts both the HTTP API server and the worker pool concurrently.
    /// The server will run until a shutdown signal (Ctrl+C) is received.
    pub async fn run(self) -> wg_core::Result<()> {
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = running.clone();

        // Create worker config from server config
        let worker_config = WorkerConfig {
            url: String::new(),
            namespace: self.config.namespace.clone(),
            num_workers: self.config.num_workers,
            fetch_timeout: self.config.fetch_timeout,
            scheduler_interval: self.config.scheduler_interval,
            retrier_interval: self.config.retrier_interval,
            batch_size: self.config.batch_size,
            shutdown_timeout: self.config.shutdown_timeout,
        };

        // Create worker pool
        let mut worker_pool =
            WorkerPool::new(worker_config, self.handler.clone(), self.backend.clone());

        // Create app state for API
        let app_state = web::Data::new(AppState {
            backend: self.backend.clone(),
        });

        // Channel to signal API server shutdown
        let (tx, rx) = oneshot::channel::<()>();

        // Spawn the API server
        let api_addr = self.config.api_addr;
        let api_handle = tokio::spawn(async move {
            let server = HttpServer::new(move || {
                App::new()
                    .app_data(app_state.clone())
                    .configure(api::configure)
            })
            .bind(api_addr)
            .expect("Failed to bind to address")
            .disable_signals()
            .run();

            let server_handle = server.handle();

            // Run server until shutdown signal
            tokio::select! {
                result = server => {
                    if let Err(e) = result {
                        tracing::error!(error = %e, "API server error");
                    }
                }
                _ = rx => {
                    tracing::info!("Shutting down API server...");
                    server_handle.stop(true).await;
                }
            }
        });

        tracing::info!(addr = %self.config.api_addr, "API server started");

        // Run worker pool (this blocks until shutdown)
        let worker_result = worker_pool.run().await;

        // Signal API server to shut down
        running_clone.store(false, Ordering::SeqCst);
        let _ = tx.send(());

        // Wait for API server to finish
        let _ = api_handle.await;

        tracing::info!("Server stopped");

        worker_result
    }
}

/// Builder for Server.
pub struct ServerBuilder<T, F, Fut>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    config: ServerConfig,
    backend: Option<SharedBackend>,
    handler: Option<F>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F, Fut> ServerBuilder<T, F, Fut>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            config: ServerConfig::default(),
            backend: None,
            handler: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Set the server configuration.
    pub fn config(mut self, config: ServerConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the backend.
    pub fn backend(mut self, backend: impl Backend + 'static) -> Self {
        self.backend = Some(SharedBackend::new(backend));
        self
    }

    /// Set the job handler.
    pub fn handler(mut self, handler: F) -> Self {
        self.handler = Some(handler);
        self
    }

    /// Set the API bind address.
    pub fn api_addr(mut self, addr: std::net::SocketAddr) -> Self {
        self.config.api_addr = addr;
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

    /// Build the server.
    pub fn build(self) -> wg_core::Result<Server<T, F, Fut>> {
        let backend = self
            .backend
            .ok_or_else(|| wg_core::WgError::Config("Backend is required".to_string()))?;

        let handler = self
            .handler
            .ok_or_else(|| wg_core::WgError::Config("Handler is required".to_string()))?;

        Ok(Server {
            config: self.config,
            backend,
            handler,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<T, F, Fut> Default for ServerBuilder<T, F, Fut>
where
    T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
