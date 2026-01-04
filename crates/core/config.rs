//! Configuration types for the job queue library.

use std::time::Duration;

/// Configuration for the Client (enqueuer).
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Backend connection URL.
    pub url: String,
    /// Namespace prefix for keys/tables.
    pub namespace: String,
}

impl ClientConfig {
    /// Create a new ClientConfig.
    pub fn new(url: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            namespace: namespace.into(),
        }
    }
}

/// Configuration for the WorkerPool.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Backend connection URL.
    pub url: String,
    /// Namespace prefix for keys/tables.
    pub namespace: String,
    /// Number of worker tasks to spawn.
    pub num_workers: usize,
    /// Timeout for pop operations.
    pub fetch_timeout: Duration,
    /// Interval for the scheduler loop.
    pub scheduler_interval: Duration,
    /// Interval for the retrier loop.
    pub retrier_interval: Duration,
    /// Batch size for scheduler and retrier.
    pub batch_size: usize,
    /// Graceful shutdown timeout.
    pub shutdown_timeout: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            namespace: "wg".to_string(),
            num_workers: 4,
            fetch_timeout: Duration::from_secs(5),
            scheduler_interval: Duration::from_secs(1),
            retrier_interval: Duration::from_secs(1),
            batch_size: 100,
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

/// Builder for WorkerConfig.
#[derive(Debug, Default)]
pub struct WorkerConfigBuilder {
    config: WorkerConfig,
}

impl WorkerConfigBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the backend URL.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.config.url = url.into();
        self
    }

    /// Set the namespace.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.config.namespace = namespace.into();
        self
    }

    /// Set the number of workers.
    pub fn num_workers(mut self, num: usize) -> Self {
        self.config.num_workers = num;
        self
    }

    /// Set the fetch timeout.
    pub fn fetch_timeout(mut self, timeout: Duration) -> Self {
        self.config.fetch_timeout = timeout;
        self
    }

    /// Set the scheduler interval.
    pub fn scheduler_interval(mut self, interval: Duration) -> Self {
        self.config.scheduler_interval = interval;
        self
    }

    /// Set the retrier interval.
    pub fn retrier_interval(mut self, interval: Duration) -> Self {
        self.config.retrier_interval = interval;
        self
    }

    /// Set the batch size for scheduler and retrier.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set the graceful shutdown timeout.
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.config.shutdown_timeout = timeout;
        self
    }

    /// Build the WorkerConfig.
    pub fn build(self) -> WorkerConfig {
        self.config
    }
}

impl WorkerConfig {
    /// Create a new builder.
    pub fn builder() -> WorkerConfigBuilder {
        WorkerConfigBuilder::new()
    }
}
