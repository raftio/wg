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
    /// Interval for heartbeat updates.
    pub heartbeat_interval: Duration,
    /// Interval for the reaper to check for dead workers.
    pub reaper_interval: Duration,
    /// Threshold for considering a worker pool stale (no heartbeat).
    pub stale_threshold: Duration,
    /// Optional custom pool ID (auto-generated if None).
    pub pool_id: Option<String>,
    /// Whether to enable the reaper for stale job recovery.
    pub enable_reaper: bool,
    /// Job type names this worker pool handles (for monitoring).
    pub job_names: Vec<String>,
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
            heartbeat_interval: Duration::from_secs(5),
            reaper_interval: Duration::from_secs(30),
            stale_threshold: Duration::from_secs(60),
            pool_id: None,
            enable_reaper: true,
            job_names: Vec::new(),
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

    /// Set the heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Set the reaper interval.
    pub fn reaper_interval(mut self, interval: Duration) -> Self {
        self.config.reaper_interval = interval;
        self
    }

    /// Set the stale threshold for detecting dead workers.
    pub fn stale_threshold(mut self, threshold: Duration) -> Self {
        self.config.stale_threshold = threshold;
        self
    }

    /// Set a custom pool ID.
    pub fn pool_id(mut self, id: impl Into<String>) -> Self {
        self.config.pool_id = Some(id.into());
        self
    }

    /// Enable or disable the reaper for stale job recovery.
    pub fn enable_reaper(mut self, enable: bool) -> Self {
        self.config.enable_reaper = enable;
        self
    }

    /// Set the job type names this worker pool handles.
    pub fn job_names(mut self, names: Vec<String>) -> Self {
        self.config.job_names = names;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_config_new() {
        let config = ClientConfig::new("redis://localhost", "myapp");
        assert_eq!(config.url, "redis://localhost");
        assert_eq!(config.namespace, "myapp");
    }

    #[test]
    fn test_client_config_with_string() {
        let config = ClientConfig::new(String::from("postgres://db"), String::from("test"));
        assert_eq!(config.url, "postgres://db");
        assert_eq!(config.namespace, "test");
    }

    #[test]
    fn test_worker_config_default() {
        let config = WorkerConfig::default();
        assert_eq!(config.url, "");
        assert_eq!(config.namespace, "wg");
        assert_eq!(config.num_workers, 4);
        assert_eq!(config.fetch_timeout, Duration::from_secs(5));
        assert_eq!(config.scheduler_interval, Duration::from_secs(1));
        assert_eq!(config.retrier_interval, Duration::from_secs(1));
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(30));
        assert_eq!(config.heartbeat_interval, Duration::from_secs(5));
        assert_eq!(config.reaper_interval, Duration::from_secs(30));
        assert_eq!(config.stale_threshold, Duration::from_secs(60));
        assert!(config.pool_id.is_none());
        assert!(config.enable_reaper);
        assert!(config.job_names.is_empty());
    }

    #[test]
    fn test_worker_config_builder_defaults() {
        let config = WorkerConfigBuilder::new().build();
        assert_eq!(config.url, "");
        assert_eq!(config.namespace, "wg");
        assert_eq!(config.num_workers, 4);
    }

    #[test]
    fn test_worker_config_builder_fluent() {
        let config = WorkerConfig::builder()
            .url("redis://localhost:6379")
            .namespace("production")
            .num_workers(8)
            .fetch_timeout(Duration::from_secs(10))
            .scheduler_interval(Duration::from_millis(500))
            .retrier_interval(Duration::from_millis(500))
            .batch_size(50)
            .shutdown_timeout(Duration::from_secs(60))
            .build();

        assert_eq!(config.url, "redis://localhost:6379");
        assert_eq!(config.namespace, "production");
        assert_eq!(config.num_workers, 8);
        assert_eq!(config.fetch_timeout, Duration::from_secs(10));
        assert_eq!(config.scheduler_interval, Duration::from_millis(500));
        assert_eq!(config.retrier_interval, Duration::from_millis(500));
        assert_eq!(config.batch_size, 50);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_worker_config_builder_partial() {
        let config = WorkerConfig::builder()
            .namespace("custom")
            .num_workers(2)
            .build();

        // Modified values
        assert_eq!(config.namespace, "custom");
        assert_eq!(config.num_workers, 2);
        // Default values preserved
        assert_eq!(config.url, "");
        assert_eq!(config.fetch_timeout, Duration::from_secs(5));
    }
}
