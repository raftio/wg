//! Server configuration.

use std::net::SocketAddr;
use std::time::Duration;

/// Configuration for the wg server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Address to bind the API server to.
    pub api_addr: SocketAddr,
    /// Number of worker tasks to spawn.
    pub num_workers: usize,
    /// Namespace prefix for keys/tables.
    pub namespace: String,
    /// Timeout for job fetch operations.
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

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            api_addr: "127.0.0.1:8080".parse().unwrap(),
            num_workers: 4,
            namespace: "wg".to_string(),
            fetch_timeout: Duration::from_secs(5),
            scheduler_interval: Duration::from_secs(1),
            retrier_interval: Duration::from_secs(1),
            batch_size: 100,
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

/// Builder for ServerConfig.
#[derive(Debug, Default)]
pub struct ServerConfigBuilder {
    config: ServerConfig,
}

impl ServerConfigBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the API bind address.
    pub fn api_addr(mut self, addr: SocketAddr) -> Self {
        self.config.api_addr = addr;
        self
    }

    /// Set the API bind address from a string.
    pub fn api_addr_str(mut self, addr: &str) -> Result<Self, std::net::AddrParseError> {
        self.config.api_addr = addr.parse()?;
        Ok(self)
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

    /// Build the ServerConfig.
    pub fn build(self) -> ServerConfig {
        self.config
    }
}

impl ServerConfig {
    /// Create a new builder.
    pub fn builder() -> ServerConfigBuilder {
        ServerConfigBuilder::new()
    }
}
