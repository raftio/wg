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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config_default() {
        let config = ServerConfig::default();
        assert_eq!(config.api_addr, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(config.num_workers, 4);
        assert_eq!(config.namespace, "wg");
        assert_eq!(config.fetch_timeout, Duration::from_secs(5));
        assert_eq!(config.scheduler_interval, Duration::from_secs(1));
        assert_eq!(config.retrier_interval, Duration::from_secs(1));
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_server_config_builder_defaults() {
        let config = ServerConfigBuilder::new().build();
        assert_eq!(config.api_addr, "127.0.0.1:8080".parse().unwrap());
        assert_eq!(config.num_workers, 4);
        assert_eq!(config.namespace, "wg");
    }

    #[test]
    fn test_server_config_builder_api_addr() {
        let addr: SocketAddr = "0.0.0.0:3000".parse().unwrap();
        let config = ServerConfig::builder().api_addr(addr).build();
        assert_eq!(config.api_addr, addr);
    }

    #[test]
    fn test_server_config_builder_api_addr_str_valid() {
        let config = ServerConfig::builder()
            .api_addr_str("192.168.1.1:9000")
            .unwrap()
            .build();
        assert_eq!(config.api_addr, "192.168.1.1:9000".parse().unwrap());
    }

    #[test]
    fn test_server_config_builder_api_addr_str_invalid() {
        let result = ServerConfig::builder().api_addr_str("not-an-address");
        assert!(result.is_err());
    }

    #[test]
    fn test_server_config_builder_namespace() {
        let config = ServerConfig::builder().namespace("production").build();
        assert_eq!(config.namespace, "production");
    }

    #[test]
    fn test_server_config_builder_num_workers() {
        let config = ServerConfig::builder().num_workers(16).build();
        assert_eq!(config.num_workers, 16);
    }

    #[test]
    fn test_server_config_builder_fetch_timeout() {
        let config = ServerConfig::builder()
            .fetch_timeout(Duration::from_secs(10))
            .build();
        assert_eq!(config.fetch_timeout, Duration::from_secs(10));
    }

    #[test]
    fn test_server_config_builder_scheduler_interval() {
        let config = ServerConfig::builder()
            .scheduler_interval(Duration::from_millis(500))
            .build();
        assert_eq!(config.scheduler_interval, Duration::from_millis(500));
    }

    #[test]
    fn test_server_config_builder_retrier_interval() {
        let config = ServerConfig::builder()
            .retrier_interval(Duration::from_millis(250))
            .build();
        assert_eq!(config.retrier_interval, Duration::from_millis(250));
    }

    #[test]
    fn test_server_config_builder_batch_size() {
        let config = ServerConfig::builder().batch_size(50).build();
        assert_eq!(config.batch_size, 50);
    }

    #[test]
    fn test_server_config_builder_shutdown_timeout() {
        let config = ServerConfig::builder()
            .shutdown_timeout(Duration::from_secs(60))
            .build();
        assert_eq!(config.shutdown_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_server_config_builder_fluent_chain() {
        let config = ServerConfig::builder()
            .api_addr_str("0.0.0.0:8080")
            .unwrap()
            .namespace("myapp")
            .num_workers(8)
            .fetch_timeout(Duration::from_secs(15))
            .scheduler_interval(Duration::from_secs(2))
            .retrier_interval(Duration::from_secs(2))
            .batch_size(200)
            .shutdown_timeout(Duration::from_secs(45))
            .build();

        assert_eq!(config.api_addr, "0.0.0.0:8080".parse().unwrap());
        assert_eq!(config.namespace, "myapp");
        assert_eq!(config.num_workers, 8);
        assert_eq!(config.fetch_timeout, Duration::from_secs(15));
        assert_eq!(config.scheduler_interval, Duration::from_secs(2));
        assert_eq!(config.retrier_interval, Duration::from_secs(2));
        assert_eq!(config.batch_size, 200);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(45));
    }

    #[test]
    fn test_server_config_clone() {
        let config1 = ServerConfig::builder()
            .namespace("clone_test")
            .num_workers(2)
            .build();
        let config2 = config1.clone();

        assert_eq!(config1.namespace, config2.namespace);
        assert_eq!(config1.num_workers, config2.num_workers);
    }

    #[test]
    fn test_server_config_debug() {
        let config = ServerConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("ServerConfig"));
        assert!(debug.contains("api_addr"));
    }

    #[test]
    fn test_server_config_builder_debug() {
        let builder = ServerConfigBuilder::new();
        let debug = format!("{:?}", builder);
        assert!(debug.contains("ServerConfigBuilder"));
    }
}
