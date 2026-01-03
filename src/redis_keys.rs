//! Redis key management with namespace support.

/// Manages Redis keys with a namespace prefix.
#[derive(Debug, Clone)]
pub struct RedisKeys {
    namespace: String,
}

impl RedisKeys {
    /// Create a new RedisKeys instance with the given namespace.
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
        }
    }

    /// Get the namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Key for the main jobs queue (LIST).
    /// Jobs ready to be processed are stored here.
    pub fn jobs(&self) -> String {
        format!("{}:jobs", self.namespace)
    }

    /// Key for the scheduled jobs sorted set (ZSET).
    /// Score is the Unix timestamp when the job should run.
    pub fn schedule(&self) -> String {
        format!("{}:schedule", self.namespace)
    }

    /// Key for the retry queue sorted set (ZSET).
    /// Score is the Unix timestamp when the job should be retried.
    pub fn retry(&self) -> String {
        format!("{}:retry", self.namespace)
    }

    /// Key for the dead letter queue (LIST).
    /// Jobs that have exhausted all retries are stored here.
    pub fn dead(&self) -> String {
        format!("{}:dead", self.namespace)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_keys() {
        let keys = RedisKeys::new("myapp");
        assert_eq!(keys.jobs(), "myapp:jobs");
        assert_eq!(keys.schedule(), "myapp:schedule");
        assert_eq!(keys.retry(), "myapp:retry");
        assert_eq!(keys.dead(), "myapp:dead");
    }
}

