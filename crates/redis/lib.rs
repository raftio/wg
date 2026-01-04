//! Redis backend for wg job queue.
//!
//! This crate provides a Redis-based storage backend for the wg job queue.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use wg_redis::RedisBackend;
//! use wg_core::Client;
//!
//! #[tokio::main]
//! async fn main() -> wg_core::Result<()> {
//!     let backend = RedisBackend::new("redis://localhost", "myapp").await?;
//!     let client = Client::new(backend);
//!     Ok(())
//! }
//! ```

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::time::Duration;
use wg_core::{Backend, Result, WgError};

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
    pub fn jobs(&self) -> String {
        format!("{}:jobs", self.namespace)
    }

    /// Key for the scheduled jobs sorted set (ZSET).
    pub fn schedule(&self) -> String {
        format!("{}:schedule", self.namespace)
    }

    /// Key for the retry queue sorted set (ZSET).
    pub fn retry(&self) -> String {
        format!("{}:retry", self.namespace)
    }

    /// Key for the dead letter queue (LIST).
    pub fn dead(&self) -> String {
        format!("{}:dead", self.namespace)
    }
}

/// Redis backend for job queue storage.
#[derive(Clone)]
pub struct RedisBackend {
    conn: ConnectionManager,
    keys: RedisKeys,
}

impl RedisBackend {
    /// Create a new Redis backend.
    pub async fn new(redis_url: &str, namespace: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| WgError::Backend(e.to_string()))?;
        let conn = ConnectionManager::new(client)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        let keys = RedisKeys::new(namespace);
        Ok(Self { conn, keys })
    }

    /// Create a new Redis backend with an existing connection manager.
    pub fn with_connection(conn: ConnectionManager, namespace: &str) -> Self {
        Self {
            conn,
            keys: RedisKeys::new(namespace),
        }
    }

    /// Get the Redis keys manager.
    pub fn keys(&self) -> &RedisKeys {
        &self.keys
    }
}

#[async_trait]
impl Backend for RedisBackend {
    async fn push_job(&self, job_json: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.lpush::<_, _, ()>(self.keys.jobs(), job_json)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn pop_job(&self, timeout: Duration) -> Result<Option<String>> {
        let mut conn = self.conn.clone();
        let result: Option<(String, String)> = conn
            .brpop(self.keys.jobs(), timeout.as_secs() as f64)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(result.map(|(_, json)| json))
    }

    async fn schedule_job(&self, job_json: &str, run_at: i64) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.zadd::<_, _, _, ()>(self.keys.schedule(), job_json, run_at)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn get_due_scheduled(&self, now: i64, limit: usize) -> Result<Vec<String>> {
        let mut conn = self.conn.clone();
        let jobs: Vec<String> = conn
            .zrangebyscore_limit(self.keys.schedule(), "-inf", now, 0, limit as isize)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(jobs)
    }

    async fn remove_scheduled(&self, job_json: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.zrem::<_, _, ()>(self.keys.schedule(), job_json)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn retry_job(&self, job_json: &str, retry_at: i64) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.zadd::<_, _, _, ()>(self.keys.retry(), job_json, retry_at)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn get_due_retries(&self, now: i64, limit: usize) -> Result<Vec<String>> {
        let mut conn = self.conn.clone();
        let jobs: Vec<String> = conn
            .zrangebyscore_limit(self.keys.retry(), "-inf", now, 0, limit as isize)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(jobs)
    }

    async fn remove_retry(&self, job_json: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.zrem::<_, _, ()>(self.keys.retry(), job_json)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn push_dead(&self, job_json: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.lpush::<_, _, ()>(self.keys.dead(), job_json)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn queue_len(&self) -> Result<usize> {
        let mut conn = self.conn.clone();
        let len: usize = conn
            .llen(self.keys.jobs())
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(len)
    }

    async fn schedule_len(&self) -> Result<usize> {
        let mut conn = self.conn.clone();
        let len: usize = conn
            .zcard(self.keys.schedule())
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(len)
    }

    async fn retry_len(&self) -> Result<usize> {
        let mut conn = self.conn.clone();
        let len: usize = conn
            .zcard(self.keys.retry())
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(len)
    }

    async fn dead_len(&self) -> Result<usize> {
        let mut conn = self.conn.clone();
        let len: usize = conn
            .llen(self.keys.dead())
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(len)
    }

    async fn list_dead(&self, limit: usize, offset: usize) -> Result<Vec<String>> {
        let mut conn = self.conn.clone();
        let jobs: Vec<String> = conn
            .lrange(
                self.keys.dead(),
                offset as isize,
                (offset + limit - 1) as isize,
            )
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(jobs)
    }

    async fn get_dead_by_id(&self, job_id: &str) -> Result<Option<String>> {
        let mut conn = self.conn.clone();
        // Scan through the dead queue to find the job by ID
        let jobs: Vec<String> = conn
            .lrange(self.keys.dead(), 0, -1)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;

        for job_json in jobs {
            if job_json.contains(job_id) {
                // Verify it's the actual job ID by parsing
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&job_json) {
                    if let Some(id) = parsed["id"]["0"].as_str() {
                        if id == job_id {
                            return Ok(Some(job_json));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    async fn remove_dead(&self, job_json: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.lrem::<_, _, ()>(self.keys.dead(), 1, job_json)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn move_scheduled_to_queue(&self, job_json: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let schedule_key = self.keys.schedule();
        let jobs_key = self.keys.jobs();

        // Use pipeline for atomicity
        let mut pipe = redis::pipe();
        pipe.atomic()
            .zrem(&schedule_key, job_json)
            .lpush(&jobs_key, job_json);

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn move_retry_to_queue(&self, job_json: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let retry_key = self.keys.retry();
        let jobs_key = self.keys.jobs();

        // Use pipeline for atomicity
        let mut pipe = redis::pipe();
        pipe.atomic()
            .zrem(&retry_key, job_json)
            .lpush(&jobs_key, job_json);

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
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

    #[test]
    fn test_redis_keys_namespace() {
        let keys = RedisKeys::new("production");
        assert_eq!(keys.namespace(), "production");
    }

    #[test]
    fn test_redis_keys_with_string() {
        let keys = RedisKeys::new(String::from("staging"));
        assert_eq!(keys.namespace(), "staging");
        assert_eq!(keys.jobs(), "staging:jobs");
    }

    #[test]
    fn test_redis_keys_empty_namespace() {
        let keys = RedisKeys::new("");
        assert_eq!(keys.jobs(), ":jobs");
        assert_eq!(keys.schedule(), ":schedule");
    }

    #[test]
    fn test_redis_keys_complex_namespace() {
        let keys = RedisKeys::new("app:v2:queue");
        assert_eq!(keys.jobs(), "app:v2:queue:jobs");
        assert_eq!(keys.schedule(), "app:v2:queue:schedule");
        assert_eq!(keys.retry(), "app:v2:queue:retry");
        assert_eq!(keys.dead(), "app:v2:queue:dead");
    }

    #[test]
    fn test_redis_keys_clone() {
        let keys1 = RedisKeys::new("test");
        let keys2 = keys1.clone();
        assert_eq!(keys1.jobs(), keys2.jobs());
        assert_eq!(keys1.namespace(), keys2.namespace());
    }

    #[test]
    fn test_redis_keys_debug() {
        let keys = RedisKeys::new("debug_test");
        let debug = format!("{:?}", keys);
        assert!(debug.contains("RedisKeys"));
        assert!(debug.contains("debug_test"));
    }
}
