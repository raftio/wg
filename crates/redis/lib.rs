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
use wg_core::{Backend, Result, WgError, WorkerPoolInfo};

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

    /// Key for the set of registered worker pools (SET).
    pub fn worker_pools(&self) -> String {
        format!("{}:worker_pools", self.namespace)
    }

    /// Key for a worker pool's heartbeat data (HASH).
    pub fn heartbeat(&self, pool_id: &str) -> String {
        format!("{}:heartbeat:{}", self.namespace, pool_id)
    }

    /// Key for a worker pool's in-progress jobs (ZSET, scored by timestamp).
    pub fn in_progress(&self, pool_id: &str) -> String {
        format!("{}:in_progress:{}", self.namespace, pool_id)
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

    // ========== Heartbeat Operations ==========

    async fn heartbeat(&self, info: &WorkerPoolInfo) -> Result<()> {
        let mut conn = self.conn.clone();
        let worker_pools_key = self.keys.worker_pools();
        let heartbeat_key = self.keys.heartbeat(&info.pool_id);

        // Serialize job_names as JSON array
        let job_names_json =
            serde_json::to_string(&info.job_names).unwrap_or_else(|_| "[]".to_string());

        // Use pipeline for atomicity
        let mut pipe = redis::pipe();
        pipe.atomic()
            .sadd(&worker_pools_key, &info.pool_id)
            .cmd("HSET")
            .arg(&heartbeat_key)
            .arg("heartbeat_at")
            .arg(info.heartbeat_at)
            .arg("started_at")
            .arg(info.started_at)
            .arg("concurrency")
            .arg(info.concurrency)
            .arg("host")
            .arg(&info.host)
            .arg("pid")
            .arg(info.pid)
            .arg("job_names")
            .arg(&job_names_json);

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn remove_heartbeat(&self, pool_id: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let worker_pools_key = self.keys.worker_pools();
        let heartbeat_key = self.keys.heartbeat(pool_id);
        let in_progress_key = self.keys.in_progress(pool_id);

        // Remove pool from set and delete heartbeat hash
        let mut pipe = redis::pipe();
        pipe.atomic()
            .srem(&worker_pools_key, pool_id)
            .del(&heartbeat_key)
            .del(&in_progress_key);

        pipe.query_async::<()>(&mut conn)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn list_worker_pools(&self) -> Result<Vec<WorkerPoolInfo>> {
        let mut conn = self.conn.clone();
        let worker_pools_key = self.keys.worker_pools();

        // Get all pool IDs
        let pool_ids: Vec<String> = conn
            .smembers(&worker_pools_key)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;

        let mut pools = Vec::with_capacity(pool_ids.len());

        for pool_id in pool_ids {
            let heartbeat_key = self.keys.heartbeat(&pool_id);

            // Get heartbeat data
            let data: Vec<(String, String)> = conn
                .hgetall(&heartbeat_key)
                .await
                .map_err(|e| WgError::Backend(e.to_string()))?;

            if data.is_empty() {
                continue;
            }

            // Parse the hash data
            let mut info = WorkerPoolInfo {
                pool_id: pool_id.clone(),
                heartbeat_at: 0,
                started_at: 0,
                concurrency: 0,
                host: String::new(),
                pid: 0,
                job_names: Vec::new(),
            };

            for (key, value) in data {
                match key.as_str() {
                    "heartbeat_at" => info.heartbeat_at = value.parse().unwrap_or(0),
                    "started_at" => info.started_at = value.parse().unwrap_or(0),
                    "concurrency" => info.concurrency = value.parse().unwrap_or(0),
                    "host" => info.host = value,
                    "pid" => info.pid = value.parse().unwrap_or(0),
                    "job_names" => {
                        info.job_names = serde_json::from_str(&value).unwrap_or_default()
                    }
                    _ => {}
                }
            }

            pools.push(info);
        }

        Ok(pools)
    }

    async fn get_stale_pools(&self, threshold_secs: u64) -> Result<Vec<String>> {
        let pools = self.list_worker_pools().await?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let stale_threshold = now - threshold_secs as i64;

        let stale_pools: Vec<String> = pools
            .into_iter()
            .filter(|p| p.heartbeat_at < stale_threshold)
            .map(|p| p.pool_id)
            .collect();

        Ok(stale_pools)
    }

    // ========== In-Progress Job Tracking ==========

    async fn mark_in_progress(&self, pool_id: &str, job_json: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let key = self.keys.in_progress(pool_id);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        conn.zadd::<_, _, _, ()>(&key, job_json, now)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn complete_in_progress(&self, pool_id: &str, job_json: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let key = self.keys.in_progress(pool_id);

        conn.zrem::<_, _, ()>(&key, job_json)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn get_in_progress_jobs(&self, pool_id: &str) -> Result<Vec<String>> {
        let mut conn = self.conn.clone();
        let key = self.keys.in_progress(pool_id);

        let jobs: Vec<String> = conn
            .zrange(&key, 0, -1)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(jobs)
    }

    async fn cleanup_pool(&self, pool_id: &str) -> Result<Vec<String>> {
        // First get all in-progress jobs
        let jobs = self.get_in_progress_jobs(pool_id).await?;

        // Then remove the heartbeat (which also cleans up in-progress)
        self.remove_heartbeat(pool_id).await?;

        Ok(jobs)
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

    #[test]
    fn test_redis_keys_heartbeat() {
        let keys = RedisKeys::new("myapp");
        assert_eq!(keys.worker_pools(), "myapp:worker_pools");
        assert_eq!(keys.heartbeat("pool-1"), "myapp:heartbeat:pool-1");
        assert_eq!(keys.in_progress("pool-1"), "myapp:in_progress:pool-1");
    }
}
