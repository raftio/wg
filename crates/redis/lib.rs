//! Redis backend for wg job queue.
//!
//! This crate provides a Redis-based storage backend for the wg job queue.
//!
//! ## Usage
//!
//! ```rust,no_run
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

/// Table prefix for all wg-created keys.
const WG_TABLE_PREFIX: &str = "_wg_tb_";

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
        format!("{}{}:jobs", WG_TABLE_PREFIX, self.namespace)
    }

    /// Key for the scheduled jobs sorted set (ZSET).
    pub fn schedule(&self) -> String {
        format!("{}{}:schedule", WG_TABLE_PREFIX, self.namespace)
    }

    /// Key for the retry queue sorted set (ZSET).
    pub fn retry(&self) -> String {
        format!("{}{}:retry", WG_TABLE_PREFIX, self.namespace)
    }

    /// Key for the dead letter queue (LIST).
    pub fn dead(&self) -> String {
        format!("{}{}:dead", WG_TABLE_PREFIX, self.namespace)
    }

    /// Key for the set of registered worker pools (SET).
    pub fn worker_pools(&self) -> String {
        format!("{}{}:worker_pools", WG_TABLE_PREFIX, self.namespace)
    }

    /// Key for a worker pool's heartbeat data (HASH).
    pub fn heartbeat(&self, pool_id: &str) -> String {
        format!("{}{}:heartbeat:{}", WG_TABLE_PREFIX, self.namespace, pool_id)
    }

    /// Key for a worker pool's in-progress jobs (ZSET, scored by timestamp).
    pub fn in_progress(&self, pool_id: &str) -> String {
        format!("{}{}:in_progress:{}", WG_TABLE_PREFIX, self.namespace, pool_id)
    }

    /// Key for a job type's max concurrency setting.
    pub fn concurrency(&self, job_name: &str) -> String {
        format!("{}{}:concurrency:{}", WG_TABLE_PREFIX, self.namespace, job_name)
    }

    /// Key for a job type's current in-flight count.
    pub fn inflight(&self, job_name: &str) -> String {
        format!("{}{}:inflight:{}", WG_TABLE_PREFIX, self.namespace, job_name)
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

    // ========== Concurrency Control ==========

    async fn set_job_concurrency(&self, job_name: &str, max: usize) -> Result<()> {
        let mut conn = self.conn.clone();
        let key = self.keys.concurrency(job_name);

        conn.set::<_, _, ()>(&key, max)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn try_acquire_concurrency(&self, job_name: &str) -> Result<bool> {
        let mut conn = self.conn.clone();
        let max_key = self.keys.concurrency(job_name);
        let inflight_key = self.keys.inflight(job_name);

        // Lua script for atomic check-and-increment
        let script = redis::Script::new(
            r#"
            local max = tonumber(redis.call('GET', KEYS[1]) or '0')
            local current = tonumber(redis.call('GET', KEYS[2]) or '0')
            if max == 0 or current < max then
                redis.call('INCR', KEYS[2])
                return 1
            end
            return 0
            "#,
        );

        let result: i32 = script
            .key(&max_key)
            .key(&inflight_key)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;

        Ok(result == 1)
    }

    async fn release_concurrency(&self, job_name: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let inflight_key = self.keys.inflight(job_name);

        // Decrement but don't go below 0
        let script = redis::Script::new(
            r#"
            local current = tonumber(redis.call('GET', KEYS[1]) or '0')
            if current > 0 then
                redis.call('DECR', KEYS[1])
            end
            return current
            "#,
        );

        script
            .key(&inflight_key)
            .invoke_async::<i32>(&mut conn)
            .await
            .map_err(|e| WgError::Backend(e.to_string()))?;

        Ok(())
    }

    async fn push_job_front(&self, job_json: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        // RPUSH puts at the end, but BRPOP pops from end,
        // so RPUSH is like pushing to front for BRPOP
        conn.rpush::<_, _, ()>(self.keys.jobs(), job_json)
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
        assert_eq!(keys.jobs(), "_wg_tb_myapp:jobs");
        assert_eq!(keys.schedule(), "_wg_tb_myapp:schedule");
        assert_eq!(keys.retry(), "_wg_tb_myapp:retry");
        assert_eq!(keys.dead(), "_wg_tb_myapp:dead");
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
        assert_eq!(keys.jobs(), "_wg_tb_staging:jobs");
    }

    #[test]
    fn test_redis_keys_empty_namespace() {
        let keys = RedisKeys::new("");
        assert_eq!(keys.jobs(), "_wg_tb_:jobs");
        assert_eq!(keys.schedule(), "_wg_tb_:schedule");
    }

    #[test]
    fn test_redis_keys_complex_namespace() {
        let keys = RedisKeys::new("app:v2:queue");
        assert_eq!(keys.jobs(), "_wg_tb_app:v2:queue:jobs");
        assert_eq!(keys.schedule(), "_wg_tb_app:v2:queue:schedule");
        assert_eq!(keys.retry(), "_wg_tb_app:v2:queue:retry");
        assert_eq!(keys.dead(), "_wg_tb_app:v2:queue:dead");
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
        assert_eq!(keys.worker_pools(), "_wg_tb_myapp:worker_pools");
        assert_eq!(keys.heartbeat("pool-1"), "_wg_tb_myapp:heartbeat:pool-1");
        assert_eq!(keys.in_progress("pool-1"), "_wg_tb_myapp:in_progress:pool-1");
    }

    #[test]
    fn test_redis_keys_concurrency() {
        let keys = RedisKeys::new("myapp");
        assert_eq!(keys.concurrency("send_email"), "_wg_tb_myapp:concurrency:send_email");
        assert_eq!(keys.inflight("send_email"), "_wg_tb_myapp:inflight:send_email");
    }
}

// ========== Integration Tests (require Redis) ==========

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::sync::Arc;

    fn redis_url() -> String {
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
    }

    fn test_namespace() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("wg_test_{}", ts)
    }

    async fn cleanup_redis(backend: &RedisBackend) {
        while backend
            .pop_job(Duration::from_secs(1))
            .await
            .unwrap()
            .is_some()
        {}
    }

    #[tokio::test]
    #[ignore = "requires running Redis server"]
    async fn test_concurrency_set_and_acquire() {
        let backend = RedisBackend::new(&redis_url(), &test_namespace())
            .await
            .expect("Failed to connect to Redis");

        let job_name = "test_job";

        // Set max concurrency to 3
        backend.set_job_concurrency(job_name, 3).await.unwrap();

        // Should acquire 3 times
        assert!(backend.try_acquire_concurrency(job_name).await.unwrap());
        assert!(backend.try_acquire_concurrency(job_name).await.unwrap());
        assert!(backend.try_acquire_concurrency(job_name).await.unwrap());

        // 4th should fail
        assert!(!backend.try_acquire_concurrency(job_name).await.unwrap());

        // Cleanup
        for _ in 0..3 {
            backend.release_concurrency(job_name).await.ok();
        }
    }

    #[tokio::test]
    #[ignore = "requires running Redis server"]
    async fn test_concurrency_release() {
        let backend = RedisBackend::new(&redis_url(), &test_namespace())
            .await
            .expect("Failed to connect to Redis");

        let job_name = "release_test";

        backend.set_job_concurrency(job_name, 2).await.unwrap();

        // Acquire 2
        assert!(backend.try_acquire_concurrency(job_name).await.unwrap());
        assert!(backend.try_acquire_concurrency(job_name).await.unwrap());
        assert!(!backend.try_acquire_concurrency(job_name).await.unwrap());

        // Release one
        backend.release_concurrency(job_name).await.unwrap();

        // Should acquire again
        assert!(backend.try_acquire_concurrency(job_name).await.unwrap());

        // Cleanup
        for _ in 0..2 {
            backend.release_concurrency(job_name).await.ok();
        }
    }

    #[tokio::test]
    #[ignore = "requires running Redis server"]
    async fn test_concurrency_unlimited() {
        let backend = RedisBackend::new(&redis_url(), &test_namespace())
            .await
            .expect("Failed to connect to Redis");

        let job_name = "unlimited";

        // max=0 means unlimited
        backend.set_job_concurrency(job_name, 0).await.unwrap();

        for _ in 0..50 {
            assert!(backend.try_acquire_concurrency(job_name).await.unwrap());
        }

        // Cleanup
        for _ in 0..50 {
            backend.release_concurrency(job_name).await.ok();
        }
    }

    #[tokio::test]
    #[ignore = "requires running Redis server"]
    async fn test_concurrent_acquire_atomicity() {
        let backend = Arc::new(
            RedisBackend::new(&redis_url(), &test_namespace())
                .await
                .expect("Failed to connect to Redis"),
        );

        let job_name = "atomic_test";
        backend.set_job_concurrency(job_name, 5).await.unwrap();

        // Spawn 20 concurrent tasks trying to acquire
        let mut handles = Vec::new();
        for _ in 0..20 {
            let backend = Arc::clone(&backend);
            let job_name = job_name.to_string();
            handles.push(tokio::spawn(async move {
                backend.try_acquire_concurrency(&job_name).await.unwrap()
            }));
        }

        let mut acquired = 0;
        for handle in handles {
            if handle.await.unwrap() {
                acquired += 1;
            }
        }

        // Exactly 5 should have acquired (the limit)
        assert_eq!(acquired, 5, "Expected exactly 5 acquires, got {}", acquired);

        // Cleanup
        for _ in 0..5 {
            backend.release_concurrency(job_name).await.ok();
        }
    }

    #[tokio::test]
    #[ignore = "requires running Redis server"]
    async fn test_push_job_front() {
        let backend = RedisBackend::new(&redis_url(), &test_namespace())
            .await
            .expect("Failed to connect to Redis");

        // Push normally
        backend.push_job(r#"{"id":"job1"}"#).await.unwrap();
        backend.push_job(r#"{"id":"job2"}"#).await.unwrap();

        // Push to front
        backend.push_job_front(r#"{"id":"urgent"}"#).await.unwrap();

        // Pop should get urgent first (use 1s timeout, as_secs truncates ms)
        let first = backend
            .pop_job(Duration::from_secs(1))
            .await
            .unwrap()
            .unwrap();
        assert!(first.contains("urgent"), "Expected urgent, got: {}", first);

        // Drain remaining
        backend.pop_job(Duration::from_secs(1)).await.ok();
        backend.pop_job(Duration::from_secs(1)).await.ok();
    }
}
