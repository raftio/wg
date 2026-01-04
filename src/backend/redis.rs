//! Redis backend implementation.

use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::time::Duration;

use crate::backend::Backend;
use crate::error::Result;

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
        let client = redis::Client::open(redis_url)?;
        let conn = ConnectionManager::new(client).await?;
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
        conn.lpush::<_, _, ()>(self.keys.jobs(), job_json).await?;
        Ok(())
    }

    async fn pop_job(&self, timeout: Duration) -> Result<Option<String>> {
        let mut conn = self.conn.clone();
        let result: Option<(String, String)> = conn
            .brpop(self.keys.jobs(), timeout.as_secs() as f64)
            .await?;
        Ok(result.map(|(_, json)| json))
    }

    async fn schedule_job(&self, job_json: &str, run_at: i64) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.zadd::<_, _, _, ()>(self.keys.schedule(), job_json, run_at)
            .await?;
        Ok(())
    }

    async fn get_due_scheduled(&self, now: i64, limit: usize) -> Result<Vec<String>> {
        let mut conn = self.conn.clone();
        let jobs: Vec<String> = conn
            .zrangebyscore_limit(self.keys.schedule(), "-inf", now, 0, limit as isize)
            .await?;
        Ok(jobs)
    }

    async fn remove_scheduled(&self, job_json: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.zrem::<_, _, ()>(self.keys.schedule(), job_json)
            .await?;
        Ok(())
    }

    async fn retry_job(&self, job_json: &str, retry_at: i64) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.zadd::<_, _, _, ()>(self.keys.retry(), job_json, retry_at)
            .await?;
        Ok(())
    }

    async fn get_due_retries(&self, now: i64, limit: usize) -> Result<Vec<String>> {
        let mut conn = self.conn.clone();
        let jobs: Vec<String> = conn
            .zrangebyscore_limit(self.keys.retry(), "-inf", now, 0, limit as isize)
            .await?;
        Ok(jobs)
    }

    async fn remove_retry(&self, job_json: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.zrem::<_, _, ()>(self.keys.retry(), job_json).await?;
        Ok(())
    }

    async fn push_dead(&self, job_json: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        conn.lpush::<_, _, ()>(self.keys.dead(), job_json).await?;
        Ok(())
    }

    async fn queue_len(&self) -> Result<usize> {
        let mut conn = self.conn.clone();
        let len: usize = conn.llen(self.keys.jobs()).await?;
        Ok(len)
    }

    async fn schedule_len(&self) -> Result<usize> {
        let mut conn = self.conn.clone();
        let len: usize = conn.zcard(self.keys.schedule()).await?;
        Ok(len)
    }

    async fn retry_len(&self) -> Result<usize> {
        let mut conn = self.conn.clone();
        let len: usize = conn.zcard(self.keys.retry()).await?;
        Ok(len)
    }

    async fn dead_len(&self) -> Result<usize> {
        let mut conn = self.conn.clone();
        let len: usize = conn.llen(self.keys.dead()).await?;
        Ok(len)
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

        pipe.query_async::<()>(&mut conn).await?;
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

        pipe.query_async::<()>(&mut conn).await?;
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
}
