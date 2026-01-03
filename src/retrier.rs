//! Retrier for moving retry jobs back to the jobs queue.

use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

use crate::error::Result;
use crate::redis_keys::RedisKeys;

/// Retrier that moves jobs from the retry queue back to the jobs queue.
pub struct Retrier {
    conn: ConnectionManager,
    keys: RedisKeys,
    interval: Duration,
    batch_size: usize,
    running: Arc<AtomicBool>,
}

impl Retrier {
    /// Create a new Retrier.
    pub fn new(
        conn: ConnectionManager,
        keys: RedisKeys,
        interval: Duration,
        batch_size: usize,
        running: Arc<AtomicBool>,
    ) -> Self {
        Self {
            conn,
            keys,
            interval,
            batch_size,
            running,
        }
    }

    /// Run the retrier loop.
    ///
    /// This will continuously check the retry queue and move jobs
    /// whose retry time has passed back to the jobs queue.
    pub async fn run(&self) -> Result<()> {
        tracing::info!("Retrier started");
        
        while self.running.load(Ordering::SeqCst) {
            if let Err(e) = self.tick().await {
                tracing::error!(error = %e, "Retrier tick failed");
            }
            
            time::sleep(self.interval).await;
        }
        
        tracing::info!("Retrier stopped");
        Ok(())
    }

    /// Process one tick of the retrier.
    async fn tick(&self) -> Result<()> {
        let now = current_timestamp();
        let mut conn = self.conn.clone();
        
        // Get jobs that are due for retry
        let jobs: Vec<String> = conn
            .zrangebyscore_limit(
                self.keys.retry(),
                "-inf",
                now,
                0,
                self.batch_size as isize,
            )
            .await?;
        
        if jobs.is_empty() {
            return Ok(());
        }
        
        tracing::debug!(count = jobs.len(), "Moving retry jobs to queue");
        
        let retry_key = self.keys.retry();
        let jobs_key = self.keys.jobs();
        
        for job_json in jobs {
            // Remove from retry queue and add to jobs queue atomically
            let mut pipe = redis::pipe();
            pipe.atomic()
                .zrem(&retry_key, &job_json)
                .lpush(&jobs_key, &job_json);
            
            pipe.query_async::<()>(&mut conn).await?;
        }
        
        Ok(())
    }
}

/// Get current Unix timestamp in seconds.
fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

