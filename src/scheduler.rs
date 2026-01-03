//! Scheduler for moving scheduled jobs to the jobs queue.

use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

use crate::error::Result;
use crate::redis_keys::RedisKeys;

/// Scheduler that moves scheduled jobs to the jobs queue when their time comes.
pub struct Scheduler {
    conn: ConnectionManager,
    keys: RedisKeys,
    interval: Duration,
    batch_size: usize,
    running: Arc<AtomicBool>,
}

impl Scheduler {
    /// Create a new Scheduler.
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

    /// Run the scheduler loop.
    ///
    /// This will continuously check the schedule queue and move jobs
    /// whose scheduled time has passed to the jobs queue.
    pub async fn run(&self) -> Result<()> {
        tracing::info!("Scheduler started");
        
        while self.running.load(Ordering::SeqCst) {
            if let Err(e) = self.tick().await {
                tracing::error!(error = %e, "Scheduler tick failed");
            }
            
            time::sleep(self.interval).await;
        }
        
        tracing::info!("Scheduler stopped");
        Ok(())
    }

    /// Process one tick of the scheduler.
    async fn tick(&self) -> Result<()> {
        let now = current_timestamp();
        let mut conn = self.conn.clone();
        
        // Get jobs that are due to run
        let jobs: Vec<String> = conn
            .zrangebyscore_limit(
                self.keys.schedule(),
                "-inf",
                now,
                0,
                self.batch_size as isize,
            )
            .await?;
        
        if jobs.is_empty() {
            return Ok(());
        }
        
        tracing::debug!(count = jobs.len(), "Moving scheduled jobs to queue");
        
        let schedule_key = self.keys.schedule();
        let jobs_key = self.keys.jobs();
        
        for job_json in jobs {
            // Remove from schedule queue and add to jobs queue atomically
            // Using a pipeline for efficiency
            let mut pipe = redis::pipe();
            pipe.atomic()
                .zrem(&schedule_key, &job_json)
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

