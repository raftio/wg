//! Scheduler for moving scheduled jobs to the jobs queue.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

use crate::backend::Backend;
use crate::error::Result;

/// Scheduler that moves scheduled jobs to the jobs queue when their time comes.
pub struct Scheduler<B: Backend> {
    backend: B,
    interval: Duration,
    batch_size: usize,
    running: Arc<AtomicBool>,
}

impl<B: Backend> Scheduler<B> {
    /// Create a new Scheduler.
    pub fn new(
        backend: B,
        interval: Duration,
        batch_size: usize,
        running: Arc<AtomicBool>,
    ) -> Self {
        Self {
            backend,
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

        // Get jobs that are due to run
        let jobs = self.backend.get_due_scheduled(now, self.batch_size).await?;

        if jobs.is_empty() {
            return Ok(());
        }

        tracing::debug!(count = jobs.len(), "Moving scheduled jobs to queue");

        for job_json in jobs {
            self.backend.move_scheduled_to_queue(&job_json).await?;
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
