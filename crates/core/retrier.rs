//! Retrier for moving retry jobs back to the jobs queue.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;

use crate::backend::Backend;
use crate::error::Result;

/// Retrier that moves jobs from the retry queue back to the jobs queue.
pub struct Retrier<B: Backend> {
    backend: B,
    interval: Duration,
    batch_size: usize,
    running: Arc<AtomicBool>,
}

impl<B: Backend> Retrier<B> {
    /// Create a new Retrier.
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

        // Get jobs that are due for retry
        let jobs = self.backend.get_due_retries(now, self.batch_size).await?;

        if jobs.is_empty() {
            return Ok(());
        }

        tracing::debug!(count = jobs.len(), "Moving retry jobs to queue");

        for job_json in jobs {
            self.backend.move_retry_to_queue(&job_json).await?;
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
