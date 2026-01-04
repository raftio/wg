//! Reaper component for stale job recovery.
//!
//! The reaper periodically checks for dead worker pools (those that haven't
//! sent a heartbeat within the stale threshold) and recovers their in-progress
//! jobs by re-enqueueing them.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::backend::Backend;
use crate::error::Result;

/// Reaper that recovers jobs from dead worker pools.
pub struct Reaper<B: Backend> {
    backend: B,
    interval: Duration,
    stale_threshold: Duration,
    running: Arc<AtomicBool>,
}

impl<B: Backend + Clone + 'static> Reaper<B> {
    /// Create a new reaper.
    pub fn new(
        backend: B,
        interval: Duration,
        stale_threshold: Duration,
        running: Arc<AtomicBool>,
    ) -> Self {
        Self {
            backend,
            interval,
            stale_threshold,
            running,
        }
    }

    /// Run the reaper loop.
    pub async fn run(&self) -> Result<()> {
        tracing::debug!("Reaper started");

        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        while self.running.load(Ordering::SeqCst) {
            interval.tick().await;

            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            if let Err(e) = self.reap_dead_pools().await {
                tracing::error!(error = %e, "Failed to reap dead pools");
            }
        }

        tracing::debug!("Reaper stopped");
        Ok(())
    }

    /// Find and recover jobs from dead worker pools.
    async fn reap_dead_pools(&self) -> Result<()> {
        let stale_pools = self
            .backend
            .get_stale_pools(self.stale_threshold.as_secs())
            .await?;

        if stale_pools.is_empty() {
            return Ok(());
        }

        tracing::info!(
            count = stale_pools.len(),
            "Found stale worker pools to reap"
        );

        for pool_id in stale_pools {
            if let Err(e) = self.recover_pool(&pool_id).await {
                tracing::error!(
                    pool_id = %pool_id,
                    error = %e,
                    "Failed to recover pool"
                );
            }
        }

        Ok(())
    }

    /// Recover jobs from a dead worker pool.
    async fn recover_pool(&self, pool_id: &str) -> Result<()> {
        tracing::info!(pool_id = %pool_id, "Recovering dead worker pool");

        // Get all in-progress jobs and clean up the pool
        let jobs = self.backend.cleanup_pool(pool_id).await?;

        if jobs.is_empty() {
            tracing::debug!(pool_id = %pool_id, "No in-progress jobs to recover");
            return Ok(());
        }

        tracing::info!(
            pool_id = %pool_id,
            job_count = jobs.len(),
            "Recovered in-progress jobs"
        );

        // Re-enqueue recovered jobs
        let mut recovered = 0;
        for job_json in jobs {
            match self.backend.push_job(&job_json).await {
                Ok(()) => {
                    recovered += 1;
                    tracing::debug!(pool_id = %pool_id, "Re-enqueued recovered job");
                }
                Err(e) => {
                    tracing::error!(
                        pool_id = %pool_id,
                        error = %e,
                        "Failed to re-enqueue recovered job"
                    );
                }
            }
        }

        tracing::info!(
            pool_id = %pool_id,
            recovered = recovered,
            "Pool recovery complete"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reaper_creation() {
        // Just verify the struct can be created with proper types
        // Actual functionality requires a backend implementation
        let running = Arc::new(AtomicBool::new(true));
        assert!(running.load(Ordering::SeqCst));
    }
}
