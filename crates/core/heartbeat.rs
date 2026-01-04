//! Heartbeat component for worker pool monitoring.
//!
//! The heartbeater periodically sends heartbeat signals to the backend,
//! allowing the system to track active worker pools and detect dead ones.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::backend::{Backend, WorkerPoolInfo};
use crate::error::Result;

/// Heartbeater that periodically updates the worker pool's heartbeat.
pub struct Heartbeater<B: Backend> {
    backend: B,
    pool_id: String,
    concurrency: usize,
    job_names: Vec<String>,
    interval: Duration,
    running: Arc<AtomicBool>,
    started_at: i64,
    host: String,
    pid: u32,
}

impl<B: Backend + Clone + 'static> Heartbeater<B> {
    /// Create a new heartbeater.
    pub fn new(
        backend: B,
        pool_id: String,
        concurrency: usize,
        job_names: Vec<String>,
        interval: Duration,
        running: Arc<AtomicBool>,
    ) -> Self {
        let host = hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        let pid = std::process::id();
        let started_at = current_timestamp();

        Self {
            backend,
            pool_id,
            concurrency,
            job_names,
            interval,
            running,
            started_at,
            host,
            pid,
        }
    }

    /// Run the heartbeat loop.
    ///
    /// This sends an initial heartbeat immediately, then continues
    /// sending heartbeats at the configured interval until stopped.
    pub async fn run(&self) -> Result<()> {
        tracing::debug!(pool_id = %self.pool_id, "Heartbeater started");

        // Send initial heartbeat immediately
        self.send_heartbeat().await?;

        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        while self.running.load(Ordering::SeqCst) {
            interval.tick().await;

            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            if let Err(e) = self.send_heartbeat().await {
                tracing::error!(
                    pool_id = %self.pool_id,
                    error = %e,
                    "Failed to send heartbeat"
                );
            }
        }

        // Remove heartbeat on shutdown
        if let Err(e) = self.backend.remove_heartbeat(&self.pool_id).await {
            tracing::error!(
                pool_id = %self.pool_id,
                error = %e,
                "Failed to remove heartbeat on shutdown"
            );
        }

        tracing::debug!(pool_id = %self.pool_id, "Heartbeater stopped");
        Ok(())
    }

    /// Send a single heartbeat.
    async fn send_heartbeat(&self) -> Result<()> {
        let info = WorkerPoolInfo {
            pool_id: self.pool_id.clone(),
            heartbeat_at: current_timestamp(),
            started_at: self.started_at,
            concurrency: self.concurrency,
            host: self.host.clone(),
            pid: self.pid,
            job_names: self.job_names.clone(),
        };

        self.backend.heartbeat(&info).await?;

        tracing::trace!(
            pool_id = %self.pool_id,
            heartbeat_at = info.heartbeat_at,
            "Heartbeat sent"
        );

        Ok(())
    }
}

/// Generate a unique pool ID.
pub fn generate_pool_id() -> String {
    let host = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let pid = std::process::id();
    let ts = current_timestamp();
    format!("{}-{}-{}", host, pid, ts)
}

/// Get current Unix timestamp in seconds.
fn current_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_pool_id() {
        let id1 = generate_pool_id();
        let id2 = generate_pool_id();

        // IDs should contain host and pid
        assert!(id1.contains('-'));

        // Two IDs generated in quick succession may be the same or different
        // depending on timing, so we just check they're valid
        assert!(!id1.is_empty());
        assert!(!id2.is_empty());
    }

    #[test]
    fn test_current_timestamp() {
        let ts = current_timestamp();
        // Should be a reasonable Unix timestamp (after year 2020)
        assert!(ts > 1577836800);
    }
}
