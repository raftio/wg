//! # wg - Multi-Backend Job Queue Library
//!
//! A Rust library for building job queues with support for multiple storage
//! backends (Redis, PostgreSQL, MySQL, SQLite), scheduling, retries, and graceful shutdown.
//!
//! ## Features
//!
//! - **Multiple backends**: Redis, PostgreSQL, MySQL, SQLite
//! - **Immediate job processing**: Enqueue jobs for immediate execution
//! - **Scheduled jobs**: Schedule jobs to run at a future time
//! - **Automatic retries**: Configurable retry logic with exponential backoff
//! - **Dead letter queue**: Failed jobs are moved to a dead queue for inspection
//! - **Graceful shutdown**: Complete in-progress jobs before stopping
//! - **Multiple workers**: Process jobs in parallel with configurable concurrency
//!
//! ## Quick Start
//!
//! ### Client (Enqueuing Jobs)
//!
//! ```rust,no_run
//! use wg::Client;
//! use serde::{Serialize, Deserialize};
//! use std::time::Duration;
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyJob {
//!     message: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> wg::Result<()> {
//!     // Using Redis backend
//!     let client = Client::connect("redis://localhost", "myapp").await?;
//!     
//!     // Enqueue for immediate processing
//!     client.enqueue(MyJob { message: "hello".into() }).await?;
//!     
//!     // Schedule for later
//!     client.schedule(MyJob { message: "later".into() }, Duration::from_secs(60)).await?;
//!     
//!     Ok(())
//! }
//! ```
//!
//! ### Worker (Processing Jobs)
//!
//! ```rust,no_run
//! use wg::{WorkerPool, JobResult, JobError};
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct MyJob {
//!     message: String,
//! }
//!
//! async fn process_job(job: MyJob) -> JobResult {
//!     println!("Processing: {}", job.message);
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() -> wg::Result<()> {
//!     let mut pool = WorkerPool::builder()
//!         .redis_url("redis://localhost")
//!         .namespace("myapp")
//!         .workers(4)
//!         .handler(process_job)
//!         .build()
//!         .await?;
//!     
//!     pool.run().await?;
//!     Ok(())
//! }
//! ```

pub mod backend;
pub mod client;
pub mod config;
pub mod error;
pub mod job;
pub mod retrier;
pub mod scheduler;
pub mod worker;

// Re-export main types
pub use backend::{Backend, SharedBackend};
pub use client::Client;
pub use config::{ClientConfig, WorkerConfig, WorkerConfigBuilder};
pub use error::{Result, WgError};
pub use job::{Job, JobId, JobOptions, JobStatus};
pub use worker::{JobError, JobResult, WorkerPool, WorkerPoolBuilder};

// Re-export backend implementations
#[cfg(feature = "redis")]
pub use backend::RedisBackend;

#[cfg(feature = "postgres")]
pub use backend::PostgresBackend;

#[cfg(feature = "mysql")]
pub use backend::MySqlBackend;

#[cfg(feature = "sqlite")]
pub use backend::SqliteBackend;
