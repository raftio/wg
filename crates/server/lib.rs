//! # wg-server - HTTP API and Worker Pool Server
//!
//! This crate provides a combined server that runs both the worker pool
//! for processing jobs and an HTTP API for monitoring and management.
//!
//! ## Features
//!
//! - **Worker Pool**: Runs configurable number of workers to process jobs
//! - **HTTP API**: Provides endpoints for:
//!   - Health check (`GET /health`)
//!   - Queue statistics (`GET /api/stats`)
//!   - Enqueue jobs (`POST /api/jobs`)
//!   - List dead jobs (`GET /api/dead`)
//!   - Retry dead jobs (`POST /api/dead/:id/retry`)
//!   - Delete dead jobs (`DELETE /api/dead/:id`)
//!
//! ## Usage
//!
//! ```rust,ignore
//! use wg_server::{Server, ServerConfig};
//! use wg_redis::RedisBackend;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct MyJob {
//!     data: String,
//! }
//!
//! async fn process_job(job: MyJob) -> wg_core::JobResult {
//!     println!("Processing: {}", job.data);
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() -> wg_core::Result<()> {
//!     let backend = RedisBackend::new("redis://localhost", "myapp").await?;
//!     
//!     let config = ServerConfig::builder()
//!         .api_addr_str("0.0.0.0:8080")?
//!         .namespace("myapp")
//!         .num_workers(4)
//!         .build();
//!
//!     let server = Server::new(config, backend, process_job);
//!     server.run().await?;
//!     
//!     Ok(())
//! }
//! ```

mod api;
mod config;
mod server;

pub use config::{ServerConfig, ServerConfigBuilder};
pub use server::{Server, ServerBuilder};


