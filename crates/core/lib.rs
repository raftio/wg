//! # wg-core - Core types and traits for job queue
//!
//! This crate provides the core abstractions for the wg job queue system:
//! - `Backend` trait for storage implementations
//! - `Job`, `JobId`, `JobOptions`, `JobStatus` types
//! - `Client` for enqueueing jobs
//! - `WorkerPool` for processing jobs
//! - Error types

mod backend;
mod client;
mod config;
mod error;
mod job;
mod retrier;
mod scheduler;
mod worker;

// Re-export main types
pub use backend::{Backend, DynBackend, SharedBackend};
pub use client::Client;
pub use config::{ClientConfig, WorkerConfig, WorkerConfigBuilder};
pub use error::{Result, WgError};
pub use job::{Job, JobId, JobOptions, JobStatus};
pub use worker::{JobError, JobResult, WorkerPool, WorkerPoolBuilder};
