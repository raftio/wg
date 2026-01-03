//! Error types for the wg job queue library.

use thiserror::Error;

/// The main error type for the wg library.
#[derive(Error, Debug)]
pub enum WgError {
    /// Redis connection or operation error.
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    /// JSON serialization/deserialization error.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Job processing error with message.
    #[error("Job processing error: {0}")]
    JobProcessing(String),

    /// Worker pool error.
    #[error("Worker pool error: {0}")]
    WorkerPool(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Job not found.
    #[error("Job not found: {0}")]
    JobNotFound(String),

    /// Timeout error.
    #[error("Timeout: {0}")]
    Timeout(String),
}

/// Result type alias using WgError.
pub type Result<T> = std::result::Result<T, WgError>;

