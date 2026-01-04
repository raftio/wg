//! Error types for the wg job queue library.

use thiserror::Error;

/// The main error type for the wg library.
#[derive(Error, Debug)]
pub enum WgError {
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

    /// Backend-specific error.
    #[error("Backend error: {0}")]
    Backend(String),
}

/// Result type alias using WgError.
pub type Result<T> = std::result::Result<T, WgError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_serialization() {
        let json_err: serde_json::Error = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = WgError::Serialization(json_err);
        let display = format!("{}", err);
        assert!(display.starts_with("Serialization error:"));
    }

    #[test]
    fn test_error_display_job_processing() {
        let err = WgError::JobProcessing("handler failed".to_string());
        assert_eq!(format!("{}", err), "Job processing error: handler failed");
    }

    #[test]
    fn test_error_display_worker_pool() {
        let err = WgError::WorkerPool("pool exhausted".to_string());
        assert_eq!(format!("{}", err), "Worker pool error: pool exhausted");
    }

    #[test]
    fn test_error_display_config() {
        let err = WgError::Config("invalid url".to_string());
        assert_eq!(format!("{}", err), "Configuration error: invalid url");
    }

    #[test]
    fn test_error_display_job_not_found() {
        let err = WgError::JobNotFound("abc-123".to_string());
        assert_eq!(format!("{}", err), "Job not found: abc-123");
    }

    #[test]
    fn test_error_display_timeout() {
        let err = WgError::Timeout("operation timed out".to_string());
        assert_eq!(format!("{}", err), "Timeout: operation timed out");
    }

    #[test]
    fn test_error_display_backend() {
        let err = WgError::Backend("connection refused".to_string());
        assert_eq!(format!("{}", err), "Backend error: connection refused");
    }

    #[test]
    fn test_error_from_serde_json() {
        let json_err: serde_json::Error = serde_json::from_str::<i32>("not a number").unwrap_err();
        let err: WgError = json_err.into();
        assert!(matches!(err, WgError::Serialization(_)));
    }

    #[test]
    fn test_error_debug() {
        let err = WgError::Backend("test".to_string());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Backend"));
        assert!(debug.contains("test"));
    }
}
