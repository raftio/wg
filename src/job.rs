//! Job definition and related types.

use serde::{Deserialize, Serialize};
use std::time::Duration;
use uuid::Uuid;

/// Unique identifier for a job.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(pub Uuid);

impl JobId {
    /// Generate a new random JobId.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Options for job execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobOptions {
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// Current retry count.
    pub retry_count: u32,
    /// Timeout for job execution.
    #[serde(with = "duration_serde")]
    pub timeout: Option<Duration>,
    /// Delay between retries (exponential backoff base).
    #[serde(with = "duration_serde")]
    pub retry_delay: Option<Duration>,
}

impl Default for JobOptions {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_count: 0,
            timeout: Some(Duration::from_secs(300)), // 5 minutes default
            retry_delay: Some(Duration::from_secs(10)), // 10 seconds base delay
        }
    }
}

impl JobOptions {
    /// Create new JobOptions with specified max retries.
    pub fn with_max_retries(max_retries: u32) -> Self {
        Self {
            max_retries,
            ..Default::default()
        }
    }

    /// Set timeout for the job.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set retry delay.
    pub fn retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = Some(delay);
        self
    }

    /// Check if the job can be retried.
    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    /// Calculate the delay for the next retry using exponential backoff.
    pub fn next_retry_delay(&self) -> Duration {
        let base = self.retry_delay.unwrap_or(Duration::from_secs(10));
        let multiplier = 2u64.pow(self.retry_count);
        base * multiplier as u32
    }

    /// Increment retry count and return the updated options.
    pub fn increment_retry(mut self) -> Self {
        self.retry_count += 1;
        self
    }
}

/// The status of a job.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    /// Job is pending execution.
    Pending,
    /// Job is scheduled for future execution.
    Scheduled,
    /// Job is currently being processed.
    Processing,
    /// Job completed successfully.
    Completed,
    /// Job failed and is queued for retry.
    Retry,
    /// Job failed and exhausted all retries.
    Dead,
}

/// A job with its payload and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job<T> {
    /// Unique job identifier.
    pub id: JobId,
    /// The job payload.
    pub payload: T,
    /// Job options (retries, timeout, etc.).
    pub options: JobOptions,
    /// Current job status.
    pub status: JobStatus,
    /// Timestamp when the job was created (Unix timestamp in seconds).
    pub created_at: i64,
    /// Optional timestamp when the job should run (for scheduled jobs).
    pub scheduled_at: Option<i64>,
    /// Optional error message from the last failure.
    pub last_error: Option<String>,
}

impl<T> Job<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    /// Create a new job with the given payload.
    pub fn new(payload: T) -> Self {
        Self {
            id: JobId::new(),
            payload,
            options: JobOptions::default(),
            status: JobStatus::Pending,
            created_at: chrono_timestamp(),
            scheduled_at: None,
            last_error: None,
        }
    }

    /// Create a new job with custom options.
    pub fn with_options(payload: T, options: JobOptions) -> Self {
        Self {
            id: JobId::new(),
            payload,
            options,
            status: JobStatus::Pending,
            created_at: chrono_timestamp(),
            scheduled_at: None,
            last_error: None,
        }
    }

    /// Schedule the job to run at a specific time.
    pub fn schedule_at(mut self, timestamp: i64) -> Self {
        self.scheduled_at = Some(timestamp);
        self.status = JobStatus::Scheduled;
        self
    }

    /// Schedule the job to run after a delay.
    pub fn schedule_in(self, delay: Duration) -> Self {
        let run_at = chrono_timestamp() + delay.as_secs() as i64;
        self.schedule_at(run_at)
    }

    /// Serialize the job to JSON.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserialize a job from JSON.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

/// Get current Unix timestamp in seconds.
fn chrono_timestamp() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Serde module for optional Duration serialization.
mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match duration {
            Some(d) => d.as_millis().serialize(serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let millis: Option<u64> = Option::deserialize(deserializer)?;
        Ok(millis.map(Duration::from_millis))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestPayload {
        message: String,
    }

    #[test]
    fn test_job_creation() {
        let job = Job::new(TestPayload {
            message: "hello".to_string(),
        });
        assert_eq!(job.status, JobStatus::Pending);
        assert_eq!(job.options.max_retries, 3);
    }

    #[test]
    fn test_job_serialization() {
        let job = Job::new(TestPayload {
            message: "test".to_string(),
        });
        let json = job.to_json().unwrap();
        let deserialized: Job<TestPayload> = Job::from_json(&json).unwrap();
        assert_eq!(deserialized.payload.message, "test");
    }

    #[test]
    fn test_job_options_retry() {
        let mut options = JobOptions::with_max_retries(3);
        assert!(options.can_retry());
        
        options = options.increment_retry();
        assert!(options.can_retry());
        
        options = options.increment_retry();
        assert!(options.can_retry());
        
        options = options.increment_retry();
        assert!(!options.can_retry());
    }
}

