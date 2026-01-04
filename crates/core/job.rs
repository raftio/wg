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

    #[test]
    fn test_job_id_uniqueness() {
        let id1 = JobId::new();
        let id2 = JobId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_job_id_display() {
        let id = JobId::new();
        let display = format!("{}", id);
        // UUID v4 format: 8-4-4-4-12 hex characters
        assert_eq!(display.len(), 36);
        assert!(display.chars().filter(|c| *c == '-').count() == 4);
    }

    #[test]
    fn test_job_id_default() {
        let id1 = JobId::default();
        let id2 = JobId::default();
        // Default creates unique IDs
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_job_id_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        let id = JobId::new();
        set.insert(id.clone());
        assert!(set.contains(&id));
    }

    #[test]
    fn test_job_options_default() {
        let options = JobOptions::default();
        assert_eq!(options.max_retries, 3);
        assert_eq!(options.retry_count, 0);
        assert_eq!(options.timeout, Some(Duration::from_secs(300)));
        assert_eq!(options.retry_delay, Some(Duration::from_secs(10)));
    }

    #[test]
    fn test_job_options_with_max_retries() {
        let options = JobOptions::with_max_retries(5);
        assert_eq!(options.max_retries, 5);
        assert_eq!(options.retry_count, 0);
    }

    #[test]
    fn test_job_options_timeout_builder() {
        let options = JobOptions::default().timeout(Duration::from_secs(60));
        assert_eq!(options.timeout, Some(Duration::from_secs(60)));
    }

    #[test]
    fn test_job_options_retry_delay_builder() {
        let options = JobOptions::default().retry_delay(Duration::from_secs(30));
        assert_eq!(options.retry_delay, Some(Duration::from_secs(30)));
    }

    #[test]
    fn test_job_options_exponential_backoff() {
        let mut options = JobOptions::default().retry_delay(Duration::from_secs(10));

        // First retry: 10 * 2^0 = 10 seconds
        assert_eq!(options.next_retry_delay(), Duration::from_secs(10));

        options = options.increment_retry();
        // Second retry: 10 * 2^1 = 20 seconds
        assert_eq!(options.next_retry_delay(), Duration::from_secs(20));

        options = options.increment_retry();
        // Third retry: 10 * 2^2 = 40 seconds
        assert_eq!(options.next_retry_delay(), Duration::from_secs(40));

        options = options.increment_retry();
        // Fourth retry: 10 * 2^3 = 80 seconds
        assert_eq!(options.next_retry_delay(), Duration::from_secs(80));
    }

    #[test]
    fn test_job_options_exponential_backoff_no_delay() {
        // When retry_delay is None, default to 10 seconds
        let options = JobOptions {
            max_retries: 3,
            retry_count: 0,
            timeout: None,
            retry_delay: None,
        };
        assert_eq!(options.next_retry_delay(), Duration::from_secs(10));
    }

    #[test]
    fn test_job_options_can_retry_zero_max() {
        let options = JobOptions::with_max_retries(0);
        assert!(!options.can_retry());
    }

    #[test]
    fn test_job_status_equality() {
        assert_eq!(JobStatus::Pending, JobStatus::Pending);
        assert_ne!(JobStatus::Pending, JobStatus::Completed);
    }

    #[test]
    fn test_job_status_serialization() {
        let status = JobStatus::Processing;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"Processing\"");

        let deserialized: JobStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, JobStatus::Processing);
    }

    #[test]
    fn test_job_with_options() {
        let options = JobOptions::with_max_retries(5).timeout(Duration::from_secs(60));
        let job = Job::with_options(
            TestPayload {
                message: "custom".to_string(),
            },
            options,
        );
        assert_eq!(job.options.max_retries, 5);
        assert_eq!(job.options.timeout, Some(Duration::from_secs(60)));
        assert_eq!(job.status, JobStatus::Pending);
    }

    #[test]
    fn test_job_schedule_at() {
        let job = Job::new(TestPayload {
            message: "scheduled".to_string(),
        });
        let run_at = chrono_timestamp() + 3600; // 1 hour from now
        let scheduled_job = job.schedule_at(run_at);

        assert_eq!(scheduled_job.status, JobStatus::Scheduled);
        assert_eq!(scheduled_job.scheduled_at, Some(run_at));
    }

    #[test]
    fn test_job_schedule_in() {
        let before = chrono_timestamp();
        let job = Job::new(TestPayload {
            message: "delayed".to_string(),
        });
        let scheduled_job = job.schedule_in(Duration::from_secs(3600));
        let after = chrono_timestamp();

        assert_eq!(scheduled_job.status, JobStatus::Scheduled);
        // scheduled_at should be approximately now + 3600
        let scheduled_at = scheduled_job.scheduled_at.unwrap();
        assert!(scheduled_at >= before + 3600);
        assert!(scheduled_at <= after + 3600);
    }

    #[test]
    fn test_job_created_at_is_set() {
        let before = chrono_timestamp();
        let job = Job::new(TestPayload {
            message: "test".to_string(),
        });
        let after = chrono_timestamp();

        assert!(job.created_at >= before);
        assert!(job.created_at <= after);
    }

    #[test]
    fn test_job_initial_state() {
        let job = Job::new(TestPayload {
            message: "test".to_string(),
        });
        assert!(job.scheduled_at.is_none());
        assert!(job.last_error.is_none());
    }

    #[test]
    fn test_job_id_serialization() {
        let id = JobId::new();
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: JobId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_duration_serde_roundtrip() {
        let options = JobOptions::default()
            .timeout(Duration::from_millis(12345))
            .retry_delay(Duration::from_millis(5000));

        let json = serde_json::to_string(&options).unwrap();
        let deserialized: JobOptions = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.timeout, Some(Duration::from_millis(12345)));
        assert_eq!(deserialized.retry_delay, Some(Duration::from_millis(5000)));
    }

    #[test]
    fn test_duration_serde_none() {
        let options = JobOptions {
            max_retries: 3,
            retry_count: 0,
            timeout: None,
            retry_delay: None,
        };

        let json = serde_json::to_string(&options).unwrap();
        let deserialized: JobOptions = serde_json::from_str(&json).unwrap();

        assert!(deserialized.timeout.is_none());
        assert!(deserialized.retry_delay.is_none());
    }
}
