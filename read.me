# wg - Redis Job Queue Library

A Rust library for building job queues backed by Redis with support for scheduling, retries, and graceful shutdown.

## Features

- **Immediate job processing** - Enqueue jobs for immediate execution
- **Scheduled jobs** - Schedule jobs to run at a future time
- **Automatic retries** - Configurable retry logic with exponential backoff
- **Dead letter queue** - Failed jobs are moved to a dead queue for inspection
- **Graceful shutdown** - Complete in-progress jobs before stopping (draining)
- **Multiple workers** - Process jobs in parallel with configurable concurrency

## Architecture

```
┌─────────────┐         ┌──────────────────────────────────────┐
│   Client    │         │              Redis                   │
│  (Enqueuer) │         │                                      │
└──────┬──────┘         │  ┌─────────────┐  ┌──────────────┐   │
       │                │  │ <nsp>:jobs  │  │ <nsp>:schedule│   │
       │ immediately    │  │   (LIST)    │  │    (ZSET)    │   │
       ├───────────────►│  └──────┬──────┘  └──────┬───────┘   │
       │                │         │                │           │
       │ schedule       │         │                │           │
       └───────────────►│         │                │           │
                        │  ┌──────▼──────┐  ┌──────▼───────┐   │
                        │  │ <nsp>:retry │  │  <nsp>:dead  │   │
                        │  │   (ZSET)    │  │    (LIST)    │   │
                        │  └─────────────┘  └──────────────┘   │
                        └──────────────────────────────────────┘
                                    │
                        ┌───────────▼────────────┐
                        │      Worker Pool       │
                        │  ┌─────┐ ┌─────┐       │
                        │  │ W1  │ │ W2  │ ...   │
                        │  └─────┘ └─────┘       │
                        │  ┌──────────────────┐  │
                        │  │    Scheduler     │  │
                        │  │   (1s loop)      │  │
                        │  └──────────────────┘  │
                        │  ┌──────────────────┐  │
                        │  │     Retrier      │  │
                        │  │   (1s loop)      │  │
                        │  └──────────────────┘  │
                        └────────────────────────┘
```

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
wg = { path = "path/to/wg" }
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

## Quick Start

### Define a Job

```rust
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize)]
struct EmailJob {
    to: String,
    subject: String,
    body: String,
}
```

### Enqueue Jobs (Client)

```rust
use wg::Client;
use std::time::Duration;

#[tokio::main]
async fn main() -> wg::Result<()> {
    let client = Client::connect("redis://localhost:6379", "myapp").await?;
    
    // Enqueue for immediate processing
    client.enqueue(EmailJob {
        to: "user@example.com".into(),
        subject: "Hello".into(),
        body: "World".into(),
    }).await?;
    
    // Schedule for 1 hour later
    client.schedule(EmailJob {
        to: "user@example.com".into(),
        subject: "Reminder".into(),
        body: "Don't forget!".into(),
    }, Duration::from_secs(3600)).await?;
    
    Ok(())
}
```

### Process Jobs (Worker)

```rust
use wg::{WorkerPool, JobResult, JobError};

async fn process_email(job: EmailJob) -> JobResult {
    println!("Sending email to: {}", job.to);
    
    // Simulate sending email
    if job.to.is_empty() {
        return Err(JobError::fatal("Invalid email address"));
    }
    
    // If something temporary fails, return retryable error
    // return Err(JobError::retryable("SMTP server unavailable"));
    
    Ok(())
}

#[tokio::main]
async fn main() -> wg::Result<()> {
    let pool = WorkerPool::builder()
        .redis_url("redis://localhost:6379")
        .namespace("myapp")
        .workers(4)
        .handler(process_email)
        .build()?;
    
    // Blocks until Ctrl+C, then drains in-progress jobs
    pool.run().await?;
    
    Ok(())
}
```

## Configuration

### WorkerConfig Options

| Option | Default | Description |
|--------|---------|-------------|
| `redis_url` | `redis://127.0.0.1:6379` | Redis connection URL |
| `namespace` | `wg` | Prefix for all Redis keys |
| `num_workers` | `4` | Number of parallel workers |
| `fetch_timeout` | `5s` | BRPOP timeout |
| `scheduler_interval` | `1s` | How often to check scheduled jobs |
| `retrier_interval` | `1s` | How often to check retry queue |
| `batch_size` | `100` | Jobs to move per scheduler/retrier tick |
| `shutdown_timeout` | `30s` | Max time to wait for draining |

### JobOptions

```rust
use wg::JobOptions;
use std::time::Duration;

let options = JobOptions::with_max_retries(5)
    .timeout(Duration::from_secs(60))
    .retry_delay(Duration::from_secs(30));

client.enqueue_with_options(my_job, options).await?;
```

## Error Handling

Job handlers return `JobResult`:

```rust
use wg::{JobResult, JobError};

async fn my_handler(job: MyJob) -> JobResult {
    // Retryable error - job will be retried with exponential backoff
    if temporary_failure {
        return Err(JobError::retryable("Service unavailable"));
    }
    
    // Fatal error - job goes directly to dead queue
    if permanent_failure {
        return Err(JobError::fatal("Invalid data"));
    }
    
    Ok(())
}
```

## Redis Keys

All keys are prefixed with the configured namespace:

| Key | Type | Description |
|-----|------|-------------|
| `{namespace}:jobs` | LIST | Jobs ready for processing |
| `{namespace}:schedule` | ZSET | Scheduled jobs (score = run timestamp) |
| `{namespace}:retry` | ZSET | Jobs waiting for retry (score = retry timestamp) |
| `{namespace}:dead` | LIST | Failed jobs that exhausted retries |

## Graceful Shutdown

When receiving SIGINT (Ctrl+C):

1. Workers stop fetching new jobs (draining mode)
2. In-progress jobs are allowed to complete
3. Scheduler and retrier stop
4. Pool exits after all jobs complete or timeout

## License

MIT

