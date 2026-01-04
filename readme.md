# wg - Multi-Backend Job Queue Library

A Rust library for building job queues with support for multiple storage backends (Redis, PostgreSQL, MySQL, SQLite), scheduling, retries, and graceful shutdown.

## Features

- **Multiple backends** - Redis, PostgreSQL, MySQL, SQLite
- **Immediate job processing** - Enqueue jobs for immediate execution
- **Scheduled jobs** - Schedule jobs to run at a future time
- **Automatic retries** - Configurable retry logic with exponential backoff
- **Dead letter queue** - Failed jobs are moved to a dead queue for inspection
- **Graceful shutdown** - Complete in-progress jobs before stopping (draining)
- **Multiple workers** - Process jobs in parallel with configurable concurrency

## Architecture

```
┌─────────────┐         ┌──────────────────────────────────────┐
│   Client    │         │           Backend (Storage)          │
│  (Enqueuer) │         │   Redis | PostgreSQL | MySQL | SQLite │
└──────┬──────┘         │                                      │
       │                │  ┌─────────────┐  ┌──────────────┐   │
       │ immediately    │  │    jobs     │  │   schedule   │   │
       ├───────────────►│  │   (queue)   │  │  (sorted)    │   │
       │                │  └──────┬──────┘  └──────┬───────┘   │
       │ schedule       │         │                │           │
       └───────────────►│         │                │           │
                        │  ┌──────▼──────┐  ┌──────▼───────┐   │
                        │  │    retry    │  │     dead     │   │
                        │  │  (sorted)   │  │   (queue)    │   │
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
wg = { path = "path/to/wg", features = ["redis"] }  # or postgres, mysql, sqlite
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

### Available Features

| Feature | Description |
|---------|-------------|
| `redis` | Redis backend (default) |
| `postgres` | PostgreSQL backend |
| `mysql` | MySQL backend |
| `sqlite` | SQLite backend |
| `full` | All backends |

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

### Using Redis Backend

```rust
use wg::Client;
use std::time::Duration;

#[tokio::main]
async fn main() -> wg::Result<()> {
    // Connect using Redis
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

### Using PostgreSQL Backend

```rust
use wg::{Client, PostgresBackend};

#[tokio::main]
async fn main() -> wg::Result<()> {
    let backend = PostgresBackend::new(
        "postgres://user:pass@localhost/mydb",
        "myapp"
    ).await?;
    
    let client = Client::new(backend);
    
    client.enqueue(EmailJob {
        to: "user@example.com".into(),
        subject: "Hello".into(),
        body: "World".into(),
    }).await?;
    
    Ok(())
}
```

### Using SQLite Backend

```rust
use wg::{Client, SqliteBackend};

#[tokio::main]
async fn main() -> wg::Result<()> {
    // File-based SQLite
    let backend = SqliteBackend::new("sqlite:jobs.db", "myapp").await?;
    
    // Or in-memory (for testing)
    // let backend = SqliteBackend::in_memory("myapp").await?;
    
    let client = Client::new(backend);
    
    client.enqueue(EmailJob {
        to: "user@example.com".into(),
        subject: "Hello".into(),
        body: "World".into(),
    }).await?;
    
    Ok(())
}
```

### Process Jobs (Worker)

```rust
use wg::{WorkerPool, JobResult, JobError};

async fn process_email(job: EmailJob) -> JobResult {
    println!("Sending email to: {}", job.to);
    
    if job.to.is_empty() {
        return Err(JobError::fatal("Invalid email address"));
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> wg::Result<()> {
    // Using Redis (default)
    let mut pool = WorkerPool::builder()
        .redis_url("redis://localhost:6379")
        .namespace("myapp")
        .workers(4)
        .handler(process_email)
        .build()
        .await?;
    
    // Blocks until Ctrl+C, then drains in-progress jobs
    pool.run().await?;
    
    Ok(())
}
```

### Worker with Custom Backend

```rust
use wg::{WorkerPool, PostgresBackend, JobResult};

#[tokio::main]
async fn main() -> wg::Result<()> {
    let backend = PostgresBackend::new(
        "postgres://user:pass@localhost/mydb",
        "myapp"
    ).await?;
    
    let mut pool = WorkerPool::builder()
        .namespace("myapp")
        .workers(4)
        .handler(process_email)
        .build_with_backend(backend)?;
    
    pool.run().await?;
    
    Ok(())
}
```

## Backend Comparison

| Feature | Redis | PostgreSQL | MySQL | SQLite |
|---------|-------|------------|-------|--------|
| Performance | Excellent | Good | Good | Moderate |
| Blocking pop | Native BRPOP | Polling | Polling | Polling |
| Clustering | Yes | Yes | Yes | No |
| Persistence | Optional | Yes | Yes | Yes |
| Best for | High throughput | Existing PG infra | Existing MySQL infra | Testing/Embedded |

## Configuration

### WorkerConfig Options

| Option | Default | Description |
|--------|---------|-------------|
| `redis_url` | `redis://127.0.0.1:6379` | Redis connection URL |
| `namespace` | `wg` | Prefix for all keys/tables |
| `num_workers` | `4` | Number of parallel workers |
| `fetch_timeout` | `5s` | Pop timeout |
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

## Storage Schema

### Redis

| Key | Type | Description |
|-----|------|-------------|
| `{namespace}:jobs` | LIST | Jobs ready for processing |
| `{namespace}:schedule` | ZSET | Scheduled jobs (score = timestamp) |
| `{namespace}:retry` | ZSET | Jobs waiting for retry |
| `{namespace}:dead` | LIST | Failed jobs |

### SQL (PostgreSQL/MySQL/SQLite)

Tables are auto-created with the namespace prefix:

| Table | Description |
|-------|-------------|
| `{namespace}_jobs` | Immediate job queue (FIFO) |
| `{namespace}_scheduled` | Scheduled jobs with `run_at` column |
| `{namespace}_retry` | Retry queue with `retry_at` column |
| `{namespace}_dead` | Dead letter queue |

## Graceful Shutdown

When receiving SIGINT (Ctrl+C):

1. Workers stop fetching new jobs (draining mode)
2. In-progress jobs are allowed to complete
3. Scheduler and retrier stop
4. Pool exits after all jobs complete or timeout

## Custom Backend

You can implement your own backend by implementing the `Backend` trait:

```rust
use wg::Backend;
use async_trait::async_trait;

#[derive(Clone)]
struct MyBackend { /* ... */ }

#[async_trait]
impl Backend for MyBackend {
    async fn push_job(&self, job_json: &str) -> wg::Result<()> { /* ... */ }
    async fn pop_job(&self, timeout: Duration) -> wg::Result<Option<String>> { /* ... */ }
    // ... implement other methods
}
```

## License

MIT
