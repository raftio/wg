# wg - Background Jobs Queue

A Rust library for building job queues with support for multiple storage backends (Redis, PostgreSQL, MySQL, SQLite), scheduling, retries, and graceful shutdown.

## Features

- **Multiple backends**: Redis, PostgreSQL, MySQL, SQLite
- **Immediate job processing**: Enqueue jobs for immediate execution
- **Scheduled jobs**: Schedule jobs to run at a future time
- **Automatic retries**: Configurable retry logic with exponential backoff
- **Dead letter queue**: Failed jobs are moved to a dead queue for inspection
- **Graceful shutdown**: Complete in-progress jobs before stopping
- **Multiple workers**: Process jobs in parallel with configurable concurrency

## Crates

| Crate | Description |
|-------|-------------|
| `wg-core` | Core types, traits, Client, WorkerPool |
| `wg-redis` | Redis backend (recommended for production) |
| `wg-postgres` | PostgreSQL backend |
| `wg-mysql` | MySQL backend |
| `wg-sqlite` | SQLite backend (great for development/testing) |

## Installation

Add the crates you need to your `Cargo.toml`:

```toml
[dependencies]
wg-core = "0.1"
wg-redis = "0.1"  # or wg-postgres, wg-mysql, wg-sqlite
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
```

## Quick Start

### Client (Enqueuing Jobs)

```rust
use wg_core::Client;
use wg_redis::RedisBackend;
use serde::{Serialize, Deserialize};
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct EmailJob {
    to: String,
    subject: String,
    body: String,
}

#[tokio::main]
async fn main() -> wg_core::Result<()> {
    // Create backend
    let backend = RedisBackend::new("redis://localhost", "myapp").await?;
    let client = Client::new(backend);
    
    // Enqueue for immediate processing
    client.enqueue(EmailJob {
        to: "user@example.com".into(),
        subject: "Hello".into(),
        body: "World".into(),
    }).await?;
    
    // Schedule for later
    client.schedule(EmailJob {
        to: "user@example.com".into(),
        subject: "Reminder".into(),
        body: "Don't forget!".into(),
    }, Duration::from_secs(3600)).await?;
    
    Ok(())
}
```

### Worker (Processing Jobs)

```rust
use wg_core::{WorkerPool, JobResult, SharedBackend};
use wg_redis::RedisBackend;
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize)]
struct EmailJob {
    to: String,
    subject: String,
    body: String,
}

async fn process_email(job: EmailJob) -> JobResult {
    println!("Sending email to: {}", job.to);
    // Send email logic here...
    Ok(())
}

#[tokio::main]
async fn main() -> wg_core::Result<()> {
    let backend = RedisBackend::new("redis://localhost", "myapp").await?;
    
    let mut pool = WorkerPool::builder()
        .backend(backend)
        .namespace("myapp")
        .workers(4)
        .handler(process_email)
        .build()?;
    
    pool.run().await?;
    Ok(())
}
```

## Backend Options

### Redis (Recommended)

Best for production use. Supports blocking pop (BRPOP) for efficient job fetching.

```rust
use wg_redis::RedisBackend;

let backend = RedisBackend::new("redis://localhost:6379", "myapp").await?;
```

### PostgreSQL

Great for when you want your job queue in the same database as your app.

```rust
use wg_postgres::PostgresBackend;

let backend = PostgresBackend::new("postgres://user:pass@localhost/db", "myapp").await?;
```

### MySQL

```rust
use wg_mysql::MySqlBackend;

let backend = MySqlBackend::new("mysql://user:pass@localhost/db", "myapp").await?;
```

### SQLite

Perfect for development, testing, or embedded applications.

```rust
use wg_sqlite::SqliteBackend;

// File-based
let backend = SqliteBackend::new("sqlite:jobs.db", "myapp").await?;

// In-memory (for testing)
let backend = SqliteBackend::in_memory("myapp").await?;
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                           CLIENT                                 │
│  ┌─────────┐  ┌──────────┐                                      │
│  │ enqueue │  │ schedule │                                      │
│  └────┬────┘  └────┬─────┘                                      │
│       │            │                                            │
└───────┼────────────┼────────────────────────────────────────────┘
        │            │
        ▼            ▼
┌───────────────────────────────────────────────────────────────────┐
│                         BACKEND                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐  ┌────────┐ │
│  │  jobs queue  │  │   schedule   │  │    retry    │  │  dead  │ │
│  │   (LIST)     │  │   (ZSET)     │  │   (ZSET)    │  │ (LIST) │ │
│  └───────┬──────┘  └──────┬───────┘  └──────┬──────┘  └────────┘ │
│          │                │                 │                     │
└──────────┼────────────────┼─────────────────┼─────────────────────┘
           │                │                 │
           ▼                ▼                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                        WORKER POOL                                │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │                      Scheduler                             │   │
│  │   Moves due jobs from schedule queue to jobs queue         │   │
│  └───────────────────────────────────────────────────────────┘   │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │                       Retrier                              │   │
│  │   Moves due jobs from retry queue to jobs queue            │   │
│  └───────────────────────────────────────────────────────────┘   │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐               │
│  │ Worker  │ │ Worker  │ │ Worker  │ │ Worker  │  ...          │
│  │   #1    │ │   #2    │ │   #3    │ │   #4    │               │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘               │
└──────────────────────────────────────────────────────────────────┘
```

## Job Options

```rust
use wg_core::{JobOptions, Job};
use std::time::Duration;

let options = JobOptions::with_max_retries(5)
    .timeout(Duration::from_secs(60))
    .retry_delay(Duration::from_secs(30));

// Enqueue with custom options
client.enqueue_with_options(my_job, options).await?;
```

## Error Handling

```rust
use wg_core::{JobResult, JobError};

async fn process_job(job: MyJob) -> JobResult {
    match do_work(&job).await {
        Ok(_) => Ok(()),
        Err(e) if e.is_temporary() => {
            // Retryable error - job will be retried
            Err(JobError::retryable(e.to_string()))
        }
        Err(e) => {
            // Fatal error - job goes to dead queue
            Err(JobError::fatal(e.to_string()))
        }
    }
}
```

## Backend Comparison

| Feature | Redis | PostgreSQL | MySQL | SQLite |
|---------|-------|------------|-------|--------|
| Blocking pop | ✓ | Polling | Polling | Polling |
| Performance | Excellent | Good | Good | Fair |
| Persistence | Optional | ✓ | ✓ | ✓ |
| Distributed | ✓ | ✓ | ✓ | ✗ |
| ACID | Partial | ✓ | ✓ | ✓ |

## SQL Schema

SQL backends automatically create the following tables:

```sql
-- Jobs queue (immediate processing)
CREATE TABLE {namespace}_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Scheduled jobs
CREATE TABLE {namespace}_scheduled (
    id BIGSERIAL PRIMARY KEY,
    job_json TEXT NOT NULL,
    run_at BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Retry queue
CREATE TABLE {namespace}_retry (
    id BIGSERIAL PRIMARY KEY,
    job_json TEXT NOT NULL,
    retry_at BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Dead letter queue
CREATE TABLE {namespace}_dead (
    id BIGSERIAL PRIMARY KEY,
    job_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
```

## License

MIT
