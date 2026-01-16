# wg - Background Job Queue

A Rust library for background job processing with multiple storage backends.

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
| `wg-client` | Admin SDK for queue management and monitoring |
| `wg-redis` | Redis backend (recommended for production) |
| `wg-postgres` | PostgreSQL backend |
| `wg-mysql` | MySQL backend |
| `wg-sqlite` | SQLite backend (great for development/testing) |

## Installation

Add the crates you need to your `Cargo.toml`:

```toml
[dependencies]
wg-core = "0.1"
wg-client = "0.1"  # optional: for queue management/monitoring
wg-redis = "0.1"   # or wg-postgres, wg-mysql, wg-sqlite
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
    client.enqueue("send_email", EmailJob {
        to: "user@example.com".into(),
        subject: "Hello".into(),
        body: "World".into(),
    }).await?;
    
    // Schedule for later
    client.schedule("send_email", EmailJob {
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

## Admin Client (Queue Management)

```rust
use wg_client::AdminClient;
use wg_redis::RedisBackend;

#[tokio::main]
async fn main() -> wg_core::Result<()> {
    let backend = RedisBackend::new("redis://localhost", "myapp").await?;
    let admin = AdminClient::new(backend);
    
    // Get queue statistics
    let stats = admin.stats().await?;
    println!("Queue: {}, Scheduled: {}, Retry: {}, Dead: {}", 
        stats.queue, stats.scheduled, stats.retry, stats.dead);
    
    // List worker pools
    let workers = admin.list_workers().await?;
    for w in workers {
        println!("Pool {} on {}: {} workers", w.pool_id, w.host, w.concurrency);
    }
    
    // List and retry dead jobs
    let dead_jobs = admin.list_dead(10, 0).await?;
    for job in dead_jobs {
        println!("Dead job {}: {}", job.id, job.last_error.unwrap_or_default());
        admin.retry_dead(&job.id).await?;
    }
    
    // Unlock stuck in-progress jobs
    admin.unlock_job("pool-id", &job_json).await?;
    
    Ok(())
}
```

## Backend Options

See [docs/backend.md](docs/backend.md) for details.

## Benchmarking

See [docs/benchmark.md](docs/benchmark.md) for an end-to-end benchmark harness (Redis/Postgres).

## Architecture

See [docs/architecture.md](docs/architecture.md) for details.

## Job Options

```rust
use wg_core::{JobOptions, Job};
use std::time::Duration;

let options = JobOptions::with_max_retries(5)
    .timeout(Duration::from_secs(60))
    .retry_delay(Duration::from_secs(30));

// Enqueue with custom options
client.enqueue_with_options("my_job", my_job, options).await?;
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

## Examples

```bash
# Basic example with SQLite (no setup required)
cargo run -p example-basic

# Redis example (requires Redis)
docker run -d -p 6379:6379 redis
cargo run -p example-redis
```

## License

MIT
