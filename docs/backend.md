# Backend

Storage backends implement the `Backend` trait to provide job queue persistence.

## Available Backends

| Backend | Crate | Best For |
|---------|-------|----------|
| Redis | `wg-redis` | Production, high throughput |
| PostgreSQL | `wg-postgres` | Same DB as app data |
| MySQL | `wg-mysql` | Same DB as app data |
| SQLite | `wg-sqlite` | Development, testing, embedded |

## Comparison

| Feature | Redis | PostgreSQL | MySQL | SQLite |
|---------|-------|------------|-------|--------|
| Blocking pop | BRPOP | Polling | Polling | Polling |
| Performance | Excellent | Good | Good | Fair |
| Persistence | Optional | Yes | Yes | Yes |
| Distributed | Yes | Yes | Yes | No |
| ACID | Partial | Yes | Yes | Yes |

## Redis

Recommended for production. Uses BRPOP for efficient blocking pop.

```rust
use wg_redis::RedisBackend;

let backend = RedisBackend::new("redis://localhost:6379", "myapp").await?;
```

**Keys:**
- `{namespace}:jobs` - LIST, immediate queue
- `{namespace}:schedule` - ZSET, scheduled jobs (score = run_at)
- `{namespace}:retry` - ZSET, retry queue (score = retry_at)
- `{namespace}:dead` - LIST, dead letter queue

**Environment:**
```bash
docker run -d -p 6379:6379 redis
```

## PostgreSQL

```rust
use wg_postgres::PostgresBackend;

let backend = PostgresBackend::new(
    "postgres://user:pass@localhost/db",
    "myapp"
).await?;
```

## MySQL

```rust
use wg_mysql::MySqlBackend;

let backend = MySqlBackend::new(
    "mysql://user:pass@localhost/db",
    "myapp"
).await?;
```

## SQLite

```rust
use wg_sqlite::SqliteBackend;

// File-based
let backend = SqliteBackend::new("sqlite:jobs.db", "myapp").await?;

// In-memory (testing)
let backend = SqliteBackend::in_memory("myapp").await?;
```

## SQL Schema

SQL backends auto-create these tables:

```sql
CREATE TABLE {namespace}_jobs (
    id BIGSERIAL PRIMARY KEY,
    job_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE {namespace}_scheduled (
    id BIGSERIAL PRIMARY KEY,
    job_json TEXT NOT NULL,
    run_at BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE {namespace}_retry (
    id BIGSERIAL PRIMARY KEY,
    job_json TEXT NOT NULL,
    retry_at BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE {namespace}_dead (
    id BIGSERIAL PRIMARY KEY,
    job_json TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
```

## Backend Trait

All backends implement:

```rust
#[async_trait]
pub trait Backend: Send + Sync {
    // Immediate queue
    async fn push_job(&self, job_json: &str) -> Result<()>;
    async fn pop_job(&self, timeout: Duration) -> Result<Option<String>>;
    
    // Schedule queue
    async fn schedule_job(&self, job_json: &str, run_at: i64) -> Result<()>;
    async fn get_due_scheduled(&self, now: i64, limit: usize) -> Result<Vec<String>>;
    async fn remove_scheduled(&self, job_json: &str) -> Result<()>;
    
    // Retry queue
    async fn retry_job(&self, job_json: &str, retry_at: i64) -> Result<()>;
    async fn get_due_retries(&self, now: i64, limit: usize) -> Result<Vec<String>>;
    async fn remove_retry(&self, job_json: &str) -> Result<()>;
    
    // Dead letter queue
    async fn push_dead(&self, job_json: &str) -> Result<()>;
    async fn list_dead(&self, limit: usize, offset: usize) -> Result<Vec<String>>;
    async fn get_dead_by_id(&self, job_id: &str) -> Result<Option<String>>;
    async fn remove_dead(&self, job_json: &str) -> Result<()>;
    
    // Stats
    async fn queue_len(&self) -> Result<usize>;
    async fn schedule_len(&self) -> Result<usize>;
    async fn retry_len(&self) -> Result<usize>;
    async fn dead_len(&self) -> Result<usize>;
}
```

## Custom Backend

Implement the `Backend` trait:

```rust
use async_trait::async_trait;
use wg_core::{Backend, Result};
use std::time::Duration;

pub struct MyBackend { /* ... */ }

#[async_trait]
impl Backend for MyBackend {
    async fn push_job(&self, job_json: &str) -> Result<()> {
        // your implementation
    }
    
    async fn pop_job(&self, timeout: Duration) -> Result<Option<String>> {
        // your implementation
    }
    
    // ... implement all trait methods
}
```

