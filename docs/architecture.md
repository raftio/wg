# Architecture

## Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                           CLIENT                                 │
│  ┌─────────┐  ┌──────────┐                                      │
│  │ enqueue │  │ schedule │                                      │
│  └────┬────┘  └────┬─────┘                                      │
└───────┼────────────┼────────────────────────────────────────────┘
        │            │
        ▼            ▼
┌───────────────────────────────────────────────────────────────────┐
│                         BACKEND                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐  ┌────────┐ │
│  │  jobs queue  │  │   schedule   │  │    retry    │  │  dead  │ │
│  │   (LIST)     │  │   (ZSET)     │  │   (ZSET)    │  │ (LIST) │ │
│  └───────┬──────┘  └──────┬───────┘  └──────┬──────┘  └────────┘ │
└──────────┼────────────────┼─────────────────┼─────────────────────┘
           │                │                 │
           ▼                ▼                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                        WORKER POOL                                │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │  Scheduler: moves due jobs from schedule -> jobs          │   │
│  └───────────────────────────────────────────────────────────┘   │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │  Retrier: moves due jobs from retry -> jobs               │   │
│  └───────────────────────────────────────────────────────────┘   │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐               │
│  │ Worker  │ │ Worker  │ │ Worker  │ │ Worker  │  ...          │
│  │   #1    │ │   #2    │ │   #3    │ │   #4    │               │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘               │
└──────────────────────────────────────────────────────────────────┘
```

## Components

### Client

Enqueues jobs to the backend. Two modes:

- `enqueue()` - immediate processing, pushes to jobs queue
- `schedule()` - delayed processing, adds to schedule sorted set with run_at timestamp

### Backend

Storage layer with 4 queues:

| Queue | Type | Purpose |
|-------|------|---------|
| jobs | LIST | Immediate processing queue (FIFO) |
| schedule | ZSET | Future jobs, sorted by run_at timestamp |
| retry | ZSET | Failed jobs awaiting retry, sorted by retry_at |
| dead | LIST | Failed jobs that exhausted retries |

### Worker Pool

Runs inside a single process:

- **Scheduler** - polls schedule queue, moves due jobs to jobs queue
- **Retrier** - polls retry queue, moves due jobs to jobs queue  
- **Workers** - N concurrent workers, each pops from jobs queue and processes

## Job Lifecycle

```
enqueue() ─────────────────────────────────────┐
                                               ▼
schedule() ──► [schedule] ──► Scheduler ──► [jobs] ──► Worker
                                               │
                                               ▼
                                           ┌───────┐
                                           │process│
                                           └───┬───┘
                              ┌────────────────┼────────────────┐
                              ▼                ▼                ▼
                             OK            retryable         fatal
                              │                │                │
                              ▼                ▼                ▼
                            done          [retry]           [dead]
                                              │
                                              ▼
                                           Retrier
                                              │
                                              ▼
                                           [jobs]
```

## Job States

| State | Description |
|-------|-------------|
| Pending | Created, waiting in jobs queue |
| Scheduled | Waiting in schedule queue |
| Processing | Being processed by a worker |
| Retry | Failed, waiting in retry queue |
| Dead | Failed, exhausted all retries |

## Retry Strategy

Exponential backoff:

```
delay = base_delay * 2^retry_count
```

Default: base_delay = 10s, max_retries = 3

| Retry | Delay |
|-------|-------|
| 1 | 10s |
| 2 | 20s |
| 3 | 40s |

## Shutdown

Graceful shutdown on SIGINT (Ctrl+C):

1. Stop fetching new jobs (draining mode)
2. Wait for in-progress jobs to complete (up to shutdown_timeout)
3. Stop scheduler and retrier
4. Exit

## Crate Structure

```
wg/
├── crates/
│   ├── core/          # Client, WorkerPool, Job, Backend trait
│   ├── redis/         # Redis backend (BRPOP for blocking)
│   ├── postgres/      # PostgreSQL backend
│   ├── mysql/         # MySQL backend
│   ├── sqlite/        # SQLite backend
│   └── server/        # HTTP API server (optional)
└── examples/
    ├── basic/         # SQLite in-memory example
    └── redis/         # Redis with scheduled jobs & retry
```

