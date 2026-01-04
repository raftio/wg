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
│          │                │                 │                     │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │  worker_pools (SET)  │  heartbeat:{id} (HASH)  │          │   │
│  │  in_progress:{id} (ZSET)                                  │   │
│  └───────────────────────────────────────────────────────────┘   │
└──────────┼────────────────┼─────────────────┼─────────────────────┘
           │                │                 │
           ▼                ▼                 ▼
┌──────────────────────────────────────────────────────────────────┐
│                        WORKER POOL                                │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │  Heartbeater: sends heartbeat every 5s                    │   │
│  └───────────────────────────────────────────────────────────┘   │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │  Reaper: recovers jobs from dead worker pools (30s)       │   │
│  └───────────────────────────────────────────────────────────┘   │
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

Storage layer with queues and monitoring data:

| Queue/Store | Type | Purpose |
|-------------|------|---------|
| jobs | LIST | Immediate processing queue (FIFO) |
| schedule | ZSET | Future jobs, sorted by run_at timestamp |
| retry | ZSET | Failed jobs awaiting retry, sorted by retry_at |
| dead | LIST | Failed jobs that exhausted retries |
| worker_pools | SET | Registered worker pool IDs |
| heartbeat:{pool_id} | HASH | Worker pool heartbeat info |
| in_progress:{pool_id} | ZSET | Jobs currently being processed |

### Worker Pool

Runs inside a single process:

- **Heartbeater** - sends heartbeat every 5s, registers pool on start, removes on shutdown
- **Reaper** - checks for dead pools every 30s, recovers their in-progress jobs
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
                                        ┌─────────────┐
                                        │ mark        │
                                        │ in_progress │
                                        └──────┬──────┘
                                               ▼
                                           ┌───────┐
                                           │process│
                                           └───┬───┘
                              ┌────────────────┼────────────────┐
                              ▼                ▼                ▼
                             OK            retryable         fatal
                              │                │                │
                              ▼                ▼                ▼
                        ┌──────────┐      [retry]           [dead]
                        │ complete │          │
                        │in_progress│         ▼
                        └──────────┘      Retrier
                              │               │
                              ▼               ▼
                            done           [jobs]
```

## Heartbeat & Stale Job Recovery

```
                    ┌─────────────────────────────┐
                    │      Worker Pool A          │
                    │  ┌─────────────────────┐    │
                    │  │ Heartbeater (5s)    │────┼──► heartbeat:{pool_a}
                    │  └─────────────────────┘    │    {heartbeat_at, host, pid, ...}
                    │  ┌─────────────────────┐    │
                    │  │ Workers processing  │────┼──► in_progress:{pool_a}
                    │  └─────────────────────┘    │    [job1, job2, ...]
                    └─────────────────────────────┘

    If Worker Pool A crashes (no heartbeat for 60s):

                    ┌─────────────────────────────┐
                    │      Worker Pool B          │
                    │  ┌─────────────────────┐    │
                    │  │ Reaper (30s)        │    │
                    │  │ - find stale pools  │────┼──► get_stale_pools(60s)
                    │  │ - cleanup_pool()    │────┼──► returns [job1, job2]
                    │  │ - re-enqueue jobs   │────┼──► push to [jobs]
                    │  └─────────────────────┘    │
                    └─────────────────────────────┘
```

## Job States

| State | Description |
|-------|-------------|
| Pending | Created, waiting in jobs queue |
| Scheduled | Waiting in schedule queue |
| Processing | Being processed by a worker (tracked in in_progress) |
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
3. Stop scheduler, retrier, heartbeater, and reaper
4. Remove heartbeat from backend
5. Exit

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `num_workers` | 4 | Number of concurrent workers |
| `heartbeat_interval` | 5s | How often to send heartbeat |
| `reaper_interval` | 30s | How often to check for dead pools |
| `stale_threshold` | 60s | Pool is dead if no heartbeat for this long |
| `enable_reaper` | true | Enable stale job recovery |
| `pool_id` | auto | Custom pool ID (auto-generated if not set) |

## Crate Structure

```
wg/
├── crates/
│   ├── core/          # Client, WorkerPool, Job, Backend trait
│   │   ├── backend.rs     # Backend trait + WorkerPoolInfo
│   │   ├── heartbeat.rs   # Heartbeater component
│   │   ├── reaper.rs      # Reaper component
│   │   ├── scheduler.rs   # Scheduler component
│   │   ├── retrier.rs     # Retrier component
│   │   └── worker.rs      # WorkerPool + Worker
│   ├── redis/         # Redis backend (BRPOP for blocking)
│   ├── postgres/      # PostgreSQL backend
│   ├── mysql/         # MySQL backend
│   ├── sqlite/        # SQLite backend
│   └── server/        # HTTP API server (optional)
└── examples/
    ├── basic/         # SQLite in-memory example
    ├── redis/         # Redis with scheduled jobs & retry
    └── server/        # HTTP API + workers
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/api/stats` | GET | Queue statistics |
| `/api/workers` | GET | List active worker pools |
| `/api/jobs` | POST | Enqueue a new job |
| `/api/dead` | GET | List dead jobs |
| `/api/dead/{id}/retry` | POST | Retry a dead job |
| `/api/dead/{id}` | DELETE | Delete a dead job |
