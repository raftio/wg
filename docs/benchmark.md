# Benchmark

This repo doesn’t ship `cargo bench` / `criterion` benches yet. Instead, it provides an **end-to-end benchmark example** that enqueues **N jobs**, processes **N jobs**, and exits automatically.

## Example benchmark binary

Run via:

```bash
cargo run -p example-bench --release
```

Note: the benchmark enqueues first, then starts the worker pool. This avoids a slowdown on Redis where `BRPOP` can block a shared connection and starve enqueues.

### Common env vars

- `WG_BENCH_BACKEND=redis|postgres`
- `WG_BENCH_JOBS` (default `20000`)
- `WG_BENCH_WORKERS` (default `8`)
- `WG_BENCH_ENQUEUE_CONCURRENCY` (default `1`)
- `WG_BENCH_MAX_CONCURRENCY` (default `0` = unlimited)
- `WG_BENCH_FETCH_TIMEOUT_MS` (default `1000`)
- `WG_BENCH_SHUTDOWN_TIMEOUT_SECS` (default `2`)
- `WG_BENCH_TICK_MS` (default `50`)
- `WG_BENCH_ENABLE_REAPER` (default `false`)
- `WG_BENCH_JOB_NAME` (default `bench_noop`)
- `WG_BENCH_NAMESPACE` (default `wg_bench_{pid}`)
  - For SQL backends the namespace is used in table names; prefer `[a-zA-Z0-9_]` only.

### Redis

Prereq: Redis running (example):

```bash
docker run -d -p 6379:6379 redis
```

Run:

```bash
WG_BENCH_BACKEND=redis \
REDIS_URL=redis://localhost \
WG_BENCH_JOBS=20000 \
WG_BENCH_WORKERS=8 \
WG_BENCH_ENQUEUE_CONCURRENCY=64 \
cargo run -p example-bench --release
```

### Postgres

Prereq: Postgres running and `DATABASE_URL` pointing at a database.

Run:

```bash
WG_BENCH_BACKEND=postgres \
DATABASE_URL=postgres://user:pass@localhost/db \
WG_BENCH_JOBS=20000 \
WG_BENCH_WORKERS=8 \
WG_BENCH_ENQUEUE_CONCURRENCY=64 \
cargo run -p example-bench --release
```

## Tips for stable results

- Always use `--release`.
- Avoid noisy logging: the benchmark sets `RUST_LOG=error` by default.
- Run multiple times and compare medians.

