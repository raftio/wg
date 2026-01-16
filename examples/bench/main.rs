//! Benchmark example: end-to-end enqueue N jobs, process N jobs, exit automatically.
//!
//! Env vars:
//! - WG_BENCH_BACKEND=redis|postgres
//! - WG_BENCH_NAMESPACE (optional; default: wg_bench_{pid})
//! - WG_BENCH_JOB_NAME (optional; default: bench_noop)
//! - WG_BENCH_JOBS (optional; default: 20000)
//! - WG_BENCH_WORKERS (optional; default: 8)
//! - WG_BENCH_ENQUEUE_CONCURRENCY (optional; default: 1)
//! - WG_BENCH_MAX_CONCURRENCY (optional; default: 0)
//! - WG_BENCH_FETCH_TIMEOUT_MS (optional; default: 1000)
//! - WG_BENCH_SHUTDOWN_TIMEOUT_SECS (optional; default: 2)
//! - WG_BENCH_TICK_MS (optional; default: 50) - scheduler/retrier/heartbeat loop cadence
//! - WG_BENCH_ENABLE_REAPER (optional; default: false)
//! - REDIS_URL (when backend=redis; default: redis://localhost)
//! - DATABASE_URL (when backend=postgres; required)

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::sync::oneshot;
use tokio::sync::Notify;
use wg_core::{Client, JobResult, WorkerConfig, WorkerPool};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NoopJob {
    i: u64,
}

#[derive(Debug, Clone, Copy)]
enum BackendKind {
    Redis,
    Postgres,
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    std::env::var(key)
        .ok()
        .map(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "y" | "on"))
        .unwrap_or(default)
}

fn sanitize_namespace(input: &str) -> String {
    // SQL backends use namespace in identifiers; keep it simple and portable.
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    // Avoid empty/degenerate namespaces.
    let trimmed = out.trim_matches('_');
    if trimmed.is_empty() {
        "wg".to_string()
    } else {
        trimmed.to_string()
    }
}

fn parse_backend_kind() -> BackendKind {
    match env_string("WG_BENCH_BACKEND", "redis").to_lowercase().as_str() {
        "redis" => BackendKind::Redis,
        "postgres" | "pg" | "postgresql" => BackendKind::Postgres,
        other => {
            eprintln!(
                "Invalid WG_BENCH_BACKEND={other}. Expected 'redis' or 'postgres'."
            );
            std::process::exit(2);
        }
    }
}

#[tokio::main]
async fn main() -> wg_core::Result<()> {
    tracing_subscriber::fmt().with_env_filter("error").init();

    let backend_kind = parse_backend_kind();
    let pid = std::process::id();

    let namespace_raw = env_string("WG_BENCH_NAMESPACE", &format!("wg_bench_{pid}"));
    let namespace = sanitize_namespace(&namespace_raw);
    let job_name = env_string("WG_BENCH_JOB_NAME", "bench_noop");

    let total_jobs = env_usize("WG_BENCH_JOBS", 20_000);
    let workers = env_usize("WG_BENCH_WORKERS", 8);
    let enqueue_concurrency = env_usize("WG_BENCH_ENQUEUE_CONCURRENCY", 1).max(1);
    let max_concurrency = env_usize("WG_BENCH_MAX_CONCURRENCY", 0);
    let fetch_timeout_ms = env_u64("WG_BENCH_FETCH_TIMEOUT_MS", 1000);
    let shutdown_timeout_secs = env_u64("WG_BENCH_SHUTDOWN_TIMEOUT_SECS", 2);
    let tick_ms = env_u64("WG_BENCH_TICK_MS", 50).max(1);
    let enable_reaper = env_bool("WG_BENCH_ENABLE_REAPER", false);

    println!("wg benchmark starting");
    println!("  backend: {}", backend_kind_str(backend_kind));
    println!("  namespace: {namespace}");
    println!("  job_name: {job_name}");
    println!("  jobs: {total_jobs}");
    println!("  workers: {workers}");
    println!("  enqueue_concurrency: {enqueue_concurrency}");
    println!("  max_concurrency: {max_concurrency}");
    println!("  fetch_timeout_ms: {fetch_timeout_ms}");
    println!("  shutdown_timeout_secs: {shutdown_timeout_secs}");
    println!("  tick_ms: {tick_ms}");
    println!("  enable_reaper: {enable_reaper}");

    let processed = Arc::new(AtomicUsize::new(0));
    let enqueued = Arc::new(AtomicUsize::new(0));
    let done_flag = Arc::new(AtomicBool::new(false));
    let done_notify = Arc::new(Notify::new());

    let handler_processed = processed.clone();
    let handler_done_flag = done_flag.clone();
    let handler_done_notify = done_notify.clone();
    let done_sender: Arc<std::sync::Mutex<Option<oneshot::Sender<std::time::Instant>>>> =
        Arc::new(std::sync::Mutex::new(None));

    let (tx, done_rx) = oneshot::channel::<std::time::Instant>();
    *done_sender.lock().expect("poisoned mutex") = Some(tx);

    let handler_done_sender = done_sender.clone();
    let handler = move |_job: NoopJob| {
        let handler_processed = handler_processed.clone();
        let handler_done_flag = handler_done_flag.clone();
        let handler_done_notify = handler_done_notify.clone();
        let handler_done_sender = handler_done_sender.clone();
        async move {
            let n = handler_processed.fetch_add(1, Ordering::SeqCst) + 1;
            if n >= total_jobs && !handler_done_flag.swap(true, Ordering::SeqCst) {
                if let Ok(mut guard) = handler_done_sender.lock() {
                    if let Some(tx) = guard.take() {
                        let _ = tx.send(std::time::Instant::now());
                    }
                }
                handler_done_notify.notify_waiters();
            }
            Ok(()) as JobResult
        }
    };

    match backend_kind {
        BackendKind::Redis => {
            let redis_url = env_string("REDIS_URL", "redis://localhost");
            let backend = wg_redis::RedisBackend::new(&redis_url).await?;

            run_bench(
                backend,
                namespace,
                job_name,
                total_jobs,
                workers,
                enqueue_concurrency,
                max_concurrency,
                fetch_timeout_ms,
                shutdown_timeout_secs,
                tick_ms,
                enable_reaper,
                handler.clone(),
                done_flag,
                done_notify,
                done_rx,
                processed,
                enqueued,
            )
            .await?;
        }
        BackendKind::Postgres => {
            let database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                eprintln!("DATABASE_URL is required when WG_BENCH_BACKEND=postgres");
                std::process::exit(2);
            });

            let backend = wg_postgres::PostgresBackend::new(&database_url).await?;
            backend.init_namespace(&namespace).await?;

            run_bench(
                backend,
                namespace,
                job_name,
                total_jobs,
                workers,
                enqueue_concurrency,
                max_concurrency,
                fetch_timeout_ms,
                shutdown_timeout_secs,
                tick_ms,
                enable_reaper,
                handler,
                done_flag,
                done_notify,
                done_rx,
                processed,
                enqueued,
            )
            .await?;
        }
    }

    Ok(())
}

fn backend_kind_str(kind: BackendKind) -> &'static str {
    match kind {
        BackendKind::Redis => "redis",
        BackendKind::Postgres => "postgres",
    }
}

async fn run_bench<B, F, Fut>(
    backend: B,
    namespace: String,
    job_name: String,
    total_jobs: usize,
    workers: usize,
    enqueue_concurrency: usize,
    max_concurrency: usize,
    fetch_timeout_ms: u64,
    shutdown_timeout_secs: u64,
    tick_ms: u64,
    enable_reaper: bool,
    handler: F,
    done_flag: Arc<AtomicBool>,
    done_notify: Arc<Notify>,
    done_rx: oneshot::Receiver<std::time::Instant>,
    processed: Arc<AtomicUsize>,
    enqueued: Arc<AtomicUsize>,
) -> wg_core::Result<()>
where
    B: wg_core::Backend + Clone + Send + Sync + 'static,
    F: Fn(NoopJob) -> Fut + Send + Sync + Clone + 'static,
    Fut: std::future::Future<Output = JobResult> + Send + 'static,
{
    let client = Client::with_backend(backend.clone(), namespace.clone());

    // Progress reporter (prints until the pool exits).
    let ns_for_progress = namespace.clone();
    let job_for_progress = job_name.clone();
    let processed_for_progress = processed.clone();
    let enqueued_for_progress = enqueued.clone();
    let progress_handle = tokio::spawn(async move {
        let start = std::time::Instant::now();
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let p = processed_for_progress.load(Ordering::Relaxed);
            let e = enqueued_for_progress.load(Ordering::Relaxed);
            eprintln!(
                "[progress] t={:.0}s ns={} job={} enqueued={} processed={}",
                start.elapsed().as_secs_f64(),
                ns_for_progress,
                job_for_progress,
                e,
                p
            );
            if p >= total_jobs {
                break;
            }
        }
    });

    // Enqueue phase (measure separately). We enqueue BEFORE starting the WorkerPool.
    //
    // Reason: the Redis backend uses a single connection manager and pop_job() uses BRPOP.
    // Starting workers first can cause blocking pops to hog the connection and slow enqueues.
    let enqueue_start = std::time::Instant::now();
    if enqueue_concurrency <= 1 {
        for i in 0..total_jobs {
            let _job_id = client.enqueue(&job_name, NoopJob { i: i as u64 }).await?;
            enqueued.fetch_add(1, Ordering::Relaxed);
        }
    } else {
        let mut set: JoinSet<wg_core::Result<()>> = JoinSet::new();
        for i in 0..total_jobs {
            while set.len() >= enqueue_concurrency {
                if let Some(res) = set.join_next().await {
                    res.map_err(|e| wg_core::WgError::WorkerPool(format!("enqueue task failed: {e}")))??;
                }
            }
            let c = client.clone();
            let jn = job_name.clone();
            let enq = enqueued.clone();
            set.spawn(async move {
                let _job_id = c.enqueue(&jn, NoopJob { i: i as u64 }).await?;
                enq.fetch_add(1, Ordering::Relaxed);
                Ok(())
            });
        }
        while let Some(res) = set.join_next().await {
            res.map_err(|e| wg_core::WgError::WorkerPool(format!("enqueue task failed: {e}")))??;
        }
    }
    let enqueue_dur = enqueue_start.elapsed();

    // Start WorkerPool after enqueueing.
    //
    // IMPORTANT: for benchmark accuracy, keep shutdown snappy by:
    // - disabling reaper by default (otherwise it can sleep up to 30s)
    // - lowering loop intervals (heartbeat/scheduler/retrier) so tasks exit quickly
    let tick = Duration::from_millis(tick_ms);
    let mut cfg = WorkerConfig::default();
    cfg.namespace = namespace.clone();
    cfg.job_name = job_name.clone();
    cfg.num_workers = workers;
    cfg.max_concurrency = max_concurrency;
    cfg.fetch_timeout = Duration::from_millis(fetch_timeout_ms);
    cfg.shutdown_timeout = Duration::from_secs(shutdown_timeout_secs);
    cfg.scheduler_interval = tick;
    cfg.retrier_interval = tick;
    cfg.heartbeat_interval = tick;
    cfg.reaper_interval = tick;
    cfg.stale_threshold = Duration::from_secs(2);
    cfg.enable_reaper = enable_reaper;
    cfg.job_names = vec![job_name.clone()];

    let process_start = std::time::Instant::now();

    let shutdown_done_flag = done_flag.clone();
    let shutdown_done_notify = done_notify.clone();
    let shutdown = async move {
        while !shutdown_done_flag.load(Ordering::SeqCst) {
            shutdown_done_notify.notified().await;
        }
    };

    let mut pool = WorkerPool::new(cfg, handler, backend);
    let pool_handle = tokio::spawn(async move { pool.run_until(shutdown).await });

    // Time-to-reach-N: when the last handler increments the counter to N.
    let reached_at = match tokio::time::timeout(Duration::from_secs(60), done_rx).await {
        Ok(Ok(t)) => Some(t),
        Ok(Err(_closed)) => None,
        Err(_timeout) => None,
    };
    let time_to_reach_n = reached_at.map(|t| t.duration_since(process_start));

    // Total time: enqueue + processing.
    let total_start = enqueue_start;
    let result = pool_handle.await;
    let process_dur = process_start.elapsed();
    let total_dur = total_start.elapsed();

    // Stop progress reporter.
    progress_handle.abort();

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(join_err) => {
            return Err(wg_core::WgError::WorkerPool(format!(
                "worker pool task failed: {join_err}"
            )));
        }
    }

    let processed_n = processed.load(Ordering::SeqCst);
    let total_secs = total_dur.as_secs_f64().max(1e-9);
    let enqueue_secs = enqueue_dur.as_secs_f64().max(1e-9);
    let process_secs = process_dur.as_secs_f64().max(1e-9);
    let reach_secs = time_to_reach_n.map(|d| d.as_secs_f64()).unwrap_or(process_secs);

    println!("wg benchmark done");
    println!("  namespace: {namespace}");
    println!("  job_name: {job_name}");
    println!("  jobs: {total_jobs}");
    println!("  workers: {workers}");
    println!("  enqueue_concurrency: {enqueue_concurrency}");
    println!("  max_concurrency: {max_concurrency}");
    println!("  processed: {processed_n}");
    println!("  enqueue_time_s: {:.6}", enqueue_secs);
    println!("  time_to_reach_n_s: {:.6}", reach_secs);
    println!("  time_to_exit_s: {:.6}", process_secs);
    println!("  total_time_s: {:.6}", total_secs);
    println!(
        "  throughput_jobs_per_s_total: {:.2}",
        (processed_n as f64) / total_secs
    );
    println!(
        "  throughput_jobs_per_s_time_to_reach_n: {:.2}",
        (processed_n as f64) / reach_secs
    );

    Ok(())
}

