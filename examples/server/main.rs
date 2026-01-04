//! Server example with HTTP API and worker pool.
//!
//! This example demonstrates:
//! - Running an HTTP API server for job management
//! - Running a worker pool for processing jobs
//! - Using the API to enqueue and monitor jobs
//!
//! Run with: `cargo run -p example-server`
//!
//! API Endpoints:
//! - GET  /health          - Health check
//! - GET  /api/stats       - Queue statistics
//! - POST /api/jobs        - Enqueue a new job
//! - GET  /api/dead        - List dead jobs
//! - POST /api/dead/:id/retry - Retry a dead job
//! - DELETE /api/dead/:id  - Delete a dead job

use serde::{Deserialize, Serialize};
use std::time::Duration;
use wg_core::JobResult;
use wg_server::{Server, ServerConfig};
use wg_sqlite::SqliteBackend;

/// Example task payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskJob {
    name: String,
    data: String,
}

/// Process a task job.
async fn process_task(job: TaskJob) -> JobResult {
    tracing::info!(name = %job.name, data = %job.data, "Processing task");

    // Simulate some work
    tokio::time::sleep(Duration::from_millis(500)).await;

    tracing::info!(name = %job.name, "Task completed");
    Ok(())
}

#[tokio::main]
async fn main() -> wg_core::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    println!("╔════════════════════════════════════════════════════════════════╗");
    println!("║               wg Server Example                                ║");
    println!("╠════════════════════════════════════════════════════════════════╣");
    println!("║  HTTP API running at: http://127.0.0.1:8080                    ║");
    println!("║                                                                ║");
    println!("║  Endpoints:                                                    ║");
    println!("║    GET  /health          - Health check                        ║");
    println!("║    GET  /api/stats       - Queue statistics                    ║");
    println!("║    POST /api/jobs        - Enqueue a new job                   ║");
    println!("║    GET  /api/dead        - List dead jobs                      ║");
    println!("║    POST /api/dead/:id/retry - Retry a dead job                 ║");
    println!("║    DELETE /api/dead/:id  - Delete a dead job                   ║");
    println!("║                                                                ║");
    println!("║  Example curl commands:                                        ║");
    println!("║    curl http://127.0.0.1:8080/health                           ║");
    println!("║    curl http://127.0.0.1:8080/api/stats                        ║");
    println!("║    curl -X POST http://127.0.0.1:8080/api/jobs \\               ║");
    println!("║         -H 'Content-Type: application/json' \\                  ║");
    println!("║         -d '{{\"payload\":{{\"name\":\"test\",\"data\":\"hello\"}}}}'       ║");
    println!("║                                                                ║");
    println!("║  Press Ctrl+C to stop the server                               ║");
    println!("╚════════════════════════════════════════════════════════════════╝");
    println!();

    // Create SQLite in-memory backend (no setup required)
    let backend = SqliteBackend::in_memory("server_example").await?;

    // Configure the server
    let config = ServerConfig::builder()
        .api_addr_str("127.0.0.1:8080")
        .expect("valid address")
        .namespace("server_example")
        .num_workers(2)
        .fetch_timeout(Duration::from_secs(5))
        .shutdown_timeout(Duration::from_secs(10))
        .build();

    // Create and run the server
    let server = Server::new(config, backend, process_task);

    tracing::info!("Starting server with 2 workers...");
    server.run().await?;

    Ok(())
}
