//! Basic example using SQLite in-memory backend.
//!
//! This example demonstrates:
//! - Creating a job queue with SQLite in-memory storage
//! - Enqueueing jobs from a client
//! - Processing jobs with a worker pool
//!
//! Run with: `cargo run -p example-basic`

use serde::{Deserialize, Serialize};
use std::time::Duration;
use wg_core::{Client, JobResult, WorkerPool};
use wg_sqlite::SqliteBackend;

/// Email notification job payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EmailJob {
    to: String,
    subject: String,
    body: String,
}

/// Process an email job.
async fn process_email(job: EmailJob) -> JobResult {
    println!("[email] sending to: {}", job.to);
    println!("        subject: {}", job.subject);
    println!("        body: {}", job.body);

    // Simulate some work
    tokio::time::sleep(Duration::from_millis(500)).await;

    println!("[email] sent\n");
    Ok(())
}

#[tokio::main]
async fn main() -> wg_core::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("wg job queue example\n");

    // Create SQLite in-memory backend (no setup required)
    let backend = SqliteBackend::in_memory("example").await?;

    let client = Client::new(backend.clone());

    let emails = vec![
        EmailJob {
            to: "alice@example.com".to_string(),
            subject: "Welcome!".to_string(),
            body: "Thanks for signing up.".to_string(),
        },
        EmailJob {
            to: "bob@example.com".to_string(),
            subject: "Your order shipped".to_string(),
            body: "Your package is on the way.".to_string(),
        },
        EmailJob {
            to: "charlie@example.com".to_string(),
            subject: "Password reset".to_string(),
            body: "Click here to reset your password.".to_string(),
        },
        EmailJob {
            to: "diana@example.com".to_string(),
            subject: "Weekly digest".to_string(),
            body: "Here's what you missed this week.".to_string(),
        },
        EmailJob {
            to: "eve@example.com".to_string(),
            subject: "New feature announcement".to_string(),
            body: "Check out our latest features!".to_string(),
        },
    ];

    println!("Enqueueing {} jobs...\n", emails.len());

    for email in emails {
        let job_id = client.enqueue("send_email", email).await?;
        println!("  enqueued: {}", job_id);
    }

    println!("\nQueue length: {}", client.queue_len().await?);
    println!("\nStarting worker pool (2 workers)...");
    println!("Press Ctrl+C to stop\n");

    let mut pool = WorkerPool::builder()
        .backend(backend)
        .namespace("example")
        .workers(2)
        .handler(process_email)
        .fetch_timeout(Duration::from_secs(2))
        .shutdown_timeout(Duration::from_secs(5))
        .build()?;

    pool.run().await?;

    Ok(())
}
