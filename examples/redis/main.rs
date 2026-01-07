//! Redis backend example with scheduled jobs and retry mechanism.
//!
//! This example demonstrates:
//! - Using Redis as a production-ready backend
//! - Scheduling jobs for future execution
//! - Retry mechanism when jobs fail
//! - Graceful shutdown
//!
//! Prerequisites:
//! - Redis server running on localhost:6379
//!
//! Run with: `cargo run -p example-redis`

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use wg_core::{Client, JobError, JobOptions, JobResult, WorkerPool};
use wg_redis::RedisBackend;

/// Payment processing job payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PaymentJob {
    order_id: String,
    amount: f64,
    currency: String,
}

/// Webhook notification job payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WebhookJob {
    url: String,
    payload: String,
    /// For demo: fail first N attempts to show retry
    fail_count: u32,
}

// Track webhook attempts for demo purposes
static WEBHOOK_ATTEMPTS: AtomicU32 = AtomicU32::new(0);

/// Process a payment job.
async fn process_payment(job: PaymentJob) -> JobResult {
    println!(
        "[payment] order={} amount={:.2} {}",
        job.order_id,
        job.amount,
        job.currency.to_uppercase()
    );

    tokio::time::sleep(Duration::from_millis(800)).await;

    println!("[payment] done\n");
    Ok(())
}

/// Process a webhook job with simulated failures.
async fn process_webhook(job: WebhookJob) -> JobResult {
    let attempt = WEBHOOK_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;

    println!("[webhook] attempt={} url={}", attempt, job.url);

    tokio::time::sleep(Duration::from_millis(300)).await;

    if attempt <= job.fail_count {
        println!("[webhook] failed, will retry\n");
        return Err(JobError::retryable("Connection timeout"));
    }

    println!("[webhook] delivered\n");
    Ok(())
}

#[tokio::main]
async fn main() -> wg_core::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("wg Redis example\n");

    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost".to_string());

    println!("Connecting to {}...", redis_url);

    let backend = match RedisBackend::new(&redis_url, "wg-example").await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("Failed to connect: {}", e);
            eprintln!("Make sure Redis is running: docker run -d -p 6379:6379 redis");
            return Err(e);
        }
    };

    println!("Connected\n");

    // Demo 1: Immediate jobs
    println!("--- Demo 1: Immediate Payment Jobs ---\n");

    let client = Client::new(backend.clone());

    let payments = vec![
        PaymentJob {
            order_id: "ORD-001".to_string(),
            amount: 99.99,
            currency: "usd".to_string(),
        },
        PaymentJob {
            order_id: "ORD-002".to_string(),
            amount: 149.50,
            currency: "eur".to_string(),
        },
    ];

    for payment in payments {
        let job_id = client.enqueue("process_payment", payment.clone()).await?;
        println!("enqueued: {} ({})", job_id, payment.order_id);
    }

    // Demo 2: Scheduled jobs
    println!("\n--- Demo 2: Scheduled Jobs (5s delay) ---\n");

    let delayed_payment = PaymentJob {
        order_id: "ORD-SCHEDULED".to_string(),
        amount: 299.00,
        currency: "usd".to_string(),
    };

    let job_id = client
        .schedule("process_payment", delayed_payment, Duration::from_secs(5))
        .await?;
    println!("scheduled: {} (runs in 5s)", job_id);

    // Demo 3: Retry mechanism
    println!("\n--- Demo 3: Retry (fails 2x then succeeds) ---\n");

    let webhook_backend = RedisBackend::new(&redis_url, "wg-webhooks").await?;
    let webhook_client = Client::new(webhook_backend.clone());

    let webhook = WebhookJob {
        url: "https://api.example.com/webhook".to_string(),
        payload: r#"{"event": "order.completed"}"#.to_string(),
        fail_count: 2,
    };

    let options = JobOptions::with_max_retries(5).retry_delay(Duration::from_secs(3));

    let job_id = webhook_client
        .enqueue_with_options("send_webhook", webhook, options)
        .await?;
    println!("enqueued: {}", job_id);

    // Run worker pools
    println!("\n--- Starting Workers ---\n");

    println!("Queue status:");
    println!("  payment jobs: {}", client.queue_len().await?);
    println!("  scheduled: {}", client.schedule_len().await?);
    println!("  webhook jobs: {}", webhook_client.queue_len().await?);
    println!("\nPress Ctrl+C to stop\n");

    let webhook_handle = tokio::spawn(async move {
        let mut pool = WorkerPool::builder()
            .backend(webhook_backend)
            .namespace("wg-webhooks")
            .workers(1)
            .handler(process_webhook)
            .fetch_timeout(Duration::from_secs(2))
            .shutdown_timeout(Duration::from_secs(5))
            .build()
            .unwrap();

        pool.run().await
    });

    let mut payment_pool = WorkerPool::builder()
        .backend(backend)
        .namespace("wg-example")
        .workers(2)
        .handler(process_payment)
        .fetch_timeout(Duration::from_secs(2))
        .shutdown_timeout(Duration::from_secs(5))
        .build()?;

    tokio::select! {
        result = payment_pool.run() => {
            result?;
        }
        _ = webhook_handle => {}
    }

    println!("\nDone");
    Ok(())
}
