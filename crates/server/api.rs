//! API module for wg server.

use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use wg_core::{Backend, SharedBackend};

/// Application state shared across handlers.
pub struct AppState {
    pub backend: SharedBackend,
}

/// Response for health check.
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}

/// Response for queue statistics.
#[derive(Serialize)]
pub struct StatsResponse {
    pub queue: usize,
    pub scheduled: usize,
    pub retry: usize,
    pub dead: usize,
}

/// Request body for enqueueing a job.
#[derive(Deserialize)]
pub struct EnqueueRequest {
    pub payload: serde_json::Value,
}

/// Response for enqueue operation.
#[derive(Serialize)]
pub struct EnqueueResponse {
    pub success: bool,
    pub message: String,
}

/// Dead job item in list response.
#[derive(Serialize)]
pub struct DeadJobItem {
    pub id: String,
    pub payload: serde_json::Value,
    pub created_at: i64,
    pub last_error: Option<String>,
}

/// Response for listing dead jobs.
#[derive(Serialize)]
pub struct DeadListResponse {
    pub jobs: Vec<DeadJobItem>,
    pub total: usize,
}

/// Query parameters for dead jobs list.
#[derive(Deserialize)]
pub struct DeadListQuery {
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
}

fn default_limit() -> usize {
    20
}

/// Generic API response.
#[derive(Serialize)]
pub struct ApiResponse {
    pub success: bool,
    pub message: String,
}

/// Configure API routes.
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("")
            .route("/health", web::get().to(health))
            .service(
                web::scope("/api")
                    .route("/stats", web::get().to(stats))
                    .route("/jobs", web::post().to(enqueue))
                    .route("/dead", web::get().to(list_dead))
                    .route("/dead/{id}/retry", web::post().to(retry_dead))
                    .route("/dead/{id}", web::delete().to(delete_dead)),
            ),
    );
}

/// Health check endpoint.
async fn health() -> impl Responder {
    HttpResponse::Ok().json(HealthResponse { status: "ok" })
}

/// Get queue statistics.
async fn stats(state: web::Data<AppState>) -> impl Responder {
    let backend = &state.backend;

    let queue = backend.queue_len().await.unwrap_or(0);
    let scheduled = backend.schedule_len().await.unwrap_or(0);
    let retry = backend.retry_len().await.unwrap_or(0);
    let dead = backend.dead_len().await.unwrap_or(0);

    HttpResponse::Ok().json(StatsResponse {
        queue,
        scheduled,
        retry,
        dead,
    })
}

/// Enqueue a new job.
async fn enqueue(state: web::Data<AppState>, body: web::Json<EnqueueRequest>) -> impl Responder {
    let backend = &state.backend;

    // Create a simple job JSON with the payload
    let job_json = serde_json::json!({
        "id": { "0": uuid::Uuid::new_v4().to_string() },
        "payload": body.payload,
        "options": {
            "max_retries": 3,
            "retry_count": 0,
            "timeout": 300000,
            "retry_delay": 10000
        },
        "status": "Pending",
        "created_at": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
        "scheduled_at": null,
        "last_error": null
    });

    match backend.push_job(&job_json.to_string()).await {
        Ok(()) => HttpResponse::Ok().json(EnqueueResponse {
            success: true,
            message: "Job enqueued successfully".to_string(),
        }),
        Err(e) => HttpResponse::InternalServerError().json(EnqueueResponse {
            success: false,
            message: format!("Failed to enqueue job: {}", e),
        }),
    }
}

/// List dead jobs.
async fn list_dead(state: web::Data<AppState>, query: web::Query<DeadListQuery>) -> impl Responder {
    let backend = &state.backend;

    let total = backend.dead_len().await.unwrap_or(0);
    let jobs_json = match backend.list_dead(query.limit, query.offset).await {
        Ok(jobs) => jobs,
        Err(e) => {
            return HttpResponse::InternalServerError().json(ApiResponse {
                success: false,
                message: format!("Failed to list dead jobs: {}", e),
            });
        }
    };

    let jobs: Vec<DeadJobItem> = jobs_json
        .into_iter()
        .filter_map(|json| {
            let parsed: serde_json::Value = serde_json::from_str(&json).ok()?;
            Some(DeadJobItem {
                id: parsed["id"]["0"].as_str()?.to_string(),
                payload: parsed["payload"].clone(),
                created_at: parsed["created_at"].as_i64().unwrap_or(0),
                last_error: parsed["last_error"].as_str().map(String::from),
            })
        })
        .collect();

    HttpResponse::Ok().json(DeadListResponse { jobs, total })
}

/// Retry a dead job by ID.
async fn retry_dead(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let job_id = path.into_inner();
    let backend = &state.backend;

    // Get the dead job by ID
    let job_json = match backend.get_dead_by_id(&job_id).await {
        Ok(Some(json)) => json,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                success: false,
                message: format!("Dead job with ID {} not found", job_id),
            });
        }
        Err(e) => {
            return HttpResponse::InternalServerError().json(ApiResponse {
                success: false,
                message: format!("Failed to get dead job: {}", e),
            });
        }
    };

    // Update job status and reset retry count
    let mut job: serde_json::Value = match serde_json::from_str(&job_json) {
        Ok(v) => v,
        Err(e) => {
            return HttpResponse::InternalServerError().json(ApiResponse {
                success: false,
                message: format!("Failed to parse job JSON: {}", e),
            });
        }
    };

    job["status"] = serde_json::json!("Pending");
    job["options"]["retry_count"] = serde_json::json!(0);
    job["last_error"] = serde_json::Value::Null;

    let updated_json = job.to_string();

    // Remove from dead queue and push to main queue
    if let Err(e) = backend.remove_dead(&job_json).await {
        return HttpResponse::InternalServerError().json(ApiResponse {
            success: false,
            message: format!("Failed to remove from dead queue: {}", e),
        });
    }

    if let Err(e) = backend.push_job(&updated_json).await {
        return HttpResponse::InternalServerError().json(ApiResponse {
            success: false,
            message: format!("Failed to push job to queue: {}", e),
        });
    }

    HttpResponse::Ok().json(ApiResponse {
        success: true,
        message: format!("Job {} moved back to queue", job_id),
    })
}

/// Delete a dead job by ID.
async fn delete_dead(state: web::Data<AppState>, path: web::Path<String>) -> impl Responder {
    let job_id = path.into_inner();
    let backend = &state.backend;

    // Get the dead job by ID to get the full JSON
    let job_json = match backend.get_dead_by_id(&job_id).await {
        Ok(Some(json)) => json,
        Ok(None) => {
            return HttpResponse::NotFound().json(ApiResponse {
                success: false,
                message: format!("Dead job with ID {} not found", job_id),
            });
        }
        Err(e) => {
            return HttpResponse::InternalServerError().json(ApiResponse {
                success: false,
                message: format!("Failed to get dead job: {}", e),
            });
        }
    };

    // Remove from dead queue
    if let Err(e) = backend.remove_dead(&job_json).await {
        return HttpResponse::InternalServerError().json(ApiResponse {
            success: false,
            message: format!("Failed to delete dead job: {}", e),
        });
    }

    HttpResponse::Ok().json(ApiResponse {
        success: true,
        message: format!("Job {} deleted", job_id),
    })
}
