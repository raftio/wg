//! API module for wg server.

use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use wg_core::{Backend, SharedBackend, WorkerPoolInfo};

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

/// Response for listing worker pools.
#[derive(Serialize)]
pub struct WorkersResponse {
    pub pools: Vec<WorkerPoolInfoResponse>,
    pub total: usize,
}

/// Worker pool info for API response.
#[derive(Serialize)]
pub struct WorkerPoolInfoResponse {
    pub pool_id: String,
    pub heartbeat_at: i64,
    pub started_at: i64,
    pub concurrency: usize,
    pub host: String,
    pub pid: u32,
    pub job_names: Vec<String>,
    /// Uptime in seconds.
    pub uptime_secs: i64,
    /// Seconds since last heartbeat.
    pub last_seen_secs: i64,
}

impl From<WorkerPoolInfo> for WorkerPoolInfoResponse {
    fn from(info: WorkerPoolInfo) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        Self {
            pool_id: info.pool_id,
            heartbeat_at: info.heartbeat_at,
            started_at: info.started_at,
            concurrency: info.concurrency,
            host: info.host,
            pid: info.pid,
            job_names: info.job_names,
            uptime_secs: now - info.started_at,
            last_seen_secs: now - info.heartbeat_at,
        }
    }
}

/// Configure API routes.
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("")
            .route("/health", web::get().to(health))
            .service(
                web::scope("/api")
                    .route("/stats", web::get().to(stats))
                    .route("/workers", web::get().to(list_workers))
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

/// List active worker pools.
async fn list_workers(state: web::Data<AppState>) -> impl Responder {
    let backend = &state.backend;

    match backend.list_worker_pools().await {
        Ok(pools) => {
            let total = pools.len();
            let pools: Vec<WorkerPoolInfoResponse> = pools.into_iter().map(|p| p.into()).collect();
            HttpResponse::Ok().json(WorkersResponse { pools, total })
        }
        Err(e) => HttpResponse::InternalServerError().json(ApiResponse {
            success: false,
            message: format!("Failed to list worker pools: {}", e),
        }),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limit() {
        assert_eq!(default_limit(), 20);
    }

    #[test]
    fn test_health_response_serialization() {
        let response = HealthResponse { status: "ok" };
        let json = serde_json::to_string(&response).unwrap();
        assert_eq!(json, r#"{"status":"ok"}"#);
    }

    #[test]
    fn test_stats_response_serialization() {
        let response = StatsResponse {
            queue: 10,
            scheduled: 5,
            retry: 2,
            dead: 1,
        };
        let json = serde_json::to_string(&response).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["queue"], 10);
        assert_eq!(parsed["scheduled"], 5);
        assert_eq!(parsed["retry"], 2);
        assert_eq!(parsed["dead"], 1);
    }

    #[test]
    fn test_enqueue_request_deserialization() {
        let json = r#"{"payload": {"task": "send_email", "to": "test@example.com"}}"#;
        let request: EnqueueRequest = serde_json::from_str(json).unwrap();

        assert_eq!(request.payload["task"], "send_email");
        assert_eq!(request.payload["to"], "test@example.com");
    }

    #[test]
    fn test_enqueue_response_serialization() {
        let response = EnqueueResponse {
            success: true,
            message: "Job created".to_string(),
        };
        let json = serde_json::to_string(&response).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["success"], true);
        assert_eq!(parsed["message"], "Job created");
    }

    #[test]
    fn test_dead_job_item_serialization() {
        let item = DeadJobItem {
            id: "abc-123".to_string(),
            payload: serde_json::json!({"task": "failed"}),
            created_at: 1704067200,
            last_error: Some("timeout".to_string()),
        };
        let json = serde_json::to_string(&item).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["id"], "abc-123");
        assert_eq!(parsed["payload"]["task"], "failed");
        assert_eq!(parsed["created_at"], 1704067200);
        assert_eq!(parsed["last_error"], "timeout");
    }

    #[test]
    fn test_dead_job_item_serialization_no_error() {
        let item = DeadJobItem {
            id: "def-456".to_string(),
            payload: serde_json::json!({}),
            created_at: 0,
            last_error: None,
        };
        let json = serde_json::to_string(&item).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert!(parsed["last_error"].is_null());
    }

    #[test]
    fn test_dead_list_response_serialization() {
        let response = DeadListResponse {
            jobs: vec![
                DeadJobItem {
                    id: "job1".to_string(),
                    payload: serde_json::json!({"n": 1}),
                    created_at: 100,
                    last_error: None,
                },
                DeadJobItem {
                    id: "job2".to_string(),
                    payload: serde_json::json!({"n": 2}),
                    created_at: 200,
                    last_error: Some("error".to_string()),
                },
            ],
            total: 2,
        };
        let json = serde_json::to_string(&response).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["total"], 2);
        assert_eq!(parsed["jobs"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_dead_list_query_defaults() {
        let json = r#"{}"#;
        let query: DeadListQuery = serde_json::from_str(json).unwrap();

        assert_eq!(query.limit, 20);
        assert_eq!(query.offset, 0);
    }

    #[test]
    fn test_dead_list_query_custom_values() {
        let json = r#"{"limit": 50, "offset": 100}"#;
        let query: DeadListQuery = serde_json::from_str(json).unwrap();

        assert_eq!(query.limit, 50);
        assert_eq!(query.offset, 100);
    }

    #[test]
    fn test_dead_list_query_partial_values() {
        let json = r#"{"limit": 10}"#;
        let query: DeadListQuery = serde_json::from_str(json).unwrap();

        assert_eq!(query.limit, 10);
        assert_eq!(query.offset, 0);
    }

    #[test]
    fn test_api_response_serialization() {
        let response = ApiResponse {
            success: false,
            message: "Something went wrong".to_string(),
        };
        let json = serde_json::to_string(&response).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["success"], false);
        assert_eq!(parsed["message"], "Something went wrong");
    }

    #[test]
    fn test_dead_list_response_empty() {
        let response = DeadListResponse {
            jobs: vec![],
            total: 0,
        };
        let json = serde_json::to_string(&response).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["total"], 0);
        assert!(parsed["jobs"].as_array().unwrap().is_empty());
    }

    #[test]
    fn test_worker_pool_info_response_from() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        let info = WorkerPoolInfo {
            pool_id: "test-pool".to_string(),
            heartbeat_at: now - 5,
            started_at: now - 3600,
            concurrency: 4,
            host: "server-1".to_string(),
            pid: 12345,
            job_names: vec!["email".to_string(), "report".to_string()],
        };

        let response: WorkerPoolInfoResponse = info.into();

        assert_eq!(response.pool_id, "test-pool");
        assert_eq!(response.concurrency, 4);
        assert_eq!(response.host, "server-1");
        assert_eq!(response.pid, 12345);
        assert_eq!(response.job_names.len(), 2);
        assert!(response.uptime_secs >= 3600);
        assert!(response.last_seen_secs >= 5);
    }

    #[test]
    fn test_workers_response_serialization() {
        let response = WorkersResponse {
            pools: vec![WorkerPoolInfoResponse {
                pool_id: "pool-1".to_string(),
                heartbeat_at: 1704067200,
                started_at: 1704063600,
                concurrency: 8,
                host: "host-1".to_string(),
                pid: 1234,
                job_names: vec!["task".to_string()],
                uptime_secs: 3600,
                last_seen_secs: 5,
            }],
            total: 1,
        };
        let json = serde_json::to_string(&response).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["total"], 1);
        assert_eq!(parsed["pools"][0]["pool_id"], "pool-1");
        assert_eq!(parsed["pools"][0]["concurrency"], 8);
    }
}
