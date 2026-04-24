use std::{collections::HashMap, sync::Arc, time::Instant};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::info;
use triad_core::types::ModuleHealth;

// ── Shared state ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AdminState {
    pub started_at: Instant,
    /// Snapshot of pattern module health, updated by the engine.
    pub module_health: Arc<tokio::sync::RwLock<HashMap<String, ModuleHealth>>>,
    pub metrics_handle: Option<PrometheusHandle>,
}

impl AdminState {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            module_health: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            metrics_handle: None,
        }
    }

    pub fn with_metrics_handle(mut self, handle: PrometheusHandle) -> Self {
        self.metrics_handle = Some(handle);
        self
    }
}

impl Default for AdminState {
    fn default() -> Self {
        Self::new()
    }
}

// ── Server ───────────────────────────────────────────────────────────────────

pub struct AdminServer {
    port: u16,
    state: AdminState,
    is_stub: bool,
}

impl AdminServer {
    pub fn new(port: u16, state: AdminState) -> Self {
        Self {
            port,
            state,
            is_stub: false,
        }
    }

    /// Stub server for unit tests — `serve()` just awaits cancellation.
    pub fn new_stub() -> Self {
        Self {
            port: 0,
            state: AdminState::new(),
            is_stub: true,
        }
    }

    pub async fn serve(&mut self, cancel: CancellationToken) -> Result<(), anyhow::Error> {
        if self.is_stub {
            cancel.cancelled().await;
            return Ok(());
        }

        let router = admin_router(self.state.clone());
        let addr = format!("0.0.0.0:{}", self.port);
        let listener = TcpListener::bind(&addr).await?;
        info!(addr = %addr, "admin HTTP server listening");

        axum::serve(listener, router)
            .with_graceful_shutdown(async move { cancel.cancelled().await })
            .await?;
        Ok(())
    }
}

// ── Router ───────────────────────────────────────────────────────────────────

pub fn admin_router(state: AdminState) -> Router {
    Router::new()
        // Health probes
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/health/started", get(started))
        // Metrics
        .route("/metrics", get(metrics_handler))
        // Patterns
        .route("/patterns", get(list_patterns))
        .route("/patterns/:name/pause", post(pause_pattern))
        .route("/patterns/:name/resume", post(resume_pattern))
        .route("/patterns/:name/replay", post(replay_pattern))
        // Operational
        .route("/lag", get(get_lag))
        .route("/dlq/:topic", get(list_dlq))
        .route("/dlq/:topic/replay", post(replay_dlq))
        .route("/dlq/:topic", delete(drop_dlq))
        .route("/registry", get(get_registry))
        // Saga
        .route("/saga", get(list_sagas))
        .route("/saga/:id", get(inspect_saga))
        .route("/saga/:id/cancel", post(cancel_saga))
        // Config
        .route("/config/reload", post(reload_config))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(state)
}

// ── Response types ────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct LiveResponse {
    status: &'static str,
    uptime_seconds: u64,
}

#[derive(Serialize)]
struct BackendHealth {
    status: &'static str,
}

#[derive(Serialize)]
struct ReadyResponse {
    status: &'static str,
    backends: HashMap<String, BackendHealth>,
    cold_start_complete: bool,
    drain_mode: bool,
    leader: bool,
}

#[derive(Serialize)]
struct StartedResponse {
    status: &'static str,
    cold_start_complete: bool,
    patterns_loaded: u32,
    startup_duration_ms: u64,
}

#[derive(Serialize)]
struct PatternSummary {
    name: String,
    pattern_type: String,
    status: String,
}

#[derive(Serialize)]
struct LagEntry {
    pattern_name: String,
    topic: String,
    partition: i32,
    lag_messages: i64,
}

#[derive(Serialize)]
struct DlqEntry {
    topic: String,
    message_count: i64,
}

#[derive(Serialize)]
struct RegistryEntry {
    name: String,
    pattern_type: String,
    version: &'static str,
}

#[derive(Serialize)]
struct SagaSummary {
    saga_id: String,
    saga_name: String,
    current_step: i32,
    status: String,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

// ── Handlers ─────────────────────────────────────────────────────────────────

async fn live(State(state): State<AdminState>) -> impl IntoResponse {
    Json(LiveResponse {
        status: "ok",
        uptime_seconds: state.started_at.elapsed().as_secs(),
    })
}

async fn ready(State(state): State<AdminState>) -> impl IntoResponse {
    let health = state.module_health.read().await;
    let all_running = health
        .values()
        .all(|h| h.state == triad_core::types::ModuleState::Running);

    let status = if all_running { "ok" } else { "degraded" };
    let backends = HashMap::from([
        ("postgres".to_string(), BackendHealth { status: "ok" }),
        ("kafka".to_string(), BackendHealth { status: "ok" }),
        ("redis".to_string(), BackendHealth { status: "ok" }),
    ]);

    Json(ReadyResponse {
        status,
        backends,
        cold_start_complete: true,
        drain_mode: false,
        leader: true,
    })
}

async fn started(State(state): State<AdminState>) -> impl IntoResponse {
    let health = state.module_health.read().await;
    Json(StartedResponse {
        status: "ok",
        cold_start_complete: true,
        patterns_loaded: health.len() as u32,
        startup_duration_ms: state.started_at.elapsed().as_millis() as u64,
    })
}

async fn metrics_handler(State(state): State<AdminState>) -> impl IntoResponse {
    match &state.metrics_handle {
        Some(handle) => (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                "text/plain; version=0.0.4",
            )],
            handle.render(),
        )
            .into_response(),
        None => (StatusCode::SERVICE_UNAVAILABLE, "metrics not configured").into_response(),
    }
}

async fn list_patterns(State(state): State<AdminState>) -> impl IntoResponse {
    let health = state.module_health.read().await;
    let patterns: Vec<PatternSummary> = health
        .iter()
        .map(|(name, h)| PatternSummary {
            name: name.clone(),
            pattern_type: "unknown".to_string(),
            status: format!("{:?}", h.state),
        })
        .collect();
    Json(patterns)
}

async fn pause_pattern(
    Path(name): Path<String>,
    State(_state): State<AdminState>,
) -> impl IntoResponse {
    info!(pattern = %name, "pause requested");
    StatusCode::ACCEPTED
}

async fn resume_pattern(
    Path(name): Path<String>,
    State(_state): State<AdminState>,
) -> impl IntoResponse {
    info!(pattern = %name, "resume requested");
    StatusCode::ACCEPTED
}

async fn replay_pattern(
    Path(name): Path<String>,
    State(_state): State<AdminState>,
) -> impl IntoResponse {
    info!(pattern = %name, "replay requested");
    StatusCode::ACCEPTED
}

async fn get_lag() -> impl IntoResponse {
    Json(Vec::<LagEntry>::new())
}

async fn list_dlq(Path(topic): Path<String>) -> impl IntoResponse {
    Json(DlqEntry {
        topic,
        message_count: 0,
    })
}

async fn replay_dlq(Path(topic): Path<String>) -> impl IntoResponse {
    info!(topic = %topic, "DLQ replay requested");
    StatusCode::ACCEPTED
}

async fn drop_dlq(Path(topic): Path<String>) -> impl IntoResponse {
    info!(topic = %topic, "DLQ drop requested");
    StatusCode::ACCEPTED
}

async fn get_registry() -> impl IntoResponse {
    let entries: Vec<RegistryEntry> = vec![
        RegistryEntry {
            name: "outbox".to_string(),
            pattern_type: "outbox".to_string(),
            version: "1.0",
        },
        RegistryEntry {
            name: "inbox".to_string(),
            pattern_type: "inbox".to_string(),
            version: "1.0",
        },
        RegistryEntry {
            name: "cdc".to_string(),
            pattern_type: "cdc".to_string(),
            version: "1.0",
        },
        RegistryEntry {
            name: "saga".to_string(),
            pattern_type: "saga".to_string(),
            version: "1.0",
        },
        RegistryEntry {
            name: "eos".to_string(),
            pattern_type: "eos".to_string(),
            version: "1.0",
        },
        RegistryEntry {
            name: "cache".to_string(),
            pattern_type: "cache".to_string(),
            version: "1.0",
        },
        RegistryEntry {
            name: "webhook".to_string(),
            pattern_type: "webhook".to_string(),
            version: "1.0",
        },
        RegistryEntry {
            name: "feature_flag".to_string(),
            pattern_type: "feature_flag".to_string(),
            version: "1.0",
        },
        RegistryEntry {
            name: "rate_limit".to_string(),
            pattern_type: "rate_limit".to_string(),
            version: "1.0",
        },
        RegistryEntry {
            name: "dlq".to_string(),
            pattern_type: "dlq".to_string(),
            version: "1.0",
        },
    ];
    Json(entries)
}

async fn list_sagas() -> impl IntoResponse {
    Json(Vec::<SagaSummary>::new())
}

async fn inspect_saga(Path(id): Path<String>) -> impl IntoResponse {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: format!("saga {id} not found"),
        }),
    )
}

async fn cancel_saga(Path(id): Path<String>) -> impl IntoResponse {
    info!(saga_id = %id, "saga cancel requested");
    StatusCode::ACCEPTED
}

async fn reload_config() -> impl IntoResponse {
    StatusCode::ACCEPTED
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;

    fn test_state() -> AdminState {
        AdminState::new()
    }

    async fn get_json(router: Router, path: &str) -> (StatusCode, serde_json::Value) {
        let req = Request::builder().uri(path).body(Body::empty()).unwrap();
        let resp = router.oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }

    #[tokio::test]
    async fn test_health_live_returns_ok() {
        let router = admin_router(test_state());
        let (status, body) = get_json(router, "/health/live").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
        assert!(body["uptime_seconds"].as_u64().is_some());
    }

    #[tokio::test]
    async fn test_health_ready_returns_ok() {
        let router = admin_router(test_state());
        let (status, body) = get_json(router, "/health/ready").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
        assert!(body["backends"].is_object());
    }

    #[tokio::test]
    async fn test_health_started_returns_ok() {
        let router = admin_router(test_state());
        let (status, body) = get_json(router, "/health/started").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
    }

    #[tokio::test]
    async fn test_list_patterns_returns_array() {
        let router = admin_router(test_state());
        let (status, body) = get_json(router, "/patterns").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_array());
    }

    #[tokio::test]
    async fn test_registry_returns_known_patterns() {
        let router = admin_router(test_state());
        let (status, body) = get_json(router, "/registry").await;
        assert_eq!(status, StatusCode::OK);
        let arr = body.as_array().unwrap();
        assert!(!arr.is_empty());
        let names: Vec<&str> = arr.iter().map(|e| e["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"outbox"));
        assert!(names.contains(&"saga"));
    }

    #[tokio::test]
    async fn test_lag_returns_array() {
        let router = admin_router(test_state());
        let (status, body) = get_json(router, "/lag").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_array());
    }

    #[tokio::test]
    async fn test_list_sagas_returns_array() {
        let router = admin_router(test_state());
        let (status, body) = get_json(router, "/saga").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_array());
    }

    #[tokio::test]
    async fn test_inspect_saga_not_found() {
        let router = admin_router(test_state());
        let req = Request::builder()
            .uri("/saga/nonexistent-id")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_admin_server_stub_exits_on_cancel() {
        let mut server = AdminServer::new_stub();
        let cancel = CancellationToken::new();
        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            cancel2.cancel();
        });
        let result =
            tokio::time::timeout(std::time::Duration::from_secs(1), server.serve(cancel)).await;
        assert!(result.is_ok());
    }
}
