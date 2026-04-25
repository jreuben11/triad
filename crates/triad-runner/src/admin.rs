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
use tokio::{net::TcpListener, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tracing::info;
use triad_core::types::ModuleHealth;

// ── Pattern control channel ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum PatternControl {
    Pause(String),
    Resume(String),
    Replay(String),
    Reload(String),
}

// ── Shared state ─────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AdminState {
    pub started_at: Instant,
    /// Snapshot of pattern module health, updated by the engine.
    pub module_health: Arc<tokio::sync::RwLock<HashMap<String, ModuleHealth>>>,
    pub metrics_handle: Option<PrometheusHandle>,
    pub pg_pool: Option<sqlx::PgPool>,
    pub kafka_brokers: Option<String>,
    pub redis_url: Option<String>,
    pub control_tx: Option<mpsc::Sender<PatternControl>>,
    pub dlq_replayer: Option<Arc<crate::patterns::dlq::DlqReplayer>>,
    pub config_path: Option<String>,
    pub shared_config: Option<Arc<tokio::sync::RwLock<triad_core::config::TriadConfig>>>,
}

impl AdminState {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            module_health: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            metrics_handle: None,
            pg_pool: None,
            kafka_brokers: None,
            redis_url: None,
            control_tx: None,
            dlq_replayer: None,
            config_path: None,
            shared_config: None,
        }
    }

    pub fn with_metrics_handle(mut self, handle: PrometheusHandle) -> Self {
        self.metrics_handle = Some(handle);
        self
    }

    pub fn with_pg_pool(mut self, pool: sqlx::PgPool) -> Self {
        self.pg_pool = Some(pool);
        self
    }

    pub fn with_kafka_brokers(mut self, brokers: impl Into<String>) -> Self {
        self.kafka_brokers = Some(brokers.into());
        self
    }

    pub fn with_redis_url(mut self, url: impl Into<String>) -> Self {
        self.redis_url = Some(url.into());
        self
    }

    pub fn with_control_tx(mut self, tx: mpsc::Sender<PatternControl>) -> Self {
        self.control_tx = Some(tx);
        self
    }

    pub fn with_dlq_replayer(mut self, replayer: Arc<crate::patterns::dlq::DlqReplayer>) -> Self {
        self.dlq_replayer = Some(replayer);
        self
    }

    pub fn with_config_path(mut self, path: impl Into<String>) -> Self {
        self.config_path = Some(path.into());
        self
    }

    pub fn with_shared_config(
        mut self,
        cfg: Arc<tokio::sync::RwLock<triad_core::config::TriadConfig>>,
    ) -> Self {
        self.shared_config = Some(cfg);
        self
    }
}

impl Default for AdminState {
    fn default() -> Self {
        Self::new()
    }
}

// ── Server ────────────────────────────────────────────────────────────────────

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

// ── Router ────────────────────────────────────────────────────────────────────

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
        // Checkpoints
        .route("/checkpoints", get(list_checkpoints))
        // Saga
        .route("/saga", get(list_sagas))
        .route("/saga/:id", get(inspect_saga))
        .route("/saga/:id/cancel", post(cancel_saga))
        // Config / pipeline
        .route("/config/reload", post(reload_config))
        .route("/pipelines/:name/reload", post(reload_pipeline))
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(state)
}

// ── Response types ─────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct LiveResponse {
    status: &'static str,
    uptime_seconds: u64,
}

#[derive(Serialize)]
struct BackendHealth {
    status: String,
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
struct SagaDetail {
    saga_id: String,
    saga_name: String,
    current_step: i32,
    status: String,
    version: i64,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
struct CheckpointEntry {
    pattern_name: String,
    pipeline_name: String,
    owner_instance_id: String,
    version: i64,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

// ── Backend health helpers ─────────────────────────────────────────────────────

async fn check_pg_health(pool: &Option<sqlx::PgPool>) -> String {
    match pool {
        None => "unconfigured".to_string(),
        Some(p) => {
            if sqlx::query_scalar::<_, i32>("SELECT 1")
                .fetch_one(p)
                .await
                .is_ok()
            {
                "ok".to_string()
            } else {
                "degraded".to_string()
            }
        }
    }
}

async fn check_kafka_health(brokers: Option<String>) -> String {
    let Some(b) = brokers else {
        return "unconfigured".to_string();
    };
    let ok = tokio::task::spawn_blocking(move || -> bool {
        use rdkafka::{
            config::ClientConfig,
            consumer::{BaseConsumer, Consumer},
        };
        let Ok(consumer): Result<BaseConsumer, _> = ClientConfig::new()
            .set("bootstrap.servers", &b)
            .set("socket.connection.setup.timeout.ms", "2000")
            .create()
        else {
            return false;
        };
        consumer
            .fetch_metadata(None, std::time::Duration::from_secs(2))
            .is_ok()
    })
    .await
    .unwrap_or(false);
    if ok {
        "ok".to_string()
    } else {
        "degraded".to_string()
    }
}

async fn check_redis_health(url: Option<String>) -> String {
    let Some(u) = url else {
        return "unconfigured".to_string();
    };
    let result: Result<(), redis::RedisError> = async {
        let client = redis::Client::open(u.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let _: String = redis::cmd("PING").query_async(&mut conn).await?;
        Ok(())
    }
    .await;
    if result.is_ok() {
        "ok".to_string()
    } else {
        "degraded".to_string()
    }
}

// ── Handlers ──────────────────────────────────────────────────────────────────

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
    drop(health);

    let (pg_status, kafka_status, redis_status) = tokio::join!(
        check_pg_health(&state.pg_pool),
        check_kafka_health(state.kafka_brokers.clone()),
        check_redis_health(state.redis_url.clone()),
    );

    let has_degraded =
        pg_status == "degraded" || kafka_status == "degraded" || redis_status == "degraded";
    let status: &'static str = if !has_degraded && all_running {
        "ok"
    } else {
        "degraded"
    };

    let mut backends = HashMap::new();
    backends.insert("postgres".to_string(), BackendHealth { status: pg_status });
    backends.insert(
        "kafka".to_string(),
        BackendHealth {
            status: kafka_status,
        },
    );
    backends.insert(
        "redis".to_string(),
        BackendHealth {
            status: redis_status,
        },
    );

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
    State(state): State<AdminState>,
) -> impl IntoResponse {
    info!(pattern = %name, "pause requested");
    if let Some(tx) = &state.control_tx {
        let _ = tx.try_send(PatternControl::Pause(name));
    }
    StatusCode::ACCEPTED
}

async fn resume_pattern(
    Path(name): Path<String>,
    State(state): State<AdminState>,
) -> impl IntoResponse {
    info!(pattern = %name, "resume requested");
    if let Some(tx) = &state.control_tx {
        let _ = tx.try_send(PatternControl::Resume(name));
    }
    StatusCode::ACCEPTED
}

async fn replay_pattern(
    Path(name): Path<String>,
    State(state): State<AdminState>,
) -> impl IntoResponse {
    info!(pattern = %name, "replay requested");
    if let Some(tx) = &state.control_tx {
        let _ = tx.try_send(PatternControl::Replay(name));
    }
    StatusCode::ACCEPTED
}

async fn get_lag(State(state): State<AdminState>) -> impl IntoResponse {
    let Some(brokers) = state.kafka_brokers.clone() else {
        return Json(Vec::<LagEntry>::new()).into_response();
    };

    let entries = tokio::task::spawn_blocking(move || -> Vec<LagEntry> {
        use rdkafka::{
            config::ClientConfig,
            consumer::{BaseConsumer, Consumer},
        };

        let Ok(consumer): Result<BaseConsumer, _> = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("socket.connection.setup.timeout.ms", "2000")
            .create()
        else {
            return Vec::new();
        };

        let Ok(metadata) = consumer.fetch_metadata(None, std::time::Duration::from_secs(2)) else {
            return Vec::new();
        };

        let mut result = Vec::new();
        for topic in metadata.topics() {
            for partition in topic.partitions() {
                let (low, high) = consumer
                    .fetch_watermarks(
                        topic.name(),
                        partition.id(),
                        std::time::Duration::from_secs(2),
                    )
                    .unwrap_or((0, 0));
                result.push(LagEntry {
                    pattern_name: "unknown".to_string(),
                    topic: topic.name().to_string(),
                    partition: partition.id(),
                    lag_messages: high - low,
                });
            }
        }
        result
    })
    .await
    .unwrap_or_default();

    Json(entries).into_response()
}

async fn list_dlq(Path(topic): Path<String>) -> impl IntoResponse {
    Json(DlqEntry {
        topic,
        message_count: 0,
    })
}

async fn replay_dlq(
    Path(topic): Path<String>,
    State(state): State<AdminState>,
) -> impl IntoResponse {
    info!(topic = %topic, "DLQ replay requested");
    if let Some(replayer) = &state.dlq_replayer {
        match replayer.replay(&topic).await {
            Ok(count) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({ "replayed": count })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response(),
        }
    } else {
        StatusCode::ACCEPTED.into_response()
    }
}

async fn drop_dlq(Path(topic): Path<String>, State(state): State<AdminState>) -> impl IntoResponse {
    info!(topic = %topic, "DLQ drop requested");
    if let Some(replayer) = &state.dlq_replayer {
        match replayer.purge(&topic).await {
            Ok(count) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({ "purged": count })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
                .into_response(),
        }
    } else {
        StatusCode::ACCEPTED.into_response()
    }
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

async fn list_checkpoints(State(state): State<AdminState>) -> impl IntoResponse {
    let Some(pool) = &state.pg_pool else {
        return Json(Vec::<CheckpointEntry>::new()).into_response();
    };

    let rows: Vec<(String, String, String, i64)> = sqlx::query_as(
        "SELECT pattern_name, pipeline_name, owner_instance_id, version \
         FROM triad.triad_checkpoints ORDER BY pattern_name",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    let entries: Vec<CheckpointEntry> = rows
        .into_iter()
        .map(
            |(pattern_name, pipeline_name, owner_instance_id, version)| CheckpointEntry {
                pattern_name,
                pipeline_name,
                owner_instance_id,
                version,
            },
        )
        .collect();

    Json(entries).into_response()
}

async fn list_sagas(State(state): State<AdminState>) -> impl IntoResponse {
    let Some(pool) = &state.pg_pool else {
        return Json(Vec::<SagaSummary>::new()).into_response();
    };

    let rows: Vec<(uuid::Uuid, String, i32, String)> = sqlx::query_as(
        "SELECT saga_id, saga_name, current_step, status \
         FROM triad.triad_saga_checkpoints ORDER BY updated_at DESC",
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    let sagas: Vec<SagaSummary> = rows
        .into_iter()
        .map(|(id, name, step, status)| SagaSummary {
            saga_id: id.to_string(),
            saga_name: name,
            current_step: step,
            status,
        })
        .collect();

    Json(sagas).into_response()
}

async fn inspect_saga(
    Path(id): Path<String>,
    State(state): State<AdminState>,
) -> impl IntoResponse {
    let Some(pool) = &state.pg_pool else {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("saga {id} not found"),
            }),
        )
            .into_response();
    };

    let Ok(uuid) = uuid::Uuid::parse_str(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("saga {id} not found"),
            }),
        )
            .into_response();
    };

    let row: Option<(
        uuid::Uuid,
        String,
        i32,
        String,
        i64,
        chrono::DateTime<chrono::Utc>,
    )> = sqlx::query_as(
        "SELECT saga_id, saga_name, current_step, status, version, updated_at \
             FROM triad.triad_saga_checkpoints WHERE saga_id = $1",
    )
    .bind(uuid)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    match row {
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("saga {id} not found"),
            }),
        )
            .into_response(),
        Some((sid, name, step, status, version, updated_at)) => Json(SagaDetail {
            saga_id: sid.to_string(),
            saga_name: name,
            current_step: step,
            status,
            version,
            updated_at,
        })
        .into_response(),
    }
}

async fn cancel_saga(Path(id): Path<String>, State(state): State<AdminState>) -> impl IntoResponse {
    info!(saga_id = %id, "saga cancel requested");
    let Some(pool) = &state.pg_pool else {
        return StatusCode::ACCEPTED.into_response();
    };

    let Ok(uuid) = uuid::Uuid::parse_str(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("saga {id} not found"),
            }),
        )
            .into_response();
    };

    let rows_affected = sqlx::query(
        "UPDATE triad.triad_saga_checkpoints \
         SET status = 'Cancelled', updated_at = now() WHERE saga_id = $1",
    )
    .bind(uuid)
    .execute(pool)
    .await
    .map(|r| r.rows_affected())
    .unwrap_or(0);

    if rows_affected > 0 {
        StatusCode::ACCEPTED.into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("saga {id} not found"),
            }),
        )
            .into_response()
    }
}

async fn reload_config(State(state): State<AdminState>) -> impl IntoResponse {
    let (Some(path), Some(shared_config)) = (&state.config_path, &state.shared_config) else {
        return StatusCode::ACCEPTED.into_response();
    };

    match triad_core::config::TriadConfig::load(path) {
        Ok(new_cfg) => {
            let mut cfg = shared_config.write().await;
            *cfg = new_cfg;
            info!(path = %path, "config reloaded");
            StatusCode::ACCEPTED.into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
            .into_response(),
    }
}

async fn reload_pipeline(
    Path(name): Path<String>,
    State(state): State<AdminState>,
) -> impl IntoResponse {
    info!(pipeline = %name, "pipeline reload requested");
    if let Some(tx) = &state.control_tx {
        let _ = tx.try_send(PatternControl::Reload(name));
    }
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

    async fn post_status(router: Router, path: &str) -> StatusCode {
        let req = Request::builder()
            .method("POST")
            .uri(path)
            .body(Body::empty())
            .unwrap();
        router.oneshot(req).await.unwrap().status()
    }

    async fn delete_status(router: Router, path: &str) -> StatusCode {
        let req = Request::builder()
            .method("DELETE")
            .uri(path)
            .body(Body::empty())
            .unwrap();
        router.oneshot(req).await.unwrap().status()
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
    async fn test_health_ready_backends_unconfigured_when_no_pool() {
        let router = admin_router(test_state());
        let (status, body) = get_json(router, "/health/ready").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["backends"]["postgres"]["status"], "unconfigured");
        assert_eq!(body["backends"]["kafka"]["status"], "unconfigured");
        assert_eq!(body["backends"]["redis"]["status"], "unconfigured");
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
    async fn test_list_checkpoints_returns_array() {
        let router = admin_router(test_state());
        let (status, body) = get_json(router, "/checkpoints").await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.is_array());
    }

    #[tokio::test]
    async fn test_pause_pattern_returns_accepted() {
        let router = admin_router(test_state());
        let status = post_status(router, "/patterns/test-pattern/pause").await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn test_resume_pattern_returns_accepted() {
        let router = admin_router(test_state());
        let status = post_status(router, "/patterns/test-pattern/resume").await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn test_replay_pattern_returns_accepted() {
        let router = admin_router(test_state());
        let status = post_status(router, "/patterns/test-pattern/replay").await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn test_pipeline_reload_returns_accepted() {
        let router = admin_router(test_state());
        let status = post_status(router, "/pipelines/test-pipeline/reload").await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn test_replay_dlq_returns_accepted_without_replayer() {
        let router = admin_router(test_state());
        let status = post_status(router, "/dlq/test-topic/replay").await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn test_drop_dlq_returns_accepted_without_replayer() {
        let router = admin_router(test_state());
        let status = delete_status(router, "/dlq/test-topic").await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn test_reload_config_returns_accepted_without_config_path() {
        let router = admin_router(test_state());
        let status = post_status(router, "/config/reload").await;
        assert_eq!(status, StatusCode::ACCEPTED);
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

    // ── Builder / setter coverage ─────────────────────────────────────────────

    #[test]
    fn test_admin_state_builder_field_setters() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<PatternControl>(1);
        let state = AdminState::new()
            .with_kafka_brokers("localhost:9092")
            .with_redis_url("redis://localhost:6379")
            .with_control_tx(tx)
            .with_config_path("/tmp/triad.yaml");
        assert_eq!(state.kafka_brokers.as_deref(), Some("localhost:9092"));
        assert_eq!(state.redis_url.as_deref(), Some("redis://localhost:6379"));
        assert!(state.control_tx.is_some());
        assert_eq!(state.config_path.as_deref(), Some("/tmp/triad.yaml"));
    }

    #[test]
    fn test_admin_state_default_impl() {
        let _state = AdminState::default();
    }

    // ── Mutation endpoint coverage ────────────────────────────────────────────

    #[tokio::test]
    async fn test_cancel_saga_no_pool_returns_accepted() {
        let router = admin_router(test_state());
        let status = post_status(router, "/saga/test-saga-id/cancel").await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }

    // ── Backend health degraded path coverage ─────────────────────────────────

    #[tokio::test]
    async fn test_ready_with_bad_redis_url_shows_degraded() {
        // Port 1 is always refused, so check_redis_health returns "degraded" quickly.
        let state = AdminState::new().with_redis_url("redis://127.0.0.1:1");
        let router = admin_router(state);
        let (status, body) = get_json(router, "/health/ready").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body["backends"]["redis"]["status"],
            "degraded",
            "unreachable Redis must report degraded"
        );
        assert_eq!(body["status"], "degraded");
    }
}
