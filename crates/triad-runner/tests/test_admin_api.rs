#![cfg(feature = "integration")]

mod common;

use std::time::Duration;

use common::containers::PgStack;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use triad_runner::admin::{AdminServer, AdminState};

async fn start_admin_server() -> (u16, CancellationToken) {
    start_admin_server_with_state(AdminState::new()).await
}

async fn start_admin_server_with_state(state: AdminState) -> (u16, CancellationToken) {
    let cancel = CancellationToken::new();
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let mut server = AdminServer::new(port, state);
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        let _ = server.serve(cancel_clone).await;
    });
    sleep(Duration::from_millis(50)).await;
    (port, cancel)
}

#[tokio::test]
async fn test_admin_health_live_returns_200() {
    let (port, cancel) = start_admin_server().await;
    let resp = reqwest::get(format!("http://127.0.0.1:{port}/health/live"))
        .await
        .expect("GET /health/live failed");
    assert_eq!(resp.status().as_u16(), 200);
    cancel.cancel();
}

#[tokio::test]
async fn test_admin_health_ready_returns_200() {
    let (port, cancel) = start_admin_server().await;
    let resp = reqwest::get(format!("http://127.0.0.1:{port}/health/ready"))
        .await
        .expect("GET /health/ready failed");
    assert_eq!(resp.status().as_u16(), 200);
    cancel.cancel();
}

#[tokio::test]
async fn test_admin_health_started_returns_200() {
    let (port, cancel) = start_admin_server().await;
    let resp = reqwest::get(format!("http://127.0.0.1:{port}/health/started"))
        .await
        .expect("GET /health/started failed");
    assert_eq!(resp.status().as_u16(), 200);
    cancel.cancel();
}

#[tokio::test]
async fn test_admin_metrics_returns_503_without_prometheus_handle() {
    let (port, cancel) = start_admin_server().await;
    let resp = reqwest::get(format!("http://127.0.0.1:{port}/metrics"))
        .await
        .expect("GET /metrics failed");
    // 503 is expected: no PrometheusHandle configured in test server.
    assert_eq!(resp.status().as_u16(), 503);
    cancel.cancel();
}

#[tokio::test]
async fn test_admin_patterns_returns_200() {
    let (port, cancel) = start_admin_server().await;
    let resp = reqwest::get(format!("http://127.0.0.1:{port}/patterns"))
        .await
        .expect("GET /patterns failed");
    assert_eq!(resp.status().as_u16(), 200);
    cancel.cancel();
}

#[tokio::test]
async fn test_admin_registry_returns_200() {
    let (port, cancel) = start_admin_server().await;
    let resp = reqwest::get(format!("http://127.0.0.1:{port}/registry"))
        .await
        .expect("GET /registry failed");
    assert_eq!(resp.status().as_u16(), 200);
    cancel.cancel();
}

#[tokio::test]
async fn test_admin_health_live_body_contains_status() {
    let (port, cancel) = start_admin_server().await;
    let body: serde_json::Value = reqwest::get(format!("http://127.0.0.1:{port}/health/live"))
        .await
        .expect("GET failed")
        .json()
        .await
        .expect("JSON parse failed");
    assert!(
        body.get("status").is_some(),
        "live response should have 'status' field, got: {body}"
    );
    cancel.cancel();
}

#[tokio::test]
async fn test_admin_all_health_routes_smoke() {
    let (port, cancel) = start_admin_server().await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    let routes = [
        ("/health/live", 200u16),
        ("/health/ready", 200),
        ("/health/started", 200),
        ("/patterns", 200),
        ("/registry", 200),
    ];

    for (route, expected) in &routes {
        let resp = client
            .get(format!("{base}{route}"))
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {route} failed: {e}"));
        assert_eq!(
            resp.status().as_u16(),
            *expected,
            "route {route} returned unexpected status {}",
            resp.status()
        );
    }

    cancel.cancel();
}

// ---------------------------------------------------------------------------
// PG-backed admin tests — cover inspect_saga / cancel_saga code paths that
// require a pool to be configured.
// ---------------------------------------------------------------------------

/// inspect_saga with a pool configured and a non-UUID path parameter must
/// return 404 (UUID parse error branch — lines 695-703 in admin.rs).
#[tokio::test]
async fn test_admin_inspect_saga_bad_uuid_with_pg_pool() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect");
    let state = AdminState::new().with_pg_pool(pool);
    let (port, cancel) = start_admin_server_with_state(state).await;

    let resp = reqwest::get(format!("http://127.0.0.1:{port}/saga/not-a-uuid"))
        .await
        .expect("GET /saga/:id failed");
    assert_eq!(
        resp.status().as_u16(),
        404,
        "non-UUID saga ID must return 404"
    );
    cancel.cancel();
}

/// inspect_saga with a pool and a valid UUID that doesn't exist in the DB
/// must return 404 (SQL None path — lines 705-729 in admin.rs).
#[tokio::test]
async fn test_admin_inspect_saga_valid_uuid_not_found_with_pg_pool() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect");
    let state = AdminState::new().with_pg_pool(pool);
    let (port, cancel) = start_admin_server_with_state(state).await;

    let id = uuid::Uuid::new_v4();
    let resp = reqwest::get(format!("http://127.0.0.1:{port}/saga/{id}"))
        .await
        .expect("GET /saga/:id failed");
    assert_eq!(resp.status().as_u16(), 404, "missing saga must return 404");
    cancel.cancel();
}

/// cancel_saga with a pool configured and a non-UUID ID must return 404
/// (UUID parse error branch — lines 748-756 in admin.rs).
#[tokio::test]
async fn test_admin_cancel_saga_bad_uuid_with_pg_pool() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect");
    let state = AdminState::new().with_pg_pool(pool);
    let (port, cancel) = start_admin_server_with_state(state).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/saga/not-a-uuid/cancel"))
        .send()
        .await
        .expect("POST /saga/:id/cancel failed");
    assert_eq!(
        resp.status().as_u16(),
        404,
        "non-UUID saga cancel must return 404"
    );
    cancel.cancel();
}

/// cancel_saga with a pool, valid UUID but no matching row must return 404
/// (SQL rows_affected==0 path — lines 768-778 in admin.rs).
#[tokio::test]
async fn test_admin_cancel_saga_valid_uuid_not_found_with_pg_pool() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect");
    let state = AdminState::new().with_pg_pool(pool);
    let (port, cancel) = start_admin_server_with_state(state).await;

    let id = uuid::Uuid::new_v4();
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{port}/saga/{id}/cancel"))
        .send()
        .await
        .expect("POST /saga/:id/cancel failed");
    assert_eq!(
        resp.status().as_u16(),
        404,
        "cancel of non-existent saga must return 404"
    );
    cancel.cancel();
}
