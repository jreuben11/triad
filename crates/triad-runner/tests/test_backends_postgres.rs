#![cfg(feature = "integration")]

mod common;

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use triad_core::{
    config::PostgresConfig,
    error::BackendError,
};
use triad_runner::backends::{CbConfig, CbState, PgBackend};

fn test_cb_config() -> CbConfig {
    use std::time::Duration;
    CbConfig {
        failure_threshold: 5,
        success_threshold: 2,
        half_open_after: Duration::from_secs(30),
        half_open_max_calls: 2,
    }
}

fn pg_config(url: &str) -> PostgresConfig {
    PostgresConfig {
        url: url.to_string(),
        replication_url: None,
        max_connections: 2,
        min_idle: None,
        connection_timeout_ms: 5_000,
        statement_timeout_ms: None,
        wal_level: None,
        replication_slot: None,
        publication: None,
        ssl_mode: "disable".to_string(),
        ssl_root_cert: None,
    }
}

// ---------------------------------------------------------------------------
// Test 1: PgBackend::new() connects to a fresh DB and runs migrations
// ---------------------------------------------------------------------------

/// `PgBackend::new()` must connect to a fresh PostgreSQL instance, apply all
/// embedded migrations, and return a configured backend with a Closed CB.
/// Uses a dedicated container so migrations run on a pristine database.
#[tokio::test]
async fn test_pg_backend_new_connects_and_runs_migrations() {
    let node = Postgres::default()
        .with_user("triad")
        .with_password("secret")
        .with_db_name("triad_test")
        .start()
        .await
        .expect("postgres container start failed");
    let port = node.get_host_port_ipv4(5432).await.expect("pg port");
    let url = format!("postgresql://triad:secret@127.0.0.1:{port}/triad_test");

    let cfg = pg_config(&url);
    let backend = PgBackend::new(&cfg, test_cb_config())
        .await
        .expect("PgBackend::new failed");

    assert_eq!(
        backend.circuit.state(),
        CbState::Closed,
        "CB must start Closed"
    );

    let row: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&backend.pool)
        .await
        .expect("health query failed");
    assert_eq!(row, 1);
}

// ---------------------------------------------------------------------------
// Test 2: PgBackend::new() returns error on bad URL
// ---------------------------------------------------------------------------

/// A connection to a non-existent server must yield `BackendError::Postgres`.
#[tokio::test]
async fn test_pg_backend_new_fails_on_bad_url() {
    let cfg = pg_config("postgresql://triad:secret@127.0.0.1:1/triad_test");
    let result = PgBackend::new(&cfg, test_cb_config()).await;
    assert!(
        matches!(result, Err(BackendError::Postgres(_))),
        "connection to unreachable host must return BackendError::Postgres"
    );
}
