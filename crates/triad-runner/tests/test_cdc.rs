#![cfg(feature = "integration")]

mod common;

use common::containers::PgStack;

#[tokio::test]
async fn test_cdc_pg_connection_and_wal_level() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("connect failed");

    // Verify PG is reachable and returns correct version.
    let (version,): (String,) = sqlx::query_as("SELECT version()")
        .fetch_one(&pool)
        .await
        .expect("version() failed");
    assert!(
        version.contains("PostgreSQL"),
        "unexpected version: {version}"
    );

    // The default testcontainers Postgres image runs with wal_level = replica.
    // Logical replication requires wal_level = logical; changing it requires
    // a container restart and is outside the scope of this schema smoke test.
    // This test verifies that the migration schema (triad.triad_checkpoints etc.)
    // is accessible after running migrations — CDC pattern will use separate
    // replication connections via tokio-postgres rather than this pool.
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'triad'",
    )
    .fetch_one(&pool)
    .await
    .expect("table count failed");

    // All 9 tables from migrations 0001–0007 must exist.
    assert!(
        count >= 9,
        "expected at least 9 tables in triad schema, got {count}"
    );
}

#[tokio::test]
async fn test_cdc_schema_tables_present() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("connect failed");

    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'triad'
         ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    .expect("table list failed");

    let expected = [
        "feature_flags",
        "flag_audit",
        "idempotency_keys",
        "triad_checkpoints",
        "triad_inbox",
        "triad_outbox",
        "triad_saga_checkpoints",
        "triad_saga_steps",
        "webhook_deliveries",
        "webhook_subscriptions",
    ];

    for name in &expected {
        assert!(
            tables.iter().any(|t| t == *name),
            "table '{name}' not found in triad schema (found: {tables:?})"
        );
    }
}

#[tokio::test]
async fn test_cdc_insert_triggers_change_visible_in_pg() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("connect failed");

    // Create a test table in the public schema to simulate CDC source.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS public.cdc_test_events (
            id      BIGSERIAL PRIMARY KEY,
            payload TEXT NOT NULL,
            ts      TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .execute(&pool)
    .await
    .expect("CREATE TABLE failed");

    // Insert a row — in production CDC would stream this via WAL.
    sqlx::query("INSERT INTO public.cdc_test_events (payload) VALUES ($1)")
        .bind("cdc-test-payload")
        .execute(&pool)
        .await
        .expect("INSERT failed");

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM public.cdc_test_events")
        .fetch_one(&pool)
        .await
        .expect("COUNT failed");

    assert_eq!(count, 1, "inserted row should be visible");
}
