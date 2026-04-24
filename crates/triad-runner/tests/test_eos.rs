#![cfg(feature = "integration")]

mod common;

use common::containers::PgStack;
use uuid::Uuid;

#[tokio::test]
async fn test_eos_pg_inbox_dedup_prevents_double_processing() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("connect failed");

    let event_id = Uuid::new_v4();

    // First delivery — should succeed.
    let mut tx = pool.begin().await.expect("begin tx");
    let rows = sqlx::query(
        "INSERT INTO triad.triad_inbox (event_id, pattern_name, pipeline_name)
         VALUES ($1, 'eos', 'pipeline-1')
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .execute(&mut *tx)
    .await
    .expect("first INSERT failed")
    .rows_affected();
    tx.commit().await.expect("commit failed");

    assert_eq!(rows, 1, "first delivery should insert");

    // Duplicate delivery — same event_id should be rejected.
    let mut tx2 = pool.begin().await.expect("begin tx2");
    let rows2 = sqlx::query(
        "INSERT INTO triad.triad_inbox (event_id, pattern_name, pipeline_name)
         VALUES ($1, 'eos', 'pipeline-1')
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .execute(&mut *tx2)
    .await
    .expect("second INSERT failed")
    .rows_affected();
    tx2.commit().await.expect("commit2 failed");

    assert_eq!(rows2, 0, "duplicate delivery should be a no-op");
}

#[tokio::test]
async fn test_eos_checkpoint_cas_prevents_duplicate_writes() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("connect failed");

    // Insert initial checkpoint at version 0.
    sqlx::query(
        "INSERT INTO triad.triad_checkpoints
         (pattern_name, pipeline_name, owner_instance_id, version)
         VALUES ('eos', 'pipeline-1', 'inst-1', 0)",
    )
    .execute(&pool)
    .await
    .expect("initial checkpoint INSERT failed");

    // CAS update: version 0 → 1 should succeed.
    let rows1 = sqlx::query(
        "UPDATE triad.triad_checkpoints SET version = 1, updated_at = now()
         WHERE pattern_name = 'eos' AND pipeline_name = 'pipeline-1' AND version = 0",
    )
    .execute(&pool)
    .await
    .expect("CAS update 1 failed")
    .rows_affected();

    assert_eq!(rows1, 1, "first CAS should succeed");

    // Replay with the same version (0) should fail — stale version.
    let rows2 = sqlx::query(
        "UPDATE triad.triad_checkpoints SET version = 1, updated_at = now()
         WHERE pattern_name = 'eos' AND pipeline_name = 'pipeline-1' AND version = 0",
    )
    .execute(&pool)
    .await
    .expect("CAS update 2 failed")
    .rows_affected();

    assert_eq!(rows2, 0, "stale CAS should not update any rows");
}

#[tokio::test]
async fn test_eos_idempotency_key_prevents_reprocessing() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("connect failed");

    let key = format!("idem-{}", Uuid::new_v4());

    // First request: store idempotency key with a 1-hour expiry.
    let rows1 = sqlx::query(
        "INSERT INTO triad.idempotency_keys (idempotency_key, pattern_name, response, expires_at)
         VALUES ($1, 'eos', $2, now() + interval '1 hour')
         ON CONFLICT (idempotency_key) DO NOTHING",
    )
    .bind(&key)
    .bind(serde_json::json!({"ok": true}))
    .execute(&pool)
    .await
    .expect("first idempotency INSERT failed")
    .rows_affected();

    assert_eq!(rows1, 1);

    // Duplicate: same key should be rejected.
    let rows2 = sqlx::query(
        "INSERT INTO triad.idempotency_keys (idempotency_key, pattern_name, response, expires_at)
         VALUES ($1, 'eos', $2, now() + interval '1 hour')
         ON CONFLICT (idempotency_key) DO NOTHING",
    )
    .bind(&key)
    .bind(serde_json::json!({"ok": true}))
    .execute(&pool)
    .await
    .expect("second idempotency INSERT failed")
    .rows_affected();

    assert_eq!(rows2, 0, "duplicate idempotency key should be rejected");

    // Can retrieve stored response.
    let response: serde_json::Value = sqlx::query_scalar(
        "SELECT response FROM triad.idempotency_keys WHERE idempotency_key = $1",
    )
    .bind(&key)
    .fetch_one(&pool)
    .await
    .expect("SELECT response failed");

    assert_eq!(response["ok"], serde_json::json!(true));
}
