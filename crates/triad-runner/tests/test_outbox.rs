#![cfg(feature = "integration")]

mod common;

use std::time::Duration;

use common::containers::PgStack;
use sqlx::Row;
use tokio::time::timeout;
use uuid::Uuid;

#[tokio::test]
async fn test_outbox_insert_and_fetch_pending() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("connect failed");

    let event_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO triad.triad_outbox (event_id, event_type, payload, kafka_topic)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(event_id)
    .bind("OrderCreated")
    .bind(serde_json::json!({"order_id": "ord-1", "amount": 100}))
    .bind("orders")
    .execute(&pool)
    .await
    .expect("INSERT failed");

    let rows = sqlx::query(
        "SELECT id, event_type, relay_status FROM triad.triad_outbox WHERE relay_status = 'pending'",
    )
    .fetch_all(&pool)
    .await
    .expect("SELECT failed");

    assert_eq!(rows.len(), 1);
    let status: String = rows[0].try_get("relay_status").unwrap();
    assert_eq!(status, "pending");
    let event_type: String = rows[0].try_get("event_type").unwrap();
    assert_eq!(event_type, "OrderCreated");
}

#[tokio::test]
async fn test_outbox_mark_published() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("connect failed");

    let id: i64 = sqlx::query_scalar(
        "INSERT INTO triad.triad_outbox (event_type, payload, kafka_topic)
         VALUES ('TestEvent', '{}', 'test-topic')
         RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .expect("INSERT RETURNING failed");

    sqlx::query(
        "UPDATE triad.triad_outbox SET relay_status = 'published', published_at = now()
         WHERE id = $1",
    )
    .bind(id)
    .execute(&pool)
    .await
    .expect("UPDATE failed");

    let status: String =
        sqlx::query_scalar("SELECT relay_status FROM triad.triad_outbox WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("SELECT failed");

    assert_eq!(status, "published");
}

#[tokio::test]
async fn test_outbox_pending_index_used() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("connect failed");

    for i in 0..5i32 {
        sqlx::query(
            "INSERT INTO triad.triad_outbox (event_type, payload, kafka_topic)
             VALUES ($1, '{}', 'topic')",
        )
        .bind(format!("Event{i}"))
        .execute(&pool)
        .await
        .expect("INSERT failed");
    }

    let count: i64 = timeout(
        Duration::from_secs(2),
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM triad.triad_outbox WHERE relay_status = 'pending'",
        )
        .fetch_one(&pool),
    )
    .await
    .expect("timeout")
    .expect("COUNT failed");

    assert_eq!(count, 5);
}

#[tokio::test]
async fn test_inbox_dedup_prevents_duplicate_processing() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("connect failed");

    let event_id = Uuid::new_v4();

    let first = sqlx::query(
        "INSERT INTO triad.triad_inbox (event_id, pattern_name, pipeline_name)
         VALUES ($1, 'outbox', 'main')
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .execute(&pool)
    .await
    .expect("first INSERT failed");

    let second = sqlx::query(
        "INSERT INTO triad.triad_inbox (event_id, pattern_name, pipeline_name)
         VALUES ($1, 'outbox', 'main')
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .execute(&pool)
    .await
    .expect("second INSERT failed");

    assert_eq!(first.rows_affected(), 1, "first insert should succeed");
    assert_eq!(second.rows_affected(), 0, "duplicate should be a no-op");
}
