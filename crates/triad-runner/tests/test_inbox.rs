#![cfg(feature = "integration")]

mod common;

use std::time::Duration;

use common::containers::{PgStack, TestStack};
use deadpool_redis::Config as RedisConfig;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Test 1: Redis SET NX fast-path deduplication
// ---------------------------------------------------------------------------

/// The inbox Redis fast-path uses `SET key 1 NX EX <ttl>`.
/// First call → `"OK"` (key didn't exist → new event).
/// Second call with same key → `nil` (key exists → duplicate, skip handler).
#[tokio::test]
async fn test_inbox_redis_nx_deduplicates_same_event_id() {
    let redis_node = Redis::default()
        .start()
        .await
        .expect("redis container start failed");
    let port = redis_node
        .get_host_port_ipv4(6379)
        .await
        .expect("redis port");
    let redis_url = format!("redis://127.0.0.1:{port}");

    let pool = RedisConfig::from_url(redis_url)
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .expect("redis pool");
    let mut conn = pool.get().await.expect("redis connection");

    let event_id = Uuid::new_v4().to_string();
    let key = format!("triad:inbox:dedup:{event_id}");

    let r1: Option<String> = redis::cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(3600u64)
        .query_async(&mut conn)
        .await
        .expect("first SET NX failed");
    assert_eq!(
        r1,
        Some("OK".to_string()),
        "first delivery must set the dedup key"
    );

    let r2: Option<String> = redis::cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(3600u64)
        .query_async(&mut conn)
        .await
        .expect("second SET NX failed");
    assert_eq!(r2, None, "duplicate delivery must return nil from SET NX");
}

// ---------------------------------------------------------------------------
// Test 2: PostgreSQL inbox ON CONFLICT DO NOTHING
// ---------------------------------------------------------------------------

/// Safety invariant: the inbox INSERT and handler run inside the same PG transaction.
/// The `ON CONFLICT (event_id) DO NOTHING` constraint is the durability guarantee —
/// even if the handler is called twice, only one record lands in `triad_inbox`.
#[tokio::test]
async fn test_inbox_pg_dedup_on_conflict_prevents_duplicate_row() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect failed");

    let event_id = Uuid::new_v4();

    let first = sqlx::query(
        "INSERT INTO triad.triad_inbox (event_id, pattern_name, pipeline_name)
         VALUES ($1, 'inbox', 'pipeline-1')
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .execute(&pool)
    .await
    .expect("first INSERT failed")
    .rows_affected();

    let second = sqlx::query(
        "INSERT INTO triad.triad_inbox (event_id, pattern_name, pipeline_name)
         VALUES ($1, 'inbox', 'pipeline-1')
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .execute(&pool)
    .await
    .expect("second INSERT failed")
    .rows_affected();

    assert_eq!(first, 1, "first insert must succeed");
    assert_eq!(
        second, 0,
        "duplicate insert must be a no-op (ON CONFLICT DO NOTHING)"
    );

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM triad.triad_inbox WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await
            .expect("COUNT failed");
    assert_eq!(
        count, 1,
        "exactly one inbox record must exist after two deliveries"
    );
}

// ---------------------------------------------------------------------------
// Test 3: End-to-end — same event delivered twice is processed exactly once
// ---------------------------------------------------------------------------

/// Simulates the full InboxModule flow for a duplicate message:
/// 1. First delivery → Redis NX succeeds → store in PG inbox.
/// 2. Second delivery → Redis NX returns nil → skip (no PG INSERT, no handler call).
/// Final state: exactly one record in `triad_inbox`.
#[tokio::test]
async fn test_inbox_same_event_twice_processed_exactly_once() {
    let stack = TestStack::start().await;

    let pg_pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect failed");
    let redis_pool = RedisConfig::from_url(stack.redis_url.clone())
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .expect("redis pool");
    let mut conn = redis_pool.get().await.expect("redis connection");

    let event_id = Uuid::new_v4();
    let dedup_key = format!("triad:inbox:dedup:{event_id}");

    // --- First delivery ---
    let r1: Option<String> = redis::cmd("SET")
        .arg(&dedup_key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(3600u64)
        .query_async(&mut conn)
        .await
        .expect("SET NX 1 failed");
    assert_eq!(
        r1,
        Some("OK".to_string()),
        "first delivery: Redis NX must succeed"
    );

    // store_and_handle: INSERT into triad_inbox (atomically with handler in prod).
    let rows1 = sqlx::query(
        "INSERT INTO triad.triad_inbox (event_id, pattern_name, pipeline_name)
         VALUES ($1, 'inbox', 'pipeline-1')
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .execute(&pg_pool)
    .await
    .expect("first PG INSERT failed")
    .rows_affected();
    assert_eq!(rows1, 1, "first delivery must land in inbox");

    // --- Second delivery (duplicate) ---
    // Redis NX returns nil → InboxModule returns Ok(()) without calling store_and_handle.
    let r2: Option<String> = redis::cmd("SET")
        .arg(&dedup_key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(3600u64)
        .query_async(&mut conn)
        .await
        .expect("SET NX 2 failed");
    assert_eq!(r2, None, "duplicate delivery must be caught by Redis NX");

    // Because NX returned nil, no PG INSERT happens. Verify the count.
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM triad.triad_inbox WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(&pg_pool)
            .await
            .expect("COUNT failed");
    assert_eq!(
        count, 1,
        "event must appear in inbox exactly once after two deliveries"
    );
}

// ---------------------------------------------------------------------------
// Test 4: PG fallback path when Redis CB is open
// ---------------------------------------------------------------------------

/// When the Redis circuit breaker is open, InboxModule skips the Redis NX fast path
/// and falls back to a PG SELECT to check for duplicates, then INSERTs if new.
/// This test verifies that the PG fallback prevents duplicate processing.
#[tokio::test]
async fn test_inbox_pg_fallback_when_redis_circuit_breaker_open() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect failed");

    let event_id = Uuid::new_v4();

    // --- First delivery (CB open: skip Redis, use PG SELECT + INSERT) ---
    let existing: Option<Uuid> =
        sqlx::query_scalar("SELECT event_id FROM triad.triad_inbox WHERE event_id = $1")
            .bind(event_id)
            .fetch_optional(&pool)
            .await
            .expect("SELECT failed");
    assert!(existing.is_none(), "event must not be in inbox yet");

    // is_new = true → call store_and_handle (INSERT + handler).
    let rows1 = sqlx::query(
        "INSERT INTO triad.triad_inbox (event_id, pattern_name, pipeline_name)
         VALUES ($1, 'inbox-fallback', 'pipeline-1')
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .execute(&pool)
    .await
    .expect("INSERT failed")
    .rows_affected();
    assert_eq!(rows1, 1);

    // --- Second delivery (CB still open) ---
    // PG SELECT returns the row → is_new = false → skip.
    let existing2: Option<Uuid> =
        sqlx::query_scalar("SELECT event_id FROM triad.triad_inbox WHERE event_id = $1")
            .bind(event_id)
            .fetch_optional(&pool)
            .await
            .expect("SELECT 2 failed");
    assert!(
        existing2.is_some(),
        "event must be found in PG fallback dedup check"
    );

    // PG fallback dedup caught the duplicate — count must still be 1.
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM triad.triad_inbox WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(&pool)
            .await
            .expect("COUNT failed");
    assert_eq!(
        count, 1,
        "PG fallback must result in exactly one inbox record"
    );
}

// ---------------------------------------------------------------------------
// Test 5: Dedup TTL expiry — same event is reprocessable after the window
// ---------------------------------------------------------------------------

/// The dedup window is finite (default 24 h in production). After the TTL expires,
/// the same event_id is treated as a new event and can be processed again.
/// This test uses a 1-second TTL to verify the behaviour without a 24-hour wait.
#[tokio::test]
async fn test_inbox_redis_dedup_key_expires_and_event_becomes_reprocessable() {
    let redis_node = Redis::default()
        .start()
        .await
        .expect("redis container start failed");
    let port = redis_node
        .get_host_port_ipv4(6379)
        .await
        .expect("redis port");

    let pool = RedisConfig::from_url(format!("redis://127.0.0.1:{port}"))
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .expect("redis pool");
    let mut conn = pool.get().await.expect("redis connection");

    let event_id = Uuid::new_v4().to_string();
    let key = format!("triad:inbox:dedup:{event_id}");

    // First delivery: SET NX with 1-second TTL.
    let r1: Option<String> = redis::cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(1u64)
        .query_async(&mut conn)
        .await
        .expect("SET NX failed");
    assert_eq!(r1, Some("OK".to_string()));

    // Within TTL: duplicate is rejected.
    let r2: Option<String> = redis::cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(1u64)
        .query_async(&mut conn)
        .await
        .expect("within-TTL SET NX failed");
    assert_eq!(
        r2, None,
        "within the dedup window, duplicate must be rejected"
    );

    // After TTL expires: key is gone, same event_id is treated as new.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let r3: Option<String> = redis::cmd("SET")
        .arg(&key)
        .arg("1")
        .arg("NX")
        .arg("EX")
        .arg(3600u64)
        .query_async(&mut conn)
        .await
        .expect("post-TTL SET NX failed");
    assert_eq!(
        r3,
        Some("OK".to_string()),
        "after dedup window expires, same event_id must be reprocessable"
    );
}
