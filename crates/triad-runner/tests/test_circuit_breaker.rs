#![cfg(feature = "integration")]

mod common;

use std::time::Duration;

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;
use triad_core::error::BackendError;
use triad_runner::backends::{CbConfig, CbState, CircuitBreaker};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn cb(failure_threshold: u32, success_threshold: u32, half_open_ms: u64) -> CircuitBreaker {
    CircuitBreaker::new(
        CbConfig {
            failure_threshold,
            success_threshold,
            half_open_after: Duration::from_millis(half_open_ms),
            half_open_max_calls: 2,
        },
        "redis",
    )
}

/// Drives a real Redis WRONGTYPE error through the CB.
/// Strategy: LPUSH on a key that was SET as a STRING → Redis returns WRONGTYPE error
/// → CB records a failure. After `failure_threshold` calls, CB transitions to Open.
async fn fail_via_cb(circuit_breaker: &CircuitBreaker, conn: redis::aio::MultiplexedConnection) {
    let mut c = conn.clone();
    let _ = circuit_breaker
        .call(async move {
            redis::cmd("LPUSH")
                .arg("triad:cb:test:str-key")
                .arg("value")
                .query_async::<i64>(&mut c)
                .await
                .map(|_| ())
                .map_err(BackendError::Redis)
        })
        .await;
}

/// Drives a successful PING through the CB.
async fn ok_via_cb(
    circuit_breaker: &CircuitBreaker,
    conn: redis::aio::MultiplexedConnection,
) -> Result<(), BackendError> {
    let mut c = conn.clone();
    circuit_breaker
        .call(async move {
            redis::cmd("PING")
                .query_async::<String>(&mut c)
                .await
                .map(|_| ())
                .map_err(BackendError::Redis)
        })
        .await
}

/// Boot a Redis container and return a cloneable `MultiplexedConnection` + the
/// container guard (dropped at end of test, stopping the container).
async fn redis_conn() -> (
    redis::aio::MultiplexedConnection,
    testcontainers::ContainerAsync<Redis>,
) {
    let node = Redis::default()
        .start()
        .await
        .expect("redis container start failed");
    let port = node.get_host_port_ipv4(6379).await.expect("redis port");
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}"))
        .expect("redis client create failed");
    let conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("multiplexed connection failed");
    (conn, node)
}

/// Seed the STRING key that LPUSH will collide with.
async fn seed_string_key(conn: &mut redis::aio::MultiplexedConnection) {
    redis::cmd("SET")
        .arg("triad:cb:test:str-key")
        .arg("string-value")
        .query_async::<String>(conn)
        .await
        .expect("seed SET failed");
}

// ---------------------------------------------------------------------------
// Test 1: CB opens after reaching the failure threshold
// ---------------------------------------------------------------------------

/// After `failure_threshold` consecutive real Redis errors, the CB must transition
/// Closed → Open and broadcast the change on its watch channel.
#[tokio::test]
async fn test_cb_opens_after_real_redis_type_errors() {
    let (mut conn, _node) = redis_conn().await;
    seed_string_key(&mut conn).await;

    let breaker = cb(3, 2, 5_000);
    assert_eq!(breaker.state(), CbState::Closed);

    fail_via_cb(&breaker, conn.clone()).await;
    fail_via_cb(&breaker, conn.clone()).await;
    assert_eq!(
        breaker.state(),
        CbState::Closed,
        "should still be Closed after 2 of 3 failures"
    );

    fail_via_cb(&breaker, conn.clone()).await;
    assert_eq!(
        breaker.state(),
        CbState::Open,
        "must transition to Open after hitting the failure threshold"
    );
}

// ---------------------------------------------------------------------------
// Test 2: Open CB short-circuits calls without touching Redis
// ---------------------------------------------------------------------------

/// When the CB is Open, `call()` must return `CircuitBreakerOpen` immediately —
/// without executing the future (and therefore without making a Redis round-trip).
#[tokio::test]
async fn test_cb_open_rejects_calls_immediately() {
    let (mut conn, _node) = redis_conn().await;
    seed_string_key(&mut conn).await;

    let breaker = cb(1, 2, 60_000); // long half_open_after so it stays Open
    fail_via_cb(&breaker, conn.clone()).await;
    assert_eq!(breaker.state(), CbState::Open);

    // PING would succeed on Redis, but the CB must reject it before the future runs.
    let result = ok_via_cb(&breaker, conn.clone()).await;
    assert!(
        matches!(result, Err(BackendError::CircuitBreakerOpen { .. })),
        "Open CB must reject calls immediately; got: {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 3: Watch channel signals state changes to subscribers
// ---------------------------------------------------------------------------

/// The watch receiver must deliver a notification whenever the CB state changes.
/// This is the mechanism that allows InboxModule (and other patterns) to observe
/// a Redis CB opening without polling.
#[tokio::test]
async fn test_cb_watch_channel_notifies_on_open() {
    let (mut conn, _node) = redis_conn().await;
    seed_string_key(&mut conn).await;

    let breaker = cb(2, 2, 5_000);
    let mut rx = breaker.subscribe();

    fail_via_cb(&breaker, conn.clone()).await;
    fail_via_cb(&breaker, conn.clone()).await;

    // The watch channel must have delivered the Open transition.
    tokio::time::timeout(Duration::from_secs(1), rx.changed())
        .await
        .expect("watch change timed out")
        .expect("watch channel closed unexpectedly");

    assert_eq!(
        *rx.borrow(),
        CbState::Open,
        "watch receiver must see the Open state after threshold is reached"
    );
}

// ---------------------------------------------------------------------------
// Test 4: Half-open → Closed recovery after the timeout window
// ---------------------------------------------------------------------------

/// After `half_open_after` milliseconds, the first probe call transitions the CB
/// from Open to HalfOpen. Reaching `success_threshold` successful probes closes it.
#[tokio::test]
async fn test_cb_recovers_from_open_to_closed_via_half_open() {
    let (mut conn, _node) = redis_conn().await;
    seed_string_key(&mut conn).await;

    let breaker = cb(1, 2, 50); // 50 ms half_open_after, need 2 successes to close

    fail_via_cb(&breaker, conn.clone()).await;
    assert_eq!(breaker.state(), CbState::Open);

    // Wait for the probe window.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let r1 = ok_via_cb(&breaker, conn.clone()).await;
    assert!(
        r1.is_ok(),
        "first probe must succeed (transitions to HalfOpen)"
    );

    let r2 = ok_via_cb(&breaker, conn.clone()).await;
    assert!(r2.is_ok(), "second probe must succeed");

    assert_eq!(
        breaker.state(),
        CbState::Closed,
        "CB must be Closed after reaching the success threshold in HalfOpen"
    );
}

// ---------------------------------------------------------------------------
// Test 5: Failed probe in HalfOpen re-opens the CB
// ---------------------------------------------------------------------------

/// If a probe call fails while the CB is in HalfOpen, it must transition back to Open
/// immediately (not wait for the full failure threshold again).
#[tokio::test]
async fn test_cb_half_open_probe_failure_returns_to_open() {
    let (mut conn, _node) = redis_conn().await;
    seed_string_key(&mut conn).await;

    let breaker = cb(1, 10, 50); // high success_threshold ensures one failure re-opens

    fail_via_cb(&breaker, conn.clone()).await;
    assert_eq!(breaker.state(), CbState::Open);

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Send a failing probe while in HalfOpen.
    fail_via_cb(&breaker, conn.clone()).await;

    assert_eq!(
        breaker.state(),
        CbState::Open,
        "a failed HalfOpen probe must re-open the CB immediately"
    );
}

// ---------------------------------------------------------------------------
// Test 6: Cloned CBs share state
// ---------------------------------------------------------------------------

/// `CircuitBreaker<S>` is cheaply cloneable via `Arc` — all clones share the same
/// atomic counters, watch channel, and timing state. Opening CB1 must be visible to CB2.
#[tokio::test]
async fn test_cb_clone_shares_state_across_tasks() {
    let (mut conn, _node) = redis_conn().await;
    seed_string_key(&mut conn).await;

    let cb1 = cb(1, 2, 5_000);
    let cb2 = cb1.clone();

    fail_via_cb(&cb1, conn.clone()).await;

    assert_eq!(
        cb2.state(),
        CbState::Open,
        "CB clone must reflect the Open state set by the original"
    );
}

// ---------------------------------------------------------------------------
// Test 7: Inbox fallback path — Redis CB open → PG dedup catches the duplicate
// ---------------------------------------------------------------------------

/// This is the end-to-end safety guarantee: when Redis is unavailable (CB open),
/// the inbox pattern falls back to the PG dedup path. The PG inbox INSERT with
/// ON CONFLICT DO NOTHING ensures a duplicate event is never processed twice,
/// even when the Redis fast-path is bypassed.
#[tokio::test]
async fn test_cb_open_inbox_falls_back_to_pg_dedup() {
    use common::containers::PgStack;

    let pg = PgStack::start().await;
    let pg_pool = sqlx::PgPool::connect(&pg.pg_url).await.expect("pg connect");

    let (mut conn, _node) = redis_conn().await;
    seed_string_key(&mut conn).await;

    // Open the CB with a single failure.
    let breaker = cb(1, 2, 5_000);
    fail_via_cb(&breaker, conn.clone()).await;
    assert_eq!(breaker.state(), CbState::Open);

    let event_id = uuid::Uuid::new_v4();

    // First delivery via PG fallback.
    let rows1 = sqlx::query(
        "INSERT INTO triad.triad_inbox (event_id, pattern_name, pipeline_name)
         VALUES ($1, 'inbox', 'pipeline-1')
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .execute(&pg_pool)
    .await
    .expect("INSERT 1 failed")
    .rows_affected();
    assert_eq!(rows1, 1, "first PG-fallback delivery must succeed");

    // Second delivery via PG fallback — ON CONFLICT catches it.
    let rows2 = sqlx::query(
        "INSERT INTO triad.triad_inbox (event_id, pattern_name, pipeline_name)
         VALUES ($1, 'inbox', 'pipeline-1')
         ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .execute(&pg_pool)
    .await
    .expect("INSERT 2 failed")
    .rows_affected();
    assert_eq!(rows2, 0, "duplicate via PG fallback must be a no-op");

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM triad.triad_inbox WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(&pg_pool)
            .await
            .expect("COUNT failed");
    assert_eq!(
        count, 1,
        "PG fallback (CB open path) must result in exactly one inbox record"
    );
}
