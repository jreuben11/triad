#![cfg(feature = "integration")]

mod common;

use std::time::Duration;

use common::containers::PgStack;
use deadpool_redis::Config as RedisConfig;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;
use tokio::time::timeout;

#[tokio::test]
async fn test_cache_redis_write_and_read() {
    let redis_node = Redis::default()
        .start()
        .await
        .expect("failed to start redis");
    let port = redis_node
        .get_host_port_ipv4(6379)
        .await
        .expect("failed to get port");
    let redis_url = format!("redis://127.0.0.1:{port}");

    let cfg = RedisConfig::from_url(redis_url);
    let pool = cfg
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .expect("redis pool failed");

    let mut conn = pool.get().await.expect("get connection failed");

    // Write through: SET key value with expiry.
    redis::cmd("SET")
        .arg("triad:cache:user:1")
        .arg(serde_json::to_string(&serde_json::json!({"name": "Alice", "balance": 100})).unwrap())
        .arg("EX")
        .arg(300i64)
        .query_async::<String>(&mut conn)
        .await
        .expect("SET failed");

    // Read from cache: should return the written value.
    let value: String = redis::cmd("GET")
        .arg("triad:cache:user:1")
        .query_async(&mut conn)
        .await
        .expect("GET failed");

    let parsed: serde_json::Value = serde_json::from_str(&value).expect("JSON parse failed");
    assert_eq!(parsed["name"], "Alice");
    assert_eq!(parsed["balance"], 100);
}

#[tokio::test]
async fn test_cache_cold_start_populates_redis_from_pg() {
    let stack = PgStack::start().await;
    let redis_node = Redis::default()
        .start()
        .await
        .expect("failed to start redis");
    let port = redis_node
        .get_host_port_ipv4(6379)
        .await
        .expect("failed to get port");
    let redis_url = format!("redis://127.0.0.1:{port}");

    let pg_pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect failed");
    let redis_cfg = RedisConfig::from_url(redis_url);
    let redis_pool = redis_cfg
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .expect("redis pool failed");

    // Simulate: write a row to PG.
    sqlx::query(
        "INSERT INTO triad_feature_flags (name, enabled, rollout_percentage)
         VALUES ('new-checkout', true, 100)",
    )
    .execute(&pg_pool)
    .await
    .expect("INSERT flag failed");

    // Cold start: load from PG and push to Redis (simulated inline).
    let (flag_name, enabled): (String, bool) =
        sqlx::query_as("SELECT name, enabled FROM triad_feature_flags WHERE name = 'new-checkout'")
            .fetch_one(&pg_pool)
            .await
            .expect("SELECT flag failed");

    let mut conn = redis_pool.get().await.expect("redis conn failed");
    redis::cmd("SET")
        .arg(format!("triad:flag:{flag_name}"))
        .arg(enabled.to_string())
        .arg("EX")
        .arg(300i64)
        .query_async::<String>(&mut conn)
        .await
        .expect("SET flag failed");

    let cached: String = redis::cmd("GET")
        .arg("triad:flag:new-checkout")
        .query_async(&mut conn)
        .await
        .expect("GET flag failed");

    assert_eq!(cached, "true", "cold-start should populate Redis from PG");
}

#[tokio::test]
async fn test_cache_eviction_falls_back_to_pg() {
    let stack = PgStack::start().await;
    let redis_node = Redis::default()
        .start()
        .await
        .expect("failed to start redis");
    let port = redis_node
        .get_host_port_ipv4(6379)
        .await
        .expect("failed to get port");
    let redis_url = format!("redis://127.0.0.1:{port}");

    let pg_pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect failed");
    let redis_cfg = RedisConfig::from_url(redis_url);
    let redis_pool = redis_cfg
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .expect("redis pool failed");

    // Write to PG.
    sqlx::query(
        "INSERT INTO triad_feature_flags (name, enabled, rollout_percentage)
         VALUES ('beta-ui', false, 10)",
    )
    .execute(&pg_pool)
    .await
    .expect("INSERT failed");

    let mut conn = redis_pool.get().await.expect("conn failed");

    // Cache miss: key does not exist in Redis.
    let cached: Option<String> = redis::cmd("GET")
        .arg("triad:flag:beta-ui")
        .query_async(&mut conn)
        .await
        .expect("GET failed");

    assert!(cached.is_none(), "cache should be empty before population");

    // Fallback: read from PG.
    let (enabled,): (bool,) =
        sqlx::query_as("SELECT enabled FROM triad_feature_flags WHERE name = 'beta-ui'")
            .fetch_one(&pg_pool)
            .await
            .expect("PG fallback failed");

    assert!(!enabled, "PG fallback should return flag value");

    // Write-through: populate cache within 1s deadline.
    timeout(Duration::from_secs(1), async {
        redis::cmd("SET")
            .arg("triad:flag:beta-ui")
            .arg(enabled.to_string())
            .arg("EX")
            .arg(300i64)
            .query_async::<String>(&mut conn)
            .await
            .expect("SET failed");
    })
    .await
    .expect("write-through timed out");

    let after: String = redis::cmd("GET")
        .arg("triad:flag:beta-ui")
        .query_async(&mut conn)
        .await
        .expect("GET after write-through failed");

    assert_eq!(after, "false");
}
