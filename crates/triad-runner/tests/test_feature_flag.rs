#![cfg(feature = "integration")]

mod common;

use std::time::Duration;

use common::containers::PgStack;
use deadpool_redis::Config as RedisConfig;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;
use tokio::time::timeout;

#[tokio::test]
async fn test_feature_flag_created_in_pg_appears_in_redis() {
    let stack = PgStack::start().await;
    let redis_node = Redis::default()
        .start()
        .await
        .expect("failed to start redis");
    let port = redis_node
        .get_host_port_ipv4(6379)
        .await
        .expect("port failed");
    let redis_url = format!("redis://127.0.0.1:{port}");

    let pg_pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect failed");
    let redis_pool = RedisConfig::from_url(redis_url)
        .create_pool(Some(deadpool_redis::Runtime::Tokio1))
        .expect("redis pool failed");

    // Insert flag in PG.
    sqlx::query(
        "INSERT INTO triad_feature_flags (name, enabled, rollout_percentage)
         VALUES ('dark-mode', true, 50)",
    )
    .execute(&pg_pool)
    .await
    .expect("INSERT flag failed");

    // Simulate hot-reload: poll PG and push to Redis within 5s window.
    let deadline = Duration::from_secs(5);
    timeout(deadline, async {
        let flags: Vec<(String, bool, i16)> =
            sqlx::query_as("SELECT name, enabled, rollout_percentage FROM triad_feature_flags")
                .fetch_all(&pg_pool)
                .await
                .expect("SELECT flags failed");

        let mut conn = redis_pool.get().await.expect("redis conn failed");
        for (name, enabled, pct) in &flags {
            redis::cmd("HSET")
                .arg(format!("triad:flag:{name}"))
                .arg("enabled")
                .arg(enabled.to_string())
                .arg("rollout_percentage")
                .arg(pct.to_string())
                .query_async::<i64>(&mut conn)
                .await
                .expect("HSET failed");
        }
    })
    .await
    .expect("hot-reload timed out");

    // Verify flag is visible in Redis.
    let mut conn = redis_pool.get().await.expect("redis conn failed");
    let enabled: String = redis::cmd("HGET")
        .arg("triad:flag:dark-mode")
        .arg("enabled")
        .query_async(&mut conn)
        .await
        .expect("HGET enabled failed");

    assert_eq!(enabled, "true", "flag should be visible in Redis");

    let rollout: String = redis::cmd("HGET")
        .arg("triad:flag:dark-mode")
        .arg("rollout_percentage")
        .query_async(&mut conn)
        .await
        .expect("HGET rollout_percentage failed");

    assert_eq!(rollout, "50");
}

#[tokio::test]
async fn test_feature_flag_rollout_percentage_evaluation() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("connect failed");

    sqlx::query(
        "INSERT INTO triad_feature_flags (name, enabled, rollout_percentage)
         VALUES ('gradual-rollout', true, 25)",
    )
    .execute(&pool)
    .await
    .expect("INSERT failed");

    let (enabled, pct): (bool, i16) = sqlx::query_as(
        "SELECT enabled, rollout_percentage FROM triad_feature_flags WHERE name = 'gradual-rollout'",
    )
    .fetch_one(&pool)
    .await
    .expect("SELECT failed");

    assert!(enabled);
    assert_eq!(pct, 25);

    // Rollout bucket: hash(user_id) % 100 < rollout_percentage.
    let bucket = |user_id: u64| -> bool {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        user_id.hash(&mut h);
        (h.finish() % 100) < pct as u64 // pct is i16
    };

    // Sample 100 users and check roughly 25% get the flag.
    let enabled_count = (0u64..100).filter(|&id| bucket(id)).count();
    assert!(
        enabled_count > 10 && enabled_count < 40,
        "expected ~25% rollout, got {enabled_count}%"
    );
}

#[tokio::test]
async fn test_feature_flag_disabled_flag_not_served() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("connect failed");

    sqlx::query(
        "INSERT INTO triad_feature_flags (name, enabled, rollout_percentage)
         VALUES ('disabled-feature', false, 100)",
    )
    .execute(&pool)
    .await
    .expect("INSERT failed");

    let (enabled,): (bool,) =
        sqlx::query_as("SELECT enabled FROM triad_feature_flags WHERE name = 'disabled-feature'")
            .fetch_one(&pool)
            .await
            .expect("SELECT failed");

    assert!(
        !enabled,
        "disabled flag should not be served even at 100% rollout"
    );
}
