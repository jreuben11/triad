use std::time::Duration;

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::{postgres::Postgres, redis::Redis};
use triad_core::config::{
    KafkaConfig, KafkaConsumerConfig, KafkaProducerConfig, KafkaSecurityConfig, PostgresConfig,
    RedisConfig, RedisMode,
};
use triad_runner::backends::{
    CircuitBreaker,
    circuit_breaker::{CbConfig, CbState},
    kafka::KafkaBackend,
    postgres::PgBackend,
    redis::{RedisBackend, RedisPool},
};

// ---------- helpers ----------

fn cb_config() -> CbConfig {
    CbConfig {
        failure_threshold: 3,
        success_threshold: 2,
        half_open_after: Duration::from_millis(500),
        half_open_max_calls: 2,
    }
}

// ---------- postgres ----------

#[tokio::test]
async fn test_pg_backend_connects_and_runs_migrations() {
    let node = Postgres::default()
        .with_user("triad")
        .with_password("secret")
        .with_db_name("triad_test")
        .start()
        .await
        .expect("failed to start postgres container");

    let port = node
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get port");
    let url = format!("postgresql://triad:secret@127.0.0.1:{}/triad_test", port);

    let cfg = PostgresConfig {
        url: url.clone(),
        replication_url: None,
        max_connections: 5,
        min_idle: None,
        connection_timeout_ms: 15_000,
        statement_timeout_ms: None,
        wal_level: None,
        replication_slot: None,
        publication: None,
        ssl_mode: "disable".to_string(),
        ssl_root_cert: None,
    };

    let backend = PgBackend::new(&cfg, cb_config())
        .await
        .expect("PgBackend::new failed");

    // Verify pool is actually connected
    let row: (i64,) = sqlx::query_as("SELECT 1")
        .fetch_one(&backend.pool)
        .await
        .expect("simple query failed");
    assert_eq!(row.0, 1);
}

// ---------- kafka ----------

// Kafka integration tests require a running broker; we test config construction
// only (no testcontainers kafka image in this phase).
#[test]
fn test_kafka_backend_constructs_from_real_like_config() {
    let cfg = KafkaConfig {
        brokers: vec!["localhost:9092".to_string()],
        security: KafkaSecurityConfig {
            protocol: "PLAINTEXT".to_string(),
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
            ssl_ca_cert: None,
        },
        producer: KafkaProducerConfig {
            transactional_id_prefix: "triad-int".to_string(),
            acks: "all".to_string(),
            compression_type: "lz4".to_string(),
            transaction_timeout_ms: 60_000,
            enable_idempotence: true,
            max_in_flight_requests: 1,
            batch_size: 131_072,
            linger_ms: 5,
            request_timeout_ms: 30_000,
        },
        consumer: KafkaConsumerConfig {
            group_id: "triad-int-consumer".to_string(),
            isolation_level: "read_committed".to_string(),
            auto_offset_reset: "earliest".to_string(),
            max_poll_interval_ms: 300_000,
            max_poll_records: 500,
            fetch_max_bytes: 52_428_800,
            session_timeout_ms: 10_000,
            heartbeat_interval_ms: 3_000,
        },
        schema_registry_url: None,
    };
    let backend = KafkaBackend::new(&cfg, cb_config());
    assert_eq!(backend.circuit.state(), CbState::Closed);
}

// ---------- redis ----------

#[tokio::test]
async fn test_redis_standalone_backend_connects() {
    let node = Redis::default()
        .start()
        .await
        .expect("failed to start redis container");

    let port = node
        .get_host_port_ipv4(6379)
        .await
        .expect("failed to get port");
    let url = format!("redis://127.0.0.1:{}", port);

    let cfg = RedisConfig {
        mode: RedisMode::Standalone,
        url,
        pool_size: 5,
        min_idle: None,
        connection_timeout_ms: 5000,
        read_timeout_ms: 1000,
        write_timeout_ms: 1000,
        max_retries: 3,
        tls: None,
    };

    let backend = RedisBackend::new(&cfg, cb_config()).expect("RedisBackend::new failed");

    // Verify a real connection works
    if let RedisPool::Standalone(pool) = backend.pool {
        let mut conn = pool.get().await.expect("pool.get() failed");
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn as &mut deadpool_redis::Connection)
            .await
            .expect("PING failed");
        assert_eq!(pong, "PONG");
    } else {
        panic!("expected Standalone pool variant");
    }
}

// ---------- circuit breaker integration ----------

#[tokio::test]
async fn test_circuit_breaker_open_after_failures() {
    use triad_core::error::BackendError;

    let cb = CircuitBreaker::<()>::new(
        CbConfig {
            failure_threshold: 2,
            success_threshold: 2,
            half_open_after: Duration::from_secs(60),
            half_open_max_calls: 1,
        },
        "test",
    );

    for _ in 0..2 {
        let _ = cb
            .call(async { Err::<(), BackendError>(BackendError::Kafka("fail".to_string())) })
            .await;
    }

    assert_eq!(cb.state(), CbState::Open);

    let result = cb.call(async { Ok::<(), BackendError>(()) }).await;
    assert!(matches!(
        result,
        Err(BackendError::CircuitBreakerOpen { .. })
    ));
}

#[tokio::test]
async fn test_circuit_breaker_half_open_and_close() {
    use triad_core::error::BackendError;

    let cb = CircuitBreaker::<()>::new(
        CbConfig {
            failure_threshold: 1,
            success_threshold: 2,
            half_open_after: Duration::from_millis(10),
            half_open_max_calls: 2,
        },
        "test",
    );

    let _ = cb
        .call(async { Err::<(), BackendError>(BackendError::Kafka("fail".to_string())) })
        .await;
    assert_eq!(cb.state(), CbState::Open);

    tokio::time::sleep(Duration::from_millis(20)).await;

    let _ = cb.call(async { Ok::<(), BackendError>(()) }).await;
    let _ = cb.call(async { Ok::<(), BackendError>(()) }).await;
    assert_eq!(cb.state(), CbState::Closed);
}
