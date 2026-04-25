#![cfg(feature = "integration")]

mod common;

use common::containers::{PgStack, TestStack};
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

/// Verify the EOS invariant: when the PG commit fails (simulated by rolling back the
/// PG transaction), the Kafka transaction must also be aborted so no messages are
/// visible to read_committed consumers.
///
/// Marked `#[ignore]` because the testcontainers Kafka image's transaction coordinator
/// fails to initialize (`ERR__TIMED_OUT` / "retry call to resume") in single-node
/// container setups without `transaction.state.log.min.isr=1` tuning.
/// Run manually with `cargo nextest run ... -- --ignored` against a real Kafka broker.
#[tokio::test]
#[ignore = "requires Kafka with transaction coordinator ready; flaky under testcontainers"]
async fn test_eos_kafka_txn_aborted_on_pg_commit_failure() {
    let stack = TestStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect failed");

    // Simulate the EOS pattern: begin both PG and Kafka transactions, then roll back PG
    // and abort Kafka (as the EOS handler would do on PG commit failure).
    let brokers = stack.kafka_brokers.clone();
    tokio::task::spawn_blocking(move || {
        use rdkafka::{
            config::ClientConfig,
            producer::{BaseProducer, BaseRecord, Producer},
        };

        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("transactional.id", "eos-abort-test-1")
            .set("enable.idempotence", "true")
            .create()
            .expect("producer create failed");

        producer
            .init_transactions(std::time::Duration::from_secs(10))
            .expect("init_transactions failed");
        producer
            .begin_transaction()
            .expect("begin_transaction failed");

        producer
            .send(
                BaseRecord::to("eos-abort-test-topic")
                    .payload(b"test-payload" as &[u8])
                    .key(b"test-key" as &[u8]),
            )
            .map_err(|(e, _)| e)
            .expect("send to queue failed");

        // PG commit would happen here. Simulated failure → abort Kafka transaction.
        producer
            .abort_transaction(std::time::Duration::from_secs(5))
            .expect("abort_transaction failed");
    })
    .await
    .expect("producer task panicked");

    // Begin a real PG transaction and roll it back to confirm PG-side consistency.
    let mut pg_tx = pool.begin().await.expect("pg begin failed");
    sqlx::query(
        "INSERT INTO triad.triad_inbox (event_id, pattern_name, pipeline_name)
         VALUES (gen_random_uuid(), 'eos-abort-test', 'pipeline-1')",
    )
    .execute(&mut *pg_tx)
    .await
    .expect("pg insert failed");
    pg_tx.rollback().await.expect("pg rollback failed");

    // Verify: read_committed consumer sees NO messages from the aborted Kafka transaction.
    let brokers = stack.kafka_brokers.clone();
    let no_messages = tokio::task::spawn_blocking(move || -> bool {
        use rdkafka::{
            config::ClientConfig,
            consumer::{BaseConsumer, Consumer},
        };

        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", "eos-abort-verify-consumer")
            .set("isolation.level", "read_committed")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("consumer create failed");

        consumer
            .subscribe(&["eos-abort-test-topic"])
            .expect("subscribe failed");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while std::time::Instant::now() < deadline {
            match consumer.poll(std::time::Duration::from_millis(200)) {
                Some(Ok(_)) => return false, // Committed message visible — should not happen
                _ => {}
            }
        }
        true
    })
    .await
    .expect("consumer task panicked");

    assert!(
        no_messages,
        "aborted Kafka transaction must not produce messages visible to read_committed consumers"
    );
}
