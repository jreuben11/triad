use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use metrics::counter;
use tracing::{error, info, warn};
use triad_core::metrics::names;
use triad_core::{
    error::PatternError,
    traits::{PatternModule, RunContext},
    types::{ModuleHealth, ModuleState},
};

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// A single Kafka consumer offset to commit inside the current transaction.
#[derive(Debug, Clone, PartialEq)]
pub struct TopicPartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

/// A message to be produced within an EOS transaction.
#[derive(Debug, Clone)]
pub struct OutboxMessage {
    pub topic: String,
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

/// Outcome of processing one event through the EOS protocol.
#[derive(Debug, Clone, PartialEq)]
pub enum EosOutcome {
    /// Transaction committed successfully.
    Committed,
    /// Event was already processed — skipped (idempotent).
    Duplicate,
    /// Transaction was aborted due to a mid-flight error.
    Aborted(String),
}

// ---------------------------------------------------------------------------
// Traits (mocked in unit tests)
// ---------------------------------------------------------------------------

/// Wraps the sequence of transactional Kafka producer calls.
///
/// Invariant: `send_offsets_to_transaction` MUST be called inside the
/// transaction before `commit_transaction` — never commit without the offsets.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait KafkaTransactor: Send + Sync {
    async fn init_transactions(&self) -> Result<(), PatternError>;
    async fn begin_transaction(&self) -> Result<(), PatternError>;
    async fn send_message(
        &self,
        topic: &str,
        key: &[u8],
        payload: &[u8],
    ) -> Result<(), PatternError>;
    /// Commit consumer offsets inside the current producer transaction.
    /// Must be called even when `offsets` is empty to satisfy EOS protocol.
    async fn send_offsets_to_transaction(
        &self,
        offsets: &[TopicPartitionOffset],
    ) -> Result<(), PatternError>;
    async fn commit_transaction(&self) -> Result<(), PatternError>;
    async fn abort_transaction(&self) -> Result<(), PatternError>;
}

/// Fast-path deduplication via Redis NX.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait EosRedisDedup: Send + Sync {
    /// Set the key with NX + EX semantics. Returns `true` = first delivery.
    async fn set_nx(&self, key: &str, ttl_secs: u64) -> Result<bool, PatternError>;
    /// Returns `true` if the Redis circuit breaker is currently open.
    fn is_circuit_open(&self) -> bool;
}

/// Fallback deduplication via PG (used when Redis circuit breaker is open).
/// Performs a SELECT + INSERT inside a single PG transaction.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait EosPgDedup: Send + Sync {
    /// Returns `true` if the event was already processed (duplicate).
    /// If not a duplicate, marks the event as processed atomically.
    async fn check_and_mark(&self, event_id: &str) -> Result<bool, PatternError>;
}

// ---------------------------------------------------------------------------
// EosCoordinator
// ---------------------------------------------------------------------------

/// Coordinates exactly-once semantics for a processing unit.
///
/// Implements the EOS protocol:
/// 1. Dedup check via Redis NX (fast path) or PG (fallback when CB open).
/// 2. Kafka transaction: `begin → produce messages → send_offsets → commit`.
///
/// Note: `inner: Arc<dyn PatternModule>` from the physical design sketch is
/// replaced by a direct `process_event` API so the coordinator is fully
/// testable with mocks without needing a real inner module.
pub struct EosCoordinator {
    name: String,
    transactor: Arc<dyn KafkaTransactor>,
    redis_dedup: Arc<dyn EosRedisDedup>,
    pg_dedup: Arc<dyn EosPgDedup>,
    dedup_window_secs: u64,
    state: ModuleState,
}

impl EosCoordinator {
    pub fn new(
        name: impl Into<String>,
        transactor: Arc<dyn KafkaTransactor>,
        redis_dedup: Arc<dyn EosRedisDedup>,
        pg_dedup: Arc<dyn EosPgDedup>,
        dedup_window_secs: u64,
    ) -> Self {
        Self {
            name: name.into(),
            transactor,
            redis_dedup,
            pg_dedup,
            dedup_window_secs,
            state: ModuleState::Starting,
        }
    }

    /// Initialize the transactional producer. Call once before the run loop.
    pub async fn init(&self) -> Result<(), PatternError> {
        self.transactor.init_transactions().await
    }

    // ------------------------------------------------------------------
    // Core EOS protocol
    // ------------------------------------------------------------------

    /// Process one event with exactly-once semantics.
    ///
    /// Protocol:
    /// 1. Dedup: Redis NX (fast) or PG fallback (when CB open).
    /// 2. `begin_transaction → send_message* → send_offsets_to_transaction → commit_transaction`.
    ///    On any failure inside the transaction: `abort_transaction`.
    pub async fn process_event(
        &self,
        event_id: &str,
        messages: &[OutboxMessage],
        offsets: &[TopicPartitionOffset],
    ) -> Result<EosOutcome, PatternError> {
        let is_new = self.dedup_check(event_id).await?;

        if !is_new {
            info!(event_id, "eos: duplicate event — skipping");
            return Ok(EosOutcome::Duplicate);
        }

        self.execute_transaction(messages, offsets).await
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Returns `true` if the event is new (not yet processed).
    async fn dedup_check(&self, event_id: &str) -> Result<bool, PatternError> {
        if self.redis_dedup.is_circuit_open() {
            warn!(event_id, "eos: Redis CB open — falling back to PG dedup");
            let already_processed = self.pg_dedup.check_and_mark(event_id).await?;
            Ok(!already_processed)
        } else {
            let key = format!("eos:{event_id}");
            self.redis_dedup.set_nx(&key, self.dedup_window_secs).await
        }
    }

    /// Execute the Kafka transaction. Abort on any mid-flight failure.
    ///
    /// Invariant: `send_offsets_to_transaction` is always called before
    /// `commit_transaction`, even when `offsets` is empty.
    async fn execute_transaction(
        &self,
        messages: &[OutboxMessage],
        offsets: &[TopicPartitionOffset],
    ) -> Result<EosOutcome, PatternError> {
        self.transactor.begin_transaction().await?;

        for msg in messages {
            if let Err(e) = self
                .transactor
                .send_message(&msg.topic, &msg.key, &msg.payload)
                .await
            {
                error!(error = %e, "eos: send_message failed — aborting transaction");
                self.transactor.abort_transaction().await?;
                counter!(names::EOS_ABORTED_TOTAL).increment(1);
                return Ok(EosOutcome::Aborted(e.to_string()));
            }
        }

        if let Err(e) = self.transactor.send_offsets_to_transaction(offsets).await {
            error!(error = %e, "eos: send_offsets_to_transaction failed — aborting");
            self.transactor.abort_transaction().await?;
            counter!(names::EOS_ABORTED_TOTAL).increment(1);
            return Ok(EosOutcome::Aborted(e.to_string()));
        }

        self.transactor.commit_transaction().await?;
        counter!(names::EOS_COMMITTED_TOTAL).increment(1);
        info!("eos: transaction committed");
        Ok(EosOutcome::Committed)
    }
}

// ---------------------------------------------------------------------------
// PatternModule impl
// ---------------------------------------------------------------------------

#[async_trait]
impl PatternModule for EosCoordinator {
    fn name(&self) -> &str {
        &self.name
    }

    fn pattern_type(&self) -> &str {
        "eos"
    }

    async fn run(&mut self, ctx: RunContext) -> Result<(), PatternError> {
        self.state = ModuleState::Running;
        self.init().await?;
        // In a real deployment the run loop consumes Kafka events and calls
        // process_event for each one. We wait here for cancellation only.
        ctx.cancel.cancelled().await;
        self.state = ModuleState::Stopped;
        Ok(())
    }

    async fn drain(&mut self, _timeout: Duration) -> Result<(), PatternError> {
        self.state = ModuleState::Draining;
        info!(name = %self.name, "eos: draining coordinator");
        self.state = ModuleState::Stopped;
        Ok(())
    }

    fn health(&self) -> ModuleHealth {
        ModuleHealth {
            state: self.state.clone(),
            lag: None,
            in_flight: None,
            last_error: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::{Sequence, predicate::*};
    use rstest::rstest;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;
    use tracing_test::traced_test;
    use triad_core::types::{PatternName, PipelineName};

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    fn make_offsets() -> Vec<TopicPartitionOffset> {
        vec![TopicPartitionOffset {
            topic: "orders".to_string(),
            partition: 0,
            offset: 42,
        }]
    }

    fn make_messages() -> Vec<OutboxMessage> {
        vec![OutboxMessage {
            topic: "out.orders".to_string(),
            key: b"k1".to_vec(),
            payload: b"payload".to_vec(),
        }]
    }

    fn coordinator(
        transactor: Arc<dyn KafkaTransactor>,
        redis: Arc<dyn EosRedisDedup>,
        pg: Arc<dyn EosPgDedup>,
    ) -> EosCoordinator {
        EosCoordinator::new("eos-test", transactor, redis, pg, 300)
    }

    // ------------------------------------------------------------------
    // Happy path: Redis NX → full transaction in correct order
    // ------------------------------------------------------------------

    #[traced_test]
    #[tokio::test]
    async fn test_eos_happy_path_redis_nx_commits_in_order() {
        let mut seq = Sequence::new();

        let mut tx = MockKafkaTransactor::new();
        tx.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| Ok(()));
        tx.expect_send_message()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _, _| Ok(()));
        tx.expect_send_offsets_to_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Ok(()));
        tx.expect_commit_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| Ok(()));
        tx.expect_abort_transaction().never();

        let mut redis = MockEosRedisDedup::new();
        redis.expect_is_circuit_open().return_const(false);
        redis
            .expect_set_nx()
            .with(eq("eos:evt-1"), eq(300u64))
            .returning(|_, _| Ok(true));

        let pg = MockEosPgDedup::new();

        let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        let result = coord
            .process_event("evt-1", &make_messages(), &make_offsets())
            .await
            .unwrap();
        assert_eq!(result, EosOutcome::Committed);
        assert!(!logs_contain("ERROR"));
    }

    // ------------------------------------------------------------------
    // Duplicate via Redis NX (already processed)
    // ------------------------------------------------------------------

    #[traced_test]
    #[tokio::test]
    async fn test_eos_duplicate_redis_nx_skips_transaction() {
        let mut tx = MockKafkaTransactor::new();
        tx.expect_begin_transaction().never();
        tx.expect_commit_transaction().never();

        let mut redis = MockEosRedisDedup::new();
        redis.expect_is_circuit_open().return_const(false);
        redis.expect_set_nx().returning(|_, _| Ok(false));

        let pg = MockEosPgDedup::new();

        let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        let result = coord
            .process_event("evt-dup", &make_messages(), &make_offsets())
            .await
            .unwrap();
        assert_eq!(result, EosOutcome::Duplicate);
        assert!(!logs_contain("ERROR"));
    }

    // ------------------------------------------------------------------
    // Redis CB open → PG fallback path — transaction still fires
    // ------------------------------------------------------------------

    #[traced_test]
    #[tokio::test]
    async fn test_eos_redis_cb_open_pg_fallback_new_event_commits() {
        let mut seq = Sequence::new();

        let mut tx = MockKafkaTransactor::new();
        tx.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| Ok(()));
        tx.expect_send_message()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _, _| Ok(()));
        tx.expect_send_offsets_to_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| Ok(()));
        tx.expect_commit_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| Ok(()));

        let mut redis = MockEosRedisDedup::new();
        redis.expect_is_circuit_open().return_const(true);
        redis.expect_set_nx().never();

        let mut pg = MockEosPgDedup::new();
        pg.expect_check_and_mark().returning(|_| Ok(false));

        let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        let result = coord
            .process_event("evt-2", &make_messages(), &make_offsets())
            .await
            .unwrap();
        assert_eq!(result, EosOutcome::Committed);
        assert!(!logs_contain("ERROR"));
    }

    // ------------------------------------------------------------------
    // Redis CB open + PG says duplicate → no transaction
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_eos_redis_cb_open_pg_says_duplicate_skips() {
        let mut tx = MockKafkaTransactor::new();
        tx.expect_begin_transaction().never();
        tx.expect_commit_transaction().never();

        let mut redis = MockEosRedisDedup::new();
        redis.expect_is_circuit_open().return_const(true);

        let mut pg = MockEosPgDedup::new();
        pg.expect_check_and_mark().returning(|_| Ok(true));

        let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        let result = coord
            .process_event("evt-dup-pg", &make_messages(), &make_offsets())
            .await
            .unwrap();
        assert_eq!(result, EosOutcome::Duplicate);
    }

    // ------------------------------------------------------------------
    // Producer failure mid-transaction → abort, never commit
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_eos_send_message_failure_aborts_never_commits() {
        let mut seq = Sequence::new();

        let mut tx = MockKafkaTransactor::new();
        tx.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| Ok(()));
        tx.expect_send_message()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _, _| {
                Err(PatternError::Backend(
                    triad_core::error::BackendError::Kafka("broker unavailable".to_string()),
                ))
            });
        tx.expect_abort_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| Ok(()));
        tx.expect_commit_transaction().never();
        tx.expect_send_offsets_to_transaction().never();

        let mut redis = MockEosRedisDedup::new();
        redis.expect_is_circuit_open().return_const(false);
        redis.expect_set_nx().returning(|_, _| Ok(true));

        let pg = MockEosPgDedup::new();

        let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        let result = coord
            .process_event("evt-fail", &make_messages(), &make_offsets())
            .await
            .unwrap();
        assert!(matches!(result, EosOutcome::Aborted(_)));
    }

    // ------------------------------------------------------------------
    // send_offsets_to_transaction failure → abort, never commit
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_eos_send_offsets_failure_aborts_never_commits() {
        let mut seq = Sequence::new();

        let mut tx = MockKafkaTransactor::new();
        tx.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| Ok(()));
        tx.expect_send_message()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _, _| Ok(()));
        tx.expect_send_offsets_to_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_| {
                Err(PatternError::Backend(
                    triad_core::error::BackendError::Kafka("offset commit failed".to_string()),
                ))
            });
        tx.expect_abort_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| Ok(()));
        // Invariant: commit must never fire when send_offsets fails
        tx.expect_commit_transaction().never();

        let mut redis = MockEosRedisDedup::new();
        redis.expect_is_circuit_open().return_const(false);
        redis.expect_set_nx().returning(|_, _| Ok(true));

        let pg = MockEosPgDedup::new();

        let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        let result = coord
            .process_event("evt-offsets-fail", &make_messages(), &make_offsets())
            .await
            .unwrap();
        assert!(matches!(result, EosOutcome::Aborted(_)));
    }

    // ------------------------------------------------------------------
    // send_offsets called even with empty offsets slice (EOS protocol)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_eos_send_offsets_called_even_when_empty() {
        let mut seq = Sequence::new();

        let mut tx = MockKafkaTransactor::new();
        tx.expect_begin_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| Ok(()));
        tx.expect_send_message()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _, _| Ok(()));
        tx.expect_send_offsets_to_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .withf(|offsets| offsets.is_empty())
            .returning(|_| Ok(()));
        tx.expect_commit_transaction()
            .times(1)
            .in_sequence(&mut seq)
            .returning(|| Ok(()));

        let mut redis = MockEosRedisDedup::new();
        redis.expect_is_circuit_open().return_const(false);
        redis.expect_set_nx().returning(|_, _| Ok(true));

        let pg = MockEosPgDedup::new();

        let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        let result = coord
            .process_event("evt-no-offsets", &make_messages(), &[])
            .await
            .unwrap();
        assert_eq!(result, EosOutcome::Committed);
    }

    // ------------------------------------------------------------------
    // init() calls init_transactions
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_eos_init_calls_init_transactions() {
        let mut tx = MockKafkaTransactor::new();
        tx.expect_init_transactions().times(1).returning(|| Ok(()));

        let redis = MockEosRedisDedup::new();
        let pg = MockEosPgDedup::new();

        let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        coord.init().await.unwrap();
    }

    // ------------------------------------------------------------------
    // PatternModule: name / pattern_type
    // ------------------------------------------------------------------

    #[test]
    fn test_eos_pattern_module_name_and_type() {
        let tx = MockKafkaTransactor::new();
        let redis = MockEosRedisDedup::new();
        let pg = MockEosPgDedup::new();

        let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        assert_eq!(coord.name(), "eos-test");
        assert_eq!(coord.pattern_type(), "eos");
    }

    // ------------------------------------------------------------------
    // health() reflects module state
    // ------------------------------------------------------------------

    #[test]
    fn test_eos_health_initial_state() {
        let tx = MockKafkaTransactor::new();
        let redis = MockEosRedisDedup::new();
        let pg = MockEosPgDedup::new();

        let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        let h = coord.health();
        assert_eq!(h.state, ModuleState::Starting);
        assert!(h.in_flight.is_none());
        assert!(h.lag.is_none());
    }

    // ------------------------------------------------------------------
    // drain() transitions to Stopped
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_eos_drain_transitions_to_stopped() {
        let tx = MockKafkaTransactor::new();
        let redis = MockEosRedisDedup::new();
        let pg = MockEosPgDedup::new();

        let mut coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        coord.drain(Duration::from_secs(5)).await.unwrap();
        assert_eq!(coord.health().state, ModuleState::Stopped);
    }

    // ------------------------------------------------------------------
    // run() sets Running state and stops on cancellation
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_eos_run_stops_on_cancellation() {
        let mut tx = MockKafkaTransactor::new();
        tx.expect_init_transactions().times(1).returning(|| Ok(()));

        let redis = MockEosRedisDedup::new();
        let pg = MockEosPgDedup::new();

        let mut coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        let token = CancellationToken::new();
        let ctx = RunContext {
            cancel: token.clone(),
            pattern: PatternName("eos-test".to_string()),
            pipeline: PipelineName("main".to_string()),
        };

        // Cancel after a short delay so run() exits
        let child = token.child_token();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            child.cancel();
        });
        token.cancel();

        coord.run(ctx).await.unwrap();
        assert_eq!(coord.health().state, ModuleState::Stopped);
    }

    // ------------------------------------------------------------------
    // Parametrised: various message counts all commit successfully
    // ------------------------------------------------------------------

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(3)]
    #[tokio::test]
    async fn test_eos_various_message_counts_commit(#[case] n: usize) {
        let mut tx = MockKafkaTransactor::new();
        tx.expect_begin_transaction().returning(|| Ok(()));
        tx.expect_send_message().returning(|_, _, _| Ok(()));
        tx.expect_send_offsets_to_transaction()
            .returning(|_| Ok(()));
        tx.expect_commit_transaction().returning(|| Ok(()));

        let mut redis = MockEosRedisDedup::new();
        redis.expect_is_circuit_open().return_const(false);
        redis.expect_set_nx().returning(|_, _| Ok(true));

        let pg = MockEosPgDedup::new();

        let messages: Vec<OutboxMessage> = (0..n)
            .map(|i| OutboxMessage {
                topic: "out".to_string(),
                key: i.to_string().into_bytes(),
                payload: b"p".to_vec(),
            })
            .collect();

        let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        let result = coord
            .process_event("evt-n", &messages, &make_offsets())
            .await
            .unwrap();
        assert_eq!(result, EosOutcome::Committed);
    }

    // ------------------------------------------------------------------
    // Redis key format: "eos:{event_id}"
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_eos_redis_key_format() {
        let mut tx = MockKafkaTransactor::new();
        tx.expect_begin_transaction().returning(|| Ok(()));
        tx.expect_send_message().returning(|_, _, _| Ok(()));
        tx.expect_send_offsets_to_transaction()
            .returning(|_| Ok(()));
        tx.expect_commit_transaction().returning(|| Ok(()));

        let mut redis = MockEosRedisDedup::new();
        redis.expect_is_circuit_open().return_const(false);
        redis
            .expect_set_nx()
            .with(eq("eos:my-custom-event-id"), always())
            .times(1)
            .returning(|_, _| Ok(true));

        let pg = MockEosPgDedup::new();

        let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
        coord
            .process_event("my-custom-event-id", &make_messages(), &make_offsets())
            .await
            .unwrap();
    }

    // ------------------------------------------------------------------
    // OutboxMessage and TopicPartitionOffset field access
    // ------------------------------------------------------------------

    #[test]
    fn test_eos_supporting_types_fields() {
        let msg = OutboxMessage {
            topic: "t".to_string(),
            key: b"k".to_vec(),
            payload: b"p".to_vec(),
        };
        assert_eq!(msg.topic, "t");

        let off = TopicPartitionOffset {
            topic: "orders".to_string(),
            partition: 1,
            offset: 99,
        };
        assert_eq!(off.partition, 1);
        assert_eq!(off.offset, 99);
    }

    // -- Prometheus metric assertions ------------------------------------------

    #[test]
    fn test_eos_committed_total_emitted_on_success() {
        use metrics_util::debugging::DebuggingRecorder;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let mut tx = MockKafkaTransactor::new();
                    tx.expect_begin_transaction().returning(|| Ok(()));
                    tx.expect_send_message().returning(|_, _, _| Ok(()));
                    tx.expect_send_offsets_to_transaction()
                        .returning(|_| Ok(()));
                    tx.expect_commit_transaction().returning(|| Ok(()));
                    let mut redis = MockEosRedisDedup::new();
                    redis.expect_is_circuit_open().return_const(false);
                    redis.expect_set_nx().returning(|_, _| Ok(true));
                    let pg = MockEosPgDedup::new();
                    let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
                    let out = coord
                        .process_event("evt-metric", &make_messages(), &make_offsets())
                        .await
                        .unwrap();
                    assert_eq!(out, EosOutcome::Committed);
                });
        });

        let snap = snapshotter.snapshot();
        let counters = snap.into_vec();
        let found = counters
            .iter()
            .any(|(k, ..)| k.key().name() == names::EOS_COMMITTED_TOTAL);
        assert!(found, "counter {} not emitted", names::EOS_COMMITTED_TOTAL);
    }

    #[test]
    fn test_eos_aborted_total_emitted_on_send_failure() {
        use metrics_util::debugging::DebuggingRecorder;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let mut tx = MockKafkaTransactor::new();
                    tx.expect_begin_transaction().returning(|| Ok(()));
                    tx.expect_send_message().returning(|_, _, _| {
                        Err(PatternError::Backend(
                            triad_core::error::BackendError::Kafka("fail".to_string()),
                        ))
                    });
                    tx.expect_abort_transaction().returning(|| Ok(()));
                    let mut redis = MockEosRedisDedup::new();
                    redis.expect_is_circuit_open().return_const(false);
                    redis.expect_set_nx().returning(|_, _| Ok(true));
                    let pg = MockEosPgDedup::new();
                    let coord = coordinator(Arc::new(tx), Arc::new(redis), Arc::new(pg));
                    let out = coord
                        .process_event("evt-abort", &make_messages(), &make_offsets())
                        .await
                        .unwrap();
                    assert!(matches!(out, EosOutcome::Aborted(_)));
                });
        });

        let snap = snapshotter.snapshot();
        let counters = snap.into_vec();
        let found = counters
            .iter()
            .any(|(k, ..)| k.key().name() == names::EOS_ABORTED_TOTAL);
        assert!(found, "counter {} not emitted", names::EOS_ABORTED_TOTAL);
    }
}
