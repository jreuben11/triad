use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use metrics::counter;
use tracing::{error, info, instrument, warn};
use triad_core::{
    error::PatternError,
    metrics::names,
    traits::{CheckpointStore, PatternModule, RunContext},
    types::{ModuleHealth, ModuleState},
};

/// Narrow repository trait for outbox operations — mockable in unit tests.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait OutboxStore: Send + Sync {
    async fn fetch_pending(&self, batch_size: i64) -> Result<Vec<OutboxRow>, PatternError>;
    async fn mark_published(&self, ids: &[i64]) -> Result<(), PatternError>;
    async fn delete_expired(&self, retention_secs: i64) -> Result<u64, PatternError>;
}

/// Narrow trait for the Kafka transactional producer used by outbox.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait OutboxProducer: Send + Sync {
    async fn send_transactional(
        &self,
        topic: &str,
        key: &[u8],
        payload: &[u8],
    ) -> Result<(), PatternError>;
}

/// A row from `triad_outbox`.
#[derive(Debug, Clone)]
pub struct OutboxRow {
    pub id: i64,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub kafka_topic: Option<String>,
    pub kafka_key: Option<String>,
}

/// Outbox relay pattern module.
///
/// Polls `triad_outbox` for pending rows, publishes them to Kafka inside an
/// EOS transaction, then marks them as published in Postgres.
///
/// Safety invariant: the Kafka producer transaction must wrap both the message
/// send and the offset commit. The store.mark_published() call updates Postgres
/// only after the producer has committed — this is enforced by sequential ordering.
pub struct OutboxModule {
    pub name: String,
    pub kafka_topic: String,
    pub poll_interval: Duration,
    pub batch_size: i64,
    pub retention_secs: Option<i64>,
    pub store: Arc<dyn OutboxStore>,
    pub producer: Arc<dyn OutboxProducer>,
    pub checkpoint: Arc<dyn CheckpointStore>,
}

impl OutboxModule {
    pub fn new(
        name: impl Into<String>,
        kafka_topic: impl Into<String>,
        store: Arc<dyn OutboxStore>,
        producer: Arc<dyn OutboxProducer>,
        checkpoint: Arc<dyn CheckpointStore>,
    ) -> Self {
        Self {
            name: name.into(),
            kafka_topic: kafka_topic.into(),
            poll_interval: Duration::from_secs(1),
            batch_size: 100,
            retention_secs: None,
            store,
            producer,
            checkpoint,
        }
    }

    #[instrument(skip(self), fields(pattern_name = %self.name, pipeline_name = "outbox"))]
    async fn relay_batch(&self) -> Result<usize, PatternError> {
        let rows = self.store.fetch_pending(self.batch_size).await?;
        if rows.is_empty() {
            return Ok(0);
        }

        let count = rows.len();
        counter!(names::OUTBOX_PENDING_TOTAL).absolute(count as u64);

        let mut ids = Vec::with_capacity(count);
        for row in &rows {
            let topic = row
                .kafka_topic
                .as_deref()
                .unwrap_or(self.kafka_topic.as_str());
            let key = row
                .kafka_key
                .as_deref()
                .map(|k| k.as_bytes().to_vec())
                .unwrap_or_else(|| row.aggregate_id.as_bytes().to_vec());
            let payload = serde_json::to_vec(&row.payload)
                .map_err(|e| PatternError::Deserialisation(e.to_string()))?;

            self.producer
                .send_transactional(topic, &key, &payload)
                .await?;
            ids.push(row.id);
        }

        self.store.mark_published(&ids).await?;
        counter!(names::KAFKA_PRODUCER_TXN_TOTAL).increment(1);
        info!(count, "outbox batch relayed");
        Ok(count)
    }
}

#[async_trait]
impl PatternModule for OutboxModule {
    fn name(&self) -> &str {
        &self.name
    }

    fn pattern_type(&self) -> &str {
        "outbox"
    }

    async fn run(&mut self, ctx: RunContext) -> Result<(), PatternError> {
        info!(
            pattern_name = %ctx.pattern.0,
            pipeline_name = %ctx.pipeline.0,
            "outbox module started"
        );

        loop {
            tokio::select! {
                _ = ctx.cancel.cancelled() => {
                    info!("outbox module cancelled");
                    return Err(PatternError::Cancelled);
                }
                _ = tokio::time::sleep(self.poll_interval) => {
                    match self.relay_batch().await {
                        Ok(0) => {}
                        Ok(n) => info!(n, "relayed outbox messages"),
                        Err(e) => {
                            error!(error = %e, "outbox relay error");
                            counter!(names::ERROR_TOTAL, "pattern_name" => self.name.clone())
                                .increment(1);
                        }
                    }
                    if let Some(ret) = self.retention_secs {
                        if let Err(e) = self.store.delete_expired(ret).await {
                            warn!(error = %e, "outbox reaper error");
                        }
                    }
                }
            }
        }
    }

    async fn drain(&mut self, timeout: Duration) -> Result<(), PatternError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(PatternError::DrainTimeout);
            }
            match self.relay_batch().await {
                Ok(0) => return Ok(()),
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }
    }

    fn health(&self) -> ModuleHealth {
        ModuleHealth {
            state: ModuleState::Running,
            lag: None,
            in_flight: None,
            last_error: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::predicate::*;
    use rstest::rstest;
    use tokio_util::sync::CancellationToken;
    use triad_core::{
        error::CheckpointError,
        traits::{CheckpointRow, CheckpointStore},
        types::{PatternName, PipelineName},
    };

    struct NoopCheckpointStore;
    #[async_trait::async_trait]
    impl CheckpointStore for NoopCheckpointStore {
        async fn load(
            &self,
            _pattern: &PatternName,
            _pipeline: &PipelineName,
        ) -> Result<Option<CheckpointRow>, CheckpointError> {
            Ok(None)
        }
        async fn save(
            &self,
            _row: &CheckpointRow,
            _expected_version: i64,
        ) -> Result<(), CheckpointError> {
            Ok(())
        }
    }

    fn make_row(id: i64) -> OutboxRow {
        OutboxRow {
            id,
            aggregate_type: "order".to_string(),
            aggregate_id: "ord-1".to_string(),
            event_type: "OrderCreated".to_string(),
            payload: serde_json::json!({"amount": 100}),
            kafka_topic: None,
            kafka_key: None,
        }
    }

    fn make_module(store: MockOutboxStore, producer: MockOutboxProducer) -> OutboxModule {
        OutboxModule {
            name: "test-outbox".to_string(),
            kafka_topic: "orders".to_string(),
            poll_interval: Duration::from_millis(10),
            batch_size: 10,
            retention_secs: None,
            store: Arc::new(store),
            producer: Arc::new(producer),
            checkpoint: Arc::new(NoopCheckpointStore),
        }
    }

    #[tokio::test]
    async fn test_outbox_relay_batch_empty_returns_zero() {
        let mut store = MockOutboxStore::new();
        store.expect_fetch_pending().returning(|_| Ok(vec![]));
        let producer = MockOutboxProducer::new();

        let module = make_module(store, producer);
        assert_eq!(module.relay_batch().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_outbox_relay_batch_publishes_and_marks() {
        let mut store = MockOutboxStore::new();
        store
            .expect_fetch_pending()
            .returning(|_| Ok(vec![make_row(1), make_row(2)]));
        store
            .expect_mark_published()
            .with(eq(vec![1i64, 2i64]))
            .returning(|_| Ok(()));

        let mut producer = MockOutboxProducer::new();
        producer
            .expect_send_transactional()
            .times(2)
            .returning(|_, _, _| Ok(()));

        let module = make_module(store, producer);
        assert_eq!(module.relay_batch().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_outbox_relay_uses_row_topic_if_set() {
        let mut row = make_row(5);
        row.kafka_topic = Some("custom-topic".to_string());

        let mut store = MockOutboxStore::new();
        store
            .expect_fetch_pending()
            .returning(move |_| Ok(vec![row.clone()]));
        store.expect_mark_published().returning(|_| Ok(()));

        let mut producer = MockOutboxProducer::new();
        producer
            .expect_send_transactional()
            .withf(|topic, _, _| topic == "custom-topic")
            .times(1)
            .returning(|_, _, _| Ok(()));

        let module = make_module(store, producer);
        assert!(module.relay_batch().await.is_ok());
    }

    #[tokio::test]
    async fn test_outbox_relay_batch_producer_error_propagates() {
        let mut store = MockOutboxStore::new();
        store
            .expect_fetch_pending()
            .returning(|_| Ok(vec![make_row(1)]));

        let mut producer = MockOutboxProducer::new();
        producer
            .expect_send_transactional()
            .returning(|_, _, _| Err(PatternError::Saga("kafka error".to_string())));

        let module = make_module(store, producer);
        assert!(module.relay_batch().await.is_err());
    }

    #[tokio::test]
    async fn test_outbox_drain_returns_ok_when_empty() {
        let mut store = MockOutboxStore::new();
        store.expect_fetch_pending().returning(|_| Ok(vec![]));
        let producer = MockOutboxProducer::new();

        let mut module = make_module(store, producer);
        assert!(module.drain(Duration::from_secs(5)).await.is_ok());
    }

    #[tokio::test]
    async fn test_outbox_run_cancels_on_token() {
        let mut store = MockOutboxStore::new();
        store.expect_fetch_pending().returning(|_| Ok(vec![]));
        let producer = MockOutboxProducer::new();

        let mut module = make_module(store, producer);
        let cancel = CancellationToken::new();
        let ctx = RunContext {
            cancel: cancel.clone(),
            pattern: PatternName("outbox".to_string()),
            pipeline: PipelineName("test".to_string()),
        };
        cancel.cancel();
        assert!(matches!(
            module.run(ctx).await,
            Err(PatternError::Cancelled)
        ));
    }

    #[rstest]
    #[case("outbox")]
    fn test_outbox_pattern_type(#[case] expected: &str) {
        let module = OutboxModule::new(
            "test",
            "orders",
            Arc::new(MockOutboxStore::new()),
            Arc::new(MockOutboxProducer::new()),
            Arc::new(NoopCheckpointStore),
        );
        assert_eq!(module.pattern_type(), expected);
    }
}
