use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use metrics::counter;
use tracing::{error, info, instrument, warn};
use triad_core::{
    error::PatternError,
    metrics::names,
    traits::{CheckpointStore, PatternModule, RunContext},
    types::{ChangeEvent, ModuleHealth, ModuleState},
};

/// Narrow trait for WAL replication operations — mockable in unit tests.
///
/// The production implementation connects to PostgreSQL via `tokio-postgres`
/// logical replication (not `sqlx`), sends `START_REPLICATION`, and decodes
/// pgoutput binary messages into `ChangeEvent`s.
///
/// Safety invariant: WAL replication uses `tokio-postgres` replication
/// connection — never the `sqlx` pool. These are separate connection types.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait WalSource: Send + Sync {
    /// Poll the replication slot for the next batch of decoded change events.
    async fn poll_changes(&mut self) -> Result<Vec<ChangeEvent>, PatternError>;

    /// Advance the replication slot LSN to acknowledge processed events.
    async fn advance_lsn(&mut self, lsn: u64) -> Result<(), PatternError>;

    /// Close the replication connection cleanly.
    async fn close(&mut self) -> Result<(), PatternError>;
}

/// Narrow trait for routing CDC change events downstream.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait ChangeEventSink: Send + Sync {
    async fn publish(&self, events: &[ChangeEvent]) -> Result<(), PatternError>;
}

/// Configuration for the CDC module.
#[derive(Debug, Clone)]
pub struct CdcConfig {
    pub tables: Vec<String>,
    pub kafka_topic_prefix: String,
    pub slot_name: String,
    pub publication: String,
    pub poll_interval: Duration,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            tables: vec![],
            kafka_topic_prefix: "triad.cdc".to_string(),
            slot_name: "triad_slot".to_string(),
            publication: "triad_publication".to_string(),
            poll_interval: Duration::from_millis(200),
        }
    }
}

/// CDC (Change Data Capture) pattern module.
///
/// Connects to PostgreSQL via the replication protocol, decodes pgoutput
/// WAL messages into `ChangeEvent`s, and routes them to Kafka.
///
/// Implementation note: pgoutput binary parsing is complex; the production
/// `WalSource` implementation handles protocol-level decoding. The module
/// itself is responsible for lifecycle management, checkpointing, and metrics.
pub struct CdcModule {
    pub name: String,
    pub config: CdcConfig,
    pub source: Box<dyn WalSource>,
    pub sink: Arc<dyn ChangeEventSink>,
    pub checkpoint: Arc<dyn CheckpointStore>,
    last_lsn: u64,
}

impl CdcModule {
    pub fn new(
        name: impl Into<String>,
        config: CdcConfig,
        source: Box<dyn WalSource>,
        sink: Arc<dyn ChangeEventSink>,
        checkpoint: Arc<dyn CheckpointStore>,
    ) -> Self {
        Self {
            name: name.into(),
            config,
            source,
            sink,
            checkpoint,
            last_lsn: 0,
        }
    }

    #[instrument(skip(self), fields(pattern_name = %self.name))]
    async fn process_batch(&mut self) -> Result<usize, PatternError> {
        let events = self.source.poll_changes().await?;
        if events.is_empty() {
            return Ok(0);
        }

        let count = events.len();
        counter!(names::CDC_EVENTS_TOTAL, "pattern_name" => self.name.clone())
            .increment(count as u64);

        // Track the highest LSN in this batch.
        let max_lsn = events.iter().map(|e| e.lsn).max().unwrap_or(0);

        self.sink.publish(&events).await?;

        if max_lsn > self.last_lsn {
            self.source.advance_lsn(max_lsn).await?;
            self.last_lsn = max_lsn;
        }

        Ok(count)
    }
}

#[async_trait]
impl PatternModule for CdcModule {
    fn name(&self) -> &str {
        &self.name
    }

    fn pattern_type(&self) -> &str {
        "cdc"
    }

    async fn run(&mut self, ctx: RunContext) -> Result<(), PatternError> {
        info!(
            pattern_name = %ctx.pattern.0,
            pipeline_name = %ctx.pipeline.0,
            slot = %self.config.slot_name,
            "cdc module started"
        );

        loop {
            tokio::select! {
                _ = ctx.cancel.cancelled() => {
                    info!("cdc module cancelled");
                    let _ = self.source.close().await;
                    return Err(PatternError::Cancelled);
                }
                _ = tokio::time::sleep(self.config.poll_interval) => {
                    match self.process_batch().await {
                        Ok(0) => {}
                        Ok(n) => info!(n, lsn = self.last_lsn, "cdc batch processed"),
                        Err(e) => {
                            error!(error = %e, "cdc batch error");
                            counter!(names::ERROR_TOTAL, "pattern_name" => self.name.clone())
                                .increment(1);
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
                warn!("cdc drain timeout");
                break;
            }
            match self.process_batch().await {
                Ok(0) => break,
                Ok(_) => {}
                Err(e) => {
                    error!(error = %e, "cdc drain error");
                    break;
                }
            }
        }
        let _ = self.source.close().await;
        Ok(())
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
    use rstest::rstest;
    use tokio_util::sync::CancellationToken;
    use triad_core::{
        error::CheckpointError,
        traits::{CheckpointRow, CheckpointStore},
        types::{EventId, Operation, PatternName, PipelineName},
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

    fn make_test_event(lsn: u64) -> ChangeEvent {
        ChangeEvent {
            id: EventId::new(),
            table: "orders".to_string(),
            schema: "public".to_string(),
            operation: Operation::Insert,
            lsn,
            occurred_at: chrono::Utc::now(),
            before: None,
            after: Some(serde_json::json!({"id": lsn})),
        }
    }

    fn make_module(source: MockWalSource, sink: MockChangeEventSink) -> CdcModule {
        CdcModule::new(
            "test-cdc",
            CdcConfig::default(),
            Box::new(source),
            Arc::new(sink),
            Arc::new(NoopCheckpointStore),
        )
    }

    #[tokio::test]
    async fn test_cdc_process_batch_empty_returns_zero() {
        let mut source = MockWalSource::new();
        source.expect_poll_changes().returning(|| Ok(vec![]));
        let sink = MockChangeEventSink::new();

        let mut module = make_module(source, sink);
        assert_eq!(module.process_batch().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_cdc_process_batch_publishes_and_advances_lsn() {
        let mut source = MockWalSource::new();
        source
            .expect_poll_changes()
            .returning(|| Ok(vec![make_test_event(100), make_test_event(200)]));
        source
            .expect_advance_lsn()
            .with(mockall::predicate::eq(200u64))
            .returning(|_| Ok(()));

        let mut sink = MockChangeEventSink::new();
        sink.expect_publish().times(1).returning(|_| Ok(()));

        let mut module = make_module(source, sink);
        let result = module.process_batch().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);
        assert_eq!(module.last_lsn, 200);
    }

    #[tokio::test]
    async fn test_cdc_process_batch_sink_error_propagates() {
        let mut source = MockWalSource::new();
        source
            .expect_poll_changes()
            .returning(|| Ok(vec![make_test_event(1)]));

        let mut sink = MockChangeEventSink::new();
        sink.expect_publish()
            .returning(|_| Err(PatternError::Saga("kafka down".to_string())));

        let mut module = make_module(source, sink);
        assert!(module.process_batch().await.is_err());
    }

    #[tokio::test]
    async fn test_cdc_lsn_only_advances_on_higher_value() {
        let mut source = MockWalSource::new();
        source
            .expect_poll_changes()
            .returning(|| Ok(vec![make_test_event(50)]));
        // advance_lsn should NOT be called since 50 < last_lsn=100
        source.expect_advance_lsn().never();

        let mut sink = MockChangeEventSink::new();
        sink.expect_publish().returning(|_| Ok(()));

        let mut module = make_module(source, sink);
        module.last_lsn = 100;
        let result = module.process_batch().await;
        assert!(result.is_ok());
        assert_eq!(module.last_lsn, 100); // unchanged
    }

    #[tokio::test]
    async fn test_cdc_run_cancels_and_closes_source() {
        let mut source = MockWalSource::new();
        source.expect_poll_changes().returning(|| Ok(vec![]));
        source.expect_close().returning(|| Ok(()));
        let sink = MockChangeEventSink::new();

        let mut module = make_module(source, sink);
        let cancel = CancellationToken::new();
        let ctx = RunContext {
            cancel: cancel.clone(),
            pattern: PatternName("cdc".to_string()),
            pipeline: PipelineName("test".to_string()),
        };
        cancel.cancel();
        assert!(matches!(
            module.run(ctx).await,
            Err(PatternError::Cancelled)
        ));
    }

    #[rstest]
    #[case("cdc")]
    fn test_cdc_pattern_type(#[case] expected: &str) {
        let module = CdcModule::new(
            "test",
            CdcConfig::default(),
            Box::new(MockWalSource::new()),
            Arc::new(MockChangeEventSink::new()),
            Arc::new(NoopCheckpointStore),
        );
        assert_eq!(module.pattern_type(), expected);
    }
}
