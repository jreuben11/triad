use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use metrics::counter;
use tracing::{error, info, instrument, warn};
use triad_core::{
    error::PatternError,
    metrics::names,
    traits::{PatternModule, RunContext},
    types::{ModuleHealth, ModuleState, SagaId, StepContext},
};

/// Application handler invoked for each deduplicated inbox message.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait InboxHandler: Send + Sync {
    async fn handle(&self, ctx: &StepContext, payload: &[u8]) -> Result<(), PatternError>;
}

/// Narrow trait for inbox deduplication and storage — mockable in unit tests.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait InboxStore: Send + Sync {
    /// Redis fast-path: SET NX with TTL. Returns true if this is a new message.
    async fn redis_dedup_nx(&self, event_id: &str, ttl_secs: u64) -> Result<bool, PatternError>;

    /// PG fallback: check `triad_inbox` for event_id.
    /// Called only when Redis CB is open — always inside a PG transaction.
    async fn pg_dedup_check(&self, event_id: &str) -> Result<bool, PatternError>;

    /// Insert into `triad_inbox` AND invoke the handler in the same PG transaction.
    /// This is the safety invariant: handler and INSERT must be atomic.
    async fn store_and_handle(
        &self,
        event_id: &str,
        pattern_name: &str,
        payload: &[u8],
        ctx: &StepContext,
        handler: &Arc<dyn InboxHandler>,
    ) -> Result<(), PatternError>;

    /// Whether the Redis circuit breaker is currently open.
    fn redis_cb_open(&self) -> bool;
}

/// Narrow trait for consuming Kafka messages in the inbox pattern — mockable.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait InboxConsumer: Send + Sync {
    async fn poll(&self) -> Result<Option<InboxMessage>, PatternError>;
    async fn commit(&self, partition: i32, offset: i64) -> Result<(), PatternError>;
}

/// A Kafka message received by the inbox module.
#[derive(Debug, Clone)]
pub struct InboxMessage {
    pub event_id: String,
    pub payload: Vec<u8>,
    pub partition: i32,
    pub offset: i64,
}

/// Inbox deduplication pattern module.
///
/// Consumes from a Kafka topic, deduplicates via Redis (fast path) or PG
/// (when Redis CB is open), then invokes the application handler inside the
/// same PG transaction as the inbox INSERT.
///
/// Safety invariants enforced:
/// - Inbox INSERT and handler.handle() happen inside the same PG transaction.
/// - When Redis CB is open, PG SELECT fallback is used — dedup is NEVER skipped.
pub struct InboxModule {
    pub name: String,
    pub pattern_name: String,
    pub dedup_window_secs: u64,
    pub consumer: Arc<dyn InboxConsumer>,
    pub store: Arc<dyn InboxStore>,
    pub handler: Arc<dyn InboxHandler>,
}

impl InboxModule {
    pub fn new(
        name: impl Into<String>,
        pattern_name: impl Into<String>,
        consumer: Arc<dyn InboxConsumer>,
        store: Arc<dyn InboxStore>,
        handler: Arc<dyn InboxHandler>,
    ) -> Self {
        Self {
            name: name.into(),
            pattern_name: pattern_name.into(),
            dedup_window_secs: 86400,
            consumer,
            store,
            handler,
        }
    }

    #[instrument(skip(self, msg), fields(event_id = %msg.event_id, pattern_name = %self.name))]
    async fn process_message(&self, msg: &InboxMessage) -> Result<(), PatternError> {
        let ctx = StepContext {
            saga_id: SagaId::new(),
            step_name: self.pattern_name.clone(),
            attempt: 0,
        };

        if self.store.redis_cb_open() {
            // Redis CB open: skip NX, fall back to PG SELECT — dedup is never skipped.
            warn!(event_id = %msg.event_id, "Redis CB open — falling back to PG dedup");
            let is_new = self.store.pg_dedup_check(&msg.event_id).await?;
            if !is_new {
                counter!(names::INBOX_DEDUP_TOTAL, "result" => "duplicate_pg").increment(1);
                info!(event_id = %msg.event_id, "inbox duplicate (PG fallback)");
                return Ok(());
            }
        } else {
            // Fast path: Redis NX
            let is_new = self
                .store
                .redis_dedup_nx(&msg.event_id, self.dedup_window_secs)
                .await?;
            if !is_new {
                counter!(names::INBOX_DEDUP_TOTAL, "result" => "duplicate_redis").increment(1);
                info!(event_id = %msg.event_id, "inbox duplicate (Redis fast path)");
                return Ok(());
            }
        }

        // New message: store_and_handle runs handler + INSERT in same PG txn.
        self.store
            .store_and_handle(
                &msg.event_id,
                &self.pattern_name,
                &msg.payload,
                &ctx,
                &self.handler,
            )
            .await?;

        counter!(names::PIPELINE_EVENTS_TOTAL, "pattern_name" => self.name.clone()).increment(1);
        Ok(())
    }
}

#[async_trait]
impl PatternModule for InboxModule {
    fn name(&self) -> &str {
        &self.name
    }

    fn pattern_type(&self) -> &str {
        "inbox"
    }

    async fn run(&mut self, ctx: RunContext) -> Result<(), PatternError> {
        info!(
            pattern_name = %ctx.pattern.0,
            pipeline_name = %ctx.pipeline.0,
            "inbox module started"
        );

        loop {
            tokio::select! {
                _ = ctx.cancel.cancelled() => {
                    info!("inbox module cancelled");
                    return Err(PatternError::Cancelled);
                }
                result = self.consumer.poll() => {
                    match result {
                        Ok(Some(msg)) => {
                            let partition = msg.partition;
                            let offset = msg.offset;
                            if let Err(e) = self.process_message(&msg).await {
                                error!(error = %e, "inbox message processing error");
                                counter!(names::ERROR_TOTAL, "pattern_name" => self.name.clone())
                                    .increment(1);
                                continue;
                            }
                            if let Err(e) = self.consumer.commit(partition, offset).await {
                                error!(error = %e, "inbox offset commit error");
                            }
                        }
                        Ok(None) => {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Err(e) => {
                            error!(error = %e, "inbox consumer poll error");
                            counter!(names::ERROR_TOTAL, "pattern_name" => self.name.clone())
                                .increment(1);
                        }
                    }
                }
            }
        }
    }

    async fn drain(&mut self, _timeout: Duration) -> Result<(), PatternError> {
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
    use triad_core::types::{PatternName, PipelineName};

    fn make_msg(event_id: &str) -> InboxMessage {
        InboxMessage {
            event_id: event_id.to_string(),
            payload: b"test-payload".to_vec(),
            partition: 0,
            offset: 1,
        }
    }

    fn make_module(
        consumer: MockInboxConsumer,
        store: MockInboxStore,
        handler: MockInboxHandler,
    ) -> InboxModule {
        InboxModule::new(
            "test-inbox",
            "test-pattern",
            Arc::new(consumer),
            Arc::new(store),
            Arc::new(handler),
        )
    }

    #[tokio::test]
    async fn test_inbox_redis_fast_path_duplicate_skipped() {
        let consumer = MockInboxConsumer::new();
        let mut store = MockInboxStore::new();
        store.expect_redis_cb_open().returning(|| false);
        store.expect_redis_dedup_nx().returning(|_, _| Ok(false)); // duplicate
        let handler = MockInboxHandler::new();

        let module = make_module(consumer, store, handler);
        let result = module.process_message(&make_msg("evt-001")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_inbox_redis_fast_path_new_message_handled() {
        let consumer = MockInboxConsumer::new();
        let mut store = MockInboxStore::new();
        store.expect_redis_cb_open().returning(|| false);
        store.expect_redis_dedup_nx().returning(|_, _| Ok(true)); // new
        store
            .expect_store_and_handle()
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        let handler = MockInboxHandler::new();

        let module = make_module(consumer, store, handler);
        let result = module.process_message(&make_msg("evt-002")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_inbox_redis_cb_open_falls_back_to_pg_not_skip() {
        let consumer = MockInboxConsumer::new();
        let mut store = MockInboxStore::new();
        // CB is open — must use PG, never Redis NX
        store.expect_redis_cb_open().returning(|| true);
        store.expect_pg_dedup_check().returning(|_| Ok(true)); // new
        store
            .expect_store_and_handle()
            .times(1)
            .returning(|_, _, _, _, _| Ok(()));
        // redis_dedup_nx must NOT be called when CB is open
        store.expect_redis_dedup_nx().never();
        let handler = MockInboxHandler::new();

        let module = make_module(consumer, store, handler);
        let result = module.process_message(&make_msg("evt-003")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_inbox_redis_cb_open_pg_duplicate_skipped() {
        let consumer = MockInboxConsumer::new();
        let mut store = MockInboxStore::new();
        store.expect_redis_cb_open().returning(|| true);
        store.expect_pg_dedup_check().returning(|_| Ok(false)); // duplicate
        store.expect_store_and_handle().never();
        store.expect_redis_dedup_nx().never();
        let handler = MockInboxHandler::new();

        let module = make_module(consumer, store, handler);
        let result = module.process_message(&make_msg("evt-004")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_inbox_run_cancels_on_token() {
        let mut consumer = MockInboxConsumer::new();
        consumer.expect_poll().returning(|| Ok(None));
        let store = MockInboxStore::new();
        let handler = MockInboxHandler::new();

        let mut module = make_module(consumer, store, handler);
        let cancel = CancellationToken::new();
        let ctx = RunContext {
            cancel: cancel.clone(),
            pattern: PatternName("inbox".to_string()),
            pipeline: PipelineName("test".to_string()),
        };
        cancel.cancel();
        let result = module.run(ctx).await;
        assert!(matches!(result, Err(PatternError::Cancelled)));
    }

    #[rstest]
    #[case("inbox")]
    fn test_inbox_pattern_type(#[case] expected: &str) {
        let module = InboxModule::new(
            "test",
            "test",
            Arc::new(MockInboxConsumer::new()),
            Arc::new(MockInboxStore::new()),
            Arc::new(MockInboxHandler::new()),
        );
        assert_eq!(module.pattern_type(), expected);
    }
}
