#![cfg(feature = "integration")]

mod common;

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tracing::{Subscriber, span};
use tracing_subscriber::{Layer, layer::Context, layer::SubscriberExt, registry::LookupSpan};
use triad_core::{
    error::{CheckpointError, PatternError},
    traits::{CheckpointRow, CheckpointStore, PatternModule, RunContext},
    types::{PatternName, PipelineName},
};
use triad_runner::patterns::outbox::{OutboxModule, OutboxProducer, OutboxRow, OutboxStore};

// ---------------------------------------------------------------------------
// Noop stubs
// ---------------------------------------------------------------------------

struct NoopOutboxStore;

#[async_trait]
impl OutboxStore for NoopOutboxStore {
    async fn fetch_pending(&self, _batch_size: i64) -> Result<Vec<OutboxRow>, PatternError> {
        Ok(vec![])
    }
    async fn mark_published(&self, _ids: &[i64]) -> Result<(), PatternError> {
        Ok(())
    }
    async fn delete_expired(&self, _retention_secs: i64) -> Result<u64, PatternError> {
        Ok(0)
    }
}

struct NoopOutboxProducer;

#[async_trait]
impl OutboxProducer for NoopOutboxProducer {
    async fn send_transactional(
        &self,
        _topic: &str,
        _key: &[u8],
        _payload: &[u8],
    ) -> Result<(), PatternError> {
        Ok(())
    }
}

struct NoopCheckpointStore;

#[async_trait]
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

// ---------------------------------------------------------------------------
// Custom Layer: captures field names from every new span
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct SpanFieldRecorder {
    // Map of span name → set of field names seen
    entries: Arc<Mutex<Vec<(String, HashSet<String>)>>>,
}

impl SpanFieldRecorder {
    fn snapshot(&self) -> Vec<(String, HashSet<String>)> {
        self.entries.lock().unwrap().clone()
    }
}

struct FieldVisitor(HashSet<String>);

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {
        self.record_str(field, "");
    }
    fn record_str(&mut self, field: &tracing::field::Field, _value: &str) {
        self.0.insert(field.name().to_string());
    }
    fn record_i64(&mut self, field: &tracing::field::Field, _value: i64) {
        self.0.insert(field.name().to_string());
    }
    fn record_u64(&mut self, field: &tracing::field::Field, _value: u64) {
        self.0.insert(field.name().to_string());
    }
    fn record_bool(&mut self, field: &tracing::field::Field, _value: bool) {
        self.0.insert(field.name().to_string());
    }
}

impl<S> Layer<S> for SpanFieldRecorder
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, _id: &span::Id, _ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor(HashSet::new());
        attrs.record(&mut visitor);
        let span_name = attrs.metadata().name().to_string();
        self.entries.lock().unwrap().push((span_name, visitor.0));
    }
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_spans_outbox_cycle_has_required_attributes() {
    let recorder = SpanFieldRecorder::default();
    let subscriber = tracing_subscriber::registry().with(recorder.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let mut module = OutboxModule {
        name: "outbox-test".to_string(),
        kafka_topic: "orders".to_string(),
        poll_interval: Duration::from_millis(5),
        batch_size: 10,
        retention_secs: None,
        store: Arc::new(NoopOutboxStore),
        producer: Arc::new(NoopOutboxProducer),
        checkpoint: Arc::new(NoopCheckpointStore),
    };

    let cancel = CancellationToken::new();
    let ctx = RunContext {
        cancel: cancel.clone(),
        pattern: PatternName("outbox-test".to_string()),
        pipeline: PipelineName("main".to_string()),
    };

    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel_clone.cancel();
    });

    let _ = module.run(ctx).await;

    let spans = recorder.snapshot();
    assert!(!spans.is_empty(), "no spans were captured");

    let relay_spans: Vec<_> = spans
        .iter()
        .filter(|(name, _)| name.contains("relay_batch"))
        .collect();
    assert!(
        !relay_spans.is_empty(),
        "no relay_batch spans captured — check #[instrument] on relay_batch()"
    );

    for (span_name, fields) in &relay_spans {
        assert!(
            fields.contains("pattern_name"),
            "span '{span_name}' missing pattern_name field"
        );
        assert!(
            fields.contains("pipeline_name"),
            "span '{span_name}' missing pipeline_name field"
        );
    }
}
