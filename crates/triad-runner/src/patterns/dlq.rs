use std::sync::Arc;

use async_trait::async_trait;
use metrics::counter;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};
use triad_core::{error::PatternError, metrics::names};

/// A message that failed processing and should be routed to the DLQ.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedMessage {
    pub source_topic: String,
    pub original_offset: i64,
    pub partition: i32,
    pub payload: Vec<u8>,
    pub key: Option<Vec<u8>>,
    pub reason: String,
    pub attempt_count: u32,
    pub error_type: String,
    pub occurred_at: chrono::DateTime<chrono::Utc>,
    pub traceparent: Option<String>,
}

impl FailedMessage {
    /// Compute the DLQ topic name for this message.
    ///
    /// Invariant: always `triad.dlq.{source_topic}` — never `{source_topic}.dlq`.
    pub fn dlq_topic(&self) -> String {
        format!("triad.dlq.{}", self.source_topic)
    }
}

/// Narrow trait for Kafka produce operations — mockable in unit tests.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait DlqProducer: Send + Sync {
    async fn produce(
        &self,
        topic: String,
        key: Option<Vec<u8>>,
        payload: Vec<u8>,
        headers: Vec<(String, String)>,
    ) -> Result<(), PatternError>;
}

/// Routes failed messages to `triad.dlq.{source_topic}`.
pub struct DlqRouter {
    producer: Arc<dyn DlqProducer>,
}

impl DlqRouter {
    pub fn new(producer: Arc<dyn DlqProducer>) -> Self {
        Self { producer }
    }

    /// Route a failed message to the appropriate DLQ topic.
    ///
    /// Adds standard DLQ metadata headers.
    #[instrument(skip(self, msg), fields(source_topic = %msg.source_topic, attempt = msg.attempt_count))]
    pub async fn route(&self, msg: &FailedMessage) -> Result<(), PatternError> {
        let dlq_topic = msg.dlq_topic();
        let ts = msg.occurred_at.to_rfc3339();
        let attempt_str = msg.attempt_count.to_string();
        let offset_str = msg.original_offset.to_string();

        let mut headers: Vec<(String, String)> = vec![
            ("triad-dlq-reason".to_string(), msg.reason.clone()),
            ("triad-dlq-attempt-count".to_string(), attempt_str.clone()),
            (
                "triad-dlq-original-topic".to_string(),
                msg.source_topic.clone(),
            ),
            ("triad-dlq-original-offset".to_string(), offset_str.clone()),
            ("triad-dlq-error-type".to_string(), msg.error_type.clone()),
            ("triad-dlq-timestamp-iso8601".to_string(), ts.clone()),
        ];

        if let Some(tp) = &msg.traceparent {
            headers.push(("triad-traceparent".to_string(), tp.clone()));
        }

        self.producer
            .produce(
                dlq_topic.clone(),
                msg.key.clone(),
                msg.payload.clone(),
                headers,
            )
            .await?;

        counter!(names::DLQ_MESSAGES_TOTAL, "source_topic" => msg.source_topic.clone())
            .increment(1);
        info!(dlq_topic = %dlq_topic, "message routed to DLQ");
        Ok(())
    }
}

/// Narrow trait for consuming DLQ messages and re-producing to original topic.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait DlqConsumer: Send + Sync {
    async fn consume_dlq(&self, topic: String) -> Result<Vec<DlqMessage>, PatternError>;
    async fn replay_to_original(&self, msg: DlqMessage) -> Result<(), PatternError>;
    async fn commit_offset(
        &self,
        topic: String,
        partition: i32,
        offset: i64,
    ) -> Result<(), PatternError>;
    async fn purge(&self, topic: String) -> Result<u64, PatternError>;
}

/// A message read from a DLQ topic.
#[derive(Debug, Clone)]
pub struct DlqMessage {
    pub dlq_topic: String,
    pub original_topic: String,
    pub payload: Vec<u8>,
    pub key: Option<Vec<u8>>,
    pub partition: i32,
    pub offset: i64,
    pub headers: std::collections::HashMap<String, String>,
}

/// Replays DLQ messages back to their original source topic and can purge DLQ topics.
pub struct DlqReplayer {
    consumer: Arc<dyn DlqConsumer>,
}

impl DlqReplayer {
    pub fn new(consumer: Arc<dyn DlqConsumer>) -> Self {
        Self { consumer }
    }

    /// Replay all messages from `triad.dlq.{topic}` to the original topic.
    #[instrument(skip(self))]
    pub async fn replay(&self, source_topic: &str) -> Result<u64, PatternError> {
        let dlq_topic = format!("triad.dlq.{}", source_topic);
        let messages = self.consumer.consume_dlq(dlq_topic.clone()).await?;
        let count = messages.len() as u64;

        for msg in messages {
            let partition = msg.partition;
            let offset = msg.offset;
            self.consumer.replay_to_original(msg).await?;
            self.consumer
                .commit_offset(dlq_topic.clone(), partition, offset)
                .await?;
        }

        info!(source_topic, count, "DLQ replay complete");
        Ok(count)
    }

    /// Purge all messages from `triad.dlq.{topic}`.
    #[instrument(skip(self))]
    pub async fn purge(&self, source_topic: &str) -> Result<u64, PatternError> {
        let dlq_topic = format!("triad.dlq.{}", source_topic);
        let purged = self.consumer.purge(dlq_topic).await?;
        warn!(source_topic, purged, "DLQ purged");
        Ok(purged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::predicate::*;
    use rstest::rstest;

    fn make_msg(source_topic: &str, attempt: u32) -> FailedMessage {
        FailedMessage {
            source_topic: source_topic.to_string(),
            original_offset: 42,
            partition: 0,
            payload: b"test-payload".to_vec(),
            key: Some(b"test-key".to_vec()),
            reason: "processing failed".to_string(),
            attempt_count: attempt,
            error_type: "PatternError::Saga".to_string(),
            occurred_at: chrono::Utc::now(),
            traceparent: None,
        }
    }

    #[rstest]
    #[case("orders", "triad.dlq.orders")]
    #[case("payments.events", "triad.dlq.payments.events")]
    #[case("my-topic", "triad.dlq.my-topic")]
    fn test_dlq_topic_naming_invariant(#[case] source: &str, #[case] expected: &str) {
        let msg = make_msg(source, 1);
        assert_eq!(msg.dlq_topic(), expected);
        // Verify it never uses the wrong format
        assert!(!msg.dlq_topic().ends_with(&format!("{}.dlq", source)));
    }

    #[tokio::test]
    async fn test_dlq_router_routes_message() {
        let mut producer = MockDlqProducer::new();
        producer
            .expect_produce()
            .withf(|topic, _key, _payload, headers| {
                topic == "triad.dlq.orders"
                    && headers.iter().any(|(k, _)| *k == "triad-dlq-reason")
                    && headers
                        .iter()
                        .any(|(k, _)| *k == "triad-dlq-original-topic")
            })
            .returning(|_, _, _, _| Ok(()));

        let router = DlqRouter::new(Arc::new(producer));
        let msg = make_msg("orders", 3);
        let result = router.route(&msg).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_dlq_router_includes_traceparent_header() {
        let mut producer = MockDlqProducer::new();
        producer
            .expect_produce()
            .withf(|_, _, _, headers| {
                headers
                    .iter()
                    .any(|(k, v)| *k == "triad-traceparent" && *v == "00-abc-01")
            })
            .returning(|_, _, _, _| Ok(()));

        let router = DlqRouter::new(Arc::new(producer));
        let mut msg = make_msg("orders", 1);
        msg.traceparent = Some("00-abc-01".to_string());
        let result = router.route(&msg).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_dlq_router_propagates_producer_error() {
        let mut producer = MockDlqProducer::new();
        producer
            .expect_produce()
            .returning(|_, _, _, _| Err(PatternError::Saga("broker down".to_string())));

        let router = DlqRouter::new(Arc::new(producer));
        let msg = make_msg("orders", 1);
        let result = router.route(&msg).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_dlq_replayer_replays_messages() {
        let dlq_msg = DlqMessage {
            dlq_topic: "triad.dlq.orders".to_string(),
            original_topic: "orders".to_string(),
            payload: b"data".to_vec(),
            key: None,
            partition: 0,
            offset: 10,
            headers: Default::default(),
        };
        let cloned = dlq_msg.clone();

        let mut consumer = MockDlqConsumer::new();
        consumer
            .expect_consume_dlq()
            .returning(move |_| Ok(vec![cloned.clone()]));
        consumer
            .expect_replay_to_original()
            .times(1)
            .returning(|_| Ok(()));
        consumer
            .expect_commit_offset()
            .times(1)
            .returning(|_, _, _| Ok(()));

        let replayer = DlqReplayer::new(Arc::new(consumer));
        let result = replayer.replay("orders").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_dlq_replayer_purge() {
        let mut consumer = MockDlqConsumer::new();
        consumer.expect_purge().returning(|_| Ok(5));

        let replayer = DlqReplayer::new(Arc::new(consumer));
        let result = replayer.purge("orders").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 5);
    }
}
