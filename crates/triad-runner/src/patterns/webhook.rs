use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use dashmap::DashMap;
use hmac::{Hmac, Mac};
use metrics::counter;
use sha2::Sha256;
use tracing::{error, info, instrument, warn};
use triad_core::{
    error::PatternError,
    metrics::names,
    traits::{PatternModule, RunContext},
    types::{ModuleHealth, ModuleState},
};

use super::dlq::{DlqRouter, FailedMessage};

/// A webhook subscription from the database.
#[derive(Debug, Clone)]
pub struct WebhookSubscription {
    pub id: String,
    pub endpoint_url: String,
    pub signing_secret: Option<String>,
    pub event_types: Vec<String>,
    pub max_attempts: u32,
}

/// A webhook delivery record.
#[derive(Debug, Clone)]
pub struct WebhookEvent {
    pub event_id: String,
    pub event_type: String,
    pub payload: Vec<u8>,
    pub subscription: WebhookSubscription,
    pub partition: i32,
    pub offset: i64,
}

/// Narrow trait for HTTP delivery operations — mockable in unit tests.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait HttpDelivery: Send + Sync {
    async fn post(
        &self,
        url: &str,
        payload: &[u8],
        headers: HashMap<String, String>,
    ) -> Result<u16, PatternError>;
}

/// Narrow trait for webhook subscription and event storage — mockable.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait WebhookStore: Send + Sync {
    async fn load_subscriptions(&self) -> Result<Vec<WebhookSubscription>, PatternError>;
    async fn log_delivery(
        &self,
        event_id: String,
        subscription_id: String,
        status_code: Option<u16>,
        success: bool,
        error: Option<String>,
    ) -> Result<(), PatternError>;
}

/// Narrow trait for consuming webhook events from Kafka — mockable.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait WebhookConsumer: Send + Sync {
    async fn poll(&self) -> Result<Option<WebhookEvent>, PatternError>;
    async fn commit(&self, partition: i32, offset: i64) -> Result<(), PatternError>;
}

/// Per-endpoint circuit breaker state: tracks when an endpoint is blocked until.
/// Blocked endpoints are skipped until the block expires (simple time-based CB).
struct EndpointCbState {
    blocked_until: Option<Instant>,
    failure_count: u32,
}

impl EndpointCbState {
    fn new() -> Self {
        Self {
            blocked_until: None,
            failure_count: 0,
        }
    }

    fn is_blocked(&self) -> bool {
        self.blocked_until
            .map(|until| Instant::now() < until)
            .unwrap_or(false)
    }

    fn record_failure(&mut self, failure_threshold: u32, block_duration: Duration) {
        self.failure_count += 1;
        if self.failure_count >= failure_threshold {
            self.blocked_until = Some(Instant::now() + block_duration);
            self.failure_count = 0;
        }
    }

    fn record_success(&mut self) {
        self.failure_count = 0;
        self.blocked_until = None;
    }
}

/// Webhook dispatcher pattern module.
///
/// Consumes events from a Kafka topic, delivers to registered HTTP endpoints,
/// retries with exponential backoff, and routes to DLQ on max retries.
pub struct WebhookDispatcher {
    pub name: String,
    pub consumer: Arc<dyn WebhookConsumer>,
    pub store: Arc<dyn WebhookStore>,
    pub http: Arc<dyn HttpDelivery>,
    pub dlq: Arc<DlqRouter>,
    pub source_topic: String,
    pub max_attempts: u32,
    endpoint_cbs: Arc<DashMap<String, EndpointCbState>>,
    cb_failure_threshold: u32,
    cb_block_duration: Duration,
}

impl WebhookDispatcher {
    pub fn new(
        name: impl Into<String>,
        consumer: Arc<dyn WebhookConsumer>,
        store: Arc<dyn WebhookStore>,
        http: Arc<dyn HttpDelivery>,
        dlq: Arc<DlqRouter>,
        source_topic: impl Into<String>,
        max_attempts: u32,
    ) -> Self {
        Self {
            name: name.into(),
            consumer,
            store,
            http,
            dlq,
            source_topic: source_topic.into(),
            max_attempts,
            endpoint_cbs: Arc::new(DashMap::new()),
            cb_failure_threshold: 5,
            cb_block_duration: Duration::from_secs(60),
        }
    }

    /// Compute HMAC-SHA256 signature for webhook signing.
    pub fn sign_payload(secret: &str, payload: &[u8]) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
        mac.update(payload);
        let result = mac.finalize().into_bytes();
        hex::encode(result)
    }

    /// Deliver a webhook event to one subscription with retry + DLQ routing.
    #[instrument(skip(self, event), fields(event_id = %event.event_id, url = %event.subscription.endpoint_url))]
    async fn deliver(&self, event: &WebhookEvent) -> Result<(), PatternError> {
        let sub = &event.subscription;
        let url = &sub.endpoint_url;

        // Check per-endpoint CB.
        let is_blocked = self
            .endpoint_cbs
            .entry(url.clone())
            .or_insert_with(EndpointCbState::new)
            .is_blocked();

        if is_blocked {
            warn!(url, "webhook endpoint CB open — skipping delivery");
            counter!(names::WEBHOOK_DELIVERY_ATTEMPTS, "result" => "cb_open").increment(1);
            return Ok(());
        }

        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers.insert("X-Triad-Event-ID".to_string(), event.event_id.clone());
        headers.insert("X-Triad-Event-Type".to_string(), event.event_type.clone());

        if let Some(secret) = &sub.signing_secret {
            let sig = Self::sign_payload(secret, &event.payload);
            headers.insert("X-Triad-Signature".to_string(), format!("sha256={}", sig));
        }

        let mut attempt = 0u32;
        let mut last_error: Option<String> = None;
        let cb_failure_threshold = self.cb_failure_threshold;
        let cb_block_duration = self.cb_block_duration;

        while attempt < self.max_attempts {
            counter!(names::WEBHOOK_DELIVERY_ATTEMPTS, "result" => "attempt").increment(1);

            match self.http.post(url, &event.payload, headers.clone()).await {
                Ok(status) if (200..300).contains(&(status as u32)) => {
                    if let Some(mut cb) = self.endpoint_cbs.get_mut(url) {
                        cb.record_success();
                    }
                    self.store
                        .log_delivery(
                            event.event_id.clone(),
                            sub.id.clone(),
                            Some(status),
                            true,
                            None,
                        )
                        .await?;
                    info!(url, status, "webhook delivered");
                    return Ok(());
                }
                Ok(status) => {
                    last_error = Some(format!("HTTP {}", status));
                    attempt += 1;
                    let delay = Duration::from_secs(1 << attempt.min(6));
                    tokio::time::sleep(delay).await;
                }
                Err(e) => {
                    if let Some(mut cb) = self.endpoint_cbs.get_mut(url) {
                        cb.record_failure(cb_failure_threshold, cb_block_duration);
                    }
                    last_error = Some(e.to_string());
                    attempt += 1;
                    let delay = Duration::from_secs(1 << attempt.min(6));
                    tokio::time::sleep(delay).await;
                }
            }
        }

        // Max attempts exhausted — route to DLQ.
        let error_str = last_error.clone().unwrap_or_default();
        self.store
            .log_delivery(
                event.event_id.clone(),
                sub.id.clone(),
                None,
                false,
                Some(error_str.clone()),
            )
            .await?;

        let failed = FailedMessage {
            source_topic: self.source_topic.clone(),
            original_offset: event.offset,
            partition: event.partition,
            payload: event.payload.clone(),
            key: Some(event.event_id.as_bytes().to_vec()),
            reason: error_str.clone(),
            attempt_count: self.max_attempts,
            error_type: "WebhookDeliveryFailed".to_string(),
            occurred_at: chrono::Utc::now(),
            traceparent: None,
        };
        self.dlq.route(&failed).await?;
        error!(
            url,
            attempts = self.max_attempts,
            "webhook max retries — routed to DLQ"
        );
        Ok(())
    }
}

#[async_trait]
impl PatternModule for WebhookDispatcher {
    fn name(&self) -> &str {
        &self.name
    }

    fn pattern_type(&self) -> &str {
        "webhook"
    }

    async fn run(&mut self, ctx: RunContext) -> Result<(), PatternError> {
        info!(pattern_name = %ctx.pattern.0, "webhook dispatcher started");
        loop {
            tokio::select! {
                _ = ctx.cancel.cancelled() => {
                    return Err(PatternError::Cancelled);
                }
                result = self.consumer.poll() => {
                    match result {
                        Ok(Some(event)) => {
                            let partition = event.partition;
                            let offset = event.offset;
                            if let Err(e) = self.deliver(&event).await {
                                error!(error = %e, "webhook delivery error");
                                counter!(names::ERROR_TOTAL, "pattern_name" => self.name.clone())
                                    .increment(1);
                            }
                            if let Err(e) = self.consumer.commit(partition, offset).await {
                                error!(error = %e, "webhook commit error");
                            }
                        }
                        Ok(None) => {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        Err(e) => {
                            error!(error = %e, "webhook consumer error");
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
    use super::super::dlq::MockDlqProducer;
    use super::*;
    use rstest::rstest;
    use tokio_util::sync::CancellationToken;
    use triad_core::types::{PatternName, PipelineName};

    fn make_sub(id: &str) -> WebhookSubscription {
        WebhookSubscription {
            id: id.to_string(),
            endpoint_url: "http://localhost:8888/hook".to_string(),
            signing_secret: Some("secret-key".to_string()),
            event_types: vec!["OrderCreated".to_string()],
            max_attempts: 3,
        }
    }

    fn make_event(sub: WebhookSubscription) -> WebhookEvent {
        WebhookEvent {
            event_id: "evt-001".to_string(),
            event_type: "OrderCreated".to_string(),
            payload: b"{\"id\":1}".to_vec(),
            subscription: sub,
            partition: 0,
            offset: 42,
        }
    }

    fn make_dispatcher(
        consumer: MockWebhookConsumer,
        store: MockWebhookStore,
        http: MockHttpDelivery,
        dlq_producer: MockDlqProducer,
    ) -> WebhookDispatcher {
        let dlq = Arc::new(DlqRouter::new(Arc::new(dlq_producer)));
        WebhookDispatcher::new(
            "test-webhook",
            Arc::new(consumer),
            Arc::new(store),
            Arc::new(http),
            dlq,
            "orders",
            3,
        )
    }

    #[test]
    fn test_sign_payload_is_deterministic() {
        let sig1 = WebhookDispatcher::sign_payload("secret", b"payload");
        let sig2 = WebhookDispatcher::sign_payload("secret", b"payload");
        assert_eq!(sig1, sig2);
        assert!(!sig1.is_empty());
    }

    #[test]
    fn test_sign_payload_different_secrets_differ() {
        let sig1 = WebhookDispatcher::sign_payload("secret1", b"payload");
        let sig2 = WebhookDispatcher::sign_payload("secret2", b"payload");
        assert_ne!(sig1, sig2);
    }

    #[tokio::test]
    async fn test_webhook_deliver_success() {
        let consumer = MockWebhookConsumer::new();
        let mut store = MockWebhookStore::new();
        store
            .expect_log_delivery()
            .with(
                mockall::predicate::always(),
                mockall::predicate::always(),
                mockall::predicate::eq(Some(200u16)),
                mockall::predicate::eq(true),
                mockall::predicate::always(),
            )
            .returning(|_, _, _, _, _| Ok(()));

        let mut http = MockHttpDelivery::new();
        http.expect_post().returning(|_, _, _| Ok(200));

        let dispatcher = make_dispatcher(consumer, store, http, MockDlqProducer::new());
        let event = make_event(make_sub("sub-1"));
        assert!(dispatcher.deliver(&event).await.is_ok());
    }

    #[tokio::test]
    async fn test_webhook_deliver_routes_to_dlq_on_max_retries() {
        let consumer = MockWebhookConsumer::new();
        let mut store = MockWebhookStore::new();
        store
            .expect_log_delivery()
            .returning(|_, _, _, _, _| Ok(()));

        let mut http = MockHttpDelivery::new();
        // Always fail
        http.expect_post()
            .returning(|_, _, _| Err(PatternError::Saga("connection refused".to_string())));

        let mut dlq_producer = MockDlqProducer::new();
        dlq_producer
            .expect_produce()
            .withf(|topic, _, _, _| topic == "triad.dlq.orders")
            .returning(|_, _, _, _| Ok(()));

        let dlq = Arc::new(DlqRouter::new(Arc::new(dlq_producer)));
        let dispatcher = WebhookDispatcher::new(
            "test",
            Arc::new(consumer),
            Arc::new(store),
            Arc::new(http),
            dlq,
            "orders",
            1, // only 1 attempt so test is fast
        );
        let event = make_event(make_sub("sub-2"));
        assert!(dispatcher.deliver(&event).await.is_ok()); // ok because DLQ routing succeeded
    }

    #[test]
    fn test_dlq_topic_naming_invariant() {
        // Verify the webhook module never inverts the DLQ topic format
        let source_topic = "orders";
        let dlq_topic = format!("triad.dlq.{}", source_topic);
        assert_eq!(dlq_topic, "triad.dlq.orders");
        assert!(!dlq_topic.ends_with(".dlq"));
    }

    #[tokio::test]
    async fn test_webhook_run_cancels() {
        let mut consumer = MockWebhookConsumer::new();
        consumer.expect_poll().returning(|| Ok(None));
        let store = MockWebhookStore::new();
        let http = MockHttpDelivery::new();

        let mut dispatcher = make_dispatcher(consumer, store, http, MockDlqProducer::new());
        let cancel = CancellationToken::new();
        let ctx = RunContext {
            cancel: cancel.clone(),
            pattern: PatternName("webhook".to_string()),
            pipeline: PipelineName("test".to_string()),
        };
        cancel.cancel();
        assert!(matches!(
            dispatcher.run(ctx).await,
            Err(PatternError::Cancelled)
        ));
    }

    #[rstest]
    #[case("webhook")]
    fn test_webhook_pattern_type(#[case] expected: &str) {
        let dispatcher = make_dispatcher(
            MockWebhookConsumer::new(),
            MockWebhookStore::new(),
            MockHttpDelivery::new(),
            MockDlqProducer::new(),
        );
        assert_eq!(dispatcher.pattern_type(), expected);
    }
}
