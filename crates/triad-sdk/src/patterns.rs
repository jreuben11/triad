use serde::Serialize;
use tracing::instrument;
use triad_core::{
    config::{OutboxPatternConfig, SagaPatternConfig, SagaStepConfig},
    error::PatternError,
    types::EventId,
};
use triad_runner::patterns::feature_flag::FlagStore;

use std::sync::Arc;

// ── OutboxPublisher ───────────────────────────────────────────────────────────

/// SDK facade for inserting events into `triad_outbox` inside a caller-owned
/// Postgres transaction.
///
/// The row is committed atomically with the caller's business write — exactly
/// the outbox pattern guarantee. The relay module (running in triad-runner)
/// will pick it up and publish it to Kafka.
#[derive(Debug)]
pub struct OutboxPublisher {
    config: OutboxPatternConfig,
}

impl OutboxPublisher {
    pub fn new(config: OutboxPatternConfig) -> Self {
        Self { config }
    }

    /// Insert an event into `triad_outbox` inside the caller's transaction.
    ///
    /// `aggregate_id` is the domain entity identifier (e.g. order ID).
    /// `event_type` is a dot-namespaced string (e.g. `"order.created"`).
    /// `payload` is serialised to JSON before storage.
    ///
    /// Safety invariant: this must be called inside the same Postgres
    /// transaction as the business write — never in a separate transaction.
    #[instrument(skip(self, tx, payload), fields(table = %self.config.table, event_type))]
    pub async fn publish<T: Serialize>(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        aggregate_type: &str,
        aggregate_id: &str,
        event_type: &str,
        payload: &T,
    ) -> Result<EventId, PatternError> {
        let id = EventId::new();
        let payload_json = serde_json::to_value(payload)
            .map_err(|e| PatternError::Deserialisation(e.to_string()))?;
        let topic = &self.config.kafka_topic;
        let table = &self.config.table;

        sqlx::query(&format!(
            "INSERT INTO {table} \
             (event_id, aggregate_type, aggregate_id, event_type, payload, kafka_topic, \
              status, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, 'pending', NOW())"
        ))
        .bind(id.0)
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(event_type)
        .bind(&payload_json)
        .bind(topic)
        .execute(&mut **tx)
        .await
        .map_err(|e| PatternError::Backend(triad_core::error::BackendError::Postgres(e)))?;

        Ok(id)
    }
}

// ── FlagEvaluator ─────────────────────────────────────────────────────────────

/// SDK facade for evaluating a feature flag: Redis hot path with PG fallback.
///
/// This wraps the runner's `FlagStore` trait (which is already Redis+PG-aware)
/// and is distinct from `triad_runner::patterns::FlagEvaluator` (the relay
/// that propagates flag changes from PG to Redis).
pub struct FlagEvaluator {
    store: Arc<dyn FlagStore>,
}

impl FlagEvaluator {
    pub fn new(store: Arc<dyn FlagStore>) -> Self {
        Self { store }
    }

    /// Evaluate whether `flag` is enabled.
    ///
    /// Hot path: read from Redis cache. Fallback: read from PG when the Redis
    /// circuit breaker is open.
    #[instrument(skip(self), fields(flag_name = flag))]
    pub async fn is_enabled(&self, flag: &str) -> Result<bool, PatternError> {
        // Hot path — Redis cache.
        if !self.store.redis_cb_open() {
            if let Some(feature) = self.store.get_from_cache(flag).await? {
                return Ok(evaluate_flag(&feature));
            }
        }

        // Fallback — PG.
        match self.store.load_one(flag).await? {
            Some(feature) => Ok(evaluate_flag(&feature)),
            None => Ok(false),
        }
    }
}

fn evaluate_flag(flag: &triad_runner::patterns::feature_flag::FeatureFlag) -> bool {
    if !flag.enabled {
        return false;
    }
    match flag.rollout_percentage {
        None | Some(100) => true,
        Some(0) => false,
        // Simple percentage check: use the flag name hash for determinism.
        Some(pct) => {
            let hash = flag
                .name
                .bytes()
                .fold(0u32, |acc, b| acc.wrapping_add(b as u32));
            (hash % 100) < pct as u32
        }
    }
}

// ── SagaBuilder ───────────────────────────────────────────────────────────────

/// Fluent builder for assembling a `SagaPatternConfig`.
///
/// ```
/// let config = SagaBuilder::new("order-saga", "orders.commands", "OrderCreate", "30s")
///     .step("reserve-inventory", "inventory.commands", "inventory.replies")
///     .step("charge-payment",    "payments.commands",  "payments.replies")
///     .on_compensation("order.compensate")
///     .build();
/// ```
pub struct SagaBuilder {
    config: SagaPatternConfig,
}

impl SagaBuilder {
    /// Create a new builder.
    ///
    /// `timeout` is a duration string accepted by the runner (e.g. `"30s"`, `"5m"`).
    pub fn new(
        name: impl Into<String>,
        trigger_topic: impl Into<String>,
        trigger_event: impl Into<String>,
        timeout: impl Into<String>,
    ) -> Self {
        Self {
            config: SagaPatternConfig {
                name: name.into(),
                trigger_topic: trigger_topic.into(),
                trigger_event: trigger_event.into(),
                timeout: timeout.into(),
                steps: Vec::new(),
            },
        }
    }

    /// Add a saga step. Steps are executed in the order they are added.
    pub fn step(
        mut self,
        name: impl Into<String>,
        command_topic: impl Into<String>,
        reply_topic: impl Into<String>,
    ) -> Self {
        self.config.steps.push(SagaStepConfig {
            name: name.into(),
            command_topic: command_topic.into(),
            reply_topic: reply_topic.into(),
            compensation: None,
            timeout: None,
        });
        self
    }

    /// Set the compensation topic for the most recently added step.
    ///
    /// Call this immediately after `step()` to associate the compensation
    /// action with that step.
    pub fn with_compensation(mut self, compensation: impl Into<String>) -> Self {
        if let Some(last) = self.config.steps.last_mut() {
            last.compensation = Some(compensation.into());
        }
        self
    }

    /// Set a per-step timeout on the most recently added step.
    pub fn with_step_timeout(mut self, timeout: impl Into<String>) -> Self {
        if let Some(last) = self.config.steps.last_mut() {
            last.timeout = Some(timeout.into());
        }
        self
    }

    /// Consume the builder and return the completed config.
    pub fn build(self) -> SagaPatternConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rstest::rstest;
    use triad_core::error::PatternError;
    use triad_runner::patterns::feature_flag::{FeatureFlag, FlagStore};

    // ── Local test FlagStore (MockFlagStore from runner is not available in
    //    external crates — cfg(test) only generates mocks within that crate) ──

    struct StubFlagStore {
        cb_open: bool,
        cache_result: Option<FeatureFlag>,
        pg_result: Option<FeatureFlag>,
    }

    impl StubFlagStore {
        fn new(
            cb_open: bool,
            cache_result: Option<FeatureFlag>,
            pg_result: Option<FeatureFlag>,
        ) -> Arc<Self> {
            Arc::new(Self {
                cb_open,
                cache_result,
                pg_result,
            })
        }
    }

    #[async_trait]
    impl FlagStore for StubFlagStore {
        async fn load_all(&self) -> Result<Vec<FeatureFlag>, PatternError> {
            Ok(self.pg_result.clone().into_iter().collect())
        }
        async fn load_one(&self, _name: &str) -> Result<Option<FeatureFlag>, PatternError> {
            Ok(self.pg_result.clone())
        }
        async fn push_to_cache(
            &self,
            _flag: &FeatureFlag,
            _ttl_secs: u64,
        ) -> Result<(), PatternError> {
            Ok(())
        }
        async fn get_from_cache(&self, _name: &str) -> Result<Option<FeatureFlag>, PatternError> {
            Ok(self.cache_result.clone())
        }
        fn redis_cb_open(&self) -> bool {
            self.cb_open
        }
    }

    fn make_flag(name: &str, enabled: bool, pct: Option<u8>) -> FeatureFlag {
        FeatureFlag {
            name: name.to_string(),
            enabled,
            rollout_percentage: pct,
            targeting_rules: None,
            updated_at: chrono::Utc::now(),
        }
    }

    // ── SagaBuilder ─────────────────────────────────────────────────────────

    #[test]
    fn test_saga_builder_no_steps() {
        let cfg = SagaBuilder::new("my-saga", "topic", "event", "30s").build();
        assert_eq!(cfg.name, "my-saga");
        assert_eq!(cfg.trigger_topic, "topic");
        assert_eq!(cfg.trigger_event, "event");
        assert_eq!(cfg.timeout, "30s");
        assert!(cfg.steps.is_empty());
    }

    #[test]
    fn test_saga_builder_with_steps() {
        let cfg = SagaBuilder::new("order-saga", "orders", "OrderCreate", "1m")
            .step("reserve", "inventory.cmd", "inventory.reply")
            .step("charge", "payments.cmd", "payments.reply")
            .build();
        assert_eq!(cfg.steps.len(), 2);
        assert_eq!(cfg.steps[0].name, "reserve");
        assert_eq!(cfg.steps[1].name, "charge");
    }

    #[test]
    fn test_saga_builder_with_compensation() {
        let cfg = SagaBuilder::new("saga", "topic", "event", "5m")
            .step("charge", "payments.cmd", "payments.reply")
            .with_compensation("payments.compensate")
            .build();
        assert_eq!(
            cfg.steps[0].compensation,
            Some("payments.compensate".to_string())
        );
    }

    #[test]
    fn test_saga_builder_step_timeout() {
        let cfg = SagaBuilder::new("saga", "topic", "event", "5m")
            .step("step-a", "cmd", "reply")
            .with_step_timeout("10s")
            .build();
        assert_eq!(cfg.steps[0].timeout, Some("10s".to_string()));
    }

    #[rstest]
    #[case("reserve-inventory", "inventory.commands", "inventory.replies")]
    #[case("charge-payment", "payments.commands", "payments.replies")]
    fn test_saga_builder_step_topics(#[case] name: &str, #[case] cmd: &str, #[case] reply: &str) {
        let cfg = SagaBuilder::new("saga", "topic", "event", "30s")
            .step(name, cmd, reply)
            .build();
        assert_eq!(cfg.steps[0].command_topic, cmd);
        assert_eq!(cfg.steps[0].reply_topic, reply);
    }

    // ── FlagEvaluator ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_flag_evaluator_enabled_from_cache() {
        let store = StubFlagStore::new(false, Some(make_flag("my-flag", true, None)), None);
        let ev = FlagEvaluator::new(store);
        assert!(ev.is_enabled("my-flag").await.unwrap());
    }

    #[tokio::test]
    async fn test_flag_evaluator_disabled_from_cache() {
        let store = StubFlagStore::new(false, Some(make_flag("off-flag", false, None)), None);
        let ev = FlagEvaluator::new(store);
        assert!(!ev.is_enabled("off-flag").await.unwrap());
    }

    #[tokio::test]
    async fn test_flag_evaluator_falls_back_to_pg_when_cb_open() {
        let store = StubFlagStore::new(true, None, Some(make_flag("my-flag", true, None)));
        let ev = FlagEvaluator::new(store);
        assert!(ev.is_enabled("my-flag").await.unwrap());
    }

    #[tokio::test]
    async fn test_flag_evaluator_falls_back_to_pg_when_cache_miss() {
        let store = StubFlagStore::new(false, None, Some(make_flag("f", true, None)));
        let ev = FlagEvaluator::new(store);
        assert!(ev.is_enabled("f").await.unwrap());
    }

    #[tokio::test]
    async fn test_flag_evaluator_returns_false_when_not_found() {
        let store = StubFlagStore::new(false, None, None);
        let ev = FlagEvaluator::new(store);
        assert!(!ev.is_enabled("unknown").await.unwrap());
    }

    // ── OutboxPublisher ──────────────────────────────────────────────────────

    #[test]
    fn test_outbox_publisher_new() {
        let cfg = OutboxPatternConfig {
            name: "orders".to_string(),
            table: "triad_outbox".to_string(),
            kafka_topic: "orders.events".to_string(),
            outbox_retention: None,
        };
        let _publisher = OutboxPublisher::new(cfg);
    }
}
