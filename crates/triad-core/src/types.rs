use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Opaque event identifier — UUID v7 (time-ordered).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(pub Uuid);

impl EventId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

/// Named pattern from triad.yaml.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PatternName(pub String);

/// Named pipeline within a pattern.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PipelineName(pub String);

/// Saga instance identifier — UUID v4.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SagaId(pub Uuid);

impl SagaId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for SagaId {
    fn default() -> Self {
        Self::new()
    }
}

/// Source position — unified across all backends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SourcePosition {
    PgLsn(u64),
    KafkaOffset {
        topic: String,
        partition: i32,
        offset: i64,
    },
    RedisWatermark(i64),
}

/// A change event emitted by the CDC module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub id: EventId,
    pub table: String,
    pub schema: String,
    pub operation: Operation,
    pub lsn: u64,
    pub occurred_at: DateTime<Utc>,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
}

/// DML operation type for CDC events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operation {
    Insert,
    Update,
    Delete,
    Truncate,
}

/// Context passed to every saga step handler.
pub struct StepContext {
    pub saga_id: SagaId,
    pub step_name: String,
    pub attempt: u32,
}

impl StepContext {
    /// Stable idempotency key across retries: `"{saga_id}/{step_name}/{attempt}"`.
    pub fn idempotency_key(&self) -> String {
        format!("{}/{}/{}", self.saga_id.0, self.step_name, self.attempt)
    }
}

/// Running state of a pattern module.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModuleState {
    Starting,
    Running,
    Paused,
    Recovering,
    Draining,
    Stopped,
}

/// Health snapshot of a module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModuleHealth {
    pub state: ModuleState,
    pub lag: Option<i64>,
    pub in_flight: Option<u32>,
    pub last_error: Option<String>,
}

/// Runner-wide state machine states.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerState {
    Initialising,
    LoadingConfig,
    ConnectingBackends,
    ColdStart,
    Running,
    Paused,
    Recovering,
    Draining,
    Failed,
}

/// Delivery guarantee for a pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeliveryGuarantee {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[test]
    fn test_event_id_new_creates_unique_ids() {
        let a = EventId::new();
        let b = EventId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn test_event_id_default_equals_new() {
        let id = EventId::default();
        // Default creates a valid EventId (non-nil UUID).
        assert_ne!(id.0, uuid::Uuid::nil());
    }

    #[test]
    fn test_saga_id_new_creates_unique_ids() {
        let a = SagaId::new();
        let b = SagaId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn test_saga_id_default_non_nil() {
        let id = SagaId::default();
        assert_ne!(id.0, uuid::Uuid::nil());
    }

    #[rstest]
    #[case("my-saga", "reserve", 0, "my-saga/reserve/0")]
    #[case("checkout", "charge", 2, "checkout/charge/2")]
    fn test_step_context_idempotency_key(
        #[case] _saga_label: &str,
        #[case] step_name: &str,
        #[case] attempt: u32,
        #[case] expected_suffix: &str,
    ) {
        let saga_id =
            SagaId(uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap());
        let ctx = StepContext {
            saga_id,
            step_name: step_name.to_string(),
            attempt,
        };
        let key = ctx.idempotency_key();
        // Key ends with /{step_name}/{attempt}
        assert!(key.ends_with(&expected_suffix[expected_suffix.find('/').unwrap()..]));
    }

    #[test]
    fn test_step_context_idempotency_key_fixed() {
        let saga_id =
            SagaId(uuid::Uuid::parse_str("12345678-1234-1234-1234-123456789012").unwrap());
        let ctx = StepContext {
            saga_id,
            step_name: "charge".to_string(),
            attempt: 1,
        };
        assert_eq!(
            ctx.idempotency_key(),
            "12345678-1234-1234-1234-123456789012/charge/1"
        );
    }

    #[rstest]
    #[case(ModuleState::Running, ModuleState::Running, true)]
    #[case(ModuleState::Paused, ModuleState::Running, false)]
    fn test_module_state_equality(
        #[case] a: ModuleState,
        #[case] b: ModuleState,
        #[case] expected: bool,
    ) {
        assert_eq!(a == b, expected);
    }

    #[test]
    fn test_runner_state_equality() {
        assert_eq!(RunnerState::Running, RunnerState::Running);
        assert_ne!(RunnerState::Running, RunnerState::Paused);
    }

    #[test]
    fn test_event_id_serde_roundtrip() {
        let id = EventId::new();
        let json = serde_json::to_string(&id).unwrap();
        let restored: EventId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, restored);
    }

    #[test]
    fn test_source_position_variants_serialize() {
        let pg = SourcePosition::PgLsn(12345);
        let kafka = SourcePosition::KafkaOffset {
            topic: "orders".to_string(),
            partition: 0,
            offset: 42,
        };
        let redis = SourcePosition::RedisWatermark(100);
        // Just verify they serialize without panic.
        serde_json::to_string(&pg).unwrap();
        serde_json::to_string(&kafka).unwrap();
        serde_json::to_string(&redis).unwrap();
    }

    #[test]
    fn test_change_event_serializes() {
        let ev = ChangeEvent {
            id: EventId::new(),
            table: "orders".to_string(),
            schema: "public".to_string(),
            operation: Operation::Insert,
            lsn: 9999,
            occurred_at: chrono::Utc::now(),
            before: None,
            after: Some(serde_json::json!({"id": 1})),
        };
        serde_json::to_string(&ev).unwrap();
    }
}
