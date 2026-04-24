use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::ConfigError;

/// Root configuration struct. Loaded from `triad.yaml` with `TRIAD_` env var overlay.
#[derive(Debug, Deserialize, Serialize)]
pub struct TriadConfig {
    pub backends: BackendsConfig,
    pub patterns: Vec<PatternConfig>,
    pub cold_start: ColdStartConfig,
    pub delivery: DeliveryConfig,
    pub observability: ObservabilityConfig,
    pub shutdown: ShutdownConfig,
    pub admin: AdminConfig,
    pub retry: RetryConfig,
    pub circuit_breakers: CircuitBreakerConfig,
}

impl TriadConfig {
    /// Load configuration from a YAML file path, with `TRIAD_` env var overlay.
    /// Env var separator is `__` (e.g. `TRIAD_BACKENDS__POSTGRES__URL`).
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let cfg = config::Config::builder()
            .add_source(config::File::with_name(path))
            .add_source(config::Environment::with_prefix("TRIAD").separator("__"))
            .build()?;
        Ok(cfg.try_deserialize()?)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        Ok(())
    }
}

/// Top-level backends configuration.
#[derive(Debug, Deserialize, Serialize)]
pub struct BackendsConfig {
    pub postgres: PostgresConfig,
    pub kafka: KafkaConfig,
    pub redis: RedisConfig,
}

/// PostgreSQL connection configuration.
#[derive(Debug, Deserialize, Serialize)]
pub struct PostgresConfig {
    pub url: String,
    pub replication_url: Option<String>,
    pub max_connections: u32,
    pub min_idle: Option<u32>,
    pub connection_timeout_ms: u64,
    pub statement_timeout_ms: Option<u64>,
    pub wal_level: Option<String>,
    pub replication_slot: Option<String>,
    pub publication: Option<String>,
    pub ssl_mode: String,
    pub ssl_root_cert: Option<String>,
}

/// Kafka broker + security + producer/consumer configuration.
#[derive(Debug, Deserialize, Serialize)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub security: KafkaSecurityConfig,
    pub producer: KafkaProducerConfig,
    pub consumer: KafkaConsumerConfig,
    pub schema_registry_url: Option<String>,
}

/// Kafka security protocol and credential configuration.
#[derive(Debug, Deserialize, Serialize)]
pub struct KafkaSecurityConfig {
    pub protocol: String,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
    pub ssl_ca_cert: Option<String>,
}

/// Kafka producer tuning — transactional ID prefix, acks, compression, etc.
#[derive(Debug, Deserialize, Serialize)]
pub struct KafkaProducerConfig {
    pub transactional_id_prefix: String,
    pub acks: String,
    pub compression_type: String,
    pub transaction_timeout_ms: u64,
    pub enable_idempotence: bool,
    pub max_in_flight_requests: u32,
    pub batch_size: u32,
    pub linger_ms: u64,
    pub request_timeout_ms: u64,
}

/// Kafka consumer group and polling configuration.
#[derive(Debug, Deserialize, Serialize)]
pub struct KafkaConsumerConfig {
    pub group_id: String,
    pub isolation_level: String,
    pub auto_offset_reset: String,
    pub max_poll_interval_ms: u64,
    pub max_poll_records: u32,
    pub fetch_max_bytes: u32,
    pub session_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
}

/// Redis connection and pool configuration.
#[derive(Debug, Deserialize, Serialize)]
pub struct RedisConfig {
    pub mode: RedisMode,
    pub url: String,
    pub pool_size: u32,
    pub min_idle: Option<u32>,
    pub connection_timeout_ms: u64,
    pub read_timeout_ms: u64,
    pub write_timeout_ms: u64,
    pub max_retries: u32,
    pub tls: Option<RedisTlsConfig>,
}

/// Redis TLS options.
#[derive(Debug, Deserialize, Serialize)]
pub struct RedisTlsConfig {
    pub ca_cert: Option<String>,
    pub insecure: bool,
}

/// Redis topology mode.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RedisMode {
    Standalone,
    Sentinel,
    Cluster,
}

/// Retry back-off policy.
#[derive(Debug, Deserialize, Serialize)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub multiplier: f64,
}

/// Circuit-breaker thresholds.
#[derive(Debug, Deserialize, Serialize)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: u32,
    pub success_threshold: u32,
    pub timeout_ms: u64,
    pub half_open_max_calls: u32,
}

/// Per-pattern configuration. Discriminated by `type:` field in YAML.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PatternConfig {
    Outbox(OutboxPatternConfig),
    Inbox(InboxPatternConfig),
    Cdc(CdcPatternConfig),
    Saga(SagaPatternConfig),
    CacheSync(CacheSyncPatternConfig),
    WriteThrough(WriteThroughPatternConfig),
    WriteBehind(WriteBehindPatternConfig),
    Webhook(WebhookPatternConfig),
    FeatureFlag(FeatureFlagPatternConfig),
    RateLimit(RateLimitPatternConfig),
    FeatureStore(FeatureStorePatternConfig),
    Idempotency(IdempotencyPatternConfig),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct OutboxPatternConfig {
    pub name: String,
    pub table: String,
    pub kafka_topic: String,
    pub outbox_retention: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct InboxPatternConfig {
    pub name: String,
    pub kafka_topic: String,
    pub consumer_group: String,
    pub dedup_window: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CdcPatternConfig {
    pub name: String,
    pub tables: Vec<String>,
    pub kafka_topic_prefix: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SagaPatternConfig {
    pub name: String,
    pub trigger_topic: String,
    pub trigger_event: String,
    pub timeout: String,
    pub steps: Vec<SagaStepConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SagaStepConfig {
    pub name: String,
    pub command_topic: String,
    pub reply_topic: String,
    pub compensation: Option<String>,
    pub timeout: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CacheSyncPatternConfig {
    pub name: String,
    pub source_topic: String,
    pub redis_key_pattern: String,
    pub ttl: Option<u64>,
    pub on_delete: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WriteThroughPatternConfig {
    pub name: String,
    pub redis_key_pattern: String,
    pub ttl: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WriteBehindPatternConfig {
    pub name: String,
    pub redis_key_pattern: String,
    pub flush_interval: String,
    pub pg_table: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WebhookPatternConfig {
    pub name: String,
    pub source_topic: String,
    pub subscription_table: String,
    pub delivery_log_table: String,
    pub signing: String,
    pub max_attempts: u32,
    pub max_delay: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FeatureFlagPatternConfig {
    pub name: String,
    pub table: String,
    pub audit_table: String,
    pub redis_key_pattern: String,
    pub propagation: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RateLimitPatternConfig {
    pub name: String,
    pub algorithm: String,
    pub redis_key_pattern: String,
    pub window: String,
    pub limit: u64,
    pub violation_topic: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FeatureStorePatternConfig {
    pub name: String,
    pub entity: String,
    pub features: Vec<FeatureConfig>,
    pub registry_table: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FeatureConfig {
    pub name: String,
    pub source: FeatureSourceConfig,
    pub redis_key_pattern: String,
    pub ttl: Option<u64>,
    pub offline_table: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FeatureSourceConfig {
    pub r#type: String,
    pub topic: Option<String>,
    pub table: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct IdempotencyPatternConfig {
    pub name: String,
    pub redis_key_pattern: String,
    pub ttl: u64,
    pub pg_backup: bool,
}

/// Cold-start strategy per-pipeline (or global default).
#[derive(Debug, Deserialize, Serialize)]
pub struct ColdStartConfig {
    pub default_strategy: ColdStartStrategy,
    pub overrides: HashMap<String, ColdStartStrategy>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ColdStartStrategy {
    PgSnapshot,
    KafkaReplay,
    DualRead,
}

/// Delivery guarantee defaults.
#[derive(Debug, Deserialize, Serialize)]
pub struct DeliveryConfig {
    pub default_guarantee: String,
    pub kafka_transaction_timeout: String,
    pub idempotency_dedup_window: String,
    pub dlq_topic_template: String,
}

/// Observability configuration (metrics, tracing, audit, alerts).
#[derive(Debug, Deserialize, Serialize)]
pub struct ObservabilityConfig {
    pub metrics: MetricsConfig,
    pub tracing: TracingConfig,
    pub audit: AuditConfig,
    pub alerts: AlertThresholdsConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MetricsConfig {
    pub provider: String,
    pub port: u16,
    pub labels: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TracingConfig {
    pub provider: String,
    pub endpoint: String,
    pub sample_rate: f64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AuditConfig {
    pub topic: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AlertThresholdsConfig {
    pub kafka_lag_threshold: u64,
    pub pg_replication_lag_threshold: String,
    pub redis_memory_threshold: f64,
}

/// Graceful-shutdown configuration.
#[derive(Debug, Deserialize, Serialize)]
pub struct ShutdownConfig {
    pub drain_timeout_seconds: u64,
    pub warn_on_forced_exit: bool,
}

/// Admin HTTP server configuration.
#[derive(Debug, Deserialize, Serialize)]
pub struct AdminConfig {
    pub port: u16,
    pub auth: AdminAuthConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AdminAuthConfig {
    pub r#type: String,
    pub token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    const MINIMAL_YAML: &str = r#"
backends:
  postgres:
    url: "postgres://localhost/testdb"
    max_connections: 5
    connection_timeout_ms: 5000
    ssl_mode: "disable"
  kafka:
    brokers:
      - "localhost:9092"
    security:
      protocol: "PLAINTEXT"
    producer:
      transactional_id_prefix: "triad"
      acks: "all"
      compression_type: "none"
      transaction_timeout_ms: 60000
      enable_idempotence: true
      max_in_flight_requests: 5
      batch_size: 16384
      linger_ms: 0
      request_timeout_ms: 30000
    consumer:
      group_id: "triad-group"
      isolation_level: "read_committed"
      auto_offset_reset: "earliest"
      max_poll_interval_ms: 300000
      max_poll_records: 500
      fetch_max_bytes: 52428800
      session_timeout_ms: 10000
      heartbeat_interval_ms: 3000
  redis:
    mode: "standalone"
    url: "redis://localhost:6379"
    pool_size: 5
    connection_timeout_ms: 5000
    read_timeout_ms: 1000
    write_timeout_ms: 1000
    max_retries: 3

patterns: []

cold_start:
  default_strategy: "pg_snapshot"
  overrides: {}

delivery:
  default_guarantee: "exactly_once"
  kafka_transaction_timeout: "60s"
  idempotency_dedup_window: "24h"
  dlq_topic_template: "triad.dlq.{source_topic}"

observability:
  metrics:
    provider: "prometheus"
    port: 9090
    labels: []
  tracing:
    provider: "otlp"
    endpoint: "http://localhost:4317"
    sample_rate: 1.0
  audit:
    topic: "triad.audit"
  alerts:
    kafka_lag_threshold: 1000
    pg_replication_lag_threshold: "30s"
    redis_memory_threshold: 0.9

shutdown:
  drain_timeout_seconds: 30
  warn_on_forced_exit: true

admin:
  port: 8080
  auth:
    type: "none"

retry:
  max_attempts: 3
  initial_delay_ms: 100
  max_delay_ms: 10000
  multiplier: 2.0

circuit_breakers:
  failure_threshold: 5
  success_threshold: 2
  timeout_ms: 60000
  half_open_max_calls: 3
"#;

    #[test]
    fn test_triad_config_deserializes_from_yaml() {
        let cfg: TriadConfig = serde_yaml::from_str(MINIMAL_YAML).unwrap();
        assert_eq!(cfg.backends.postgres.url, "postgres://localhost/testdb");
        assert_eq!(cfg.backends.postgres.max_connections, 5);
        assert_eq!(cfg.backends.kafka.brokers, vec!["localhost:9092"]);
        assert_eq!(cfg.backends.redis.pool_size, 5);
        assert!(cfg.patterns.is_empty());
        assert_eq!(cfg.admin.port, 8080);
        assert_eq!(cfg.retry.max_attempts, 3);
        assert_eq!(cfg.circuit_breakers.failure_threshold, 5);
        assert_eq!(cfg.shutdown.drain_timeout_seconds, 30);
    }

    #[test]
    fn test_validate_returns_ok() {
        let cfg: TriadConfig = serde_yaml::from_str(MINIMAL_YAML).unwrap();
        assert!(cfg.validate().is_ok());
    }

    #[rstest]
    #[case(
        r#"{"type": "outbox", "name": "orders-out", "table": "outbox", "kafka_topic": "orders"}"#,
        "outbox"
    )]
    #[case(
        r#"{"type": "inbox", "name": "orders-in", "kafka_topic": "orders", "consumer_group": "g1", "dedup_window": "24h"}"#,
        "inbox"
    )]
    #[case(
        r#"{"type": "cdc", "name": "orders-cdc", "tables": ["orders"], "kafka_topic_prefix": "cdc"}"#,
        "cdc"
    )]
    #[case(
        r#"{"type": "idempotency", "name": "idemp", "redis_key_pattern": "idemp:{id}", "ttl": 3600, "pg_backup": false}"#,
        "idempotency"
    )]
    fn test_pattern_config_variants_deserialize(#[case] json: &str, #[case] variant_name: &str) {
        let pc: PatternConfig = serde_json::from_str(json).unwrap();
        let actual = match &pc {
            PatternConfig::Outbox(_) => "outbox",
            PatternConfig::Inbox(_) => "inbox",
            PatternConfig::Cdc(_) => "cdc",
            PatternConfig::Saga(_) => "saga",
            PatternConfig::CacheSync(_) => "cache_sync",
            PatternConfig::WriteThrough(_) => "write_through",
            PatternConfig::WriteBehind(_) => "write_behind",
            PatternConfig::Webhook(_) => "webhook",
            PatternConfig::FeatureFlag(_) => "feature_flag",
            PatternConfig::RateLimit(_) => "rate_limit",
            PatternConfig::FeatureStore(_) => "feature_store",
            PatternConfig::Idempotency(_) => "idempotency",
        };
        assert_eq!(actual, variant_name);
    }

    #[test]
    fn test_redis_mode_deserializes() {
        let m: RedisMode = serde_json::from_str(r#""standalone""#).unwrap();
        assert!(matches!(m, RedisMode::Standalone));
        let m: RedisMode = serde_json::from_str(r#""sentinel""#).unwrap();
        assert!(matches!(m, RedisMode::Sentinel));
        let m: RedisMode = serde_json::from_str(r#""cluster""#).unwrap();
        assert!(matches!(m, RedisMode::Cluster));
    }

    #[test]
    fn test_cold_start_strategy_deserializes() {
        let s: ColdStartStrategy = serde_json::from_str(r#""pg_snapshot""#).unwrap();
        assert!(matches!(s, ColdStartStrategy::PgSnapshot));
        let s: ColdStartStrategy = serde_json::from_str(r#""kafka_replay""#).unwrap();
        assert!(matches!(s, ColdStartStrategy::KafkaReplay));
        let s: ColdStartStrategy = serde_json::from_str(r#""dual_read""#).unwrap();
        assert!(matches!(s, ColdStartStrategy::DualRead));
    }

    #[test]
    fn test_config_with_patterns_deserializes() {
        let yaml = format!(
            "{}\n",
            MINIMAL_YAML.replace(
                "patterns: []",
                r#"patterns:
  - type: outbox
    name: orders-outbox
    table: outbox
    kafka_topic: orders.created"#
            )
        );
        let cfg: TriadConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(cfg.patterns.len(), 1);
        assert!(matches!(&cfg.patterns[0], PatternConfig::Outbox(c) if c.name == "orders-outbox"));
    }

    #[test]
    fn test_postgres_config_fields() {
        let cfg: TriadConfig = serde_yaml::from_str(MINIMAL_YAML).unwrap();
        assert!(cfg.backends.postgres.replication_url.is_none());
        assert!(cfg.backends.postgres.min_idle.is_none());
        assert_eq!(cfg.backends.postgres.ssl_mode, "disable");
    }

    #[test]
    fn test_kafka_producer_fields() {
        let cfg: TriadConfig = serde_yaml::from_str(MINIMAL_YAML).unwrap();
        let p = &cfg.backends.kafka.producer;
        assert_eq!(p.acks, "all");
        assert!(p.enable_idempotence);
        assert_eq!(p.transaction_timeout_ms, 60000);
    }

    #[test]
    fn test_admin_auth_none() {
        let cfg: TriadConfig = serde_yaml::from_str(MINIMAL_YAML).unwrap();
        assert_eq!(cfg.admin.auth.r#type, "none");
        assert!(cfg.admin.auth.token.is_none());
    }
}
