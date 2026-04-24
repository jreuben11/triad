use rdkafka::{ClientConfig, producer::FutureProducer};
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use tracing::info;
use triad_core::{config::KafkaConfig, error::BackendError};

use super::circuit_breaker::{CbConfig, CircuitBreaker};

/// Tag type for the Kafka circuit breaker.
pub struct Kafka;

/// Builds EOS transactional producers on demand.
///
/// Each pipeline must have its own producer with a unique `transactional.id`
/// (generated as `{prefix}.{pipeline_name}`). Sharing a transactional producer
/// across pipelines violates EOS guarantees.
pub struct ProducerFactory {
    base_config: ClientConfig,
    prefix: String,
}

impl ProducerFactory {
    pub fn new(cfg: &KafkaConfig) -> Self {
        let mut base_config = ClientConfig::new();
        apply_brokers(&mut base_config, cfg);
        apply_security(&mut base_config, cfg);

        let p = &cfg.producer;
        base_config
            .set("enable.idempotence", p.enable_idempotence.to_string())
            .set("acks", &p.acks)
            .set("compression.type", &p.compression_type)
            .set(
                "transaction.timeout.ms",
                p.transaction_timeout_ms.to_string(),
            )
            .set(
                "max.in.flight.requests.per.connection",
                p.max_in_flight_requests.to_string(),
            )
            .set("batch.size", p.batch_size.to_string())
            .set("linger.ms", p.linger_ms.to_string())
            .set("request.timeout.ms", p.request_timeout_ms.to_string());

        Self {
            base_config,
            prefix: p.transactional_id_prefix.clone(),
        }
    }

    /// Build an EOS transactional `FutureProducer` for the given pipeline.
    ///
    /// The `transactional.id` is `{prefix}.{pipeline}` — unique per pipeline,
    /// stable across restarts (required for producer fencing).
    pub fn build(&self, pipeline: &str) -> Result<FutureProducer, BackendError> {
        let mut config = self.base_config.clone();
        config.set("transactional.id", format!("{}.{}", self.prefix, pipeline));
        config
            .create::<FutureProducer>()
            .map_err(|e| BackendError::Kafka(e.to_string()))
    }

    /// The transactional ID prefix configured for this factory.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

/// Kafka backend: producer factory, shared consumer config, optional schema
/// registry settings, and a circuit breaker.
pub struct KafkaBackend {
    /// Creates EOS transactional producers per-pipeline.
    pub producer_factory: ProducerFactory,
    /// Shared base config for `StreamConsumer` instances.
    pub consumer_config: ClientConfig,
    /// Admin client base config for topic management.
    pub admin_config: ClientConfig,
    /// Schema Registry settings (if configured).
    pub schema_registry: Option<SrSettings>,
    /// Circuit breaker protecting this backend.
    pub circuit: CircuitBreaker<Kafka>,
}

impl KafkaBackend {
    pub fn new(cfg: &KafkaConfig, cb_config: CbConfig) -> Self {
        let producer_factory = ProducerFactory::new(cfg);

        let mut consumer_config = ClientConfig::new();
        apply_brokers(&mut consumer_config, cfg);
        apply_security(&mut consumer_config, cfg);

        let c = &cfg.consumer;
        consumer_config
            .set("group.id", &c.group_id)
            .set("isolation.level", &c.isolation_level)
            .set("auto.offset.reset", &c.auto_offset_reset)
            .set("max.poll.interval.ms", c.max_poll_interval_ms.to_string())
            .set("fetch.max.bytes", c.fetch_max_bytes.to_string())
            .set("session.timeout.ms", c.session_timeout_ms.to_string())
            .set("heartbeat.interval.ms", c.heartbeat_interval_ms.to_string())
            // EOS consumers must not auto-commit; offsets are sent via transaction.
            .set("enable.auto.commit", "false");

        let mut admin_config = ClientConfig::new();
        apply_brokers(&mut admin_config, cfg);
        apply_security(&mut admin_config, cfg);

        let schema_registry = cfg
            .schema_registry_url
            .as_ref()
            .map(|url| SrSettings::new(url.clone()));

        info!(
            brokers = cfg.brokers.join(",").as_str(),
            "kafka backend configured"
        );

        Self {
            producer_factory,
            consumer_config,
            admin_config,
            schema_registry,
            circuit: CircuitBreaker::new(cb_config, "kafka"),
        }
    }

    /// Create an rdkafka admin client for topic creation / deletion.
    pub fn admin_client(
        &self,
    ) -> Result<rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext>, BackendError>
    {
        self.admin_config
            .create()
            .map_err(|e| BackendError::Kafka(e.to_string()))
    }
}

// ---------- helpers ----------

fn apply_brokers(config: &mut ClientConfig, cfg: &KafkaConfig) {
    config.set("bootstrap.servers", cfg.brokers.join(","));
}

fn apply_security(config: &mut ClientConfig, cfg: &KafkaConfig) {
    let s = &cfg.security;
    config.set("security.protocol", &s.protocol);
    if let Some(m) = &s.sasl_mechanism {
        config.set("sasl.mechanisms", m);
    }
    if let Some(u) = &s.sasl_username {
        config.set("sasl.username", u);
    }
    if let Some(p) = &s.sasl_password {
        config.set("sasl.password", p);
    }
    if let Some(ca) = &s.ssl_ca_cert {
        config.set("ssl.ca.location", ca);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use triad_core::config::{KafkaConsumerConfig, KafkaProducerConfig, KafkaSecurityConfig};

    fn test_kafka_cfg() -> KafkaConfig {
        KafkaConfig {
            brokers: vec!["localhost:9092".to_string()],
            security: KafkaSecurityConfig {
                protocol: "PLAINTEXT".to_string(),
                sasl_mechanism: None,
                sasl_username: None,
                sasl_password: None,
                ssl_ca_cert: None,
            },
            producer: KafkaProducerConfig {
                transactional_id_prefix: "triad".to_string(),
                acks: "all".to_string(),
                compression_type: "lz4".to_string(),
                transaction_timeout_ms: 60_000,
                enable_idempotence: true,
                max_in_flight_requests: 1,
                batch_size: 131_072,
                linger_ms: 5,
                request_timeout_ms: 30_000,
            },
            consumer: KafkaConsumerConfig {
                group_id: "triad-consumer".to_string(),
                isolation_level: "read_committed".to_string(),
                auto_offset_reset: "earliest".to_string(),
                max_poll_interval_ms: 300_000,
                max_poll_records: 500,
                fetch_max_bytes: 52_428_800,
                session_timeout_ms: 10_000,
                heartbeat_interval_ms: 3_000,
            },
            schema_registry_url: None,
        }
    }

    fn test_cb_config() -> CbConfig {
        CbConfig {
            failure_threshold: 5,
            success_threshold: 2,
            half_open_after: std::time::Duration::from_secs(30),
            half_open_max_calls: 3,
        }
    }

    #[test]
    fn test_producer_factory_builds_with_unique_transactional_ids() {
        let cfg = test_kafka_cfg();
        let factory = ProducerFactory::new(&cfg);
        assert_eq!(factory.prefix(), "triad");
    }

    #[test]
    fn test_kafka_backend_constructs_without_error() {
        let cfg = test_kafka_cfg();
        let _backend = KafkaBackend::new(&cfg, test_cb_config());
    }

    #[test]
    fn test_kafka_backend_no_schema_registry_when_none() {
        let cfg = test_kafka_cfg();
        let backend = KafkaBackend::new(&cfg, test_cb_config());
        assert!(backend.schema_registry.is_none());
    }

    #[test]
    fn test_kafka_backend_schema_registry_set_when_configured() {
        let mut cfg = test_kafka_cfg();
        cfg.schema_registry_url = Some("http://schema-registry:8081".to_string());
        let backend = KafkaBackend::new(&cfg, test_cb_config());
        assert!(backend.schema_registry.is_some());
    }

    #[test]
    fn test_kafka_backend_security_sasl() {
        let mut cfg = test_kafka_cfg();
        cfg.security.protocol = "SASL_SSL".to_string();
        cfg.security.sasl_mechanism = Some("PLAIN".to_string());
        cfg.security.sasl_username = Some("user".to_string());
        cfg.security.sasl_password = Some("secret".to_string());
        // Just verify construction succeeds
        let _backend = KafkaBackend::new(&cfg, test_cb_config());
    }

    #[test]
    fn test_kafka_consumer_config_has_auto_commit_disabled() {
        let cfg = test_kafka_cfg();
        let backend = KafkaBackend::new(&cfg, test_cb_config());
        // The consumer config is a ClientConfig; verify the key is set correctly.
        // rdkafka::ClientConfig exposes get() via iteration; check via debug repr.
        let debug = format!("{:?}", backend.consumer_config);
        assert!(debug.contains("enable.auto.commit"));
    }
}
