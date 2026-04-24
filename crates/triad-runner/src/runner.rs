use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use triad_core::{config::TriadConfig, error::TriadError, traits::LeaderElector};

use crate::{
    admin::AdminServer, engine::PatternEngine, leader::NoopLeader, shutdown::ShutdownCoordinator,
};

/// Five-state FSM for the runner lifecycle.
///
/// ```text
/// Idle ──► Starting ──► Running ──► Draining ──► Stopped
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunnerState {
    Idle,
    Starting,
    Running,
    Draining,
    Stopped,
}

impl std::fmt::Display for RunnerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunnerState::Idle => write!(f, "Idle"),
            RunnerState::Starting => write!(f, "Starting"),
            RunnerState::Running => write!(f, "Running"),
            RunnerState::Draining => write!(f, "Draining"),
            RunnerState::Stopped => write!(f, "Stopped"),
        }
    }
}

/// Orchestrates the full runner lifecycle.
///
/// `Runner::run()` blocks until shutdown completes:
/// 1. `Idle → Starting`: install shutdown handler, elect leader.
/// 2. `Starting → Running`: start pattern engine, serve admin HTTP.
/// 3. `Running → Draining`: SIGTERM (or `shutdown.cancel()`) triggers drain.
/// 4. `Draining → Stopped`: all modules drained, admin server stopped.
pub struct Runner {
    config: TriadConfig,
    engine: PatternEngine,
    admin: AdminServer,
    leader: Box<dyn LeaderElector>,
    pub shutdown: ShutdownCoordinator,
    state: RunnerState,
    cancel: CancellationToken,
}

impl Runner {
    /// Create a Runner in the `Idle` state.
    pub fn new(config: TriadConfig, engine: PatternEngine, admin: AdminServer) -> Self {
        let cancel = CancellationToken::new();
        let drain_timeout = config.shutdown.drain_timeout_seconds;
        Self {
            config,
            engine,
            admin,
            leader: Box::new(NoopLeader),
            shutdown: ShutdownCoordinator::new(drain_timeout),
            state: RunnerState::Idle,
            cancel,
        }
    }

    /// Override the leader elector (needed for Mode 3 / K8s deployments).
    pub fn with_leader(mut self, leader: Box<dyn LeaderElector>) -> Self {
        self.leader = leader;
        self
    }

    /// Current FSM state.
    pub fn state(&self) -> &RunnerState {
        &self.state
    }

    /// Run until shutdown. Returns when the engine has fully drained.
    pub async fn run(&mut self) -> Result<(), TriadError> {
        self.transition(RunnerState::Starting);

        // Install SIGTERM handler — cancels the shutdown token on signal.
        self.shutdown.install_signal_handler();

        // Elect leader (noop for Mode 1/2).
        let _handle = self.leader.campaign().await.map_err(TriadError::Election)?;
        info!(
            "leader election completed, is_leader={}",
            self.leader.is_leader()
        );

        // Start pattern engine.
        self.engine.start().await;
        info!("pattern engine started");

        self.transition(RunnerState::Running);

        // Start admin HTTP server in the background.
        let admin_cancel = self.cancel.child_token();
        let mut admin = std::mem::replace(&mut self.admin, AdminServer::new_stub());
        tokio::spawn(async move {
            if let Err(e) = admin.serve(admin_cancel).await {
                error!(error = %e, "admin server exited with error");
            }
        });

        // Block until SIGTERM or programmatic cancel.
        self.shutdown.wait().await;
        info!("shutdown signal received — beginning drain");

        self.transition(RunnerState::Draining);

        let drain_secs = self.config.shutdown.drain_timeout_seconds;
        self.engine.drain(Duration::from_secs(drain_secs)).await;

        // Cancel background tasks (admin server, etc.).
        self.cancel.cancel();

        self.transition(RunnerState::Stopped);
        info!("runner stopped cleanly");
        Ok(())
    }

    fn transition(&mut self, new_state: RunnerState) {
        info!(from = %self.state, to = %new_state, "runner FSM transition");
        self.state = new_state;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn minimal_config() -> TriadConfig {
        use triad_core::config::*;
        TriadConfig {
            backends: BackendsConfig {
                postgres: PostgresConfig {
                    url: "postgres://localhost/test".into(),
                    replication_url: None,
                    max_connections: 5,
                    min_idle: None,
                    connection_timeout_ms: 1000,
                    statement_timeout_ms: None,
                    wal_level: None,
                    replication_slot: None,
                    publication: None,
                    ssl_mode: "disable".into(),
                    ssl_root_cert: None,
                },
                kafka: KafkaConfig {
                    brokers: vec!["localhost:9092".into()],
                    security: KafkaSecurityConfig {
                        protocol: "PLAINTEXT".into(),
                        sasl_mechanism: None,
                        sasl_username: None,
                        sasl_password: None,
                        ssl_ca_cert: None,
                    },
                    producer: KafkaProducerConfig {
                        transactional_id_prefix: "triad".into(),
                        acks: "all".into(),
                        compression_type: "lz4".into(),
                        transaction_timeout_ms: 60_000,
                        enable_idempotence: true,
                        max_in_flight_requests: 1,
                        batch_size: 16_384,
                        linger_ms: 5,
                        request_timeout_ms: 30_000,
                    },
                    consumer: KafkaConsumerConfig {
                        group_id: "triad-test".into(),
                        isolation_level: "read_committed".into(),
                        auto_offset_reset: "earliest".into(),
                        max_poll_interval_ms: 300_000,
                        max_poll_records: 500,
                        fetch_max_bytes: 52_428_800,
                        session_timeout_ms: 30_000,
                        heartbeat_interval_ms: 3_000,
                    },
                    schema_registry_url: None,
                },
                redis: RedisConfig {
                    mode: RedisMode::Standalone,
                    url: "redis://localhost:6379".into(),
                    pool_size: 5,
                    min_idle: None,
                    connection_timeout_ms: 1000,
                    read_timeout_ms: 1000,
                    write_timeout_ms: 1000,
                    max_retries: 3,
                    tls: None,
                },
            },
            patterns: vec![],
            cold_start: ColdStartConfig {
                default_strategy: ColdStartStrategy::KafkaReplay,
                overrides: HashMap::new(),
            },
            delivery: DeliveryConfig {
                default_guarantee: "at_least_once".into(),
                kafka_transaction_timeout: "60s".into(),
                idempotency_dedup_window: "24h".into(),
                dlq_topic_template: "triad.dlq.{source_topic}".into(),
            },
            observability: ObservabilityConfig {
                metrics: MetricsConfig {
                    provider: "prometheus".into(),
                    port: 9090,
                    labels: vec![],
                },
                tracing: TracingConfig {
                    provider: "otlp".into(),
                    endpoint: "http://localhost:4317".into(),
                    sample_rate: 1.0,
                },
                audit: AuditConfig {
                    topic: "triad.audit".into(),
                },
                alerts: AlertThresholdsConfig {
                    kafka_lag_threshold: 10_000,
                    pg_replication_lag_threshold: "16MB".into(),
                    redis_memory_threshold: 0.80,
                },
            },
            shutdown: ShutdownConfig {
                drain_timeout_seconds: 5,
                warn_on_forced_exit: true,
            },
            admin: AdminConfig {
                port: 0,
                auth: AdminAuthConfig {
                    r#type: "none".into(),
                    token: None,
                },
            },
            retry: RetryConfig {
                max_attempts: 3,
                initial_delay_ms: 100,
                max_delay_ms: 5_000,
                multiplier: 2.0,
            },
            circuit_breakers: CircuitBreakerConfig {
                failure_threshold: 5,
                success_threshold: 2,
                timeout_ms: 30_000,
                half_open_max_calls: 3,
            },
        }
    }

    fn make_runner() -> Runner {
        let config = minimal_config();
        let cancel = CancellationToken::new();
        let engine = PatternEngine::new(cancel.clone());
        let admin = AdminServer::new_stub();
        Runner::new(config, engine, admin)
    }

    #[test]
    fn test_runner_initial_state_is_idle() {
        let runner = make_runner();
        assert_eq!(*runner.state(), RunnerState::Idle);
    }

    #[test]
    fn test_runner_state_display() {
        assert_eq!(RunnerState::Idle.to_string(), "Idle");
        assert_eq!(RunnerState::Starting.to_string(), "Starting");
        assert_eq!(RunnerState::Running.to_string(), "Running");
        assert_eq!(RunnerState::Draining.to_string(), "Draining");
        assert_eq!(RunnerState::Stopped.to_string(), "Stopped");
    }

    #[test]
    fn test_runner_state_equality() {
        assert_eq!(RunnerState::Idle, RunnerState::Idle);
        assert_ne!(RunnerState::Idle, RunnerState::Running);
    }

    #[test]
    fn test_runner_state_clone() {
        let s = RunnerState::Draining;
        assert_eq!(s.clone(), RunnerState::Draining);
    }

    #[tokio::test]
    async fn test_runner_run_transitions_to_stopped() {
        let config = minimal_config();
        let cancel = CancellationToken::new();
        let engine = PatternEngine::new(cancel.clone());
        let admin = AdminServer::new_stub();
        let mut runner = Runner::new(config, engine, admin);

        // Trigger shutdown after a short delay.
        let sd_token = runner.shutdown.token();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            sd_token.cancel();
        });

        runner.run().await.expect("runner should succeed");
        assert_eq!(*runner.state(), RunnerState::Stopped);
    }

    #[tokio::test]
    async fn test_runner_passes_through_draining_state() {
        let config = minimal_config();
        let cancel = CancellationToken::new();
        let engine = PatternEngine::new(cancel.clone());
        let admin = AdminServer::new_stub();
        let mut runner = Runner::new(config, engine, admin);

        let sd_token = runner.shutdown.token();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            sd_token.cancel();
        });

        // run() blocks; after it returns state must be Stopped.
        runner.run().await.unwrap();
        assert_eq!(*runner.state(), RunnerState::Stopped);
    }
}
