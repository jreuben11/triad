use thiserror::Error;

/// Top-level error type for the Triad library.
#[derive(Error, Debug)]
pub enum TriadError {
    #[error("backend: {0}")]
    Backend(#[from] BackendError),
    #[error("pattern: {0}")]
    Pattern(#[from] PatternError),
    #[error("config: {0}")]
    Config(#[from] ConfigError),
    #[error("shutdown: {0}")]
    Shutdown(#[from] ShutdownError),
    #[error("checkpoint: {0}")]
    Checkpoint(#[from] CheckpointError),
    #[error("election: {0}")]
    Election(#[from] ElectionError),
}

/// Backend-level errors (Postgres, Kafka, Redis, circuit breaker, pool).
#[derive(Error, Debug)]
pub enum BackendError {
    #[error("postgres: {0}")]
    Postgres(#[from] sqlx::Error),
    #[error("kafka: {0}")]
    Kafka(String),
    #[error("redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("circuit breaker open for {backend}")]
    CircuitBreakerOpen { backend: String },
    #[error("connection pool exhausted for {backend}")]
    PoolExhausted { backend: String },
}

/// Pattern-level errors (encompasses backend errors plus domain failures).
#[derive(Error, Debug)]
pub enum PatternError {
    #[error("backend: {0}")]
    Backend(#[from] BackendError),
    #[error("deser: {0}")]
    Deserialisation(String),
    #[error("schema: {0}")]
    Schema(String),
    #[error("saga: {0}")]
    Saga(String),
    #[error("drain timeout")]
    DrainTimeout,
    #[error("cancelled")]
    Cancelled,
}

/// Configuration parse or validation errors.
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("parse: {0}")]
    Parse(#[from] config::ConfigError),
    #[error("validation: {0}")]
    Validation(String),
    #[error("missing field: {0}")]
    MissingField(String),
}

/// Shutdown / drain errors.
#[derive(Error, Debug)]
pub enum ShutdownError {
    #[error("drain timed out after {seconds}s")]
    Timeout { seconds: u64 },
}

/// Checkpoint store errors.
#[derive(Error, Debug)]
pub enum CheckpointError {
    #[error("db: {0}")]
    Db(#[from] sqlx::Error),
    #[error("optimistic lock conflict: version mismatch")]
    VersionConflict,
}

/// Leader election errors.
#[derive(Error, Debug)]
pub enum ElectionError {
    #[error("k8s api: {0}")]
    K8sApi(String),
    #[error("lease lost")]
    LeaseLost,
}

/// Type aliases that map the trait-level error to `PatternError`.
pub type SourceError = PatternError;
pub type SinkError = PatternError;
pub type TransformError = PatternError;

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(BackendError::Kafka("broker down".to_string()), "kafka: broker down")]
    #[case(
        BackendError::CircuitBreakerOpen { backend: "postgres".to_string() },
        "circuit breaker open for postgres"
    )]
    #[case(
        BackendError::PoolExhausted { backend: "redis".to_string() },
        "connection pool exhausted for redis"
    )]
    fn test_backend_error_display(#[case] err: BackendError, #[case] expected: &str) {
        assert_eq!(err.to_string(), expected);
    }

    #[rstest]
    #[case(PatternError::Deserialisation("bad json".to_string()), "deser: bad json")]
    #[case(PatternError::Schema("unknown field".to_string()), "schema: unknown field")]
    #[case(PatternError::Saga("step failed".to_string()), "saga: step failed")]
    #[case(PatternError::DrainTimeout, "drain timeout")]
    #[case(PatternError::Cancelled, "cancelled")]
    fn test_pattern_error_display(#[case] err: PatternError, #[case] expected: &str) {
        assert_eq!(err.to_string(), expected);
    }

    #[rstest]
    #[case(ConfigError::Validation("port out of range".to_string()), "validation: port out of range")]
    #[case(ConfigError::MissingField("backends.kafka".to_string()), "missing field: backends.kafka")]
    fn test_config_error_display(#[case] err: ConfigError, #[case] expected: &str) {
        assert_eq!(err.to_string(), expected);
    }

    #[test]
    fn test_shutdown_error_display() {
        let err = ShutdownError::Timeout { seconds: 30 };
        assert_eq!(err.to_string(), "drain timed out after 30s");
    }

    #[test]
    fn test_checkpoint_error_display_version_conflict() {
        let err = CheckpointError::VersionConflict;
        assert_eq!(
            err.to_string(),
            "optimistic lock conflict: version mismatch"
        );
    }

    #[rstest]
    #[case(ElectionError::K8sApi("403 Forbidden".to_string()), "k8s api: 403 Forbidden")]
    #[case(ElectionError::LeaseLost, "lease lost")]
    fn test_election_error_display(#[case] err: ElectionError, #[case] expected: &str) {
        assert_eq!(err.to_string(), expected);
    }

    #[test]
    fn test_backend_error_converts_to_pattern_error() {
        let be = BackendError::Kafka("timeout".to_string());
        let pe: PatternError = be.into();
        assert!(matches!(pe, PatternError::Backend(_)));
    }

    #[test]
    fn test_pattern_error_converts_to_triad_error() {
        let pe = PatternError::DrainTimeout;
        let te: TriadError = pe.into();
        assert!(matches!(te, TriadError::Pattern(_)));
    }

    #[test]
    fn test_config_error_converts_to_triad_error() {
        let ce = ConfigError::Validation("bad".to_string());
        let te: TriadError = ce.into();
        assert!(matches!(te, TriadError::Config(_)));
    }
}
