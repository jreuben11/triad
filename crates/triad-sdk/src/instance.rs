use std::{sync::Arc, time::Duration};

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use triad_core::{
    config::TriadConfig,
    error::{PatternError, ShutdownError, TriadError},
};
use triad_runner::backends::{PgBackend, RedisBackend, circuit_breaker::CbConfig};

/// Handles to backends and the shutdown signal, bundled for SDK consumers.
pub struct InstanceHandles {
    pub pg: Arc<PgBackend>,
    pub redis: Arc<RedisBackend>,
    pub cancel: CancellationToken,
}

/// Mode 1 in-process embedding of the Triad pattern engine.
///
/// Connects backends on `start()`, exposes shared handles, and supervises
/// any registered tasks via an internal `JoinSet`. Call `shutdown()` from
/// your SIGTERM handler to drain gracefully.
pub struct TriadInstance {
    pg: Arc<PgBackend>,
    redis: Arc<RedisBackend>,
    cancel: CancellationToken,
    tasks: JoinSet<Result<(), PatternError>>,
}

impl TriadInstance {
    /// Connect backends and return a running instance.
    ///
    /// Pattern modules are not started automatically — they are registered
    /// separately via builder APIs and spawned into the internal task set.
    pub async fn start(config: &TriadConfig) -> Result<Self, TriadError> {
        let cb_config = CbConfig::from(&config.circuit_breakers);

        info!("triad-sdk: connecting postgres backend");
        let pg = PgBackend::new(&config.backends.postgres, cb_config.clone())
            .await
            .map_err(TriadError::Backend)?;

        info!("triad-sdk: connecting redis backend");
        let redis =
            RedisBackend::new(&config.backends.redis, cb_config).map_err(TriadError::Backend)?;

        info!("triad-sdk: instance started (Mode 1)");
        Ok(Self {
            pg: Arc::new(pg),
            redis: Arc::new(redis),
            cancel: CancellationToken::new(),
            tasks: JoinSet::new(),
        })
    }

    /// Shared handle to the Postgres backend pool.
    pub fn pg(&self) -> Arc<PgBackend> {
        Arc::clone(&self.pg)
    }

    /// Shared handle to the Redis backend.
    pub fn redis(&self) -> Arc<RedisBackend> {
        Arc::clone(&self.redis)
    }

    /// Returns a child cancellation token that callers can use to watch for
    /// shutdown; firing the root token cancels all children.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel.child_token()
    }

    /// Convenience bundle of all shared handles.
    pub fn handles(&self) -> InstanceHandles {
        InstanceHandles {
            pg: self.pg(),
            redis: self.redis(),
            cancel: self.cancel_token(),
        }
    }

    /// Signal all tasks to stop and wait up to `timeout` for them to finish.
    ///
    /// Returns `ShutdownError::Timeout` if tasks do not finish in time;
    /// individual task errors are logged but not re-raised.
    pub async fn shutdown(mut self, timeout: Duration) -> Result<(), ShutdownError> {
        info!(
            timeout_secs = timeout.as_secs(),
            "triad-sdk: shutdown initiated"
        );

        self.cancel.cancel();

        let result = tokio::time::timeout(timeout, async {
            while let Some(outcome) = self.tasks.join_next().await {
                match outcome {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => error!(error = %e, "task exited with error during drain"),
                    Err(e) => error!(error = %e, "task panicked during drain"),
                }
            }
        })
        .await;

        match result {
            Ok(()) => {
                info!("triad-sdk: all tasks drained cleanly");
                Ok(())
            }
            Err(_elapsed) => Err(ShutdownError::Timeout {
                seconds: timeout.as_secs(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;
    use triad_core::{
        config::{RedisConfig, RedisMode},
        error::ShutdownError,
    };
    use triad_runner::backends::circuit_breaker::CircuitBreaker;

    fn make_cb() -> CbConfig {
        CbConfig {
            failure_threshold: 5,
            success_threshold: 2,
            half_open_after: Duration::from_secs(60),
            half_open_max_calls: 3,
        }
    }

    fn make_redis(cb: CbConfig) -> Arc<RedisBackend> {
        let cfg = RedisConfig {
            mode: RedisMode::Standalone,
            url: "redis://localhost:6379".to_string(),
            pool_size: 1,
            min_idle: None,
            connection_timeout_ms: 100,
            read_timeout_ms: 100,
            write_timeout_ms: 100,
            max_retries: 1,
            tls: None,
        };
        Arc::new(RedisBackend::new(&cfg, cb).unwrap())
    }

    fn make_pg(cb: CbConfig) -> Arc<PgBackend> {
        let pool = sqlx::PgPool::connect_lazy("postgres://localhost/triad_test").unwrap();
        let repl: tokio_postgres::Config = "host=localhost dbname=triad_test".parse().unwrap();
        Arc::new(PgBackend {
            pool,
            repl_config: repl,
            circuit: CircuitBreaker::new(cb, "postgres"),
        })
    }

    fn make_instance() -> TriadInstance {
        let cb = make_cb();
        TriadInstance {
            pg: make_pg(cb.clone()),
            redis: make_redis(cb),
            cancel: CancellationToken::new(),
            tasks: JoinSet::new(),
        }
    }

    #[tokio::test]
    async fn test_shutdown_with_no_tasks_succeeds() {
        let instance = make_instance();
        let result = instance.shutdown(Duration::from_millis(100)).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_shutdown_cancels_child_tokens() {
        let instance = make_instance();
        let child = instance.cancel_token();
        assert!(!child.is_cancelled());
        instance.shutdown(Duration::from_millis(50)).await.unwrap();
        assert!(child.is_cancelled());
    }

    #[tokio::test]
    async fn test_shutdown_waits_for_tasks_that_respect_cancellation() {
        let mut instance = make_instance();
        let child = instance.cancel.child_token();
        instance.tasks.spawn(async move {
            child.cancelled().await;
            Ok(())
        });
        let result = instance.shutdown(Duration::from_millis(200)).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_shutdown_error_display() {
        let err = ShutdownError::Timeout { seconds: 30 };
        assert!(err.to_string().contains("30"));
    }

    #[tokio::test]
    async fn test_pg_handle_is_shared() {
        let instance = make_instance();
        let a = instance.pg();
        let b = instance.pg();
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[tokio::test]
    async fn test_redis_handle_is_shared() {
        let instance = make_instance();
        let a = instance.redis();
        let b = instance.redis();
        assert!(Arc::ptr_eq(&a, &b));
    }
}
