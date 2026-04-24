use std::io;

use deadpool_redis::Runtime;
use tracing::info;
use triad_core::{
    config::{RedisConfig, RedisMode},
    error::BackendError,
};

use super::circuit_breaker::{CbConfig, CircuitBreaker};

/// Tag type for the Redis circuit breaker.
pub struct Redis;

/// Dispatch enum for the three supported Redis topology modes.
///
/// Standalone and Cluster use `deadpool-redis` managed pools. Sentinel uses
/// `redis::sentinel::SentinelClient` directly because `deadpool-redis` 0.16
/// does not include a sentinel pool — connections are obtained on demand via
/// `SentinelClient::get_async_master_connection()`.
pub enum RedisPool {
    Standalone(deadpool_redis::Pool),
    Cluster(deadpool_redis::cluster::Pool),
    /// Sentinel-aware client. Obtain connections via `get_async_master_connection()`.
    Sentinel(redis::sentinel::SentinelClient),
}

/// Redis backend: pool dispatch enum plus a circuit breaker.
pub struct RedisBackend {
    pub pool: RedisPool,
    pub circuit: CircuitBreaker<Redis>,
}

impl RedisBackend {
    /// Build the appropriate pool variant from `RedisConfig`.
    ///
    /// Pool construction is synchronous and lazy (no real connection is made
    /// until the first call to `get()` / `get_async_master_connection()`).
    pub fn new(cfg: &RedisConfig, cb_config: CbConfig) -> Result<Self, BackendError> {
        let pool = match cfg.mode {
            RedisMode::Standalone => {
                let config = deadpool_redis::Config {
                    url: Some(cfg.url.clone()),
                    pool: Some(deadpool_redis::PoolConfig {
                        max_size: cfg.pool_size as usize,
                        ..Default::default()
                    }),
                    connection: None,
                };
                let p = config
                    .create_pool(Some(Runtime::Tokio1))
                    .map_err(|e| map_create_err(&e))?;
                RedisPool::Standalone(p)
            }

            RedisMode::Cluster => {
                let config = deadpool_redis::cluster::Config {
                    urls: Some(vec![cfg.url.clone()]),
                    connections: None,
                    pool: Some(deadpool_redis::cluster::PoolConfig {
                        max_size: cfg.pool_size as usize,
                        ..Default::default()
                    }),
                };
                let p = config
                    .create_pool(Some(Runtime::Tokio1))
                    .map_err(|e| map_create_err(&e))?;
                RedisPool::Cluster(p)
            }

            RedisMode::Sentinel => {
                // `SentinelClient::build()` is synchronous — no connection is
                // made here. Consumers call `get_async_master_connection()` to
                // obtain a real connection when needed.
                let client = redis::sentinel::SentinelClient::build(
                    vec![cfg.url.as_str()],
                    sentinel_master_name(&cfg.url),
                    None,
                    redis::sentinel::SentinelServerType::Master,
                )
                .map_err(BackendError::Redis)?;
                RedisPool::Sentinel(client)
            }
        };

        info!(
            mode = mode_name(&pool),
            url = cfg.url.as_str(),
            pool_size = cfg.pool_size,
            "redis pool configured"
        );

        Ok(Self {
            pool,
            circuit: CircuitBreaker::new(cb_config, "redis"),
        })
    }
}

// ---------- helpers ----------

fn mode_name(pool: &RedisPool) -> &'static str {
    match pool {
        RedisPool::Standalone(_) => "standalone",
        RedisPool::Cluster(_) => "cluster",
        RedisPool::Sentinel(_) => "sentinel",
    }
}

/// Extract master name from a Redis URL path component, defaulting to "mymaster".
/// E.g. `redis://host:26379/mymaster` → "mymaster"; `redis://host:26379` → "mymaster".
fn sentinel_master_name(url: &str) -> String {
    // Split on '/', skip scheme and host, take the first non-empty path segment.
    url.split('/')
        .filter(|s| !s.is_empty() && !s.ends_with(':'))
        .nth(1) // index 0 = "host:port", index 1 = path segment
        .filter(|s| !s.is_empty())
        .map(String::from)
        .unwrap_or_else(|| "mymaster".to_string())
}

fn map_create_err(e: &impl std::fmt::Display) -> BackendError {
    BackendError::Redis(redis::RedisError::from(io::Error::other(e.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cb_config() -> CbConfig {
        CbConfig {
            failure_threshold: 5,
            success_threshold: 2,
            half_open_after: std::time::Duration::from_secs(30),
            half_open_max_calls: 3,
        }
    }

    fn base_cfg() -> RedisConfig {
        RedisConfig {
            mode: RedisMode::Standalone,
            url: "redis://localhost:6379".to_string(),
            pool_size: 10,
            min_idle: None,
            connection_timeout_ms: 5000,
            read_timeout_ms: 1000,
            write_timeout_ms: 1000,
            max_retries: 3,
            tls: None,
        }
    }

    #[test]
    fn test_standalone_pool_builds_without_error() {
        let backend = RedisBackend::new(&base_cfg(), cb_config());
        assert!(
            backend.is_ok(),
            "standalone build error: {:?}",
            backend.err()
        );
    }

    #[test]
    fn test_standalone_pool_is_correct_variant() {
        let backend = RedisBackend::new(&base_cfg(), cb_config()).unwrap();
        assert!(matches!(backend.pool, RedisPool::Standalone(_)));
    }

    #[test]
    fn test_cluster_pool_builds_without_error() {
        let cfg = RedisConfig {
            mode: RedisMode::Cluster,
            url: "redis://localhost:6379".to_string(),
            pool_size: 5,
            ..base_cfg()
        };
        let backend = RedisBackend::new(&cfg, cb_config());
        assert!(backend.is_ok(), "cluster build error: {:?}", backend.err());
    }

    #[test]
    fn test_cluster_pool_is_correct_variant() {
        let cfg = RedisConfig {
            mode: RedisMode::Cluster,
            ..base_cfg()
        };
        let backend = RedisBackend::new(&cfg, cb_config()).unwrap();
        assert!(matches!(backend.pool, RedisPool::Cluster(_)));
    }

    #[test]
    fn test_sentinel_client_builds_without_connection() {
        let cfg = RedisConfig {
            mode: RedisMode::Sentinel,
            url: "redis://localhost:26379".to_string(),
            ..base_cfg()
        };
        let backend = RedisBackend::new(&cfg, cb_config());
        assert!(backend.is_ok(), "sentinel build error: {:?}", backend.err());
    }

    #[test]
    fn test_sentinel_pool_is_correct_variant() {
        let cfg = RedisConfig {
            mode: RedisMode::Sentinel,
            url: "redis://localhost:26379".to_string(),
            ..base_cfg()
        };
        let backend = RedisBackend::new(&cfg, cb_config()).unwrap();
        assert!(matches!(backend.pool, RedisPool::Sentinel(_)));
    }

    #[test]
    fn test_sentinel_master_name_from_url_path() {
        assert_eq!(
            sentinel_master_name("redis://host:26379/custom_master"),
            "custom_master"
        );
        assert_eq!(sentinel_master_name("redis://host:26379"), "mymaster");
        assert_eq!(sentinel_master_name("redis://host:26379/"), "mymaster");
    }

    #[test]
    fn test_sentinel_master_name_defaults_on_invalid_url() {
        assert_eq!(sentinel_master_name("not-a-url"), "mymaster");
    }

    #[test]
    fn test_redis_backend_circuit_starts_closed() {
        let backend = RedisBackend::new(&base_cfg(), cb_config()).unwrap();
        assert_eq!(
            backend.circuit.state(),
            crate::backends::circuit_breaker::CbState::Closed
        );
    }
}
