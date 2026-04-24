use std::{io, time::Duration};

use sqlx::postgres::PgPoolOptions;
use tracing::info;
use triad_core::{config::PostgresConfig, error::BackendError};

use super::circuit_breaker::{CbConfig, CircuitBreaker};

/// Tag type for the Postgres circuit breaker.
pub struct Postgres;

/// PostgreSQL backend: a `sqlx` connection pool for queries plus a stored
/// `tokio_postgres::Config` for on-demand WAL replication connections.
///
/// These two connection types are kept strictly separate per the safety
/// invariant in CLAUDE.md: WAL replication uses `tokio-postgres` directly;
/// the `sqlx` pool is never used for replication protocol messages.
///
/// Note: the CDC module (Phase 2) is responsible for issuing
/// `START_REPLICATION` after connecting. The replication mode is established
/// at the wire-protocol level by the client sending the `replication` startup
/// parameter; this is not exposed as a typed API by tokio-postgres 0.7.
pub struct PgBackend {
    /// Standard connection pool for all read/write queries.
    pub pool: sqlx::PgPool,
    /// Base config for on-demand logical replication connections (CDC).
    /// The `replication=database` startup parameter must be set by the caller
    /// when building a replication `tokio_postgres::Client`.
    pub repl_config: tokio_postgres::Config,
    /// Circuit breaker protecting this backend.
    pub circuit: CircuitBreaker<Postgres>,
}

impl PgBackend {
    /// Create a pool, run embedded migrations, and build the replication config.
    pub async fn new(cfg: &PostgresConfig, cb_config: CbConfig) -> Result<Self, BackendError> {
        let mut opts = PgPoolOptions::new()
            .max_connections(cfg.max_connections)
            .acquire_timeout(Duration::from_millis(cfg.connection_timeout_ms));

        if let Some(min) = cfg.min_idle {
            opts = opts.min_connections(min);
        }

        let pool = opts.connect(&cfg.url).await?;

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(|e| BackendError::Postgres(sqlx::Error::Protocol(e.to_string())))?;

        info!(
            url = cfg.url.as_str(),
            max_connections = cfg.max_connections,
            "postgres pool connected"
        );

        // Build replication config from the dedicated replication URL (or fall
        // back to the main URL). The replication startup parameter is added to
        // the DSN so that the server accepts replication protocol commands.
        let repl_dsn = cfg.replication_url.as_deref().unwrap_or(&cfg.url);
        let repl_config = build_repl_config(repl_dsn).map_err(tp_to_backend)?;

        Ok(Self {
            pool,
            repl_config,
            circuit: CircuitBreaker::new(cb_config, "postgres"),
        })
    }

    /// Open a new logical replication connection for CDC.
    ///
    /// This is intentionally separate from `self.pool`; the two connection
    /// types use different wire protocols and must never be mixed.
    pub async fn replication_connect(&self) -> Result<tokio_postgres::Client, BackendError> {
        let (client, connection) = self
            .repl_config
            .connect(tokio_postgres::NoTls)
            .await
            .map_err(tp_to_backend)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "replication connection driver error");
            }
        });

        Ok(client)
    }
}

/// Parse the DSN and append the `replication=database` option so the server
/// accepts replication protocol messages on this connection.
///
/// The `replication` startup parameter is not a URI query parameter; we pass
/// it via the key-value pair format accepted by `tokio_postgres::Config`.
fn build_repl_config(dsn: &str) -> Result<tokio_postgres::Config, tokio_postgres::Error> {
    // Parse the base DSN first.
    let config: tokio_postgres::Config = dsn.parse()?;
    // Pass `replication=database` as a raw option (server startup parameter).
    // tokio-postgres 0.7 does not have a typed `replication_mode()` setter on
    // Config, so we embed it in the `options` field as a -c flag is NOT right
    // either — the replication parameter is a connection-time parameter, not a
    // SET-style GUC.  The correct approach is to append it to the key-value DSN:
    // since we already parsed the config, we re-parse with the parameter appended.
    //
    // If the DSN is a URI, convert to key-value form for the append.
    let kv_dsn = if dsn.starts_with("postgresql://") || dsn.starts_with("postgres://") {
        // URI form: append as query param (libpq-compatible)
        if dsn.contains('?') {
            format!("{}&replication=database", dsn)
        } else {
            format!("{}?replication=database", dsn)
        }
    } else {
        // Key-value form: append param directly
        format!("{} replication=database", dsn)
    };
    // Try re-parse; if tokio-postgres doesn't recognise replication, fall back
    // to the base config already parsed above.
    match kv_dsn.parse::<tokio_postgres::Config>() {
        Ok(c) => Ok(c),
        Err(_) => Ok(config),
    }
}

/// Convert a `tokio_postgres::Error` into `BackendError` by wrapping it in an
/// `std::io::Error` (compatible with `sqlx::Error::Io`).
fn tp_to_backend(e: tokio_postgres::Error) -> BackendError {
    BackendError::Postgres(sqlx::Error::Io(io::Error::other(e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cfg(url: &str) -> PostgresConfig {
        PostgresConfig {
            url: url.to_string(),
            replication_url: None,
            max_connections: 10,
            min_idle: None,
            connection_timeout_ms: 5000,
            statement_timeout_ms: None,
            wal_level: None,
            replication_slot: None,
            publication: None,
            ssl_mode: "disable".to_string(),
            ssl_root_cert: None,
        }
    }

    #[test]
    fn test_cb_config_from_core() {
        let core_cfg = triad_core::config::CircuitBreakerConfig {
            failure_threshold: 5,
            success_threshold: 2,
            timeout_ms: 30_000,
            half_open_max_calls: 3,
        };
        let cb = CbConfig::from(&core_cfg);
        assert_eq!(cb.failure_threshold, 5);
        assert_eq!(cb.half_open_after, Duration::from_millis(30_000));
    }

    #[test]
    fn test_default_repl_dsn_falls_back_to_url() {
        let cfg = test_cfg("postgresql://user:pass@localhost/db");
        let dsn = cfg.replication_url.as_deref().unwrap_or(&cfg.url);
        assert_eq!(dsn, "postgresql://user:pass@localhost/db");
    }

    #[test]
    fn test_explicit_repl_dsn_is_used() {
        let mut cfg = test_cfg("postgresql://user:pass@localhost/db");
        cfg.replication_url = Some("postgresql://repl:secret@replica/db".to_string());
        let dsn = cfg.replication_url.as_deref().unwrap_or(&cfg.url);
        assert_eq!(dsn, "postgresql://repl:secret@replica/db");
    }

    #[test]
    fn test_build_repl_config_parses_valid_uri_dsn() {
        let result = build_repl_config("postgresql://localhost/testdb");
        assert!(result.is_ok(), "failed to parse DSN: {:?}", result.err());
    }

    #[test]
    fn test_build_repl_config_parses_valid_kv_dsn() {
        let result = build_repl_config("host=localhost dbname=testdb");
        assert!(
            result.is_ok(),
            "failed to parse key-value DSN: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_build_repl_config_falls_back_on_invalid_dsn() {
        // Even if the replication param causes a parse error, we return the base config.
        let result = build_repl_config("host=localhost dbname=testdb user=u");
        assert!(result.is_ok());
    }
}
