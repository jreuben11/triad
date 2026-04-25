use std::{sync::Arc, time::Duration};

use pyo3::prelude::*;
use tokio::sync::Mutex;
use triad_runner::backends::{
    PgBackend, RedisBackend,
    circuit_breaker::{CbConfig, CircuitBreaker},
};

use crate::get_rt;

/// A live sqlx transaction wrapped for Python use.
///
/// Obtained via `async with instance.transaction() as tx:`.
#[pyclass]
pub struct PyTransaction {
    pub(crate) tx: Arc<Mutex<Option<sqlx::Transaction<'static, sqlx::Postgres>>>>,
}

/// Async context manager that opens a Postgres transaction on enter.
///
/// Usage:
///
/// ```python
/// async with instance.transaction() as tx:
///     await publisher.publish(tx, "order", "1", "order.created", payload)
/// ```
#[pyclass]
pub struct PyTransactionCM {
    pool: sqlx::PgPool,
    tx: Arc<Mutex<Option<sqlx::Transaction<'static, sqlx::Postgres>>>>,
}

#[pymethods]
impl PyTransactionCM {
    /// Enter the context manager: begin a transaction and return it.
    async fn __aenter__(slf: Py<Self>) -> PyResult<PyTransaction> {
        let (pool, tx_arc) = Python::attach(|py| {
            let this = slf.borrow(py);
            (this.pool.clone(), Arc::clone(&this.tx))
        });

        let (send, recv) = futures::channel::oneshot::channel();
        get_rt().spawn(async move {
            let result = pool.begin().await.map_err(|e| e.to_string());
            let _ = send.send(result);
        });

        let tx = recv
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

        {
            let mut guard = tx_arc.lock().await;
            *guard = Some(tx);
        }
        let tx_arc_clone = Arc::clone(&tx_arc);

        Ok(PyTransaction { tx: tx_arc_clone })
    }

    /// Exit the context manager: commit on success, rollback on exception.
    async fn __aexit__(
        slf: Py<Self>,
        exc_type: Py<PyAny>,
        _exc_val: Py<PyAny>,
        _exc_tb: Py<PyAny>,
    ) -> PyResult<bool> {
        let (tx_arc, has_exc) = Python::attach(|py| {
            let this = slf.borrow(py);
            let has_exc = !exc_type.is_none(py);
            (Arc::clone(&this.tx), has_exc)
        });

        let (send, recv) = futures::channel::oneshot::channel::<Result<(), String>>();
        get_rt().spawn(async move {
            let mut guard = tx_arc.lock().await;
            let result = if let Some(tx) = guard.take() {
                if has_exc {
                    tx.rollback().await.map_err(|e| e.to_string())
                } else {
                    tx.commit().await.map_err(|e| e.to_string())
                }
            } else {
                Ok(())
            };
            let _ = send.send(result);
        });

        recv.await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

        Ok(false) // don't suppress exceptions
    }
}

#[pymethods]
impl PyTransaction {
    /// Execute a SQL statement inside this transaction.
    ///
    /// Returns the number of rows affected.
    async fn execute(slf: Py<Self>, sql: String) -> PyResult<u64> {
        let tx_arc = Python::attach(|py| Arc::clone(&slf.borrow(py).tx));

        let (send, recv) = futures::channel::oneshot::channel::<Result<u64, String>>();
        get_rt().spawn(async move {
            let mut guard = tx_arc.lock().await;
            let result = match guard.as_mut() {
                Some(tx) => sqlx::query(&sql)
                    .execute(&mut **tx)
                    .await
                    .map(|r| r.rows_affected())
                    .map_err(|e| e.to_string()),
                None => Err("transaction already consumed".to_string()),
            };
            let _ = send.send(result);
        });

        recv.await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }

    /// Fetch a single row's first column value as a JSON string. Returns None if no rows.
    async fn fetch_optional(slf: Py<Self>, sql: String) -> PyResult<Option<String>> {
        let tx_arc = Python::attach(|py| Arc::clone(&slf.borrow(py).tx));

        let (send, recv) = futures::channel::oneshot::channel::<Result<Option<String>, String>>();
        get_rt().spawn(async move {
            let mut guard = tx_arc.lock().await;
            let result = match guard.as_mut() {
                Some(tx) => {
                    use sqlx::Row;
                    sqlx::query(&sql)
                        .fetch_optional(&mut **tx)
                        .await
                        .map_err(|e| e.to_string())
                        .map(|opt_row| {
                            opt_row.and_then(|row| {
                                let val: Result<serde_json::Value, _> = row.try_get(0);
                                val.ok().map(|v| v.to_string())
                            })
                        })
                }
                None => Err("transaction already consumed".to_string()),
            };
            let _ = send.send(result);
        });

        recv.await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }
}

/// Mode 1 in-process triad instance: Postgres + Redis backends.
///
/// Usage:
///
/// ```python
/// instance = await TriadInstance.start(pg_url="postgres://localhost/mydb")
/// async with instance.transaction() as tx:
///     await publisher.publish(tx, ...)
/// await instance.shutdown()
/// ```
#[pyclass]
pub struct PyTriadInstance {
    pub(crate) pg: Arc<PgBackend>,
    // Redis is held to keep the connection alive; accessed via flags/cache patterns.
    #[allow(dead_code)]
    pub(crate) redis: Arc<RedisBackend>,
}

fn default_cb() -> CbConfig {
    CbConfig {
        failure_threshold: 5,
        success_threshold: 2,
        half_open_after: Duration::from_secs(60),
        half_open_max_calls: 3,
    }
}

#[pymethods]
impl PyTriadInstance {
    /// Connect backends and return a running instance.
    ///
    /// Args:
    ///     pg_url: Postgres DSN (e.g. "postgres://user:pass@host/db")
    ///     redis_url: Redis DSN (e.g. "redis://localhost:6379"), optional
    #[staticmethod]
    #[pyo3(signature = (pg_url, redis_url = None))]
    async fn start(pg_url: String, redis_url: Option<String>) -> PyResult<PyTriadInstance> {
        let redis_url = redis_url.unwrap_or_else(|| "redis://localhost:6379".to_string());
        let cb = default_cb();

        let (send, recv) = futures::channel::oneshot::channel::<
            Result<(Arc<PgBackend>, Arc<RedisBackend>), String>,
        >();

        let cb_pg = cb.clone();
        let cb_redis = cb;
        get_rt().spawn(async move {
            let pool = match sqlx::PgPool::connect(&pg_url).await {
                Ok(p) => p,
                Err(e) => {
                    let _ = send.send(Err(format!("postgres connect failed: {e}")));
                    return;
                }
            };
            let repl_config: tokio_postgres::Config = pg_url
                .parse()
                .unwrap_or_else(|_| "host=localhost".parse().unwrap());

            let pg = Arc::new(PgBackend {
                pool,
                repl_config,
                circuit: CircuitBreaker::new(cb_pg, "postgres"),
            });

            let redis_cfg = triad_core::config::RedisConfig {
                mode: triad_core::config::RedisMode::Standalone,
                url: redis_url,
                pool_size: 5,
                min_idle: None,
                connection_timeout_ms: 5000,
                read_timeout_ms: 5000,
                write_timeout_ms: 5000,
                max_retries: 3,
                tls: None,
            };
            let redis = match RedisBackend::new(&redis_cfg, cb_redis) {
                Ok(r) => Arc::new(r),
                Err(e) => {
                    let _ = send.send(Err(format!("redis connect failed: {e}")));
                    return;
                }
            };

            let _ = send.send(Ok((pg, redis)));
        });

        let (pg, redis) = recv
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

        Ok(PyTriadInstance { pg, redis })
    }

    /// Drain any background tasks and close connections.
    ///
    /// The connection pools close gracefully when all references are dropped.
    async fn shutdown(slf: Py<Self>, timeout_secs: Option<f64>) -> PyResult<()> {
        let _timeout = timeout_secs.unwrap_or(30.0);
        // Connection pools close when the Arcs are dropped.
        // The Python GC will handle this when the instance is no longer referenced.
        let _ = slf;
        Ok(())
    }

    /// Return an async context manager that opens a Postgres transaction.
    fn transaction(slf: PyRef<'_, Self>) -> PyTransactionCM {
        PyTransactionCM {
            pool: slf.pg.pool.clone(),
            tx: Arc::new(Mutex::new(None)),
        }
    }
}
