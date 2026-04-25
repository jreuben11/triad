use std::sync::Arc;

use async_trait::async_trait;
use pyo3::prelude::*;
use triad_core::error::PatternError;
use triad_runner::patterns::feature_flag::{FeatureFlag, FlagStore};
use triad_sdk::patterns::FlagEvaluator;

use crate::get_rt;

// ── Minimal FlagStore backed by a Postgres pool only ─────────────────────────

/// A FlagStore that reads from Postgres only (no Redis). Used as a simple
/// default when building PyFlagEvaluator directly. The circuit breaker is
/// always reported as "open" so the PG path is always used.
struct PgOnlyFlagStore {
    pool: sqlx::PgPool,
}

#[async_trait]
impl FlagStore for PgOnlyFlagStore {
    async fn load_all(&self) -> Result<Vec<FeatureFlag>, PatternError> {
        let rows: Vec<(
            String,
            bool,
            Option<i16>,
            Option<serde_json::Value>,
            chrono::DateTime<chrono::Utc>,
        )> = sqlx::query_as(
            "SELECT name, enabled, rollout_percentage, targeting_rules, updated_at \
             FROM triad_feature_flags ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| PatternError::Backend(triad_core::error::BackendError::Postgres(e)))?;

        Ok(rows
            .into_iter()
            .map(|(name, enabled, pct, rules, updated_at)| FeatureFlag {
                name,
                enabled,
                rollout_percentage: pct.map(|p| p as u8),
                targeting_rules: rules,
                updated_at,
            })
            .collect())
    }

    async fn load_one(&self, name: &str) -> Result<Option<FeatureFlag>, PatternError> {
        let row: Option<(
            bool,
            Option<i16>,
            Option<serde_json::Value>,
            chrono::DateTime<chrono::Utc>,
        )> = sqlx::query_as(
            "SELECT enabled, rollout_percentage, targeting_rules, updated_at \
             FROM triad_feature_flags WHERE name = $1",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| PatternError::Backend(triad_core::error::BackendError::Postgres(e)))?;

        Ok(row.map(|(enabled, pct, rules, updated_at)| FeatureFlag {
            name: name.to_string(),
            enabled,
            rollout_percentage: pct.map(|p| p as u8),
            targeting_rules: rules,
            updated_at,
        }))
    }

    async fn push_to_cache(&self, _flag: &FeatureFlag, _ttl_secs: u64) -> Result<(), PatternError> {
        Ok(()) // no-op — PG-only store
    }

    async fn get_from_cache(&self, _name: &str) -> Result<Option<FeatureFlag>, PatternError> {
        Ok(None) // no cache
    }

    fn redis_cb_open(&self) -> bool {
        true // always use PG fallback
    }
}

// ── In-memory FlagStore for testing without a real database ──────────────────

struct MemFlagStore {
    flags: std::collections::HashMap<String, FeatureFlag>,
}

#[async_trait]
impl FlagStore for MemFlagStore {
    async fn load_all(&self) -> Result<Vec<FeatureFlag>, PatternError> {
        Ok(self.flags.values().cloned().collect())
    }

    async fn load_one(&self, name: &str) -> Result<Option<FeatureFlag>, PatternError> {
        Ok(self.flags.get(name).cloned())
    }

    async fn push_to_cache(&self, _flag: &FeatureFlag, _ttl_secs: u64) -> Result<(), PatternError> {
        Ok(())
    }

    async fn get_from_cache(&self, _name: &str) -> Result<Option<FeatureFlag>, PatternError> {
        Ok(None)
    }

    fn redis_cb_open(&self) -> bool {
        true
    }
}

// ── PyFlagEvaluator ──────────────────────────────────────────────────────────

/// SDK facade for evaluating feature flags.
///
/// Usage (with Postgres):
///
/// ```python
/// evaluator = await FlagEvaluator.from_pg("postgres://localhost/mydb")
/// enabled = await evaluator.is_enabled("my-feature")
/// ```
///
/// Usage (in-memory, for testing):
///
/// ```python
/// evaluator = FlagEvaluator.from_flags({"my-feature": True, "other": False})
/// assert await evaluator.is_enabled("my-feature")
/// ```
#[pyclass]
pub struct PyFlagEvaluator {
    inner: Arc<FlagEvaluator>,
}

#[pymethods]
impl PyFlagEvaluator {
    /// Build an evaluator backed by a Postgres connection pool.
    ///
    /// Args:
    ///     pg_url: Postgres DSN (e.g. "postgres://localhost/mydb")
    #[staticmethod]
    async fn from_pg(pg_url: String) -> PyResult<PyFlagEvaluator> {
        let (send, recv) = futures::channel::oneshot::channel::<Result<sqlx::PgPool, String>>();
        get_rt().spawn(async move {
            let result = sqlx::PgPool::connect(&pg_url)
                .await
                .map_err(|e| e.to_string());
            let _ = send.send(result);
        });

        let pool = recv
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

        let store: Arc<dyn FlagStore> = Arc::new(PgOnlyFlagStore { pool });
        Ok(PyFlagEvaluator {
            inner: Arc::new(FlagEvaluator::new(store)),
        })
    }

    /// Build an evaluator backed by an in-memory flag map (for testing).
    ///
    /// Args:
    ///     flags: dict of flag_name → enabled (bool)
    #[staticmethod]
    fn from_flags(flags: &Bound<'_, PyAny>) -> PyResult<PyFlagEvaluator> {
        let flag_map: std::collections::HashMap<String, bool> = flags.extract()?;
        let now = chrono::Utc::now();
        let store_flags: std::collections::HashMap<String, FeatureFlag> = flag_map
            .into_iter()
            .map(|(name, enabled)| {
                let flag = FeatureFlag {
                    name: name.clone(),
                    enabled,
                    rollout_percentage: None,
                    targeting_rules: None,
                    updated_at: now,
                };
                (name, flag)
            })
            .collect();

        let store: Arc<dyn FlagStore> = Arc::new(MemFlagStore { flags: store_flags });
        Ok(PyFlagEvaluator {
            inner: Arc::new(FlagEvaluator::new(store)),
        })
    }

    /// Evaluate whether a feature flag is enabled.
    ///
    /// Returns True if the flag exists and is enabled, False otherwise.
    async fn is_enabled(slf: Py<Self>, flag: String) -> PyResult<bool> {
        let evaluator = Python::attach(|py| Arc::clone(&slf.borrow(py).inner));

        let (send, recv) = futures::channel::oneshot::channel::<Result<bool, String>>();
        get_rt().spawn(async move {
            let result = evaluator.is_enabled(&flag).await.map_err(|e| e.to_string());
            let _ = send.send(result);
        });

        recv.await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }
}
