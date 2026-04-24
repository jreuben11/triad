use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use metrics::counter;
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};
use triad_core::{
    error::PatternError,
    metrics::names,
    traits::{PatternModule, RunContext},
    types::{ModuleHealth, ModuleState},
};

/// A feature flag definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureFlag {
    pub name: String,
    pub enabled: bool,
    pub rollout_percentage: Option<u8>,
    pub targeting_rules: Option<serde_json::Value>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

/// Result of evaluating a feature flag for a given context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlagEvaluation {
    Enabled,
    Disabled,
    NotFound,
}

/// Narrow trait for feature flag storage — mockable in unit tests.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait FlagStore: Send + Sync {
    /// Read all flags from PG (for polling propagation mode).
    async fn load_all(&self) -> Result<Vec<FeatureFlag>, PatternError>;
    /// Read a specific flag from PG (Redis CB open fallback).
    async fn load_one(&self, name: &str) -> Result<Option<FeatureFlag>, PatternError>;
    /// Store flag in Redis.
    async fn push_to_cache(&self, flag: &FeatureFlag, ttl_secs: u64) -> Result<(), PatternError>;
    /// Read flag from Redis cache.
    async fn get_from_cache(&self, name: &str) -> Result<Option<FeatureFlag>, PatternError>;
    fn redis_cb_open(&self) -> bool;
}

/// Feature flag distributor: polls PG for changes and pushes to Redis.
pub struct FlagDistributor {
    pub name: String,
    pub store: Arc<dyn FlagStore>,
    pub poll_interval: Duration,
    pub ttl_secs: u64,
}

impl FlagDistributor {
    pub fn new(
        name: impl Into<String>,
        store: Arc<dyn FlagStore>,
        poll_interval: Duration,
        ttl_secs: u64,
    ) -> Self {
        Self {
            name: name.into(),
            store,
            poll_interval,
            ttl_secs,
        }
    }

    #[instrument(skip(self), fields(pattern_name = %self.name))]
    async fn sync_flags(&self) -> Result<usize, PatternError> {
        let flags = self.store.load_all().await?;
        let count = flags.len();
        for flag in &flags {
            if let Err(e) = self.store.push_to_cache(flag, self.ttl_secs).await {
                warn!(flag = %flag.name, error = %e, "failed to push flag to Redis");
            }
        }
        info!(count, "feature flags synced to Redis");
        Ok(count)
    }
}

#[async_trait]
impl PatternModule for FlagDistributor {
    fn name(&self) -> &str {
        &self.name
    }

    fn pattern_type(&self) -> &str {
        "feature_flag"
    }

    async fn run(&mut self, ctx: RunContext) -> Result<(), PatternError> {
        info!(pattern_name = %ctx.pattern.0, "feature-flag distributor started");
        loop {
            tokio::select! {
                _ = ctx.cancel.cancelled() => {
                    return Err(PatternError::Cancelled);
                }
                _ = tokio::time::sleep(self.poll_interval) => {
                    if let Err(e) = self.sync_flags().await {
                        warn!(error = %e, "feature flag sync error");
                        counter!(names::ERROR_TOTAL, "pattern_name" => self.name.clone())
                            .increment(1);
                    }
                }
            }
        }
    }

    async fn drain(&mut self, _timeout: Duration) -> Result<(), PatternError> {
        Ok(())
    }

    fn health(&self) -> ModuleHealth {
        ModuleHealth {
            state: ModuleState::Running,
            lag: None,
            in_flight: None,
            last_error: None,
        }
    }
}

/// Feature flag evaluator: reads from Redis (fast path) or PG (CB fallback).
///
/// Used by the SDK to evaluate flags at request time.
pub struct FlagEvaluator {
    pub store: Arc<dyn FlagStore>,
}

impl FlagEvaluator {
    pub fn new(store: Arc<dyn FlagStore>) -> Self {
        Self { store }
    }

    /// Evaluate a flag for a given entity key.
    ///
    /// Fast path: Redis GET. CB open fallback: PG SELECT.
    #[instrument(skip(self), fields(flag_name = %name))]
    pub async fn evaluate(
        &self,
        name: &str,
        entity_key: Option<&str>,
    ) -> Result<FlagEvaluation, PatternError> {
        counter!(names::FEATURE_FLAG_EVALUATIONS).increment(1);

        let flag = if self.store.redis_cb_open() {
            warn!(flag = %name, "Redis CB open — falling back to PG for flag evaluation");
            self.store.load_one(name).await?
        } else {
            match self.store.get_from_cache(name).await? {
                Some(f) => Some(f),
                None => self.store.load_one(name).await?,
            }
        };

        match flag {
            None => Ok(FlagEvaluation::NotFound),
            Some(f) if !f.enabled => Ok(FlagEvaluation::Disabled),
            Some(f) => {
                // Rollout percentage check: hash entity_key into 0..100 bucket.
                if let (Some(pct), Some(key)) = (f.rollout_percentage, entity_key) {
                    let bucket = bucket_for_key(key);
                    if bucket >= pct {
                        return Ok(FlagEvaluation::Disabled);
                    }
                }
                Ok(FlagEvaluation::Enabled)
            }
        }
    }
}

/// Deterministic 0..100 bucket for a key (simple hash modulo).
fn bucket_for_key(key: &str) -> u8 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    (hasher.finish() % 100) as u8
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use tokio_util::sync::CancellationToken;
    use triad_core::types::{PatternName, PipelineName};

    fn make_flag(name: &str, enabled: bool) -> FeatureFlag {
        FeatureFlag {
            name: name.to_string(),
            enabled,
            rollout_percentage: None,
            targeting_rules: None,
            updated_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_flag_distributor_sync_pushes_all_flags() {
        let mut store = MockFlagStore::new();
        store
            .expect_load_all()
            .returning(|| Ok(vec![make_flag("ff-a", true), make_flag("ff-b", false)]));
        store
            .expect_push_to_cache()
            .times(2)
            .returning(|_, _| Ok(()));

        let dist = FlagDistributor::new("test", Arc::new(store), Duration::from_secs(10), 300);
        let result = dist.sync_flags().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_flag_distributor_run_cancels() {
        let mut store = MockFlagStore::new();
        store.expect_load_all().returning(|| Ok(vec![]));
        store.expect_push_to_cache().returning(|_, _| Ok(()));

        let mut dist =
            FlagDistributor::new("test", Arc::new(store), Duration::from_millis(50), 300);
        let cancel = CancellationToken::new();
        let ctx = RunContext {
            cancel: cancel.clone(),
            pattern: PatternName("feature_flag".to_string()),
            pipeline: PipelineName("test".to_string()),
        };
        cancel.cancel();
        assert!(matches!(dist.run(ctx).await, Err(PatternError::Cancelled)));
    }

    #[tokio::test]
    async fn test_flag_evaluator_redis_hit_enabled() {
        let mut store = MockFlagStore::new();
        store.expect_redis_cb_open().returning(|| false);
        store
            .expect_get_from_cache()
            .returning(|_| Ok(Some(make_flag("my-flag", true))));

        let eval = FlagEvaluator::new(Arc::new(store));
        let result = eval.evaluate("my-flag", None).await.unwrap();
        assert_eq!(result, FlagEvaluation::Enabled);
    }

    #[tokio::test]
    async fn test_flag_evaluator_redis_hit_disabled() {
        let mut store = MockFlagStore::new();
        store.expect_redis_cb_open().returning(|| false);
        store
            .expect_get_from_cache()
            .returning(|_| Ok(Some(make_flag("my-flag", false))));

        let eval = FlagEvaluator::new(Arc::new(store));
        let result = eval.evaluate("my-flag", None).await.unwrap();
        assert_eq!(result, FlagEvaluation::Disabled);
    }

    #[tokio::test]
    async fn test_flag_evaluator_redis_miss_falls_back_to_pg() {
        let mut store = MockFlagStore::new();
        store.expect_redis_cb_open().returning(|| false);
        store.expect_get_from_cache().returning(|_| Ok(None));
        store
            .expect_load_one()
            .returning(|_| Ok(Some(make_flag("ff-x", true))));

        let eval = FlagEvaluator::new(Arc::new(store));
        let result = eval.evaluate("ff-x", None).await.unwrap();
        assert_eq!(result, FlagEvaluation::Enabled);
    }

    #[tokio::test]
    async fn test_flag_evaluator_redis_cb_open_uses_pg() {
        let mut store = MockFlagStore::new();
        store.expect_redis_cb_open().returning(|| true);
        // get_from_cache should NOT be called when CB is open
        store.expect_get_from_cache().never();
        store
            .expect_load_one()
            .returning(|_| Ok(Some(make_flag("ff-y", true))));

        let eval = FlagEvaluator::new(Arc::new(store));
        let result = eval.evaluate("ff-y", None).await.unwrap();
        assert_eq!(result, FlagEvaluation::Enabled);
    }

    #[tokio::test]
    async fn test_flag_evaluator_not_found() {
        let mut store = MockFlagStore::new();
        store.expect_redis_cb_open().returning(|| false);
        store.expect_get_from_cache().returning(|_| Ok(None));
        store.expect_load_one().returning(|_| Ok(None));

        let eval = FlagEvaluator::new(Arc::new(store));
        let result = eval.evaluate("unknown-flag", None).await.unwrap();
        assert_eq!(result, FlagEvaluation::NotFound);
    }

    #[tokio::test]
    async fn test_flag_evaluator_rollout_percentage_below_bucket_enabled() {
        let mut flag = make_flag("pct-flag", true);
        flag.rollout_percentage = Some(100); // 100% → everyone gets it

        let mut store = MockFlagStore::new();
        store.expect_redis_cb_open().returning(|| false);
        store
            .expect_get_from_cache()
            .returning(move |_| Ok(Some(flag.clone())));

        let eval = FlagEvaluator::new(Arc::new(store));
        let result = eval.evaluate("pct-flag", Some("user-abc")).await.unwrap();
        // bucket_for_key("user-abc") will be < 100, so enabled
        assert_eq!(result, FlagEvaluation::Enabled);
    }

    #[rstest]
    #[case("feature_flag")]
    fn test_flag_distributor_pattern_type(#[case] expected: &str) {
        let dist = FlagDistributor::new(
            "test",
            Arc::new(MockFlagStore::new()),
            Duration::from_secs(1),
            60,
        );
        assert_eq!(dist.pattern_type(), expected);
    }

    #[test]
    fn test_bucket_for_key_is_deterministic() {
        let b1 = bucket_for_key("user-123");
        let b2 = bucket_for_key("user-123");
        assert_eq!(b1, b2);
    }

    #[test]
    fn test_bucket_for_key_in_range() {
        for key in ["a", "user-1", "org-99", "api-key-abc123"] {
            let b = bucket_for_key(key);
            assert!(b < 100, "bucket {b} out of range for key {key}");
        }
    }
}
