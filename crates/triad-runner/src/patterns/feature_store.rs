use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use metrics::{counter, histogram};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument, warn};
use triad_core::{error::PatternError, metrics::names};

/// A feature vector for a single entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureVector {
    pub entity_id: String,
    pub features: std::collections::HashMap<String, serde_json::Value>,
    pub computed_at: DateTime<Utc>,
}

impl FeatureVector {
    pub fn freshness_secs(&self) -> f64 {
        (Utc::now() - self.computed_at).num_seconds() as f64
    }
}

/// Narrow trait for online feature serving (Redis) — mockable.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait OnlineFeatureStore: Send + Sync {
    async fn get(
        &self,
        entity_id: &str,
        features: Vec<String>,
    ) -> Result<Option<FeatureVector>, PatternError>;
    async fn set(&self, vector: &FeatureVector, ttl_secs: u64) -> Result<(), PatternError>;
    fn is_available(&self) -> bool;
}

/// Narrow trait for offline feature serving (PG) — mockable.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait OfflineFeatureStore: Send + Sync {
    async fn get(
        &self,
        entity_id: &str,
        features: Vec<String>,
    ) -> Result<Option<FeatureVector>, PatternError>;
}

/// Online feature server: Redis primary, PG offline fallback, freshness tracking.
///
/// Lookup path:
///   1. Redis GET (online store, fast)
///   2. On miss / Redis unavailable: PG SELECT (offline store)
///   3. On PG hit: populate Redis for next request
pub struct FeatureServer {
    pub name: String,
    pub online: Arc<dyn OnlineFeatureStore>,
    pub offline: Arc<dyn OfflineFeatureStore>,
    pub ttl_secs: u64,
    pub max_freshness_secs: f64,
}

impl FeatureServer {
    pub fn new(
        name: impl Into<String>,
        online: Arc<dyn OnlineFeatureStore>,
        offline: Arc<dyn OfflineFeatureStore>,
        ttl_secs: u64,
        max_freshness_secs: f64,
    ) -> Self {
        Self {
            name: name.into(),
            online,
            offline,
            ttl_secs,
            max_freshness_secs,
        }
    }

    /// Look up features for an entity.
    #[instrument(skip(self), fields(entity_id, pattern_name = %self.name))]
    pub async fn lookup(
        &self,
        entity_id: &str,
        features: &[&str],
    ) -> Result<Option<FeatureVector>, PatternError> {
        let start = std::time::Instant::now();
        let feature_vec: Vec<String> = features.iter().map(|s| s.to_string()).collect();

        if self.online.is_available() {
            match self.online.get(entity_id, feature_vec.clone()).await? {
                Some(vector) => {
                    let freshness = vector.freshness_secs();
                    histogram!(names::FEATURE_STORE_FRESHNESS).record(freshness);

                    if freshness > self.max_freshness_secs {
                        warn!(
                            entity_id,
                            freshness, "feature vector stale — refreshing from offline"
                        );
                        // Attempt refresh from offline store.
                        if let Some(fresh) =
                            self.offline.get(entity_id, feature_vec.clone()).await?
                        {
                            let _ = self.online.set(&fresh, self.ttl_secs).await;
                            histogram!(names::FEATURE_STORE_LOOKUP_DURATION)
                                .record(start.elapsed().as_secs_f64());
                            return Ok(Some(fresh));
                        }
                    }

                    counter!(names::CACHE_HIT_TOTAL, "store" => "feature").increment(1);
                    histogram!(names::FEATURE_STORE_LOOKUP_DURATION)
                        .record(start.elapsed().as_secs_f64());
                    return Ok(Some(vector));
                }
                None => {
                    counter!(names::CACHE_MISS_TOTAL, "store" => "feature").increment(1);
                }
            }
        } else {
            warn!(
                entity_id,
                "online feature store unavailable — using offline"
            );
        }

        // Offline fallback (PG).
        let vector = self.offline.get(entity_id, feature_vec).await?;
        if let Some(ref v) = vector {
            if self.online.is_available() {
                let _ = self.online.set(v, self.ttl_secs).await;
            }
        }

        histogram!(names::FEATURE_STORE_LOOKUP_DURATION).record(start.elapsed().as_secs_f64());
        Ok(vector)
    }
}

/// Feature store pattern module: refreshes the online store from the offline store.
pub struct FeatureStoreModule {
    pub name: String,
    pub server: Arc<FeatureServer>,
    pub refresh_interval: Duration,
    pub entity_ids: Vec<String>,
    pub features: Vec<String>,
}

impl FeatureStoreModule {
    pub fn new(
        name: impl Into<String>,
        server: Arc<FeatureServer>,
        refresh_interval: Duration,
        entity_ids: Vec<String>,
        features: Vec<String>,
    ) -> Self {
        Self {
            name: name.into(),
            server,
            refresh_interval,
            entity_ids,
            features,
        }
    }

    #[instrument(skip(self), fields(pattern_name = %self.name))]
    async fn refresh_all(&self) -> Result<usize, PatternError> {
        let feature_refs: Vec<&str> = self.features.iter().map(|s| s.as_str()).collect();
        let mut count = 0;
        for entity_id in &self.entity_ids {
            let _ = self.server.lookup(entity_id, &feature_refs).await?;
            count += 1;
        }
        info!(count, "feature store refresh complete");
        Ok(count)
    }
}

#[async_trait]
impl triad_core::traits::PatternModule for FeatureStoreModule {
    fn name(&self) -> &str {
        &self.name
    }

    fn pattern_type(&self) -> &str {
        "feature_store"
    }

    async fn run(&mut self, ctx: triad_core::traits::RunContext) -> Result<(), PatternError> {
        info!(pattern_name = %ctx.pattern.0, "feature-store module started");
        loop {
            tokio::select! {
                _ = ctx.cancel.cancelled() => {
                    return Err(PatternError::Cancelled);
                }
                _ = tokio::time::sleep(self.refresh_interval) => {
                    if let Err(e) = self.refresh_all().await {
                        warn!(error = %e, "feature store refresh error");
                    }
                }
            }
        }
    }

    async fn drain(&mut self, _timeout: Duration) -> Result<(), PatternError> {
        Ok(())
    }

    fn health(&self) -> triad_core::types::ModuleHealth {
        triad_core::types::ModuleHealth {
            state: triad_core::types::ModuleState::Running,
            lag: None,
            in_flight: None,
            last_error: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::collections::HashMap;
    use tokio_util::sync::CancellationToken;
    use triad_core::{
        traits::{PatternModule, RunContext},
        types::{PatternName, PipelineName},
    };

    fn make_vector(entity_id: &str, fresh: bool) -> FeatureVector {
        let computed_at = if fresh {
            Utc::now()
        } else {
            Utc::now() - chrono::Duration::hours(2)
        };
        FeatureVector {
            entity_id: entity_id.to_string(),
            features: HashMap::from([("score".to_string(), serde_json::json!(0.95))]),
            computed_at,
        }
    }

    fn make_server(
        online: MockOnlineFeatureStore,
        offline: MockOfflineFeatureStore,
    ) -> Arc<FeatureServer> {
        Arc::new(FeatureServer::new(
            "test-fs",
            Arc::new(online),
            Arc::new(offline),
            300,
            3600.0,
        ))
    }

    #[tokio::test]
    async fn test_feature_server_online_hit_returns_fresh() {
        let mut online = MockOnlineFeatureStore::new();
        online.expect_is_available().returning(|| true);
        online
            .expect_get()
            .returning(|_, _| Ok(Some(make_vector("user-1", true))));
        let offline = MockOfflineFeatureStore::new();

        let server = make_server(online, offline);
        let result = server.lookup("user-1", &["score"]).await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_feature_server_online_miss_falls_back_to_offline() {
        let mut online = MockOnlineFeatureStore::new();
        online.expect_is_available().returning(|| true);
        online.expect_get().returning(|_, _| Ok(None));
        online.expect_set().times(1).returning(|_, _| Ok(()));
        let mut offline = MockOfflineFeatureStore::new();
        offline
            .expect_get()
            .returning(|_, _| Ok(Some(make_vector("user-2", true))));

        let server = make_server(online, offline);
        let result = server.lookup("user-2", &["score"]).await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_feature_server_online_unavailable_uses_offline() {
        let mut online = MockOnlineFeatureStore::new();
        online.expect_is_available().returning(|| false);
        // online.get should NOT be called when unavailable
        online.expect_get().never();
        let mut offline = MockOfflineFeatureStore::new();
        offline
            .expect_get()
            .returning(|_, _| Ok(Some(make_vector("user-3", true))));

        let server = make_server(online, offline);
        let result = server.lookup("user-3", &["score"]).await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_feature_server_stale_vector_refreshes_from_offline() {
        let stale = make_vector("user-4", false); // 2 hours old
        let fresh = make_vector("user-4", true);

        let mut online = MockOnlineFeatureStore::new();
        online.expect_is_available().returning(|| true);
        online
            .expect_get()
            .returning(move |_, _| Ok(Some(stale.clone())));
        online.expect_set().times(1).returning(|_, _| Ok(()));

        let mut offline = MockOfflineFeatureStore::new();
        offline
            .expect_get()
            .returning(move |_, _| Ok(Some(fresh.clone())));

        let server = Arc::new(FeatureServer::new(
            "test",
            Arc::new(online),
            Arc::new(offline),
            300,
            60.0, // max freshness = 60s (stale is 2h)
        ));
        let result = server.lookup("user-4", &["score"]).await.unwrap();
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_feature_server_entity_not_found() {
        let mut online = MockOnlineFeatureStore::new();
        online.expect_is_available().returning(|| true);
        online.expect_get().returning(|_, _| Ok(None));
        let mut offline = MockOfflineFeatureStore::new();
        offline.expect_get().returning(|_, _| Ok(None));

        let server = make_server(online, offline);
        let result = server.lookup("unknown", &["score"]).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_feature_store_module_run_cancels() {
        let mut online = MockOnlineFeatureStore::new();
        online.expect_is_available().returning(|| false);
        let offline = MockOfflineFeatureStore::new();

        let server = make_server(online, offline);
        let mut module =
            FeatureStoreModule::new("test", server, Duration::from_secs(60), vec![], vec![]);
        let cancel = CancellationToken::new();
        let ctx = RunContext {
            cancel: cancel.clone(),
            pattern: PatternName("feature_store".to_string()),
            pipeline: PipelineName("test".to_string()),
        };
        cancel.cancel();
        assert!(matches!(
            module.run(ctx).await,
            Err(PatternError::Cancelled)
        ));
    }

    #[rstest]
    #[case("feature_store")]
    fn test_feature_store_pattern_type(#[case] expected: &str) {
        let online = MockOnlineFeatureStore::new();
        let offline = MockOfflineFeatureStore::new();
        let server = make_server(online, offline);
        let module =
            FeatureStoreModule::new("test", server, Duration::from_secs(1), vec![], vec![]);
        assert_eq!(module.pattern_type(), expected);
    }

    #[test]
    fn test_feature_vector_freshness_fresh() {
        let v = make_vector("x", true);
        assert!(v.freshness_secs() < 5.0);
    }

    #[test]
    fn test_feature_vector_freshness_stale() {
        let v = make_vector("x", false);
        assert!(v.freshness_secs() > 3600.0);
    }
}
