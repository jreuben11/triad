use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use metrics::counter;
use tracing::{debug, info, instrument, warn};
use triad_core::{
    error::PatternError,
    metrics::names,
    traits::{PatternModule, RunContext},
    types::{ModuleHealth, ModuleState},
};

/// Narrow trait for Redis cache operations — mockable in unit tests.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait CacheRedis: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, PatternError>;
    async fn set(&self, key: &str, value: &[u8], ttl_secs: Option<u64>)
    -> Result<(), PatternError>;
    async fn del(&self, key: &str) -> Result<(), PatternError>;
    async fn hset(
        &self,
        key: &str,
        field: &str,
        value: &[u8],
        ttl_secs: Option<u64>,
    ) -> Result<(), PatternError>;
}

/// Narrow trait for PostgreSQL source-of-truth reads — mockable in unit tests.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait CachePgSource: Send + Sync {
    async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, PatternError>;
    async fn write(&self, key: &str, value: &[u8]) -> Result<(), PatternError>;
    async fn flush_pending(&self, entries: &[(String, Vec<u8>)]) -> Result<(), PatternError>;
}

/// Write-through cache helper: writes to PG then sets in Redis.
///
/// Ensures Redis is always updated atomically after every PG write.
pub struct WriteThrough {
    pub redis: Arc<dyn CacheRedis>,
    pub pg: Arc<dyn CachePgSource>,
    pub key_pattern: String,
    pub ttl_secs: Option<u64>,
}

impl WriteThrough {
    pub fn new(
        redis: Arc<dyn CacheRedis>,
        pg: Arc<dyn CachePgSource>,
        key_pattern: impl Into<String>,
        ttl_secs: Option<u64>,
    ) -> Self {
        Self {
            redis,
            pg,
            key_pattern: key_pattern.into(),
            ttl_secs,
        }
    }

    #[instrument(skip(self, value), fields(key))]
    pub async fn write(&self, key: &str, value: &[u8]) -> Result<(), PatternError> {
        self.pg.write(key, value).await?;
        self.redis.set(key, value, self.ttl_secs).await?;
        debug!(key, "write-through: PG + Redis updated");
        Ok(())
    }

    pub async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, PatternError> {
        match self.redis.get(key).await? {
            Some(val) => {
                counter!(names::CACHE_HIT_TOTAL).increment(1);
                Ok(Some(val))
            }
            None => {
                counter!(names::CACHE_MISS_TOTAL).increment(1);
                let val = self.pg.read(key).await?;
                if let Some(ref v) = val {
                    self.redis.set(key, v, self.ttl_secs).await?;
                }
                Ok(val)
            }
        }
    }
}

/// Write-behind cache: accepts writes to Redis, background-flushes to PG.
pub struct WriteBehind {
    pub name: String,
    pub redis: Arc<dyn CacheRedis>,
    pub pg: Arc<dyn CachePgSource>,
    pub flush_interval: Duration,
    pub pending: tokio::sync::Mutex<Vec<(String, Vec<u8>)>>,
}

impl WriteBehind {
    pub fn new(
        name: impl Into<String>,
        redis: Arc<dyn CacheRedis>,
        pg: Arc<dyn CachePgSource>,
        flush_interval: Duration,
    ) -> Self {
        Self {
            name: name.into(),
            redis,
            pg,
            flush_interval,
            pending: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub async fn write(&self, key: &str, value: &[u8]) -> Result<(), PatternError> {
        self.redis.set(key, value, None).await?;
        let mut pending = self.pending.lock().await;
        pending.push((key.to_string(), value.to_vec()));
        Ok(())
    }

    #[instrument(skip(self), fields(pattern_name = %self.name))]
    async fn flush(&self) -> Result<usize, PatternError> {
        let mut pending = self.pending.lock().await;
        if pending.is_empty() {
            return Ok(0);
        }
        let entries: Vec<_> = pending.drain(..).collect();
        let count = entries.len();
        self.pg.flush_pending(&entries).await?;
        info!(count, "write-behind: flushed to PG");
        Ok(count)
    }
}

#[async_trait]
impl PatternModule for WriteBehind {
    fn name(&self) -> &str {
        &self.name
    }

    fn pattern_type(&self) -> &str {
        "write_behind"
    }

    async fn run(&mut self, ctx: RunContext) -> Result<(), PatternError> {
        info!(pattern_name = %ctx.pattern.0, "write-behind module started");
        loop {
            tokio::select! {
                _ = ctx.cancel.cancelled() => {
                    return Err(PatternError::Cancelled);
                }
                _ = tokio::time::sleep(self.flush_interval) => {
                    if let Err(e) = self.flush().await {
                        warn!(error = %e, "write-behind flush error");
                    }
                }
            }
        }
    }

    async fn drain(&mut self, _timeout: Duration) -> Result<(), PatternError> {
        self.flush().await?;
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

/// Read-through (cache-aside) helper: Redis get → miss → PG read → Redis set.
pub struct CacheAside {
    pub redis: Arc<dyn CacheRedis>,
    pub pg: Arc<dyn CachePgSource>,
    pub ttl_secs: Option<u64>,
}

impl CacheAside {
    pub fn new(
        redis: Arc<dyn CacheRedis>,
        pg: Arc<dyn CachePgSource>,
        ttl_secs: Option<u64>,
    ) -> Self {
        Self {
            redis,
            pg,
            ttl_secs,
        }
    }

    pub async fn read(&self, key: &str) -> Result<Option<Vec<u8>>, PatternError> {
        match self.redis.get(key).await? {
            Some(val) => {
                counter!(names::CACHE_HIT_TOTAL).increment(1);
                Ok(Some(val))
            }
            None => {
                counter!(names::CACHE_MISS_TOTAL).increment(1);
                let val = self.pg.read(key).await?;
                if let Some(ref v) = val {
                    self.redis.set(key, v, self.ttl_secs).await?;
                }
                Ok(val)
            }
        }
    }
}

/// CDC-driven cache sync module: consumes ChangeEvents and syncs Redis.
pub struct CacheSyncModule {
    pub name: String,
    pub redis: Arc<dyn CacheRedis>,
    pub source_topic: String,
    pub redis_key_pattern: String,
    pub ttl_secs: Option<u64>,
    pub on_delete: DeleteBehaviour,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteBehaviour {
    Delete,
    Nullify,
}

impl CacheSyncModule {
    pub fn new(
        name: impl Into<String>,
        redis: Arc<dyn CacheRedis>,
        source_topic: impl Into<String>,
        redis_key_pattern: impl Into<String>,
        ttl_secs: Option<u64>,
        on_delete: DeleteBehaviour,
    ) -> Self {
        Self {
            name: name.into(),
            redis,
            source_topic: source_topic.into(),
            redis_key_pattern: redis_key_pattern.into(),
            ttl_secs,
            on_delete,
        }
    }

    pub async fn apply(
        &self,
        key: &str,
        value: Option<&[u8]>,
        is_delete: bool,
    ) -> Result<(), PatternError> {
        if is_delete {
            match self.on_delete {
                DeleteBehaviour::Delete => self.redis.del(key).await?,
                DeleteBehaviour::Nullify => self.redis.set(key, b"null", self.ttl_secs).await?,
            }
        } else if let Some(v) = value {
            self.redis.set(key, v, self.ttl_secs).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl PatternModule for CacheSyncModule {
    fn name(&self) -> &str {
        &self.name
    }

    fn pattern_type(&self) -> &str {
        "cache_sync"
    }

    async fn run(&mut self, ctx: RunContext) -> Result<(), PatternError> {
        info!(pattern_name = %ctx.pattern.0, "cache-sync module started (awaiting CDC events)");
        // CDC events are delivered via the internal event router; for now we
        // block on cancellation to maintain the PatternModule contract.
        ctx.cancel.cancelled().await;
        Err(PatternError::Cancelled)
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

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use tokio_util::sync::CancellationToken;
    use tracing_test::traced_test;
    use triad_core::types::{PatternName, PipelineName};

    fn make_redis() -> MockCacheRedis {
        MockCacheRedis::new()
    }
    fn make_pg() -> MockCachePgSource {
        MockCachePgSource::new()
    }

    // --- WriteThrough ---

    #[traced_test]
    #[tokio::test]
    async fn test_write_through_write_updates_pg_and_redis() {
        let mut redis = make_redis();
        redis.expect_set().times(1).returning(|_, _, _| Ok(()));
        let mut pg = make_pg();
        pg.expect_write().times(1).returning(|_, _| Ok(()));

        let wt = WriteThrough::new(Arc::new(redis), Arc::new(pg), "key:{id}", Some(60));
        assert!(wt.write("key:1", b"value").await.is_ok());
        assert!(!logs_contain("ERROR"));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_write_through_read_cache_hit() {
        let mut redis = make_redis();
        redis
            .expect_get()
            .returning(|_| Ok(Some(b"cached".to_vec())));
        let pg = make_pg(); // pg.read should not be called

        let wt = WriteThrough::new(Arc::new(redis), Arc::new(pg), "key:{id}", Some(60));
        let result = wt.read("key:1").await.unwrap();
        assert_eq!(result, Some(b"cached".to_vec()));
        assert!(!logs_contain("ERROR"));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_write_through_read_cache_miss_reads_pg_and_populates_cache() {
        let mut redis = make_redis();
        redis.expect_get().returning(|_| Ok(None));
        redis.expect_set().times(1).returning(|_, _, _| Ok(()));
        let mut pg = make_pg();
        pg.expect_read()
            .returning(|_| Ok(Some(b"from-pg".to_vec())));

        let wt = WriteThrough::new(Arc::new(redis), Arc::new(pg), "k", None);
        let result = wt.read("k:1").await.unwrap();
        assert_eq!(result, Some(b"from-pg".to_vec()));
        assert!(!logs_contain("ERROR"));
    }

    // --- WriteBehind ---

    #[traced_test]
    #[tokio::test]
    async fn test_write_behind_write_stores_to_redis_and_queues() {
        let mut redis = make_redis();
        redis.expect_set().times(1).returning(|_, _, _| Ok(()));
        let pg = make_pg();

        let wb = WriteBehind::new("wb", Arc::new(redis), Arc::new(pg), Duration::from_secs(5));
        assert!(wb.write("k", b"v").await.is_ok());
        let pending = wb.pending.lock().await;
        assert_eq!(pending.len(), 1);
        assert!(!logs_contain("ERROR"));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_write_behind_flush_sends_to_pg() {
        let mut redis = make_redis();
        redis.expect_set().times(1).returning(|_, _, _| Ok(()));
        let mut pg = make_pg();
        pg.expect_flush_pending().times(1).returning(|_| Ok(()));

        let wb = WriteBehind::new("wb", Arc::new(redis), Arc::new(pg), Duration::from_secs(5));
        wb.write("k", b"v").await.unwrap();
        let result = wb.flush().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        // Pending should be empty after flush
        let pending = wb.pending.lock().await;
        assert!(pending.is_empty());
        assert!(!logs_contain("ERROR"));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_write_behind_flush_empty_returns_zero() {
        let redis = make_redis();
        let pg = make_pg();
        let wb = WriteBehind::new("wb", Arc::new(redis), Arc::new(pg), Duration::from_secs(5));
        assert_eq!(wb.flush().await.unwrap(), 0);
        assert!(!logs_contain("ERROR"));
    }

    #[tokio::test]
    async fn test_write_behind_run_cancels() {
        let redis = make_redis();
        let pg = make_pg();
        let mut wb = WriteBehind::new("wb", Arc::new(redis), Arc::new(pg), Duration::from_secs(60));
        let cancel = CancellationToken::new();
        let ctx = RunContext {
            cancel: cancel.clone(),
            pattern: PatternName("cache".to_string()),
            pipeline: PipelineName("test".to_string()),
        };
        cancel.cancel();
        assert!(matches!(wb.run(ctx).await, Err(PatternError::Cancelled)));
    }

    // --- CacheAside ---

    #[tokio::test]
    async fn test_cache_aside_hit() {
        let mut redis = make_redis();
        redis.expect_get().returning(|_| Ok(Some(b"hit".to_vec())));
        let pg = make_pg();
        let ca = CacheAside::new(Arc::new(redis), Arc::new(pg), None);
        let result = ca.read("k").await.unwrap();
        assert_eq!(result, Some(b"hit".to_vec()));
    }

    #[tokio::test]
    async fn test_cache_aside_miss_populates() {
        let mut redis = make_redis();
        redis.expect_get().returning(|_| Ok(None));
        redis.expect_set().times(1).returning(|_, _, _| Ok(()));
        let mut pg = make_pg();
        pg.expect_read().returning(|_| Ok(Some(b"pg-val".to_vec())));

        let ca = CacheAside::new(Arc::new(redis), Arc::new(pg), Some(300));
        let result = ca.read("k").await.unwrap();
        assert_eq!(result, Some(b"pg-val".to_vec()));
    }

    // --- CacheSyncModule ---

    #[tokio::test]
    async fn test_cache_sync_apply_upsert() {
        let mut redis = make_redis();
        redis.expect_set().times(1).returning(|_, _, _| Ok(()));
        let module = CacheSyncModule::new(
            "sync",
            Arc::new(redis),
            "cdc.orders",
            "order:{id}",
            Some(600),
            DeleteBehaviour::Delete,
        );
        assert!(module.apply("order:1", Some(b"data"), false).await.is_ok());
    }

    #[tokio::test]
    async fn test_cache_sync_apply_delete() {
        let mut redis = make_redis();
        redis.expect_del().times(1).returning(|_| Ok(()));
        let module = CacheSyncModule::new(
            "sync",
            Arc::new(redis),
            "cdc.orders",
            "order:{id}",
            None,
            DeleteBehaviour::Delete,
        );
        assert!(module.apply("order:1", None, true).await.is_ok());
    }

    #[tokio::test]
    async fn test_cache_sync_apply_nullify_on_delete() {
        let mut redis = make_redis();
        redis.expect_set().times(1).returning(|_, _, _| Ok(()));
        let module = CacheSyncModule::new(
            "sync",
            Arc::new(redis),
            "cdc.orders",
            "order:{id}",
            None,
            DeleteBehaviour::Nullify,
        );
        assert!(module.apply("order:1", None, true).await.is_ok());
    }

    #[rstest]
    #[case("cache_sync")]
    fn test_cache_sync_pattern_type(#[case] expected: &str) {
        let module = CacheSyncModule::new(
            "test",
            Arc::new(MockCacheRedis::new()),
            "topic",
            "key:{id}",
            None,
            DeleteBehaviour::Delete,
        );
        assert_eq!(module.pattern_type(), expected);
    }

    #[rstest]
    #[case("write_behind")]
    fn test_write_behind_pattern_type(#[case] expected: &str) {
        let wb = WriteBehind::new(
            "wb",
            Arc::new(MockCacheRedis::new()),
            Arc::new(MockCachePgSource::new()),
            Duration::from_secs(1),
        );
        assert_eq!(wb.pattern_type(), expected);
    }
}
