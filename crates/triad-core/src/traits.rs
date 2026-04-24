use async_trait::async_trait;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use crate::{
    error::{CheckpointError, ElectionError, PatternError},
    types::{ModuleHealth, PatternName, PipelineName},
};

/// Produces a stream of events from a source (WAL, Kafka topic, Redis stream).
#[async_trait]
#[cfg_attr(test, mockall::automock(type Event = Vec<u8>; type Position = u64;))]
pub trait Source: Send + Sync {
    type Event: Send + 'static;
    type Position: Send + Clone + 'static;

    async fn poll(&mut self) -> Result<Vec<Self::Event>, PatternError>;
    async fn checkpoint(&mut self, pos: &Self::Position) -> Result<(), PatternError>;
    async fn seek(&mut self, pos: &Self::Position) -> Result<(), PatternError>;
    fn name(&self) -> &str;
}

/// Writes events to a sink (Kafka topic, PostgreSQL table, Redis key).
#[async_trait]
#[cfg_attr(test, mockall::automock(type Event = Vec<u8>;))]
pub trait Sink: Send + Sync {
    type Event: Send + 'static;

    async fn write(&mut self, events: &[Self::Event]) -> Result<(), PatternError>;
    async fn flush(&mut self) -> Result<(), PatternError>;
    fn name(&self) -> &str;
}

/// Stateless event transform.
pub trait Transform: Send + Sync {
    type Input: Send + 'static;
    type Output: Send + 'static;

    fn apply(&self, event: Self::Input) -> Result<Self::Output, crate::error::TransformError>;
}

/// A running pattern pipeline — the unit of work in the engine.
#[async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait PatternModule: Send + Sync {
    fn name(&self) -> &str;
    fn pattern_type(&self) -> &str;

    /// Run until the cancel token fires. Implementors must respect cancellation.
    async fn run(&mut self, ctx: RunContext) -> Result<(), PatternError>;

    /// Graceful drain: finish in-flight work, flush, commit. Returns when clean or timeout fires.
    async fn drain(&mut self, timeout: Duration) -> Result<(), PatternError>;

    fn health(&self) -> ModuleHealth;
}

/// Persists and restores per-pipeline source positions.
#[async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait CheckpointStore: Send + Sync {
    async fn load(
        &self,
        pattern: &PatternName,
        pipeline: &PipelineName,
    ) -> Result<Option<CheckpointRow>, CheckpointError>;

    /// Returns `Err(CheckpointError::VersionConflict)` if the optimistic lock fails.
    async fn save(&self, row: &CheckpointRow, expected_version: i64)
    -> Result<(), CheckpointError>;
}

/// Leader election abstraction (noop for Mode 1/2; K8s Lease for Mode 3).
#[async_trait]
#[cfg_attr(test, mockall::automock)]
pub trait LeaderElector: Send + Sync {
    async fn campaign(&self) -> Result<LeaderHandle, ElectionError>;
    fn is_leader(&self) -> bool;
}

/// Cancellable context passed to pattern module run loops.
pub struct RunContext {
    pub cancel: CancellationToken,
    pub pattern: PatternName,
    pub pipeline: PipelineName,
}

/// Row stored in `triad_checkpoints`.
pub struct CheckpointRow {
    pub pattern_name: PatternName,
    pub pipeline_name: PipelineName,
    pub owner_instance_id: String,
    pub version: i64,
    pub pg_lsn: Option<u64>,
    pub kafka_offsets: Option<serde_json::Value>,
    pub redis_watermark: Option<i64>,
}

/// Held by the leader while the lease is valid; drop to release the permit.
pub struct LeaderHandle {
    _inner: tokio::sync::OwnedSemaphorePermit,
}

impl LeaderHandle {
    pub fn new(permit: tokio::sync::OwnedSemaphorePermit) -> Self {
        Self { _inner: permit }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_leader_handle_new() {
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&sem).acquire_owned().await.unwrap();
        let _handle = LeaderHandle::new(permit);
        // Semaphore is now held; acquiring another should fail immediately.
        assert!(sem.try_acquire().is_err());
    }

    #[tokio::test]
    async fn test_leader_handle_drop_releases_permit() {
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        {
            let permit = Arc::clone(&sem).acquire_owned().await.unwrap();
            let _handle = LeaderHandle::new(permit);
        }
        // After drop, permit is returned.
        assert!(sem.try_acquire().is_ok());
    }

    #[test]
    fn test_run_context_fields() {
        let ctx = RunContext {
            cancel: CancellationToken::new(),
            pattern: PatternName("outbox".to_string()),
            pipeline: PipelineName("main".to_string()),
        };
        assert_eq!(ctx.pattern.0, "outbox");
        assert_eq!(ctx.pipeline.0, "main");
        assert!(!ctx.cancel.is_cancelled());
    }

    #[test]
    fn test_checkpoint_row_fields() {
        let row = CheckpointRow {
            pattern_name: PatternName("cdc".to_string()),
            pipeline_name: PipelineName("orders".to_string()),
            owner_instance_id: "node-1".to_string(),
            version: 3,
            pg_lsn: Some(12345),
            kafka_offsets: None,
            redis_watermark: None,
        };
        assert_eq!(row.version, 3);
        assert_eq!(row.pg_lsn, Some(12345));
    }
}
