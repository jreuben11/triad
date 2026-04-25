#![cfg(feature = "integration")]

mod common;

use common::containers::PgStack;
use triad_core::{
    error::CheckpointError,
    traits::{CheckpointRow, CheckpointStore},
    types::{PatternName, PipelineName},
};
use triad_runner::checkpoint::PgCheckpointStore;

// ---------------------------------------------------------------------------
// Test 1: Load returns None when no row exists
// ---------------------------------------------------------------------------

/// On a freshly migrated schema, loading a checkpoint that was never saved
/// must return `Ok(None)`.
#[tokio::test]
async fn test_checkpoint_load_returns_none_on_empty() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect");
    let store = PgCheckpointStore::new(pool, "test-instance".to_string());

    let result = store
        .load(
            &PatternName("outbox".to_string()),
            &PipelineName("pipeline-1".to_string()),
        )
        .await
        .expect("load failed");

    assert!(result.is_none(), "fresh DB must return None for unknown checkpoint");
}

// ---------------------------------------------------------------------------
// Test 2: Save (INSERT) then load round-trip
// ---------------------------------------------------------------------------

/// `save()` with `expected_version = 0` inserts a new row.
/// A subsequent `load()` must return that row with `version = 1`.
#[tokio::test]
async fn test_checkpoint_save_and_load_roundtrip() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect");
    let store = PgCheckpointStore::new(pool, "test-instance".to_string());

    let pattern = PatternName("outbox".to_string());
    let pipeline = PipelineName("pipeline-1".to_string());

    let row = CheckpointRow {
        pattern_name: pattern.clone(),
        pipeline_name: pipeline.clone(),
        owner_instance_id: "test-instance".to_string(),
        version: 0,
        pg_lsn: Some(0x16AF6A8F4),
        kafka_offsets: Some(serde_json::json!({"part0": 42})),
        redis_watermark: Some(99),
    };

    store
        .save(&row, 0)
        .await
        .expect("INSERT (expected_version=0) failed");

    let loaded = store
        .load(&pattern, &pipeline)
        .await
        .expect("load after save failed")
        .expect("checkpoint must exist after save");

    assert_eq!(loaded.version, 1, "version must be incremented to 1 after INSERT");
    assert_eq!(loaded.redis_watermark, Some(99));
    assert_eq!(loaded.owner_instance_id, "test-instance");
}

// ---------------------------------------------------------------------------
// Test 3: CAS UPDATE and version conflict detection
// ---------------------------------------------------------------------------

/// A second `save()` with the correct `expected_version` must succeed.
/// A third `save()` using a stale `expected_version` must return `VersionConflict`.
#[tokio::test]
async fn test_checkpoint_version_conflict_returns_error() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("pg connect");
    let store = PgCheckpointStore::new(pool, "test-instance".to_string());

    let pattern = PatternName("saga".to_string());
    let pipeline = PipelineName("pipeline-2".to_string());

    let row = CheckpointRow {
        pattern_name: pattern.clone(),
        pipeline_name: pipeline.clone(),
        owner_instance_id: "test-instance".to_string(),
        version: 0,
        pg_lsn: None,
        kafka_offsets: None,
        redis_watermark: None,
    };

    // First save: INSERT (version 0 → stored as 1).
    store.save(&row, 0).await.expect("first save failed");

    // Second save: CAS UPDATE with expected_version=1 → stored as 2.
    store.save(&row, 1).await.expect("second save failed");

    // Third save: stale expected_version=1 (DB now has version=2) → VersionConflict.
    let err = store
        .save(&row, 1)
        .await
        .expect_err("stale CAS must return error");
    assert!(
        matches!(err, CheckpointError::VersionConflict),
        "stale expected_version must yield VersionConflict; got: {err:?}"
    );
}
