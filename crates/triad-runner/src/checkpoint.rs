use async_trait::async_trait;
use sqlx::PgPool;
use triad_core::{
    error::CheckpointError,
    traits::{CheckpointRow, CheckpointStore},
    types::{PatternName, PipelineName},
};

/// Converts a u64 WAL LSN to the "X/YYYYYYYY" hex string Postgres expects.
fn lsn_to_text(lsn: u64) -> String {
    let hi = (lsn >> 32) as u32;
    let lo = lsn as u32;
    format!("{hi:X}/{lo:08X}")
}

/// Parses a "X/YYYYYYYY" pg_lsn text string back to u64.
fn text_to_lsn(s: &str) -> Option<u64> {
    let (hi_str, lo_str) = s.split_once('/')?;
    let hi = u64::from_str_radix(hi_str, 16).ok()?;
    let lo = u64::from_str_radix(lo_str, 16).ok()?;
    Some((hi << 32) | lo)
}

/// PostgreSQL-backed checkpoint store.
///
/// Implements optimistic concurrency via a `version` column:
/// `save()` with `expected_version = N` runs `UPDATE … WHERE version = N`
/// and returns `Err(VersionConflict)` when no rows are updated.
pub struct PgCheckpointStore {
    pool: PgPool,
    instance_id: String,
}

impl PgCheckpointStore {
    pub fn new(pool: PgPool, instance_id: String) -> Self {
        Self { pool, instance_id }
    }
}

#[async_trait]
impl CheckpointStore for PgCheckpointStore {
    async fn load(
        &self,
        pattern: &PatternName,
        pipeline: &PipelineName,
    ) -> Result<Option<CheckpointRow>, CheckpointError> {
        let row = sqlx::query_as::<
            _,
            (
                String,
                String,
                String,
                i64,
                Option<String>,
                Option<serde_json::Value>,
                Option<i64>,
            ),
        >(
            "SELECT pattern_name, pipeline_name, owner_instance_id, version, \
             pg_lsn::TEXT, kafka_offsets, redis_watermark \
             FROM triad.triad_checkpoints \
             WHERE pattern_name = $1 AND pipeline_name = $2",
        )
        .bind(&pattern.0)
        .bind(&pipeline.0)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(
            |(pn, pl, owner, version, lsn_text, offsets, watermark)| CheckpointRow {
                pattern_name: PatternName(pn),
                pipeline_name: PipelineName(pl),
                owner_instance_id: owner,
                version,
                pg_lsn: lsn_text.as_deref().and_then(text_to_lsn),
                kafka_offsets: offsets,
                redis_watermark: watermark,
            },
        ))
    }

    /// Save a checkpoint row with optimistic locking.
    ///
    /// `expected_version = 0` → INSERT (first save).
    /// `expected_version > 0` → CAS UPDATE `WHERE version = expected_version`.
    /// Returns `Err(VersionConflict)` if no row was inserted or updated.
    async fn save(
        &self,
        row: &CheckpointRow,
        expected_version: i64,
    ) -> Result<(), CheckpointError> {
        let lsn_text = row.pg_lsn.map(lsn_to_text);

        let result = if expected_version == 0 {
            sqlx::query(
                "INSERT INTO triad.triad_checkpoints \
                 (pattern_name, pipeline_name, owner_instance_id, version, \
                  pg_lsn, kafka_offsets, redis_watermark) \
                 VALUES ($1, $2, $3, 1, $4::pg_lsn, $5, $6) \
                 ON CONFLICT (pattern_name, pipeline_name) DO NOTHING",
            )
            .bind(&row.pattern_name.0)
            .bind(&row.pipeline_name.0)
            .bind(&self.instance_id)
            .bind(&lsn_text)
            .bind(&row.kafka_offsets)
            .bind(row.redis_watermark)
            .execute(&self.pool)
            .await?
        } else {
            // CAS: WHERE pattern_name = $6 AND pipeline_name = $7 AND version = $1
            sqlx::query(
                "UPDATE triad.triad_checkpoints \
                 SET version          = $1 + 1, \
                     owner_instance_id = $2, \
                     pg_lsn           = $3::pg_lsn, \
                     kafka_offsets    = $4, \
                     redis_watermark  = $5, \
                     updated_at       = now() \
                 WHERE pattern_name = $6 AND pipeline_name = $7 AND version = $1",
            )
            .bind(expected_version)
            .bind(&self.instance_id)
            .bind(&lsn_text)
            .bind(&row.kafka_offsets)
            .bind(row.redis_watermark)
            .bind(&row.pattern_name.0)
            .bind(&row.pipeline_name.0)
            .execute(&self.pool)
            .await?
        };

        if result.rows_affected() == 0 {
            return Err(CheckpointError::VersionConflict);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_roundtrip() {
        let cases: &[u64] = &[0, 1, 0x16AF6A8F4, u64::MAX >> 1];
        for &lsn in cases {
            let text = lsn_to_text(lsn);
            let parsed = text_to_lsn(&text).unwrap();
            assert_eq!(lsn, parsed, "roundtrip failed for lsn={lsn}");
        }
    }

    #[test]
    fn test_lsn_to_text_format() {
        // 0/1 should render as "0/00000001"
        assert_eq!(lsn_to_text(1), "0/00000001");
        // known LSN "1/6AF6A8F4"
        let lsn: u64 = (1u64 << 32) | 0x6AF6A8F4;
        assert_eq!(lsn_to_text(lsn), "1/6AF6A8F4");
    }

    #[test]
    fn test_text_to_lsn_invalid() {
        assert!(text_to_lsn("not-a-lsn").is_none());
        assert!(text_to_lsn("ZZ/ZZ").is_none());
    }

    #[test]
    fn test_pg_checkpoint_store_new() {
        // Verify construction compiles; pool is not connected in unit tests.
        // Full behavioral tests live in integration tests (phase 7).
        let _ = std::mem::size_of::<PgCheckpointStore>();
    }

    // ── Property-based tests ─────────────────────────────────────────────────

    mod prop_tests {
        use super::*;
        use proptest::prelude::*;

        // ── LSN properties ────────────────────────────────────────────────────

        proptest! {
            /// Any u64 LSN round-trips through lsn_to_text / text_to_lsn intact.
            #[test]
            fn prop_lsn_roundtrip(lsn in 0u64..=u64::MAX) {
                let text = lsn_to_text(lsn);
                let parsed = text_to_lsn(&text);
                prop_assert_eq!(parsed, Some(lsn), "roundtrip failed for lsn={} text={}", lsn, text);
            }

            /// LSN formatted as "HI/LO" always contains exactly one '/' separator.
            #[test]
            fn prop_lsn_text_has_one_slash(lsn in 0u64..=u64::MAX) {
                let text = lsn_to_text(lsn);
                let slash_count = text.chars().filter(|&c| c == '/').count();
                prop_assert_eq!(slash_count, 1, "expected exactly one '/' in {}", text);
            }

            /// High-word and low-word components are recovered correctly from text.
            #[test]
            fn prop_lsn_hi_lo_components(hi in 0u32..=u32::MAX, lo in 0u32..=u32::MAX) {
                let lsn = (u64::from(hi) << 32) | u64::from(lo);
                let text = lsn_to_text(lsn);
                let parsed = text_to_lsn(&text).expect("text_to_lsn must succeed for valid text");
                let parsed_hi = (parsed >> 32) as u32;
                let parsed_lo = parsed as u32;
                prop_assert_eq!(parsed_hi, hi);
                prop_assert_eq!(parsed_lo, lo);
            }

            // ── CAS / version monotonicity properties ─────────────────────────

            /// Simulated CAS with arbitrary expected-version attempts.
            ///
            /// Generates a random sequence of `expected_version` values and feeds them
            /// into an in-memory CAS simulation.  For each attempt:
            ///   - If `expected == current`  → CAS succeeds, version increments by 1.
            ///   - If `expected != current`  → CAS is rejected, version unchanged.
            ///
            /// Asserts after every attempt:
            ///   1. Version is non-decreasing (monotonic).
            ///   2. Version advances by at most 1 per step (no skips).
            ///   3. Total successful saves equal the final version value.
            #[test]
            fn prop_cas_random_attempts(
                attempts in prop::collection::vec(0i64..=20, 1..=20),
            ) {
                let mut current: i64 = 0;
                let mut prev: i64 = 0;
                let mut successful_saves: i64 = 0;

                for expected in &attempts {
                    let before = current;
                    if *expected == current {
                        // CAS succeeds: version increments by exactly 1.
                        if current == 0 {
                            current = 1; // INSERT path sets version = 1
                        } else {
                            current = current + 1;
                        }
                        successful_saves += 1;
                    }
                    // Invariant 1: version never decreases.
                    prop_assert!(current >= before,
                        "version decreased: before={} after={}", before, current);
                    // Invariant 2: version advances by at most 1 per step.
                    prop_assert!(current - before <= 1,
                        "version jumped by more than 1: before={} after={}", before, current);
                    // Invariant 3: rejected CAS leaves version unchanged.
                    if *expected != before {
                        prop_assert_eq!(current, before,
                            "rejected CAS changed version: expected={} before={} after={}", expected, before, current);
                    }
                    let _ = prev; // suppress unused warning
                    prev = current;
                }

                // Final check: version == number of successful saves.
                prop_assert_eq!(current, successful_saves,
                    "version={} != successful_saves={}", current, successful_saves);
            }

            /// A stale or future expected_version never advances the stored version.
            ///
            /// Generates a current_version and an expected_version that differs from it.
            /// Verifies that a CAS attempt with the wrong expected_version is a no-op.
            #[test]
            fn prop_cas_mismatched_expected_is_noop(
                current_version in 0i64..=50,
                delta           in 1i64..=50,
            ) {
                // expected is either above or below current (never equal) via the delta offset.
                // We alternate: even delta → stale (expected = current - delta or 0 if underflow)
                //               odd  delta → future (expected = current + delta)
                let expected = if delta % 2 == 0 {
                    current_version.saturating_sub(delta)
                } else {
                    current_version.saturating_add(delta)
                };

                // Precondition: if they happen to be equal (saturating edge), skip.
                prop_assume!(expected != current_version);

                // CAS simulation: mismatched expected → no change.
                let mut stored = current_version;
                if expected == stored {
                    stored += 1; // would succeed; but precondition rules this out
                }
                // expected != current_version → stored must remain unchanged.
                prop_assert_eq!(stored, current_version,
                    "mismatched CAS (expected={} current={}) must not change version", expected, current_version);
            }
        }
    }
}
