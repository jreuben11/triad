Implement the full integration and load test suite per §8 of triad-physical-design.md.
Runs after all feature branches are merged to main.

## Before starting
Read `/home/jreuben1/Code/triad/claude-best-practices-learned.md` and apply all documented invariants immediately — do not rediscover known pitfalls.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/tests`

## Setup
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-tests
```

## Tasks
1. `tests/integration/helpers.rs` — `TestStack` struct that boots PG + Kafka + Redis via `testcontainers-modules` once per binary, exposes connection handles
2. `tests/integration/test_outbox.rs` — outbox write → Kafka produce → inbox consume round-trip with EOS
3. `tests/integration/test_cdc.rs` — PG INSERT → WAL → `ChangeEvent` stream received within 2s
4. `tests/integration/test_saga.rs` — happy path (all steps succeed) + compensation path (step 3 fails, steps 2+1 compensated)
5. `tests/integration/test_eos.rs` — exactly-once: crash producer mid-transaction, verify message appears exactly once on consumer
6. `tests/integration/test_cache.rs` — cold start populates Redis from PG; write-through updates both; eviction falls back to PG
7. `tests/integration/test_webhook.rs` — delivery to `wiremock` server; retry on 500; DLQ after max retries
8. `tests/integration/test_feature_flag.rs` — flag created in PG → appears in Redis within 5s hot reload window
9. `tests/integration/test_admin_api.rs` — HTTP smoke test of all admin endpoints

## Done criteria
- All tests in `#[cfg(feature = "integration")]` blocks
- `cargo nextest run --workspace --features integration` passes
- `cargo llvm-cov nextest --workspace --fail-under-lines 80` passes
- Mark all completed items `[x]` in `project-plan.md` (at `/home/jreuben1/Code/triad/project-plan.md`)
- If you discovered any new pitfalls (permission prompts, cargo/git gotchas), add them to `claude-best-practices-learned.md`
- Commit implementation **together with** `project-plan.md` and `claude-best-practices-learned.md` in a single commit on branch `feat/tests`

Output <promise>DONE</promise> when all criteria are met.
