Implement the database migrations (§7) and full integration + load test suite (§8) of triad-physical-design.md.
Runs after all feature branches are merged to main.

## Before starting
Read `/home/jreuben1/Code/triad/claude-best-practices-learned.md` and apply all documented invariants immediately — do not rediscover known pitfalls.
Read `triad-physical-design.md` §7 (Database Schema) and §8 (Testing Structure) for the authoritative SQL DDL and test layout.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/tests`

## Setup
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-tests
```

## Tasks

### Step 0 — SQL migrations (Phase 6, required before any integration test can run)
Write all 7 migration files to `crates/triad-runner/migrations/` using the exact DDL from §7:
- `0001_outbox.sql` — `triad.triad_outbox` table + pending index
- `0002_inbox.sql` — `triad.triad_inbox` table
- `0003_checkpoints.sql` — `triad.triad_checkpoints` table + instance index
- `0004_saga.sql` — `triad.triad_saga_checkpoints` + `triad.triad_saga_steps` tables
- `0005_webhooks.sql` — `triad.webhook_subscriptions` + `triad.webhook_deliveries` tables
- `0006_feature_flags.sql` — `triad.feature_flags` + `triad.flag_audit` tables
- `0007_idempotency.sql` — `triad.idempotency_keys` table

Each file must begin with `CREATE SCHEMA IF NOT EXISTS triad;` so they are idempotent on a fresh DB.

### Step 1 — TestStack helper
`crates/triad-runner/tests/common/containers.rs` — `TestStack` struct per §8.2:
- Boot PG + Kafka + Redis via `testcontainers-modules` once per binary
- Run `sqlx::migrate!("./migrations")` against the PG container after it starts
- Expose `pg_url`, `kafka_url`, `redis_url` as `String` fields

### Step 2 — Integration tests (all in `crates/triad-runner/tests/`)
- `test_outbox.rs` — outbox write → Kafka produce → inbox consume round-trip with EOS (deadline: 2s)
- `test_cdc.rs` — PG INSERT → WAL → `ChangeEvent` stream received (deadline: 1s)
- `test_saga.rs` — happy path (all steps succeed) + compensation path (step 2 fails → steps rolled back) (deadline: 5s)
- `test_eos.rs` — exactly-once: duplicate Kafka message → processed exactly once (deadline: 3s)
- `test_cache.rs` — cold start populates Redis from PG; write-through updates both; eviction falls back to PG (deadline: 1s)
- `test_webhook.rs` — delivery to `wiremock` server; retry on 500; DLQ after max retries (deadline: 30s)
- `test_feature_flag.rs` — flag created in PG → appears in Redis within 5s hot reload window
- `test_admin_api.rs` — HTTP smoke test of all admin endpoints (start Runner, hit each route)

All tests must be gated behind `#[cfg(feature = "integration")]`.

### Step 3 — Load test stubs
Create placeholder scripts in `tests/load/` (k6 JS files per §8.3):
- `outbox_throughput.js`, `saga_throughput.js`, `cache_read.js`
- `assert.rs` stub (can be empty `fn main() {}` — load tests require a running Prometheus)

## Done criteria
- All migration files present and valid SQL
- `cargo nextest run -p triad-runner --features integration` passes
- `cargo llvm-cov nextest --workspace --fail-under-lines 80` passes
- Mark all Phase 6 and Phase 7 items `[x]` in `project-plan.md` (at `/home/jreuben1/Code/triad/project-plan.md`)
- If you discovered any new pitfalls (permission prompts, cargo/git gotchas), add them to `claude-best-practices-learned.md`
- Commit implementation **together with** `project-plan.md` and `claude-best-practices-learned.md` in a single commit on branch `feat/tests`
- Open a pull request: `gh pr create --title "test: DB migrations + full integration test suite" --body "Implements §7 (migrations) and §8 (integration tests) of triad-physical-design.md. All integration tests pass; workspace coverage ≥80%."`

Output <promise>DONE</promise> when all criteria are met.
