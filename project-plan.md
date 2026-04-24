# Triad — Project Plan

> Tracks implementation progress. Check off each item as it is merged to `main`.
> Agent strategy: **parallel agents** for independent modules, **Ralph loops** for modules requiring iterative TDD.

---

## Worktree layout

| Worktree path | Branch | Agent strategy |
|---|---|---|
| `/home/jreuben1/Code/triad` | `main` | integration / merge |
| `.../triad-worktrees/triad-proto` | `feat/triad-proto` | parallel agent |
| `.../triad-worktrees/triad-core` | `feat/triad-core` | parallel agent |
| `.../triad-worktrees/triad-runner-backends` | `feat/triad-runner-backends` | parallel agent |
| `.../triad-worktrees/triad-runner-patterns-cdc-outbox` | `feat/triad-runner-patterns-cdc-outbox` | parallel agent |
| `.../triad-worktrees/triad-runner-patterns-saga-eos` | `feat/triad-runner-patterns-saga-eos` | **`/loop`** (self-paced TDD) |
| `.../triad-worktrees/triad-runner-engine` | `feat/triad-runner-engine` | **`/loop`** (self-paced TDD) |
| `.../triad-worktrees/triad-sdk` | `feat/triad-sdk` | parallel agent |
| `.../triad-worktrees/triad-cli` | `feat/triad-cli` | parallel agent |
| `.../triad-worktrees/tests` | `feat/tests` | parallel agent |

---

## Agent Launch Configuration

Drives `/zellij-launch phase N`. Each row = one zellij tab.
Worktrees base: `/home/jreuben1/Code/triad-worktrees/` — prompts relative to repo root.

| Batch | Tab name | Worktree | CARGO_TARGET_DIR | Prompt | /loop? |
|-------|----------|----------|------------------|--------|--------|
| 0 | phase0-proto | triad-proto | /tmp/triad-target-proto | scripts/prompts/triad-proto.md | no |
| 0 | phase0-core | triad-core | /tmp/triad-target-core | scripts/prompts/triad-core.md | no |
| 1 | phase1-backends | triad-runner-backends | /tmp/triad-target-runner-backends | scripts/prompts/triad-runner-backends.md | no |
| 2 | phase2-cdc-outbox | triad-runner-patterns-cdc-outbox | /tmp/triad-target-patterns-1 | scripts/prompts/triad-runner-patterns-cdc-outbox.md | no |
| 2 | phase2-saga-eos | triad-runner-patterns-saga-eos | /tmp/triad-target-saga-eos | scripts/prompts/triad-runner-patterns-saga-eos.md | yes |
| 3 | phase3-engine | triad-runner-engine | /tmp/triad-target-engine | scripts/prompts/triad-runner-engine.md | yes |
| 3 | phase3-sdk | triad-sdk | /tmp/triad-target-sdk | scripts/prompts/triad-sdk.md | no |
| 3 | phase3-cli | triad-cli | /tmp/triad-target-cli | scripts/prompts/triad-cli.md | no |
| 4 | phase4-tests | tests | /tmp/triad-target-tests | scripts/prompts/tests.md | no |

A `status` tab (interactive `claude` session) is always appended — run `/project-status` in it to check progress.

---

## Phase 0 — Foundation (no dependencies, run in parallel)

These two crates have no inter-crate dependencies and can be implemented simultaneously.

### `triad-proto` — `feat/triad-proto`

- [x] Write `proto/triad_admin.proto` (all message types + RPC service per §2.1)
- [x] Write `build.rs` with `tonic_build::configure()` per §2.2
- [x] Verify `cargo build -p triad-proto` compiles cleanly
- [x] Commit and merge → `main` (8206433)

### `triad-core` — `feat/triad-core`

- [x] `types.rs` — all domain types: `EventId`, `PatternName`, `PipelineName`, `SagaId`, `SourcePosition`, `ChangeEvent`, `Operation`, `StepContext`, `ModuleState`, `ModuleHealth`, `RunnerState`, `DeliveryGuarantee` (§3.1)
- [x] `traits.rs` — `Source`, `Sink`, `Transform`, `PatternModule`, `CheckpointStore`, `LeaderElector` with `#[automock]` gated behind `#[cfg(test)]` (§3.2)
- [x] `error.rs` — full `thiserror` error hierarchy `TriadError` + domain variants (§3.3)
- [x] `config.rs` — complete `TriadConfig` struct tree matching `triad.yaml` + all sub-configs incl. `RetryConfig`, `CircuitBreakerConfig`, `KafkaSecurityConfig` (§3.4)
- [x] `metrics.rs` — all 44 metric name constants + histogram bucket sets (§3.5)
- [x] Unit tests: 96.84% line coverage (90 tests; rstest parameterised) — config parsing, error display/From, type constructors, traits
- [x] `cargo clippy -p triad-core -- -D warnings` clean
- [x] Commit on `feat/triad-core` (976cff1)

---

## Phase 1 — Backends (depends on Phase 0, run in parallel after merge)

### `triad-runner` backends — `feat/triad-runner-backends`

- [x] `backends/postgres.rs` — `PgBackend`: sqlx pool init, migration runner, replication connection via `tokio-postgres` (§4.1)
- [x] `backends/kafka.rs` — `KafkaBackend`: `ProducerFactory` (EOS transactional), `ConsumerFactory`, topic admin (§4.2)
- [x] `backends/redis.rs` — `RedisPool` enum: Standalone / Cluster / Sentinel dispatch; deadpool-redis 0.16 has no sentinel pool so Sentinel uses `redis::sentinel::SentinelClient` directly (§4.3)
- [x] `backends/circuit_breaker.rs` — `CircuitBreaker<S>` with `watch::Sender<CbState>`, full state machine (§4.4)
- [x] Unit tests for each backend using mockall traits; no real I/O in unit tests (36 tests, all pass)
- [x] Integration tests in `tests/integration/` using `testcontainers-modules` for PG + Redis; gated behind `integration` feature
- [x] `cargo clippy -p triad-runner -- -D warnings` clean
- [x] Commit and open PR → `main` (e108cd5, PR #1)

---

## Phase 2 — Pattern Modules (depends on Phase 1)

Split into two worktrees to allow parallel work on independent patterns.

### CDC, Outbox, Inbox, Cache, Webhook, FeatureFlag, RateLimit, DLQ, FeatureStore — `feat/triad-runner-patterns-cdc-outbox`

**Strategy: parallel agent** (these patterns are complex but self-contained, mostly I/O with clear specs)

- [x] `patterns/outbox.rs` — poll `triad_outbox`, produce to Kafka inside transaction, mark published (§4.5)
- [x] `patterns/inbox.rs` — consume Kafka, dedup via `triad_inbox` in same PG txn, invoke handler (§4.5)
- [x] `patterns/cdc.rs` — WAL replication slot → `pgoutput` decoding → `ChangeEvent` stream (§4.5)
- [x] `patterns/cache.rs` — write-through / write-behind / read-through / cold-start modes (§4.5)
- [x] `patterns/webhook.rs` — HTTP delivery with retries, DLQ `triad.dlq.{topic}`, circuit breaker (§4.5)
- [x] `patterns/feature_flag.rs` — PG flag table → Redis distribution with hot reload (§4.5)
- [x] `patterns/rate_limit.rs` — Redis sliding window + token bucket (§4.5)
- [x] `patterns/dlq.rs` — `DlqRouter`: route to `triad.dlq.{source_topic}`, replay, purge (§4.5)
- [x] `patterns/feature_store.rs` — online/offline feature serving (§4.5)
- [x] Unit tests per pattern (mocked backends)
- [x] Commit and open PR → `main`

### Saga Orchestrator + EOS Coordinator — `feat/triad-runner-patterns-saga-eos`

**Strategy: `/loop`** — complex state machines + transaction semantics require iterative TDD.

Launch: `/zellij-launch phase 2` → switch to `phase2-saga-eos` tab → type `/loop`.

- [ ] `patterns/saga.rs` — durable saga orchestrator with compensation, `JoinSet` steps, PG checkpoint (§4.5)
- [ ] `patterns/eos.rs` — exactly-once coordinator: Kafka txn + Redis NX + PG outbox (§4.5)
- [ ] Unit tests: 90%+ coverage
- [ ] Integration test: end-to-end saga with compensation scenario
- [ ] Integration test: EOS with simulated producer crash mid-transaction
- [ ] Commit and open PR → `main`

---

## Phase 3 — Engine + Runner + Shutdown (depends on Phase 2)

**Strategy: `/loop`** — supervisor FSM and cancellation token wiring require iterative TDD.

Launch: `/zellij-launch phase 3` → switch to `phase3-engine` tab → type `/loop`.
```

- [ ] `engine.rs` — `PatternEngine`: `JoinSet` supervisor, restart on panic, backpressure controller (§4.6)
- [ ] `runner.rs` — `Runner` FSM: `Idle → Starting → Running → Draining → Stopped` (§4.7)
- [ ] `shutdown.rs` — SIGTERM handler, drain with timeout, ordered teardown (§4.6)
- [ ] `checkpoint.rs` — `PgCheckpointStore`: CAS UPDATE with `version` column (§3.2)
- [ ] `leader/mod.rs` — `NoopLeader` (always-wins) + `K8sLeaseLeader` behind `#[cfg(feature="kubernetes")]` (§4.9)
- [ ] Unit tests for FSM transitions, shutdown sequencing
- [ ] Commit and open PR → `main`

---

## Phase 4 — Admin HTTP + CLI (depends on Phase 3, run in parallel)

### Admin server — part of `feat/triad-runner-engine`

- [ ] `admin/http.rs` — Axum router: all endpoints from §4.8 incl. `/registry`
- [ ] `admin/handlers.rs` — handler implementations
- [ ] Health handler returns correct JSON schema per §21.3
- [ ] `/metrics` handler emits Prometheus text format
- [ ] Unit tests: each route tested with `axum::test`
- [ ] Commit (part of engine PR)

### `triad-cli` — `feat/triad-cli`

**Strategy: parallel agent** (CLI is mostly glue code; Clap derive + admin HTTP client)

- [ ] `main.rs` — Clap `Cli` derive tree: `run`, `status`, `pattern`, `checkpoint`, `dlq`, `pipeline`, `config` subcommands (§6.1)
- [ ] `commands/admin/mod.rs` — `AdminClient`: GET/POST/DELETE via `reqwest` (§6.2)
- [ ] `commands/run.rs` — load config, start `Runner`, block on SIGTERM
- [ ] All subcommands wired to AdminClient methods
- [ ] `cargo clippy -p triad-cli -- -D warnings` clean
- [ ] Commit and open PR → `main`

---

## Phase 5 — SDK (depends on Phase 1, can run in parallel with Phase 3)

### `triad-sdk` — `feat/triad-sdk`

**Strategy: parallel agent**

- [ ] `instance.rs` — `TriadInstance::start()` / `shutdown()`, embeds `Runner` in-process (§5.1)
- [ ] `middleware.rs` — `IdempotencyLayer` + `RateLimitLayer` as `tower::Layer` impls (§5.2)
- [ ] `patterns.rs` — SDK facades: `OutboxPublisher`, `FlagEvaluator`, `SagaBuilder` (§5.3)
- [ ] `aggregate.rs` — event sourcing aggregate helper
- [ ] `idempotency.rs` — idempotency key helpers
- [ ] Unit tests with mocked `Runner`
- [ ] Compile check in Mode 1 configuration (no `kubernetes` feature)
- [ ] Commit and open PR → `main`

---

## Phase 6 — Database Migrations (can run in parallel with Phase 1)

- [ ] `0001_triad_outbox.sql` — outbox table + relay_status index
- [ ] `0002_triad_inbox.sql` — inbox dedup table
- [ ] `0003_triad_checkpoints.sql` — checkpoints with `version BIGINT` + optimistic lock
- [ ] `0004_triad_saga.sql` — `triad_saga_checkpoints` + `triad_saga_steps`
- [ ] `0005_webhook.sql` — `webhook_subscriptions` + `webhook_deliveries`
- [ ] `0006_feature_flags.sql` — `feature_flags` + `flag_audit`
- [ ] `0007_idempotency_keys.sql` — idempotency key store
- [ ] `sqlx migrate run` succeeds against a fresh Postgres (testcontainers)
- [ ] Commit (part of backends PR or separate)

---

## Phase 7 — Integration + Load Tests — `feat/tests`

**Strategy: parallel agent after Phase 6 complete**

- [ ] `tests/integration/helpers.rs` — `TestStack`: boots PG + Kafka + Redis via testcontainers once per binary
- [ ] `tests/integration/test_outbox.rs` — outbox → Kafka → inbox round-trip
- [ ] `tests/integration/test_cdc.rs` — PG WAL → ChangeEvent stream
- [ ] `tests/integration/test_saga.rs` — happy path + compensation path
- [ ] `tests/integration/test_eos.rs` — exactly-once with simulated crash
- [ ] `tests/integration/test_cache.rs` — cold start + write-through + eviction
- [ ] `tests/integration/test_webhook.rs` — delivery with `wiremock`, retry, DLQ
- [ ] `tests/integration/test_feature_flag.rs` — PG → Redis hot reload
- [ ] `tests/integration/test_admin_api.rs` — all HTTP endpoints
- [ ] `tests/load/` — k6 / wrk scripts for throughput baselines
- [ ] All integration tests pass: `cargo nextest run --workspace --features integration`
- [ ] Commit and open PR → `main`

---

## Phase 8 — Final integration gate

- [ ] `cargo check --workspace` clean
- [ ] `cargo clippy --workspace -- -D warnings` clean
- [ ] `cargo fmt --check` clean
- [ ] `cargo nextest run --workspace` — all unit tests pass
- [ ] `cargo nextest run --workspace --features integration` — all integration tests pass
- [ ] `cargo llvm-cov --workspace --fail-under-lines 80` — coverage ≥ 80% overall
- [ ] `cargo llvm-cov --package triad-runner --fail-under-lines 90` — runner ≥ 90%
- [ ] Merge all feature branches to `main`
- [ ] Tag `v0.1.0`

---

## Merge order (dependency-respecting)

```
Phase 0: triad-proto ──┐
Phase 0: triad-core  ──┤ (parallel)
                       ▼
Phase 1: triad-runner-backends
Phase 6: migrations  ──┤ (can merge with Phase 1)
                       ▼
Phase 2: patterns-cdc-outbox ──┐
Phase 2: patterns-saga-eos   ──┤ (parallel within phase)
Phase 5: triad-sdk           ──┘
                               ▼
Phase 3: triad-runner-engine (+ admin)
Phase 4: triad-cli           ──┐ (parallel with engine)
                               ▼
Phase 7: tests
                               ▼
Phase 8: final gate → v0.1.0
```

---

## /loop usage

`/loop` runs the current session prompt on self-paced iterations until the task is complete.
Run it inside the agent tab after `/zellij-launch` opens it.

Two tabs require `/loop` (marked in the Agent Launch Configuration table):

| Batch | Tab | Why /loop |
|-------|-----|-----------|
| 2 | `phase2-saga-eos` | Complex state machines — TDD cycle: write test → implement → cargo test → fix → repeat |
| 3 | `phase3-engine` | Concurrency/FSM — TDD cycle until all supervisor + shutdown tests pass |

### Workflow
```
# Terminal: launch the batch
/zellij-launch phase 2

# Switch to phase2-saga-eos tab, then type:
/loop

# Switch to phase2-cdc-outbox tab — no /loop needed, agent finishes on its own
```

### Stopping a loop early
```
/loop stop
```

---

## Progress tracking

Update this file as work completes. Each checkbox maps to a commit on the feature branch.

To see current status:
```bash
git log --oneline --all --graph   # all branches at a glance
git worktree list                  # active worktrees
```

To check aggregate status (git, checklist, cargo check, tests per worktree):
```
/project-status
```
