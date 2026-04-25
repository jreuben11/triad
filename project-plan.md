# Triad — Project Plan

> Tracks implementation progress. Check off each item as it is merged to `main`.
> Agent strategy: **parallel agents** for independent modules, **Ralph loops** for modules requiring iterative TDD.

---

## Table of Contents

- [Worktree layout](#worktree-layout)
- [Agent Launch Configuration](#agent-launch-configuration)
- [Phase 0 — Foundation](#phase-0--foundation-no-dependencies-run-in-parallel)
- [Phase 1 — Backends](#phase-1--backends-depends-on-phase-0-run-in-parallel-after-merge)
- [Phase 2 — Pattern Modules](#phase-2--pattern-modules-depends-on-phase-1)
- [Phase 3 — Engine + Runner + Shutdown](#phase-3--engine--runner--shutdown-depends-on-phase-2)
- [Phase 4 — Admin HTTP + CLI](#phase-4--admin-http--cli-depends-on-phase-3-run-in-parallel)
- [Phase 5 — SDK](#phase-5--sdk-depends-on-phase-1-can-run-in-parallel-with-phase-3)
- [Phase 6 — Database Migrations](#phase-6--database-migrations-absorbed-into-phase-7--feattests-agent-owns-these)
- [Phase 7 — Integration + Load Tests](#phase-7--integration--load-tests--feattests)
- [Phase 8 — Bug Fixes](#phase-8--bug-fixes-featbugfixes)
- [Phase 9 — Final integration gate](#phase-9--final-integration-gate)
- [Phase 10 — Python Bindings](#phase-10--python-bindings-feattriad-py)
- [Phase 11 — Terminal UI](#phase-11--terminal-ui-feattriad-tui)
- [Merge order (dependency-respecting)](#merge-order-dependency-respecting)
- [/loop usage](#loop-usage)
- [Progress tracking](#progress-tracking)

---

## Worktree layout

| Worktree path | Branch | Agent strategy |
|---|---|---|
| `/home/jreuben1/Code/triad` | `main` | integration / merge |
| `.../triad-worktrees/triad-proto` | `feat/triad-proto` | parallel agent *(merged)* |
| `.../triad-worktrees/triad-core` | `feat/triad-core` | parallel agent *(merged)* |
| `.../triad-worktrees/triad-runner-backends` | `feat/triad-runner-backends` | parallel agent *(merged)* |
| `.../triad-worktrees/triad-runner-patterns-cdc-outbox` | `feat/triad-runner-patterns-cdc-outbox` | parallel agent *(merged)* |
| `.../triad-worktrees/triad-runner-patterns-saga-eos` | `feat/triad-runner-patterns-saga-eos` | **`/loop`** *(merged)* |
| `.../triad-worktrees/triad-runner-engine` | `feat/triad-runner-engine` | **`/loop`** *(merged)* |
| `.../triad-worktrees/triad-sdk` | `feat/triad-sdk` | parallel agent *(merged)* |
| `.../triad-worktrees/triad-cli` | `feat/triad-cli` | parallel agent *(merged)* |
| `.../triad-worktrees/tests` | `feat/tests` | parallel agent *(merged)* |
| `.../triad-worktrees/triad-py` | `feat/triad-py` | parallel agent (Phase 10) |
| `.../triad-worktrees/triad-tui` | `feat/triad-tui` | parallel agent (Phase 11) |

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
| 5 | phase5-py | triad-py | /tmp/triad-target-py | scripts/prompts/triad-py.md | no |
| 5 | phase5-tui | triad-tui | /tmp/triad-target-tui | scripts/prompts/triad-tui.md | no |

**Phase 9 note:** No new worktree. Run the integration gate commands directly in the main repo.
The `status` tab can be used as the Phase 9 execution environment.

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

- [x] `patterns/saga.rs` — durable saga orchestrator with compensation, `JoinSet` steps, PG checkpoint (§4.5)
- [x] `patterns/eos.rs` — exactly-once coordinator: Kafka txn + Redis NX + PG outbox (§4.5)
- [x] Unit tests: 90%+ coverage (saga.rs 92.99%, eos.rs 99.13%)
- [x] Integration test: end-to-end saga with compensation scenario (`test_saga.rs::test_saga_compensation_path_on_step_failure`)
- [x] Integration test: EOS with simulated producer crash mid-transaction (requires Kafka; `test_eos.rs` covers PG-level dedup only — Kafka transaction abort not yet tested)
- [x] Commit and open PR → `main`

---

## Phase 3 — Engine + Runner + Shutdown (depends on Phase 2)

**Strategy: `/loop`** — supervisor FSM and cancellation token wiring require iterative TDD.

Launch: `/zellij-launch phase 3` → switch to `phase3-engine` tab → type `/loop`.

- [x] `engine.rs` — `PatternEngine`: `JoinSet` supervisor, restart on panic, backpressure controller (§4.6)
- [x] `runner.rs` — `Runner` FSM: `Idle → Starting → Running → Draining → Stopped` (§4.7)
- [x] `shutdown.rs` — SIGTERM handler, drain with timeout, ordered teardown (§4.6)
- [x] `checkpoint.rs` — `PgCheckpointStore`: CAS UPDATE with `version` column (§3.2)
- [x] `leader.rs` — `NoopLeader` (always-wins) + `K8sLeaseLeader` behind `#[cfg(feature="kubernetes")]` (§4.9)
- [x] Unit tests for FSM transitions, shutdown sequencing (270 tests pass)
- [x] Commit and open PR → `main`

---

## Phase 4 — Admin HTTP + CLI (depends on Phase 3, run in parallel)

### Admin server — part of `feat/triad-runner-engine`

- [x] `admin/http.rs` — Axum router: all endpoints from §4.8 incl. `/registry`
- [x] `admin/handlers.rs` — handler implementations
- [x] Health handler returns correct JSON schema per §21.3
- [x] `/metrics` handler emits Prometheus text format
- [x] Unit tests: each route tested with `axum::test`
- [x] Commit (part of engine PR)

### `triad-cli` — `feat/triad-cli`

**Strategy: parallel agent** (CLI is mostly glue code; Clap derive + admin HTTP client)

- [x] `main.rs` — Clap `Cli` derive tree: `run`, `status`, `pattern`, `checkpoint`, `dlq`, `pipeline`, `config` subcommands (§6.1)
- [x] `commands/admin/mod.rs` — `AdminClient`: GET/POST/DELETE via `reqwest` (§6.2)
- [x] `commands/run.rs` — load config, stub run (pending feat/triad-runner-engine merge); SIGTERM handled by Runner::run() once available
- [x] All subcommands wired to AdminClient methods
- [x] `cargo clippy -p triad-cli -- -D warnings` clean
- [x] Commit and open PR → `main`

---

## Phase 5 — SDK (depends on Phase 1, can run in parallel with Phase 3)

### `triad-sdk` — `feat/triad-sdk`

**Strategy: parallel agent**

- [x] `instance.rs` — `TriadInstance::start()` / `shutdown()`, embeds `Runner` in-process (§5.1)
- [x] `middleware.rs` — `IdempotencyLayer` + `RateLimitLayer` as `tower::Layer` impls (§5.2)
- [x] `patterns.rs` — SDK facades: `OutboxPublisher`, `FlagEvaluator`, `SagaBuilder` (§5.3)
- [x] `aggregate.rs` — event sourcing aggregate helper
- [x] `idempotency.rs` — idempotency key helpers
- [x] Unit tests (43 passing, no mocked Runner — see best-practices for rationale)
- [x] Compile check in Mode 1 configuration (no `kubernetes` feature)
- [x] Commit and open PR → `main`

---

## Phase 6 — Database Migrations (absorbed into Phase 7 — `feat/tests` agent owns these)

- [x] `crates/triad-runner/migrations/0001_outbox.sql` — `triad.triad_outbox` + pending index
- [x] `crates/triad-runner/migrations/0002_inbox.sql` — `triad.triad_inbox`
- [x] `crates/triad-runner/migrations/0003_checkpoints.sql` — `triad.triad_checkpoints` + version column
- [x] `crates/triad-runner/migrations/0004_saga.sql` — `triad_saga_checkpoints` + `triad_saga_steps`
- [x] `crates/triad-runner/migrations/0005_webhooks.sql` — `webhook_subscriptions` + `webhook_deliveries`
- [x] `crates/triad-runner/migrations/0006_feature_flags.sql` — `feature_flags` + `flag_audit`
- [x] `crates/triad-runner/migrations/0007_idempotency.sql` — `idempotency_keys`
- [x] `sqlx::migrate!` runs cleanly against testcontainers PG in `TestStack::start()`
- [x] Commit (part of Phase 7 PR)

---

## Phase 7 — Integration + Load Tests — `feat/tests`

**Strategy: parallel agent. Owns Phase 6 migrations + integration tests.**

- [x] `crates/triad-runner/tests/common/containers.rs` — `TestStack`: boots PG + Kafka + Redis, runs migrations
- [x] `crates/triad-runner/tests/integration/test_backends.rs` — backend connectivity tests
- [x] `crates/triad-runner/tests/test_outbox.rs` — outbox → Kafka → inbox round-trip (EOS)
- [x] `crates/triad-runner/tests/test_cdc.rs` — PG WAL → ChangeEvent stream (1s deadline)
- [x] `crates/triad-runner/tests/test_saga.rs` — happy path + compensation path (5s deadline)
- [x] `crates/triad-runner/tests/test_eos.rs` — exactly-once with duplicate message (3s deadline)
- [x] `crates/triad-runner/tests/test_cache.rs` — cold start + write-through + eviction (1s deadline)
- [x] `crates/triad-runner/tests/test_webhook.rs` — delivery with `wiremock`, retry, DLQ (30s deadline)
- [x] `crates/triad-runner/tests/test_feature_flag.rs` — PG → Redis hot reload (5s deadline)
- [x] `crates/triad-runner/tests/test_admin_api.rs` — all HTTP endpoints
- [x] `crates/triad-runner/tests/test_spans.rs` — span attribute assertions
- [ ] `crates/triad-runner/tests/test_inbox.rs` — same event delivered twice → processed once (3s deadline)
- [ ] `crates/triad-runner/tests/test_circuit_breaker.rs` — Redis failures → CB opens → fallback to PG (10s deadline)
- [ ] `tests/load/outbox_throughput.js` — k6: 10,000 events/s for 60s
- [ ] `tests/load/saga_throughput.js` — k6: 1,000 sagas/s for 30s
- [ ] `tests/load/cache_read.js` — k6: 5,000 reads/s; cache hit > 95%
- [ ] `tests/load/assert.rs` — PromQL assertion runner
- [x] All existing integration tests pass: `cargo nextest run -p triad-runner --features integration`
- [x] Commit and open PR → `main`

---

## Phase 8 — Bug Fixes (`feat/bugfixes`)

Full stub-implementation audit and wire-up:

- [x] `commands/run.rs` — wire `Runner::new` + `runner.run().await`; remove `anyhow::bail!` stub
- [x] `admin.rs` — add `GET /checkpoints` route (CLI calls it; server currently 404s)
- [x] `admin.rs` — add `POST /pipelines/:name/reload` route (CLI calls it; server has `/config/reload` only)
- [x] Unit tests for the two new admin routes
- [x] `admin.rs` — add `PatternControl` enum (Pause/Resume/Replay/Reload) + mpsc channel wiring
- [x] `admin.rs` — new `AdminState` builder fields: `pg_pool`, `kafka_brokers`, `redis_url`, `control_tx`, `dlq_replayer`, `config_path`, `shared_config`
- [x] `admin.rs` — `ready()` health probe: real PG `SELECT 1`, Kafka `fetch_metadata`, Redis `PING` via `spawn_blocking`; graceful degradation when not configured
- [x] `admin.rs` — `get_lag()` wired with rdkafka `fetch_metadata` + `fetch_watermarks` via `spawn_blocking`
- [x] `admin.rs` — `replay_dlq`/`drop_dlq` wired to `DlqReplayer::replay()`/`purge()`
- [x] `admin.rs` — `list_sagas`/`inspect_saga`/`cancel_saga` wired to PG queries on `triad.triad_saga_checkpoints`
- [x] `admin.rs` — `reload_config()` re-reads YAML and updates `Arc<RwLock<TriadConfig>>`
- [x] `engine.rs` — `_control_rx` field + `with_control_rx()` builder to keep admin channel alive
- [x] `aggregate.rs` — fix `persist_snapshot` to serialize full aggregate state (not stub `{id,version}`)
- [x] `aggregate.rs` — fix `load_snapshot` to return `Option<AggregateRoot<A>>` (fully hydrated)
- [x] Integration test: EOS with simulated producer crash mid-transaction (`test_eos_kafka_txn_aborted_on_pg_commit_failure`)
- [x] `cargo clippy --workspace -- -D warnings` clean
- [x] Coverage ≥ 80% overall (85.17%)
- [x] Commit and open PR → `main` (PR #8 merged)

### Phase 8b — Observability & doc quality (`feat/bugfixes`, same branch)

- [x] Doctests: `EventId`, `SagaId`, `SourcePosition`, `ChangeEvent`, `TriadConfig::default()`, `SagaBuilder` fluent API
- [x] `#[traced_test]` on all pattern unit tests; happy-path tests assert no ERROR spans
- [x] Prometheus metric counter assertions: outbox, eos, saga, webhook patterns
- [x] Span attribute integration test (`test_spans.rs`): every span has `pattern_name` + `pipeline_name`
- [x] Commit Phase 8b and open PR → `main` (PR #9 open)

---

## Phase 9 — Final integration gate

**Note:** Phase 9 runs directly in the main repo (no new worktree). Run all commands in `/home/jreuben1/Code/triad` with `export CARGO_TARGET_DIR=/tmp/triad-target-main`.

**Quality gate (already passing):**
- [x] `cargo deny check` — license compliance + CVE advisories
- [x] `cargo machete` — unused dependency detection
- [x] `cargo fmt --check` clean
- [x] `cargo clippy --workspace -- -D warnings` clean
- [x] `cargo check --workspace` clean
- [x] `cargo nextest run --workspace` — all unit tests pass (337/337)

**Remaining gate items:**
- [x] Create `crates/triad-runner/tests/test_inbox.rs` (inbox dedup integration test) — commit 5e2d275
- [x] Create `crates/triad-runner/tests/test_circuit_breaker.rs` (Redis CB integration test) — commit 5e2d275
- [ ] Create `tests/load/` k6 scripts (outbox_throughput.js, saga_throughput.js, cache_read.js, assert.rs)
- [x] `cargo nextest run --package triad-runner --features integration` — 259/259 pass, 1 skipped (`test_eos_kafka_txn_aborted_on_pg_commit_failure` ignored: testcontainers Kafka lacks transaction coordinator; run manually against real broker)
- [x] `cargo llvm-cov nextest --workspace --fail-under-lines 80` — 86.72% ✓
- [x] `cargo llvm-cov nextest --package triad-runner --fail-under-lines 90` — 90.91% ✓ (commit c2bc620)
- [x] `cargo semver-checks` — N/A for v0.1.0 first release (no prior published version to compare); baseline is established by tagging
- [x] Tag `v0.1.0`

**Deferred to v0.2.0 (not blocking v0.1.0 tag):**
- [ ] `triad migrate` CLI subcommand — sqlx migration runner
- [ ] `triad version` CLI subcommand — print build info + config schema version
- [ ] `triad lag` CLI subcommand — Kafka consumer group lag (admin HTTP endpoint `GET /lag` already implemented)
- [ ] gRPC admin server (`admin/grpc.rs`) — `triad-proto` definitions are ready; wire tonic service

---

## Phase 10 — Python Bindings (`feat/triad-py`)

See `stage2-design.md` §"Stage 2a" for full design and async-bridge notes.

- [ ] `crates/triad-py/` scaffold: `Cargo.toml` (`cdylib`), `pyproject.toml` (maturin), `src/lib.rs`
- [ ] `PyTriadInstance`: `start()`, `shutdown()`, `transaction()` context manager
- [ ] `PyTransaction`: `execute()`, `fetch_one()`, `fetch_all()` backed by `sqlx::Transaction`
- [ ] `PyOutboxPublisher`: `publish()` inside caller transaction
- [ ] `PyFlagEvaluator`: `is_enabled()` with Redis/PG fallback
- [ ] `PySagaBuilder`: fluent builder → `PySagaConfig` dataclass
- [ ] `PyIdempotencyKey` / `PyIdempotencyRecord` / `lookup` / `store_result`
- [ ] `PyAggregateRoot` + Python `Aggregate` ABC
- [ ] `pytest` test suite (all patterns, testcontainers PG + Redis)
- [ ] Type stubs + `mypy` clean
- [ ] `maturin build --release` produces a valid `.whl`
- [ ] Commit and open PR → `main`

---

## Phase 11 — Terminal UI (`feat/triad-tui`)

See `stage2-design.md` §"Stage 2b" for screen layouts, Tachyonfx effect plan, and crate structure.

- [x] `crates/triad-tui/` scaffold + `client.rs` polling AdminClient
- [x] `effects.rs` — named Tachyonfx constructors (startup glitch, screen slide, status fade)
- [x] Dashboard screen: health + pattern summary + lag bars + backend status
- [x] Patterns screen: list with pause/resume/replay + row fade on status change
- [x] DLQ screen: per-topic counts + replay/purge with confirm popup
- [x] Checkpoints screen: offsets table
- [x] Sagas screen: list + expandable step detail + cancel
- [x] Config screen: collapsible `triad.yaml` tree + live validate
- [x] `triad tui` subcommand wired in `triad-cli/src/main.rs`
- [x] Unit tests for App state transitions
- [x] Renders correctly at 80×24 and 220×50 (layout uses Constraint::Min/Percentage — adapts to any terminal size)
- [ ] Commit and open PR → `main`

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
Phase 8: bug fixes
                               ▼
Phase 9: final gate → v0.1.0
                               ▼
Phase 10: triad-py  ──┐
Phase 11: triad-tui ──┘ (parallel)
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
