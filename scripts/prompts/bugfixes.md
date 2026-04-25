Fix all stub implementations identified in the Phase 8 audit. Every item in `## Phase 8 — Bug Fixes` of `project-plan.md` must be addressed.

## Before starting
Read `/home/jreuben1/Code/triad/claude-best-practices-learned.md` and apply all documented invariants immediately — do not rediscover known pitfalls.
Read `project-plan.md` §"Phase 8 — Bug Fixes" for the full item list.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/bugfixes`

## Setup
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-bugfixes
```

## Tasks (work through in order)

### 1 — Wire `commands/run.rs`
`crates/triad-cli/src/commands/run.rs` currently calls `anyhow::bail!("not implemented")`.
Replace it with:
- Load `TriadConfig` from the path in `RunArgs`
- Construct `Runner::new(config)`
- Call `runner.run().await`
- Handle SIGTERM via `tokio::signal::unix`

### 2 — Add missing admin routes
`crates/triad-runner/src/admin.rs` is missing two routes the CLI calls:
- `GET /checkpoints` — query `triad.triad_checkpoints` and return list
- `POST /pipelines/:name/reload` — trigger config reload for named pipeline

Wire these into the Axum router and write unit tests with `axum::test`.

### 3 — Fix `ready()` health probe
`admin.rs ready()` hardcodes `"ok"` for all backends. Replace with real probes:
- PG: `SELECT 1` via sqlx pool
- Kafka: metadata fetch via `rdkafka` AdminClient
- Redis: `PING` via deadpool-redis

### 4 — Wire pattern control signals (pause/resume/replay)
`pause_pattern()`, `resume_pattern()`, `replay_pattern()` all log-only.
The `PatternEngine` has module state — wire `mpsc` or `watch` channels so the HTTP handlers actually signal the running module. If the engine doesn't have a control channel yet, add one.

### 5 — Wire Kafka lag query (`get_lag()`)
`get_lag()` returns `[]`. Use `rdkafka` consumer-group metadata to return real consumer lag per topic-partition.

### 6 — Wire DLQ handlers (`list_dlq`, `replay_dlq`, `drop_dlq`)
These log-only. Wire to the `DlqRouter` in `patterns/dlq.rs`:
- `list_dlq` → count messages in `triad.dlq.{topic}`
- `replay_dlq` → re-publish from DLQ to source topic
- `drop_dlq` → purge the DLQ topic

### 7 — Wire saga handlers (`list_sagas`, `inspect_saga`, `cancel_saga`)
These return empty/404. Wire to PG queries against `triad.triad_saga_checkpoints`:
- `list_sagas` → SELECT all rows, return summary list
- `inspect_saga` → SELECT by saga_id, return full checkpoint
- `cancel_saga` → UPDATE status to 'Cancelled' for the given saga_id

### 8 — Wire `reload_config()`
Returns 202 with no action. Trigger actual config file re-read and propagation to backends. If hot-reload isn't implemented in `Runner`, add a `reload()` method that re-reads the YAML and updates the config Arc.

### 9 — Fix `AggregateRoot` snapshot serialization
`crates/triad-sdk/src/aggregate.rs`:
- `persist_snapshot()` currently only stores `{aggregate_id, version}` — serialize the full aggregate state using `serde_json::to_value(self.state())`
- `load_snapshot()` must deserialize back to the concrete state type via `serde_json::from_value`
- Add `Aggregate::state()` method to the trait if not present, or use an existing serialization hook

### 10 — EOS Kafka crash integration test
`crates/triad-runner/tests/test_eos.rs` has no Kafka. Add:
- `test_eos_kafka_txn_aborted_on_pg_commit_failure` — use a `PgStack` + Kafka container, inject a PG commit failure mid-transaction, verify the Kafka producer transaction is aborted and no message is delivered

## Phase 8b — Observability & doc quality (same branch, do after Phase 8)

### 11 — Doctests
- `triad-core/src/types.rs` — add `/// # Examples` blocks for `EventId`, `SagaId`, `SourcePosition`, `ChangeEvent` constructors
- `triad-core/src/config.rs` — doctest showing `TriadConfig::default()` round-trips through `serde_json`
- `triad-sdk/src/patterns.rs` — doctest showing `SagaBuilder` fluent API produces a valid `SagaConfig`
- Verify `cargo test --doc -p triad-core -p triad-sdk` passes

### 12 — Tracing in tests
- Add `tracing-test = "0.2"` to `[dev-dependencies]` in `triad-runner/Cargo.toml`
- Annotate all pattern unit tests with `#[traced_test]`
- Add `tracing_subscriber::fmt().with_test_writer().init()` to `TestStack::start()`
- Assert no `ERROR`-level spans on happy-path unit tests for `outbox`, `eos`, `saga`, `cache`, `webhook`

### 13 — Prometheus metric assertions
- Add `metrics-util = { version = "0.17", features = ["debugging"] }` to `[dev-dependencies]` in `triad-runner/Cargo.toml`
- `outbox` unit tests: assert `triad_outbox_published_total` and `triad_outbox_errors_total`
- `eos` unit tests: assert `triad_eos_committed_total` and `triad_eos_aborted_total`
- `saga` unit tests: assert `triad_saga_completed_total` and `triad_saga_rolled_back_total`
- `webhook` unit tests: assert `triad_webhook_delivered_total` and `triad_webhook_dlq_total`

### 14 — Span attribute invariant test
- Add `opentelemetry_sdk = { version = "0.26", features = ["testing"] }` to `[dev-dependencies]` in `triad-runner/Cargo.toml`
- `tests/test_spans.rs` — start Runner against testcontainers stack, run one outbox cycle, assert every emitted span has `pattern_name` and `pipeline_name` fields

## Done criteria
- `commands/run.rs` has no `bail!` stub — real Runner wiring
- All 12 admin handler stubs replaced with real implementations
- `AggregateRoot::persist_snapshot()` serializes full state
- EOS Kafka crash integration test passes
- Doctests pass: `cargo test --doc -p triad-core -p triad-sdk`
- All pattern unit tests annotated with `#[traced_test]`; no spurious ERROR spans on happy paths
- Metric counter assertions present for outbox, eos, saga, webhook
- Span attribute integration test passes
- `cargo fmt --check` clean
- `cargo clippy --workspace -- -D warnings` clean
- `cargo nextest run --workspace` passes
- `cargo nextest run --workspace --features integration` passes
- `cargo llvm-cov nextest --workspace --fail-under-lines 80` passes
- Mark all Phase 8 and Phase 8b items `[x]` in `project-plan.md` (at `/home/jreuben1/Code/triad/project-plan.md`)
- Update `claude-best-practices-learned.md` with any new pitfalls discovered
- Commit implementation **together with** `project-plan.md` and `claude-best-practices-learned.md` in a single commit on branch `feat/bugfixes`
- Open PR: `gh pr create --title "fix: wire stubs, add observability tests (Phase 8+8b)" --body "Fixes all stub handlers, wires CLI run command, fixes AggregateRoot snapshot, adds EOS Kafka crash test, adds doctests, traced_test annotations, metric counter assertions, and span attribute integration test. All tests pass; coverage ≥80%."`

Output <promise>DONE</promise> when all criteria are met.
