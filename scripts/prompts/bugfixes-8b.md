Add observability and doc quality improvements (Phase 8b) to the existing `feat/bugfixes` branch.
PR #8 is already open for this branch — add commits on top of it; do NOT open a new PR.

## Before starting
Read `/home/jreuben1/Code/triad/claude-best-practices-learned.md` and apply all documented invariants immediately.
Read `project-plan.md` §"Phase 8b" for the full item list.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/bugfixes`

## Setup
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-bugfixes
```

## Tasks

### 1 — Doctests
Add `/// # Examples` doc-test blocks to:
- `crates/triad-core/src/types.rs` — `EventId::new()`, `SagaId` construction, `SourcePosition`, `ChangeEvent` constructors
- `crates/triad-core/src/config.rs` — show `TriadConfig::default()` serializes/deserializes cleanly via `serde_json`
- `crates/triad-sdk/src/patterns.rs` — show `SagaBuilder` fluent API builds a valid `SagaConfig`

Verify: `cargo test --doc -p triad-core -p triad-sdk` passes.

### 2 — Tracing in tests
- Add to `crates/triad-runner/Cargo.toml` `[dev-dependencies]`: `tracing-test = "0.2"`
- Annotate unit tests in `patterns/outbox.rs`, `patterns/eos.rs`, `patterns/saga.rs`, `patterns/cache.rs`, `patterns/webhook.rs` with `#[traced_test]` (import: `use tracing_test::traced_test;`)
- In `crates/triad-runner/tests/common/containers.rs` `TestStack::start()`: add `let _ = tracing_subscriber::fmt().with_test_writer().try_init();`
- In each annotated happy-path test, assert no ERROR log was emitted: `tracing_test` exposes `logs_contain` — assert `!logs_contain("ERROR")`

### 3 — Prometheus metric assertions
- Add to `crates/triad-runner/Cargo.toml` `[dev-dependencies]`: `metrics-util = { version = "0.17", features = ["debugging"] }`
- In unit tests for each pattern, install a `DebuggingRecorder` before running the operation, then snapshot and assert:
  - `patterns/outbox.rs`: `triad_outbox_published_total` increments on success; `triad_outbox_errors_total` on failure
  - `patterns/eos.rs`: `triad_eos_committed_total` on success; `triad_eos_aborted_total` on abort
  - `patterns/saga.rs`: `triad_saga_completed_total` on happy path; `triad_saga_rolled_back_total` on compensation
  - `patterns/webhook.rs`: `triad_webhook_delivered_total` on 2xx; `triad_webhook_dlq_total` after max retries

Pattern for installing the recorder in a test:
```rust
use metrics_util::debugging::DebuggingRecorder;
let recorder = DebuggingRecorder::new();
let snapshotter = recorder.snapshotter();
metrics::set_global_recorder(recorder).ok(); // ok() because other tests may have set it
// ... run operation ...
let snap = snapshotter.snapshot();
let counters: Vec<_> = snap.into_vec();
let found = counters.iter().any(|(k, _, _)| k.name() == "triad_outbox_published_total");
assert!(found, "counter not emitted");
```

Note: `metrics::set_global_recorder` is a one-time global. Gate metric assertion tests with `#[serial_test::serial]` if multiple tests fight over the recorder, or use `metrics::with_local_recorder` if available in the version used.

### 4 — Span attribute invariant test
- Add to `crates/triad-runner/Cargo.toml` `[dev-dependencies]` (integration feature): `opentelemetry_sdk = { version = "0.26", features = ["testing"] }`
- Create `crates/triad-runner/tests/test_spans.rs` gated `#![cfg(feature = "integration")]`
- Test: `test_spans_outbox_cycle_has_required_attributes`
  - Start Runner against testcontainers PG+Kafka+Redis
  - Install `opentelemetry_sdk::testing::trace::InMemorySpanExporter`
  - Run one outbox poll cycle
  - Collect finished spans from the exporter
  - Assert every span has attributes `pattern_name` and `pipeline_name` set (non-empty strings)

## Done criteria
- `cargo test --doc -p triad-core -p triad-sdk` passes
- All pattern unit tests have `#[traced_test]`; happy-path tests assert no ERROR spans
- Metric counter assertions present and passing for outbox, eos, saga, webhook
- `test_spans.rs` integration test passes
- `cargo fmt --check` clean
- `cargo clippy --workspace -- -D warnings` clean
- `cargo nextest run --workspace` passes
- `cargo nextest run --workspace --features integration` passes
- Mark all Phase 8b items `[x]` in `project-plan.md` (at `/home/jreuben1/Code/triad/project-plan.md`)
- Update `claude-best-practices-learned.md` with any new pitfalls
- Commit all changes **together with** `project-plan.md` and `claude-best-practices-learned.md` on branch `feat/bugfixes`
- Do NOT open a new PR — PR #8 already covers this branch

Output <promise>DONE</promise> when all criteria are met.
