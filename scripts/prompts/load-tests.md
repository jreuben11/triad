# Agent task: goose load test crate

## Worktree
`/home/jreuben1/Code/triad-worktrees/load-tests` — branch `feat/load-tests`

```bash
export CARGO_TARGET_DIR=/tmp/triad-target-load-tests
```

## Context

The project plan has one unchecked item in Phase 9:
> `[ ] Create tests/load/ k6 scripts (outbox_throughput.js, saga_throughput.js, cache_read.js, assert.rs)`

We are implementing load tests in Rust using the `goose` crate instead of k6 JS scripts.
`goose` is a Rust async load testing framework (https://docs.rs/goose) built on Tokio — it is
the Rust-native equivalent of Locust. Scripts compile and run as Rust binaries against a live
Triad stack (PostgreSQL + Kafka + Redis + `triad run`). These are NOT unit tests and do NOT
run in CI — they are developer-run performance validation binaries.

The admin HTTP server runs on port 8080. The patterns under test are the outbox, saga, and
cache patterns already implemented in `triad-runner`.

## Goal

Create a `tests/load/` crate in the workspace with four files:

### Crate layout

```
tests/load/
  Cargo.toml          # workspace member, three [[bin]] targets
  src/
    lib.rs            # shared helpers
    bin/
      outbox_throughput.rs
      saga_throughput.rs
      cache_read.rs
```

### 1. Add to workspace

In the root `Cargo.toml`, add `"tests/load"` to the `[workspace] members` array.

### 2. `tests/load/Cargo.toml`

```toml
[package]
name    = "triad-loadtest"
version = "0.1.0"
edition = "2021"
publish = false

[lib]

[[bin]]
name = "outbox_throughput"
path = "src/bin/outbox_throughput.rs"

[[bin]]
name = "saga_throughput"
path = "src/bin/saga_throughput.rs"

[[bin]]
name = "cache_read"
path = "src/bin/cache_read.rs"

[dependencies]
goose    = "0.17"
tokio    = { version = "1", features = ["full"] }
serde_json = "1"
```

### 3. `tests/load/src/lib.rs` — shared helpers

Provide:
- `base_url() -> String` — reads `TRIAD_BASE_URL` env var, defaults to `http://localhost:8080`
- `check_health(user: &mut GooseUser) -> TransactionResult` — GET `/health/ready`, asserts status 200
- `random_id() -> String` — generates a random hex string (use `std::collections::hash_map::DefaultHasher` seeded from `SystemTime`)
- `assert_p95(metrics: &GooseMetrics, scenario: &str, max_ms: u64)` — reads `metrics.scenarios[scenario].response_time_percentile_95` and panics with a clear message if it exceeds `max_ms`
- `scrape_metrics() -> HashMap<String, f64>` — synchronous HTTP GET to `{base_url()}/metrics`,
  parses the Prometheus text exposition format line-by-line, returns a map of
  `metric_name -> value` (skip lines starting with `#`; parse `<name> <value>` or
  `<name>{<labels>} <value>`; use the `reqwest::blocking` client or `std::net::TcpStream`
  if you want to avoid an extra async dep — `reqwest` with `blocking` feature is fine)
- `assert_counter_increased(before: f64, after: f64, metric: &str)` — panics with a clear
  message if `after <= before`

Add `reqwest = { version = "0.12", features = ["blocking"] }` to `[dependencies]` in
`tests/load/Cargo.toml` for the blocking scrape helper.

### 4. `tests/load/src/bin/outbox_throughput.rs`

Load test for the transactional outbox pattern:
- **Before** starting goose: call `scrape_metrics()` and record the value of
  `triad_outbox_relay_published_total` as `counter_before`
- Register one task: GET `/lag` (measures outbox Kafka consumer lag)
- Users: ramp from 1 → 50 over 30 s, hold 60 s, ramp down 30 s
  (set via `GooseDefault::Users`, `GooseDefault::HatchRate`, `GooseDefault::RunTime`)
- After execution, call `assert_p95` with threshold 200 ms
- **After** execution: call `scrape_metrics()` again and call
  `assert_counter_increased(counter_before, counter_after, "triad_outbox_relay_published_total")`
- Print final metrics summary to stdout

### 5. `tests/load/src/bin/saga_throughput.rs`

Load test for the saga pattern:
- **Before**: scrape and record `triad_saga_completed_total` as `counter_before`
- Register one task: GET `/saga` (list in-flight sagas)
- Users: ramp from 1 → 20 over 30 s, hold 60 s, ramp down
- After execution, `assert_p95` threshold 500 ms
- Assert error rate < 2%: `metrics.scenarios["SagaThroughput"].fail_count / total_count < 0.02`
- **After**: assert `triad_saga_completed_total` increased

### 6. `tests/load/src/bin/cache_read.rs`

Load test for the Redis cache-aside read path:
- **Before**: scrape and record `triad_cache_hits_total` and `triad_cache_misses_total`
- Register one task: GET `/health/ready` (exercises Redis liveness check)
- Users: ramp from 1 → 100 over 20 s, hold 60 s, ramp down
- After execution, `assert_p95` threshold 50 ms
- Assert error rate < 0.5%
- **After**: scrape again, compute hit rate `hits_delta / (hits_delta + misses_delta)`,
  print it (do not assert a minimum — the stack may be cold)

### 7. `tests/load/README.md`

10–20 lines explaining how to run:

```
# Triad Load Tests

These are Rust-native load tests using the `goose` crate.
Requires a running Triad stack (Postgres + Kafka + Redis + `triad run`).

## Run

cargo run --manifest-path tests/load/Cargo.toml --bin outbox_throughput
cargo run --manifest-path tests/load/Cargo.toml --bin saga_throughput
cargo run --manifest-path tests/load/Cargo.toml --bin cache_read

## Override target URL

TRIAD_BASE_URL=http://prod:8080 cargo run --manifest-path tests/load/Cargo.toml --bin cache_read

## Goose flags (appended after --)

cargo run --bin outbox_throughput -- --users 10 --run-time 30s --report-file report.html
```

## Goose pattern reference

```rust
use goose::prelude::*;
use triad_loadtest::{base_url, check_health};

async fn task_get_lag(user: &mut GooseUser) -> TransactionResult {
    let _res = user.get("/lag").await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    let metrics = GooseAttack::initialize()?
        .set_default(GooseDefault::Host, base_url().as_str())?
        .set_default(GooseDefault::Users, 50_usize)?
        .set_default(GooseDefault::HatchRate, "1.67")?   // 50 users / 30 s
        .set_default(GooseDefault::RunTime, 120_usize)?  // 30s ramp + 60s hold + 30s ramp
        .register_scenario(
            scenario!("OutboxThroughput")
                .register_transaction(transaction!(check_health).set_on_start())
                .register_transaction(transaction!(task_get_lag)),
        )
        .execute()
        .await?;

    triad_loadtest::assert_p95(&metrics, "OutboxThroughput", 200);
    Ok(())
}
```

## Quality gate

The load test crate must compile cleanly as part of the workspace:

```bash
cargo fmt --check --manifest-path /home/jreuben1/Code/triad-worktrees/load-tests/Cargo.toml
cargo clippy --workspace --manifest-path /home/jreuben1/Code/triad-worktrees/load-tests/Cargo.toml -- -D warnings
cargo build --manifest-path /home/jreuben1/Code/triad-worktrees/load-tests/Cargo.toml
```

Do NOT run `cargo nextest` — these are not unit tests, they need a live stack.
`cargo build` (exit 0) is the gate; runtime behaviour is validated manually.

## Three-file discipline

Before committing, update:
- `project-plan.md` — check off the `[ ] Create tests/load/` item in Phase 9; update the description to say "goose Rust binaries" not "k6 scripts"
- `CLAUDE.md` — no change needed
- `claude-best-practices-learned.md` — record any goose/workspace gotcha found

Also add `tests/load/README.md` as described above.

## Agent event publishing

```bash
printf '{"ts":"%s","agent":"load-tests","phase":9,"event":"phase_started","detail":"writing goose load test crate","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl

printf '{"ts":"%s","agent":"load-tests","phase":9,"event":"agent_done","detail":"goose crate + README committed, PR opened","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
```

## Done criteria

- [ ] `tests/load/` added to workspace `Cargo.toml` members
- [ ] `tests/load/Cargo.toml` — three `[[bin]]` targets, `goose = "0.17"` + `reqwest` blocking dep
- [ ] `tests/load/src/lib.rs` — `base_url`, `check_health`, `random_id`, `assert_p95`, `scrape_metrics`, `assert_counter_increased`
- [ ] `tests/load/src/bin/outbox_throughput.rs` — compiles, scrapes `triad_outbox_relay_published_total` before/after
- [ ] `tests/load/src/bin/saga_throughput.rs` — compiles, scrapes `triad_saga_completed_total` before/after
- [ ] `tests/load/src/bin/cache_read.rs` — compiles, computes cache hit rate from `triad_cache_hits_total` / `triad_cache_misses_total`
- [ ] `tests/load/README.md` — run instructions including Prometheus scrape note
- [ ] `cargo build --manifest-path tests/load/Cargo.toml` exits 0
- [ ] `cargo clippy -- -D warnings` clean across workspace
- [ ] Phase 9 load-test checkbox checked in `project-plan.md`
- [ ] Three-file discipline committed
- [ ] PR opened against `main`
