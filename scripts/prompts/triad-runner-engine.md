Implement `engine.rs`, `runner.rs`, `checkpoint.rs`, `shutdown.rs`, `leader/`, and `admin/`
per §4.6–§4.9 of triad-physical-design.md.
Use TDD: write failing tests first, then implement.

## Before starting
Read `/home/jreuben1/Code/triad/claude-best-practices-learned.md` and apply all documented invariants immediately — do not rediscover known pitfalls.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/triad-runner-engine`

## Setup
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-engine
```

## Tasks

### checkpoint.rs
- `PgCheckpointStore` implementing `CheckpointStore` trait from triad-core
- UPDATE must use `WHERE id = $1 AND version = $2` — optimistic locking, return `Err(StaleVersion)` on mismatch

### shutdown.rs
- Install SIGTERM handler via `tokio::signal`
- `ShutdownCoordinator`: holds root `CancellationToken`, calls `cancel()` on SIGTERM
- Respects `drain_timeout_seconds` from config — force-kills after timeout

### leader/mod.rs
- `NoopLeader`: always returns `is_leader() = true` (Mode 1 + Mode 2)
- `K8sLeaseLeader`: acquires `coordination.k8s.io/v1 Lease`, renews every 5s, `leaseDurationSeconds = 15`
- Gate `K8sLeaseLeader` behind `#[cfg(feature = "kubernetes")]`

### engine.rs
- `PatternEngine`: holds `JoinSet<()>` of pattern module tasks
- Supervisor loop: if a task panics/errors, log error + restart it (with backoff)
- Backpressure controller: if Redis memory > 80% threshold, call `consumer.pause(partitions)`
- Receives root `CancellationToken` from Runner, propagates child tokens to each module

### runner.rs
- `Runner` FSM: `Idle → Starting → Running → Draining → Stopped`
- `Starting`: init backends, apply migrations, elect leader, start engine
- `Draining`: cancel all tasks, wait for drain with timeout, flush checkpoints
- Expose `run()` async fn that blocks until `Stopped`

### admin/http.rs + admin/handlers.rs
- Axum router with all endpoints from §4.8 of physical design incl. `/registry`
- Health handlers return JSON matching §21.3 schema
- `/metrics` handler emits Prometheus text format via `metrics-exporter-prometheus`

## Done criteria
- `cargo test -p triad-runner` unit tests pass (FSM transitions, shutdown sequence, CAS checkpoint)
- `cargo clippy -p triad-runner -- -D warnings` clean
- `cargo check --workspace` clean (engine integrates with all other crates)
- Mark all completed items `[x]` in `project-plan.md` (at `/home/jreuben1/Code/triad/project-plan.md`)
- If you discovered any new pitfalls (permission prompts, cargo/git gotchas), add them to `claude-best-practices-learned.md`
- Commit implementation **together with** `project-plan.md` and `claude-best-practices-learned.md` in a single commit on branch `feat/triad-runner-engine`
- Open a pull request: `gh pr create --title "feat(runner): implement PatternEngine, Runner FSM, shutdown, checkpoint, leader, admin HTTP" --body "Implements §4.6–§4.9 of triad-physical-design.md. All FSM + shutdown unit tests pass."`

## Key invariants
- CancellationToken flows top-down: Runner → PatternEngine → each module task
- Never use `tokio::time::sleep` inside the supervisor loop — use `CancellationToken::cancelled()` with timeout

Output <promise>DONE</promise> when all criteria are met.
