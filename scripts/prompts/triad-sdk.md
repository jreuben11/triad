Implement `triad-sdk` per §5 of triad-physical-design.md.
Runs after triad-runner-backends is merged.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/triad-sdk`

## Setup
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-sdk
```

## Tasks
1. `src/instance.rs` — `TriadInstance::start(config)` embeds a `Runner` in-process and returns handles; `shutdown()` triggers graceful drain
2. `src/middleware.rs` — `IdempotencyLayer` and `RateLimitLayer` as `tower::Layer` implementations usable with Axum
3. `src/patterns.rs` — SDK facades: `OutboxPublisher` (wraps sqlx INSERT into triad_outbox), `FlagEvaluator` (Redis lookup with PG fallback), `SagaBuilder` (fluent API to define saga steps)
4. `src/aggregate.rs` — event-sourcing aggregate helper: apply events, persist snapshot
5. `src/idempotency.rs` — idempotency key generation and lookup helpers

## Done criteria
- `cargo check -p triad-sdk` clean (Mode 1 configuration — no `kubernetes` feature)
- `cargo test -p triad-sdk` passes
- `cargo clippy -p triad-sdk -- -D warnings` clean
- Commit on branch `feat/triad-sdk`

## Constraints
- No `anyhow` in this crate — use `thiserror` or re-export `TriadError`
- SDK must compile without `kubernetes` feature active

Output <promise>DONE</promise> when all criteria are met.
