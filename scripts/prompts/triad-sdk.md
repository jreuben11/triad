Implement `triad-sdk` per §5 of triad-physical-design.md.
Runs after triad-runner-backends is merged.

## Before starting
Read `/home/jreuben1/Code/triad/claude-best-practices-learned.md` and apply all documented invariants immediately — do not rediscover known pitfalls.

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
- Mark all completed items `[x]` in `project-plan.md` (at `/home/jreuben1/Code/triad/project-plan.md`)
- If you discovered any new pitfalls (permission prompts, cargo/git gotchas), add them to `claude-best-practices-learned.md`
- Commit implementation **together with** `project-plan.md` and `claude-best-practices-learned.md` in a single commit on branch `feat/triad-sdk`

## Constraints
- No `anyhow` in this crate — use `thiserror` or re-export `TriadError`
- SDK must compile without `kubernetes` feature active

Output <promise>DONE</promise> when all criteria are met.
