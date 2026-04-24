# Triad — Claude Code Instructions

## Project summary

Triad is a Rust library + runner implementing all PostgreSQL × Kafka × Redis integration patterns as composable, observable, exactly-once primitives. See `triad-system-design.md` for the conceptual design and `triad-physical-design.md` for the full implementation plan with code sketches.

## Workspace layout

```
crates/
  triad-proto/    # protobuf definitions compiled by tonic-build
  triad-core/     # shared types, traits, config, error hierarchy, metric names
  triad-sdk/      # Mode 1: application-facing SDK (embed in-process)
  triad-runner/   # Mode 2/3: pattern engine, backend clients, admin HTTP server
  triad-cli/      # Mode 2/3: `triad` binary + admin CLI client
```

Dependency direction: `cli → runner → core ← sdk`, `runner → proto`.

## Common commands

```bash
cargo check --workspace                                          # fast compile check
cargo nextest run --workspace                                    # parallelised unit tests (preferred over cargo test)
cargo nextest run --workspace --features integration             # + testcontainers integration tests
cargo nextest run --workspace --test-threads 8                   # explicit parallelism cap
cargo clippy --workspace -- -D warnings                          # lints — all warnings are errors
cargo fmt --check                                                # formatting gate
cargo llvm-cov nextest --workspace --html                        # coverage report (HTML)
cargo llvm-cov nextest --workspace --fail-under-lines 80         # coverage gate (CI)
cargo llvm-cov nextest -p triad-runner --fail-under-lines 90     # runner coverage gate
```

Install `cargo-nextest` if not present: `cargo install cargo-nextest cargo-llvm-cov`

## Rust best practices for this project

### Error handling
- Library crates (`triad-core`, `triad-sdk`, `triad-runner`, `triad-proto`): use `thiserror` with typed error enums. Never use `anyhow` in library crates.
- Binary crate (`triad-cli`): use `anyhow` for top-level error propagation.
- Never use `unwrap()` or `expect()` in non-test code. Use `?` propagation.
- All public functions return `Result<T, TriadError>` or a domain-specific error type.

### Async
- All async code uses `tokio`. No `async-std`, no `futures::executor`.
- Use `tokio::task::JoinSet` for supervising concurrent pattern modules in `engine.rs`.
- Use `tokio_util::sync::CancellationToken` for shutdown propagation — not channels or booleans.
- Cancellation tokens flow top-down: `Runner → PatternEngine → each module`.

### Traits and generics
- Core traits (`Source`, `Sink`, `Transform`, `PatternModule`, `CheckpointStore`, `LeaderElector`) live in `triad-core/src/traits.rs`.
- All traits are `#[async_trait]` and object-safe. Prefer `Box<dyn Trait>` over generics at module boundaries.
- `#[automock]` annotations on traits are gated behind `#[cfg(test)]` to avoid mockall pulling into prod builds.

### Configuration
- All config is loaded via the `config` crate (YAML + env layering). No hardcoded values.
- Config structs live in `triad-core/src/config.rs` and derive `serde::Deserialize`.
- Environment variable overrides use `TRIAD_` prefix and `__` as separator (e.g. `TRIAD_KAFKA__BROKERS`).

### Observability
- Use `tracing` macros everywhere (`trace!`, `debug!`, `info!`, `warn!`, `error!`). Never `println!` or `eprintln!` in library/runner code.
- Metric name constants live in `triad-core/src/metrics.rs`. Never inline metric name strings.
- Every span must include `pattern_name` and `pipeline_name` fields.
- Audit events go to the `triad.audit` Kafka topic, not stdout.

### Safety invariants — do not violate
- **EOS**: the Kafka producer transaction must wrap both the message send and `send_offsets_to_transaction`. Never commit a Kafka transaction without the offset.
- **Optimistic locking**: checkpoint updates must use `WHERE id = $1 AND version = $2`. Never update without the version check.
- **DLQ naming**: always use the template `triad.dlq.{source_topic}`. Never hardcode or use `{source_topic}.dlq`.
- **Inbox deduplication**: insert into `triad_inbox` must happen inside the same PG transaction as the business write.
- **Redis fallback**: when the Redis circuit breaker is open, inbox `INSERT` falls back to a PG transaction — never skip deduplication entirely.
- **WAL replication**: uses `tokio-postgres` replication connection (not the `sqlx` pool). These are separate connection types and must not be mixed.

### Kubernetes feature flag
- K8s leader election code (`K8sLeaseLeader`, kube client) is gated behind `#[cfg(feature = "kubernetes")]` in `triad-runner`.
- The `NoopLeader` (always-leader) is the default and must always compile without the feature.

## Pre-commit gate — ALL must pass before any commit

```bash
cargo fmt --check                                            # 1. formatting
cargo clippy --workspace -- -D warnings                      # 2. lints
cargo check --workspace                                      # 3. compile
cargo nextest run --workspace                                # 4. all unit tests
cargo llvm-cov nextest --workspace --fail-under-lines 80    # 5. coverage threshold
```

Never commit with failing tests. Never commit with clippy warnings. If tests fail, fix them before committing — do not skip or `#[ignore]` tests without adding a tracking comment with the reason.

## Git workflow in this repo

This repo uses git worktrees for parallel agent development. Each feature has its own worktree:
```
/home/jreuben1/Code/triad-worktrees/<feature>/   ← each agent works here
/home/jreuben1/Code/triad/                        ← main branch, integration point
```

When working in a worktree, set `CARGO_TARGET_DIR` to avoid conflicts with other worktrees:
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-$(git branch --show-current)
```

Commit within the worktree branch. Do not push directly to `main`.

## Testing requirements

### Coverage target
- **Minimum 80% line coverage** per crate, measured with `cargo-llvm-cov`.
- **Critical modules** (`checkpoint.rs`, `engine.rs`, `backends/kafka.rs`, `backends/postgres.rs`): target 90%+.
- Coverage is checked with: `cargo llvm-cov --workspace --fail-under-lines 80`.

### Test organisation
```
src/
  module.rs
  module/
    tests.rs        # unit tests for module (in same crate)
tests/
  integration/
    test_*.rs       # testcontainers integration tests
```

### Unit tests
- Use `#[rstest]` for parameterised cases. Avoid copy-pasting test bodies.
- Mock all external I/O with `mockall` `#[automock]` traits. Unit tests must not touch the network, filesystem, or real backend processes.
- Async unit tests use `#[tokio::test]`.
- Test function names: `test_<unit>_<scenario>_<expected_outcome>`.

### Integration tests
- Use `testcontainers-modules` (Kafka, PostgreSQL, Redis) — not docker-compose.
- Share a single `TestStack` helper (defined in `tests/integration/helpers.rs`) that boots all three containers once per test binary.
- Webhook delivery tests use `wiremock`.
- Integration tests are gated behind `#[cfg(feature = "integration")]` and the `integration` feature flag.

### Property-based and fuzz targets
- For parsing code (protobuf, config, WAL event decoding): add `proptest` or `cargo-fuzz` targets.

## Key design documents

- `triad-system-design.md` — conceptual design, all patterns, deployment modes, observability SLOs, durability model
- `triad-physical-design.md` — Rust implementation plan: all structs, traits, SQL DDL, test matrix

## Deployment modes (summary)

| Mode | How | Leader |
|---|---|---|
| 1 | `Triad::start()` embedded in app | `NoopLeader` (always leader) |
| 2 | `triad run` standalone binary | `NoopLeader` |
| 3 | K8s Deployment + `kubernetes` feature | `K8sLeaseLeader` (15s lease) |

## Admin API endpoints (port 8080)

```
GET  /health/live      liveness probe
GET  /health/ready     readiness probe (checks PG + Kafka + Redis + Schema Registry)
GET  /health/started   startup probe
GET  /metrics          Prometheus text format
GET  /patterns         list all pattern modules + state
POST /patterns/{name}/pause
POST /patterns/{name}/resume
GET  /registry         pattern module registry
GET  /checkpoints      list checkpoint offsets
POST /pipelines/{name}/reload
GET  /dlq/{topic}      list DLQ messages
POST /dlq/{topic}/replay
DELETE /dlq/{topic}    purge DLQ
```

## Launching agents

This project uses the `/zellij-launch` skill (must be inside a zellij session).

### Starting a phase
```
/zellij-launch phase 0    # proto + core (parallel agents)
/zellij-launch phase 1    # backends (parallel agent)
/zellij-launch phase 2    # cdc-outbox (parallel) + saga-eos (/loop)
/zellij-launch phase 3    # engine (/loop) + sdk + cli (parallel)
/zellij-launch phase 4    # integration tests (parallel agent)
```

The skill reads the **Agent Launch Configuration** table in `project-plan.md` — that table is the single source of truth for which worktrees, targets, prompts, and `/loop` flags apply to each batch.

### When to use /loop vs parallel agents

| Agent strategy | When to use |
|---|---|
| **Parallel agent** (default) | Module is self-contained with a clear spec — agent runs once, commits, done |
| **`/loop`** | Complex concurrency or state machines (saga, EOS, engine FSM) — needs iterative TDD cycles |

After `/zellij-launch`, check the skill output for "Run /loop in: ..." and switch to those tabs to start the loop.

### Checking progress
Run `/project-status` in any Claude Code tab (or the dedicated `status` tab opened by `/zellij-launch`) to see: git branches, worktree list, plan checklist, cargo check result, and pass/fail per worktree.

### Agent prompts
Per-agent task descriptions live in `scripts/prompts/<name>.md`. Each prompt specifies the worktree, done criteria, and key invariants. Read these before modifying agent behaviour.

## Self-optimisation instructions

**After completing each module or phase:**

1. **Update `project-plan.md`**: check off completed items with `[x]`. Add any new sub-tasks discovered during implementation.

2. **Update this file (`CLAUDE.md`)** when you discover:
   - A new invariant that must be respected across modules (add it to the "Safety invariants" section)
   - A pattern or anti-pattern that caused test failures (add a rule to the relevant section)
   - A crate API gotcha discovered via compiler errors (add a comment in "Rust best practices")

3. **Commit CLAUDE.md and project-plan.md changes** together with the implementation commit so the next agent session starts with current context.

4. **Do not pad CLAUDE.md** with information derivable from the code itself. Only add things that would surprise a future agent reading fresh context.
