Implement `triad-runner` backend modules per §4.1–§4.4 of triad-physical-design.md.
This runs after `triad-core` is merged to main.

## Before starting
Read `/home/jreuben1/Code/triad/claude-best-practices-learned.md` and apply all documented invariants immediately — do not rediscover known pitfalls.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/triad-runner-backends`

## Setup
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-runner-backends
```

## Tasks
1. Create `src/backends/mod.rs` — re-exports all backend modules
2. `src/backends/postgres.rs` — `PgBackend`: sqlx pool init, migration runner via `sqlx::migrate!`, replication connection via `tokio-postgres` (§4.1)
3. `src/backends/kafka.rs` — `KafkaBackend`: `ProducerFactory` (EOS transactional producers), `ConsumerFactory`, topic admin via rdkafka (§4.2)
4. `src/backends/redis.rs` — `RedisPool` enum: Standalone/Cluster/Sentinel dispatch via `deadpool-redis` (§4.3)
5. `src/backends/circuit_breaker.rs` — `CircuitBreaker<S>` with `tokio::sync::watch::Sender<CbState>` for zero-cost state broadcast (§4.4)
6. Update `src/backends.rs` → `src/backends/mod.rs` (convert from flat file to module directory)

## Testing requirements
- Unit tests using `mockall` mocked traits — no real network I/O in unit tests
- Integration tests in `tests/integration/test_backends.rs` using `testcontainers-modules` (PG + Kafka + Redis)
- Integration tests gated behind `#[cfg(feature = "integration")]`

## Done criteria
- `cargo check -p triad-runner` compiles cleanly
- `cargo test -p triad-runner` (unit tests only) passes
- `cargo clippy -p triad-runner -- -D warnings` is clean
- Mark all completed items `[x]` in `project-plan.md` (at `/home/jreuben1/Code/triad/project-plan.md`)
- If you discovered any new pitfalls (permission prompts, cargo/git gotchas), add them to `claude-best-practices-learned.md`
- Commit implementation **together with** `project-plan.md` and `claude-best-practices-learned.md` in a single commit on branch `feat/triad-runner-backends`

## Key invariants (from CLAUDE.md)
- WAL replication uses `tokio-postgres` replication connection — NOT the sqlx pool
- Circuit breaker state broadcast via `watch::Sender<CbState>`, not channels or atomics

Output <promise>DONE</promise> when all criteria are met.
