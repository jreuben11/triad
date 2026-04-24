Implement `triad-core` per §3 of triad-physical-design.md.

## Before starting
Read `/home/jreuben1/Code/triad/claude-best-practices-learned.md` and apply all documented invariants immediately — do not rediscover known pitfalls.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/triad-core`

## Tasks (implement in this order — later files depend on earlier ones)
1. `src/types.rs` — all domain types: `EventId`, `PatternName`, `PipelineName`, `SagaId`, `SourcePosition`, `ChangeEvent`, `Operation`, `StepContext` (with `idempotency_key()`), `ModuleState`, `ModuleHealth`, `RunnerState`, `DeliveryGuarantee` (§3.1)
2. `src/traits.rs` — traits: `Source`, `Sink`, `Transform`, `PatternModule`, `CheckpointStore`, `LeaderElector`; all are `#[async_trait]`; `#[automock]` gated behind `#[cfg(test)]` (§3.2)
3. `src/error.rs` — `thiserror` error hierarchy: `TriadError` with all domain variants (§3.3)
4. `src/config.rs` — full `TriadConfig` struct tree: `BackendsConfig`, `PostgresConfig`, `KafkaConfig`, `KafkaSecurityConfig`, `KafkaProducerConfig`, `KafkaConsumerConfig`, `RedisConfig`, `RedisTlsConfig`, `RetryConfig`, `CircuitBreakerConfig`, `PatternConfig` enum, all pattern sub-configs, `ObservabilityConfig`, `AdminConfig`, `ShutdownConfig` (§3.4)
5. `src/metrics.rs` — all 44 metric name constants + histogram bucket `const` arrays (§3.5)

## Testing requirements
- Write unit tests for: config deserialization from YAML string, error `Display` impl, type constructors
- Use `#[rstest]` for parameterised cases
- Target 80%+ line coverage

## Done criteria
- `cargo test -p triad-core` passes with zero failures
- `cargo clippy -p triad-core -- -D warnings` is clean
- `cargo fmt --check -p triad-core` is clean
- Mark all completed items `[x]` in `project-plan.md` (at `/home/jreuben1/Code/triad/project-plan.md`)
- If you discovered any new pitfalls (permission prompts, cargo/git gotchas), add them to `claude-best-practices-learned.md`
- Commit implementation **together with** `project-plan.md` and `claude-best-practices-learned.md` in a single commit on branch `feat/triad-core`

## Constraints
- `thiserror` for errors, never `anyhow` in this library crate
- No `unwrap()` or `expect()` outside of `#[cfg(test)]` blocks
- Never use `println!` — this is a library crate
- Do not modify any other crate

Output <promise>DONE</promise> when all criteria are met.
