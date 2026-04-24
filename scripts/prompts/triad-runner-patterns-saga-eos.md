Implement `patterns/saga.rs` and `patterns/eos.rs` per §4.5 of triad-physical-design.md.
Use TDD: write failing tests first, then implement, then fix until green.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/triad-runner-patterns-saga-eos`

## Setup
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-saga-eos
```

## TDD cycle (repeat until done)
1. Write a failing test in `src/patterns/saga/tests.rs` or `src/patterns/eos/tests.rs`
2. Run `cargo test -p triad-runner 2>&1 | tail -30` — verify it fails for the right reason
3. Implement just enough to make it pass
4. Run `cargo test -p triad-runner` — verify green
5. Refactor, then repeat

## saga.rs requirements
- `SagaOrchestrator`: runs steps in sequence via `JoinSet`
- Each step gets a `StepContext` with `idempotency_key()` → `{saga_id}:{step_name}`
- On step failure: run compensation steps in reverse order
- Persist state to `triad_saga_checkpoints` after each step
- Checkpoint UPDATE must use: `WHERE id = $1 AND version = $version` (optimistic locking — CAS)
- On crash recovery: reload from `triad_saga_checkpoints`, re-run from last incomplete step
- `drain()`: flush all in-flight saga states to PG before shutdown

## eos.rs requirements
- `EosCoordinator`: wraps Kafka producer transaction + Redis NX + PG outbox in atomic unit
- Transaction sequence: `init_transactions → begin_transaction → produce → send_offsets_to_transaction → commit_transaction`
- The Kafka transaction MUST wrap both the message send AND `send_offsets_to_transaction` — never commit without the offsets
- On Redis CB open: fall back to PG outbox path — never drop the message

## Testing requirements
- 90%+ line coverage on both files
- Test saga happy path, compensation path, crash recovery
- Test EOS with simulated producer failure mid-transaction
- Use `mockall` for all backend mocks

## Done criteria
- `cargo test -p triad-runner` passes with zero failures
- `cargo llvm-cov -p triad-runner` shows ≥90% for saga.rs and eos.rs
- `cargo clippy -p triad-runner -- -D warnings` clean
- Commit on branch `feat/triad-runner-patterns-saga-eos`

Output <promise>DONE</promise> when all criteria are met.
