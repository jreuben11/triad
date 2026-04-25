# Agent task: add proptest to triad-core and triad-runner

## Worktree
`/home/jreuben1/Code/triad-worktrees/proptest` — branch `feat/proptest`

```bash
export CARGO_TARGET_DIR=/tmp/triad-target-proptest
```

## Context

The project already uses `rstest` for parameterised tests. We now want to add
property-based testing with `proptest` for modules where hand-written cases can't
cover the input space. The CLAUDE.md already calls this out under "Property-based
and fuzz targets". No proptest tests exist yet.

## Goal

Add `proptest` tests to exactly these three target modules (highest ROI):

1. **`triad-core` — config roundtrip** (`crates/triad-core/src/config.rs`)
   Property: serialise a `TriadConfig` to JSON, deserialise it back, assert equality.
   Generate arbitrary (but structurally valid) configs with proptest strategies.

2. **`triad-runner` — checkpoint version monotonicity** (`crates/triad-runner/src/checkpoint.rs`)
   Property: given a sequence of version numbers in any order, after applying the
   optimistic-lock update rule (`WHERE version = $old`), the resulting version sequence
   must be monotonically increasing. No skips allowed.
   (Unit-test with mock `CheckpointStore`, not a real DB.)

3. **`triad-runner` — WAL LSN ordering** (`crates/triad-runner/src/backends/postgres.rs` or
   wherever `SourcePosition`/LSN parsing lives in triad-core)
   Property: parsing a valid LSN string then formatting it back produces the original
   string. Generate LSNs as `(u32, u32)` pairs formatted as `"X/Y"` hex strings.

## Setup

Add to `[dev-dependencies]` in the relevant `Cargo.toml` files:

```toml
proptest = "1"
```

Do NOT add proptest to `[dependencies]` (production). Dev-only.

## How to write proptest tests

Follow this pattern (do not use `#[tokio::test]` for proptest — run sync):

```rust
#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_lsn_roundtrip(hi in 0u32..u32::MAX, lo in 0u32..u32::MAX) {
            let lsn_str = format!("{hi:X}/{lo:X}");
            // parse → format → assert equals original
            let parsed = parse_lsn(&lsn_str).unwrap();
            prop_assert_eq!(format_lsn(parsed), lsn_str);
        }
    }
}
```

Put property tests in a separate `mod prop_tests` block inside the existing `#[cfg(test)]`
section, or in the existing `tests.rs` submodule if one exists. Do NOT create a separate file.

## Quality gate

Run in order after edits:

```bash
cargo fmt --check --manifest-path /home/jreuben1/Code/triad-worktrees/proptest/Cargo.toml
cargo clippy --workspace --manifest-path /home/jreuben1/Code/triad-worktrees/proptest/Cargo.toml -- -D warnings
cargo nextest run --workspace --manifest-path /home/jreuben1/Code/triad-worktrees/proptest/Cargo.toml
```

`cargo nextest run` must show all proptest tests passing. Proptest runs 256 cases by
default — if any case fails, the failure output will show the minimised counterexample.
Fix the bug (or the property if it was wrong), then re-run.

## Three-file discipline

Before committing, update:
- `project-plan.md` — add a Phase 12 proptest section, check it off
- `CLAUDE.md` — add "property-based tests belong in `mod prop_tests` inside existing test module" if not already there
- `claude-best-practices-learned.md` — record any proptest + nextest gotcha

## Agent event publishing

```bash
printf '{"ts":"%s","agent":"proptest","phase":12,"event":"phase_started","detail":"adding proptest to config/checkpoint/LSN modules","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl

printf '{"ts":"%s","agent":"proptest","phase":12,"event":"gate_passed","detail":"3 property suites passing","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl

printf '{"ts":"%s","agent":"proptest","phase":12,"event":"agent_done","detail":"PR opened","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
```

## Done criteria

- [ ] `proptest = "1"` added to dev-dependencies in triad-core and triad-runner `Cargo.toml`
- [ ] Config roundtrip property test: ≥ 3 properties, all passing
- [ ] Checkpoint version monotonicity property test: ≥ 2 properties, all passing
- [ ] LSN roundtrip property test: ≥ 2 properties, all passing
- [ ] All existing tests still pass (`cargo nextest run --workspace`)
- [ ] `cargo clippy -- -D warnings` clean
- [ ] Three-file discipline committed
- [ ] PR opened against `main`
