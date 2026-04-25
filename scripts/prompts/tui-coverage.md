# Agent task: triad-tui coverage restoration

## Worktree
`/home/jreuben1/Code/triad-worktrees/tui-coverage` — branch `feat/tui-coverage`

```bash
export CARGO_TARGET_DIR=/tmp/triad-target-tui-coverage
```

## Context

The workspace line coverage is currently ~77.8%, below the 80% threshold enforced by
`cargo llvm-cov nextest --workspace --fail-under-lines 80`. The culprit is `triad-tui`,
which was added in Phase 11 and has several under-covered files:

| File | Coverage |
|---|---|
| `src/client.rs` | ~24% |
| `src/main.rs` | ~0% |
| `src/screens/dlq.rs` | ~57% |
| `src/screens/sagas.rs` | ~60% |
| `src/screens/config.rs` | ~76% |

The `src/app/input.rs` is at ~76% (marginal). The other files are fine.

## Goal

Raise workspace line coverage to ≥ 80% by adding unit tests to the under-covered
triad-tui files. Do NOT mock the network or write integration tests — all tests must
be pure unit tests that run without any running services.

## Strategy

`src/main.rs` and `src/client.rs` contain runtime I/O that cannot be unit-tested
without a live server. Do NOT try to test those — focus on the screen rendering and
input handling files where coverage can be gained without backends.

The screens use Ratatui's `Frame`/`Buffer` rendering pattern already established in
the existing tests in `src/app/tests.rs` and `src/app/mod.rs`. Follow the same
pattern: create an `AppData` with test fixtures, call `render_*()`, assert on the
rendered buffer content.

Priority order (highest coverage gain first):
1. `src/screens/dlq.rs` — render DLQ list with populated data, render empty state,
   render with selected row, exercise scroll behaviour
2. `src/screens/sagas.rs` — render saga list with data, render detail expanded state,
   exercise the step detail toggle
3. `src/screens/config.rs` — render with config loaded, render loading/error state,
   exercise the collapsible tree keys
4. `src/app/input.rs` — exercise the per-screen key handlers that aren't yet covered
   (check existing tests first to avoid duplication)

## How to add tests

Each screen module has a `tests` submodule at the bottom (or add one). Use the
`ratatui::backend::TestBackend` + `ratatui::Terminal` pattern:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};

    fn test_terminal(w: u16, h: u16) -> Terminal<TestBackend> {
        Terminal::new(TestBackend::new(w, h)).unwrap()
    }
}
```

Look at `src/app/mod.rs` tests for the exact `AppData` construction pattern already
in use. Import from `crate::client::AppData` and fill fields with test fixtures.

## Quality gate

Run this sequence in order after each edit:

```bash
cargo fmt --check --manifest-path /home/jreuben1/Code/triad-worktrees/tui-coverage/Cargo.toml
cargo clippy --workspace --manifest-path /home/jreuben1/Code/triad-worktrees/tui-coverage/Cargo.toml -- -D warnings
cargo nextest run --workspace --manifest-path /home/jreuben1/Code/triad-worktrees/tui-coverage/Cargo.toml
cargo llvm-cov nextest --workspace --manifest-path /home/jreuben1/Code/triad-worktrees/tui-coverage/Cargo.toml --fail-under-lines 80
```

The last command must exit 0 before you commit.

## Three-file discipline

Before committing, update:
- `project-plan.md` — add a Phase 12 section for this task and check it off
- `CLAUDE.md` — add any new Ratatui testing gotcha discovered
- `claude-best-practices-learned.md` — record any non-obvious pattern

## Agent event publishing

Publish status events to `/tmp/triad-agent-events.jsonl` at key milestones:

```bash
# On startup:
printf '{"ts":"%s","agent":"tui-coverage","phase":12,"event":"phase_started","detail":"raising triad-tui coverage to 80%%","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl

# After quality gate passes:
printf '{"ts":"%s","agent":"tui-coverage","phase":12,"event":"gate_passed","detail":"workspace coverage >= 80%%","coverage_pct":80.5}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl

# When done:
printf '{"ts":"%s","agent":"tui-coverage","phase":12,"event":"agent_done","detail":"PR opened","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
```

## Done criteria

- [ ] `cargo llvm-cov nextest --workspace --fail-under-lines 80` exits 0
- [ ] All new tests are pure unit tests (no network, no file I/O)
- [ ] `cargo clippy --workspace -- -D warnings` clean
- [ ] `cargo fmt --check` clean
- [ ] Three-file discipline committed
- [ ] PR opened against `main`
