# Agent task: adversarial QA — CLI surface (Phase 14)

## Worktree
`/home/jreuben1/Code/triad-worktrees/qa-cli` — branch `feat/qa-cli`

```bash
export CARGO_TARGET_DIR=/tmp/triad-target-qa-cli
```

## Role

You are a black-box QA agent. Your oracle is `triad-system-design.md` §12.2.
**Do not read `crates/*/src/`**. Derive expected behaviour from the design doc only.
You run in `/loop` mode — keep iterating until the findings file reaches `State: PASSED`.

## Setup: start the test stack

Build the binary first, then use the existing TestStack helper for containers:

```bash
cargo build --bin triad --manifest-path /home/jreuben1/Code/triad-worktrees/qa-cli/Cargo.toml
```

In your Rust integration tests (add to `tests/integration/qa_cli.rs`, feature-gated on `integration`),
boot PG + Kafka + Redis via the `TestStack` helper in `tests/integration/helpers.rs`,
then spawn the `triad` binary as a subprocess pointing at the container addresses.

## What to test (Phase 14 Surface 1 checklist)

Test each item adversarially — not just the happy path:

1. Every command in §12.2 (`status`, `run`, `patterns list/pause/resume`, `dlq list/replay/drop`,
   `saga list/inspect/cancel`, `lag`, `version`, `config reload`) against a live server
2. `triad status` shows real backend health; verify it reflects container state changes
3. `triad patterns pause <name>` → `triad patterns list` shows `paused` for that pattern
4. `triad dlq list` returns messages after publishing poison-pill events to the DLQ topic
5. `triad saga list` returns rows from `triad_saga_checkpoints` in PG
6. `triad lag` shows non-zero after publishing test events before the consumer catches up
7. Structured JSON log output includes `trace_id`, `pattern_name`, `pipeline_name` on every operation
8. `triad_pipeline_events_total` Prometheus counter advances after CLI operations
9. Boundary cases: unknown pattern name → error message + non-zero exit, missing subcommand → help text

## Findings protocol

Write findings to `/tmp/qa-findings-cli.md`:

```
# QA Findings: CLI
Last-run: <timestamp>
State: TESTING | FOUND | ALL_FIXED | PASSED

## Finding N: <title>
Status: FOUND | FIXED | VERIFIED
Design ref: §X.Y "<exact quote from triad-system-design.md>"
Expected: <what the design says>
Actual: <what happened, with evidence>
Repro: <exact command or code snippet>
Observability: <log line / metric name+value>
Fix: (blank until fixed)
```

Set `State: FOUND` when any finding is unresolved. Set `State: PASSED` when all findings are
`Status: VERIFIED`. **Do not commit or open a PR.** Terminate this `/loop` when `State: PASSED`.

## Agent event publishing

```bash
printf '{"ts":"%s","agent":"qa-cli","phase":14,"event":"phase_started","detail":"CLI black-box QA starting","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
printf '{"ts":"%s","agent":"qa-cli","phase":14,"event":"step_done","detail":"<what you just tested>","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
printf '{"ts":"%s","agent":"qa-cli","phase":14,"event":"agent_done","detail":"State: PASSED — all CLI findings verified","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
```

## Done criteria

- [ ] All 9 items in "What to test" exercised with adversarial inputs
- [ ] Every finding in `/tmp/qa-findings-cli.md` reaches `Status: VERIFIED`
- [ ] `/tmp/qa-findings-cli.md` `State: PASSED`
- [ ] No commits or PRs opened by this agent
