# Agent task: adversarial QA — TUI surface (Phase 14)

## Worktree
`/home/jreuben1/Code/triad-worktrees/qa-tui` — branch `feat/qa-tui`

```bash
export CARGO_TARGET_DIR=/tmp/triad-target-qa-tui
```

## Role

You are a black-box QA agent. Your oracle is `triad-system-design.md` (TUI section)
and the screen descriptions in `scripts/prompts/triad-tui.md`.
**Do not read `crates/triad-tui/src/`** except public documentation.
You run in `/loop` mode — keep iterating until the findings file reaches `State: PASSED`.

## Setup: start the test stack

In your Rust integration tests (add to `tests/integration/qa_tui.rs`, feature-gated on `integration`),
use the `TestStack` helper in `tests/integration/helpers.rs` to boot PG + Kafka + Redis,
spawn the `triad` server, and then drive the TUI via the `triad-tui` crate's testable
`App` struct (if it exposes one) or by asserting against the REST API side-effects.

For terminal rendering assertions, use the `ratatui::backend::TestBackend` with a fixed
80×24 buffer and then a 220×50 buffer. Assert no layout overflow (no `…` truncation on
required fields, no overlapping widgets).

## What to test (Phase 14 Surface 4 checklist)

Test each item adversarially — especially container failure and edge-case inputs:

1. **Dashboard polling** — boot stack, wait one poll cycle, assert Dashboard screen shows
   `healthy`. Then stop Redis container → assert next poll shows `degraded` within 10 s.
2. **Patterns pause/resume** — navigate to Patterns screen, trigger pause action on a pattern,
   then verify `GET /patterns` REST response shows `paused`. Resume and re-verify.
3. **DLQ message count** — produce 5 test messages directly to `triad.dlq.test-topic`,
   navigate to DLQ screen, assert count ≥ 5. Replay one → assert count decrements.
4. **Sagas screen** — insert 2 rows directly into `triad_saga_checkpoints` in PG,
   navigate to Sagas screen, assert both rows appear.
5. **Config screen** — navigate to Config screen, assert full `triad.yaml` content renders.
   Trigger "live validate" with a deliberately invalid YAML fragment → assert error banner.
6. **Tachyonfx effects** — assert no `panic` on state transitions (screen switches, data refresh).
   Run with `RUST_BACKTRACE=1`; any panic in test output is a finding.
7. **Clean exit** — send `q` keypress → process exits with code 0 within 2 s, no zombie child
   processes (check with `ps aux` after exit). Ctrl-C path: same assertion.
8. **Terminal size extremes** — render at 80×24: assert no truncation of critical labels.
   Render at 220×50: assert no layout overflow or empty widget columns.

## Findings protocol

Write findings to `/tmp/qa-findings-tui.md`:

```
# QA Findings: TUI
Last-run: <timestamp>
State: TESTING | FOUND | ALL_FIXED | PASSED

## Finding N: <title>
Status: FOUND | FIXED | VERIFIED
Design ref: §X.Y "<exact quote from triad-system-design.md or triad-tui.md>"
Expected: <what the design says>
Actual: <what happened, with evidence>
Repro: <test code snippet or cargo nextest invocation>
Observability: <panic backtrace / log line / metric>
Fix: (blank until fixed)
```

Set `State: FOUND` when any finding is unresolved. Set `State: PASSED` when all findings are
`Status: VERIFIED`. **Do not commit or open a PR.** Terminate this `/loop` when `State: PASSED`.

## Agent event publishing

```bash
printf '{"ts":"%s","agent":"qa-tui","phase":14,"event":"phase_started","detail":"TUI black-box QA starting","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
printf '{"ts":"%s","agent":"qa-tui","phase":14,"event":"step_done","detail":"<what you just tested>","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
printf '{"ts":"%s","agent":"qa-tui","phase":14,"event":"agent_done","detail":"State: PASSED — all TUI findings verified","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
```

## Done criteria

- [ ] All 8 test scenarios exercised including container failure and size extremes
- [ ] No panics under `RUST_BACKTRACE=1` across all state transitions
- [ ] Clean exit verified for both `q` and Ctrl-C paths
- [ ] Every finding in `/tmp/qa-findings-tui.md` reaches `Status: VERIFIED`
- [ ] `/tmp/qa-findings-tui.md` `State: PASSED`
- [ ] No commits or PRs opened by this agent
