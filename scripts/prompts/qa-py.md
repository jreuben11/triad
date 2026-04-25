# Agent task: adversarial QA — Python bindings surface (Phase 14)

## Worktree
`/home/jreuben1/Code/triad-worktrees/qa-py` — branch `feat/qa-py`

```bash
export CARGO_TARGET_DIR=/tmp/triad-target-qa-py
```

## Role

You are a black-box QA agent. Your oracle is `triad-system-design.md` (Python bindings section)
and the public API surface of the `triad` Python package.
**Do not read `crates/*/src/`**. Derive expected behaviour from the design doc and
the installed package's type stubs (`triad/*.pyi`).
You run in `/loop` mode — keep iterating until the findings file reaches `State: PASSED`.

## Setup: build and install the package

```bash
cd /home/jreuben1/Code/triad-worktrees/qa-py/crates/triad-py
uv run maturin develop
```

Install test dependencies:
```bash
uv add --dev pytest pytest-asyncio testcontainers
```

Use `testcontainers` (Python package) to boot PG + Kafka + Redis in your pytest fixtures.

## What to test (Phase 14 Surface 3 checklist)

Test each item adversarially — especially error and abort paths:

1. **Outbox abort isolation** — `PyOutboxPublisher.publish()` inside a `PyTransaction` that is
   subsequently aborted must NOT deliver any Kafka message. Verify by polling the topic.
2. **Flag hot-reload** — `PyFlagEvaluator.is_enabled()` for a flag set in PG must appear from
   Redis within the 5-second hot-reload window. Verify with `time.sleep(6)` then re-check.
3. **Saga compensation** — `PySagaBuilder` with two steps where step 2 raises → step 1's
   compensation is called. Assert compensation side-effect (e.g. DB row rollback).
4. **Idempotency cache** — two calls with the same `PyIdempotencyKey` → second returns the
   cached response without re-processing. Confirm with a call counter mock.
5. **asyncio interop** — all async methods work under `asyncio.run()` and `pytest-asyncio`
   without deadlock, leaked tasks, or "Event loop is closed" errors.
6. **Type stubs** — run `uv run mypy --strict tests/` on your test file. Zero `Any` surprises;
   all return types are concrete (no `Unknown`).
7. **Boundary cases** — pass `None`, empty strings, negative integers where the API accepts
   typed values. Assert `TypeError` or a typed `TriadError`, not an unhandled panic.

## Findings protocol

Write findings to `/tmp/qa-findings-py.md`:

```
# QA Findings: Python
Last-run: <timestamp>
State: TESTING | FOUND | ALL_FIXED | PASSED

## Finding N: <title>
Status: FOUND | FIXED | VERIFIED
Design ref: §X.Y "<exact quote from triad-system-design.md>"
Expected: <what the design says>
Actual: <what happened, with evidence>
Repro: <pytest snippet or python -c '...' command>
Observability: <exception type / log line / metric>
Fix: (blank until fixed)
```

Set `State: FOUND` when any finding is unresolved. Set `State: PASSED` when all findings are
`Status: VERIFIED`. **Do not commit or open a PR.** Terminate this `/loop` when `State: PASSED`.

## Agent event publishing

```bash
printf '{"ts":"%s","agent":"qa-py","phase":14,"event":"phase_started","detail":"Python bindings black-box QA starting","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
printf '{"ts":"%s","agent":"qa-py","phase":14,"event":"step_done","detail":"<what you just tested>","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
printf '{"ts":"%s","agent":"qa-py","phase":14,"event":"agent_done","detail":"State: PASSED — all Python findings verified","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
```

## Done criteria

- [ ] All 7 test scenarios exercised with adversarial inputs including abort/error paths
- [ ] `uv run pytest` passes with testcontainers PG + Redis + Kafka
- [ ] `uv run mypy --strict` shows zero `Any` violations on the test file
- [ ] Every finding in `/tmp/qa-findings-py.md` reaches `Status: VERIFIED`
- [ ] `/tmp/qa-findings-py.md` `State: PASSED`
- [ ] No commits or PRs opened by this agent
