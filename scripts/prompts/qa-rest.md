# Agent task: adversarial QA — REST API surface (Phase 14)

## Worktree
`/home/jreuben1/Code/triad-worktrees/qa-rest` — branch `feat/qa-rest`

```bash
export CARGO_TARGET_DIR=/tmp/triad-target-qa-rest
```

## Role

You are a black-box QA agent. Your oracles are the Admin API endpoints table in `CLAUDE.md`
and `triad-system-design.md` §9 (observability/admin) and §17.3 (OTel span attributes).
**Do not read `crates/*/src/`**. Derive expected behaviour from design docs only.
You run in `/loop` mode — keep iterating until the findings file reaches `State: PASSED`.

## Setup: start the test stack

In your Rust integration tests (add to `tests/integration/qa_rest.rs`, feature-gated on `integration`),
use the `TestStack` helper in `tests/integration/helpers.rs` to boot PG + Kafka + Redis,
then spawn the `triad` server and use `reqwest` to hit the admin API on port 8080.

## What to test (Phase 14 Surface 2 checklist)

Test each item adversarially:

1. All routes in the CLAUDE.md "Admin API endpoints" table respond correctly:
   - `GET /health/live`, `/health/ready`, `/health/started`
   - `GET /metrics` (Prometheus text format, non-empty)
   - `GET /patterns`, `POST /patterns/:name/pause`, `POST /patterns/:name/resume`
   - `POST /patterns/:name/replay`, `GET /registry`, `GET /checkpoints`
   - `POST /pipelines/:name/reload`, `GET /lag`, `GET /dlq/:topic`, `POST /dlq/:topic/replay`
   - `DELETE /dlq/:topic`, `GET /saga`, `GET /saga/:id`, `POST /saga/:id/cancel`
   - `POST /config/reload`, `GET /metrics/cardinality`
2. Boundary conditions: unknown pattern name → 404; malformed JSON body → 400; missing required
   fields → 422; method not allowed → 405
3. `GET /health/ready` with Redis stopped → degraded response (not 200)
4. `POST /patterns/:name/pause` → `GET /patterns` confirms state flips to `paused`
5. `GET /lag` returns non-empty data after publishing test messages ahead of consumer
6. Saga routes query and modify real PG `triad_saga_checkpoints` table
7. OTel spans on each request carry `triad.pattern.name` and `triad.pipeline.name` attributes (§17.3)
8. `triad_pipeline_events_total` counter in `/metrics` advances after triggering events via REST

## Findings protocol

Write findings to `/tmp/qa-findings-rest.md`:

```
# QA Findings: REST
Last-run: <timestamp>
State: TESTING | FOUND | ALL_FIXED | PASSED

## Finding N: <title>
Status: FOUND | FIXED | VERIFIED
Design ref: §X.Y "<exact quote from triad-system-design.md or CLAUDE.md>"
Expected: <what the design says>
Actual: <what happened, with evidence>
Repro: <curl command or reqwest snippet>
Observability: <log line / metric name+value / span attribute>
Fix: (blank until fixed)
```

Set `State: FOUND` when any finding is unresolved. Set `State: PASSED` when all findings are
`Status: VERIFIED`. **Do not commit or open a PR.** Terminate this `/loop` when `State: PASSED`.

## Agent event publishing

```bash
printf '{"ts":"%s","agent":"qa-rest","phase":14,"event":"phase_started","detail":"REST black-box QA starting","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
printf '{"ts":"%s","agent":"qa-rest","phase":14,"event":"step_done","detail":"<what you just tested>","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
printf '{"ts":"%s","agent":"qa-rest","phase":14,"event":"agent_done","detail":"State: PASSED — all REST findings verified","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
```

## Done criteria

- [ ] All admin routes tested including boundary/error cases
- [ ] Health degradation verified with a killed container
- [ ] Saga routes verified against real PG data
- [ ] OTel span attributes verified on at least 3 routes
- [ ] Every finding in `/tmp/qa-findings-rest.md` reaches `Status: VERIFIED`
- [ ] `/tmp/qa-findings-rest.md` `State: PASSED`
- [ ] No commits or PRs opened by this agent
