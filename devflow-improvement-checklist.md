# Triad — Dev-Flow Improvement Checklist

> Generated after Phase 8b completion via end-of-phase introspective review.
> Work through all items before starting Phase 9.

---

## 1. `/project-status` skill — phase-grouped TODO display

- [x] Replace flat "first 10 `[ ]` lines" with phase-grouped output:
      Current phase shows all remaining items; future phases show header + item count only
- [x] Add "CURRENT PHASE" label to the lowest-numbered phase that has any `[ ]` items
- [x] Add "UPCOMING PHASES" section listing future phases with remaining item counts

---

## 2. `claude-best-practices-learned.md` — gap fixes

- [x] Add rule: stale worktree cleanup after squash-merge
      (`git worktree remove --force <path>` + `git branch -D <branch>`)
- [x] Add rule: Agent Launch Config table must be updated for new phases before auto-launch works
      (Phases 9/10/11 have no rows — auto-launch step 10 can never trigger them)
- [x] Clarify `--delete-branch` omission when branch is checked out in a worktree (move to PR section)
- [x] Add sqlx::query! prohibition enforcement note (grep check in Phase 9 gate)

---

## 3. `triad-system-design.md` — language translation (Go → Rust)

- [x] §12.1 Mode 1: replace Go API example (`triad.Start`) with Rust `TriadInstance::start()`
- [x] §12.1 Mode 1: replace Go signal handler (channels, `go func`) with Rust tokio equivalent
- [x] §12.1 diagram: "cdc goroutine" → "cdc task", "outbox goroutine" → "outbox task" etc.
- [x] §12.0 table: "goroutines scale vertically" → "tokio tasks scale vertically"
- [x] §12.1 text: "runs as goroutines" → "runs as tokio async tasks"
- [x] §12.2 CLI table: "Go version" → "Rust version" in `triad version` description
- [x] §14 language discussion: note Rust was chosen; update rationale accordingly
- [x] §13 unimplemented patterns: mark `lock`, `session`, `enrich`, `state_store`, `fanout`,
      `cqrs`, `pipeline`, `tenant`, `search_index`, `redis_stream` as "v0.2.0 scope"
- [x] §6 cold-start: mark strategies A and B as "planned, not implemented in v0.1.0"
- [x] §14 Schema Registry open question: add decision note (post-v0.1.0, use `apache_avro` crate)
- [x] §15.3 metric names: add cross-reference note to `triad-core/src/metrics.rs`

---

## 4. Tooling additions

- [x] Add `criterion = "0.5"` to workspace dev-dependencies (micro-benchmarks)
- [x] Add `proptest = "1"` to workspace dev-dependencies (property-based testing)
- [x] Add `insta = "1"` to workspace dev-dependencies (snapshot testing for admin API responses)
- [x] Add `console-subscriber` optional feature to `triad-runner/Cargo.toml` (tokio-console support)
- [x] Create `deny.toml` scaffold for `cargo-deny` (license + advisory checks)
- [x] Document tool install commands in `CLAUDE.md`: cargo-deny, cargo-machete, cargo-semver-checks

---

## 5. `project-plan.md` — Phase 9 additions

- [x] Add Phase 9 item: `cargo deny check` (license compliance + CVE advisories)
- [x] Add Phase 9 item: `cargo machete` (unused dependency detection)
- [x] Add Phase 9 item: `cargo semver-checks` (API compatibility baseline for v0.1.0)
- [x] Add Phase 9 item: stale worktree cleanup before tagging
- [x] Add Agent Launch Config note: Phase 9 is manual gate (no worktree); Phases 10/11 need rows added

---

## 6. `CLAUDE.md` (triad repo) updates

- [x] Add "End-of-Phase Introspective Review" protocol section
- [x] Add tooling commands section (cargo-deny, cargo-machete, cargo-semver-checks, tokio-console)
- [x] Add design doc review step to introspective checklist

---

## 7. `~/Documents/CLAUDE.md` updates

- [x] Add end-of-phase introspective review requirement (applies to all projects)

---

## 8. Stale worktree hygiene

- [x] Identify and list merged worktrees still on disk
- [x] Remove worktrees for all branches merged to main
      (triad-proto, triad-core, triad-runner-backends, triad-runner-patterns-cdc-outbox,
       triad-runner-patterns-saga-eos, triad-runner-engine, triad-sdk, triad-cli, tests, bugfixes)

---

## Phase 9 gate ✓ COMPLETE (v0.1.0 tagged)

- [x] `cargo deny check`
- [x] `cargo machete`
- [x] `cargo fmt --check`
- [x] `cargo clippy --workspace -- -D warnings`
- [x] `cargo check --workspace`
- [x] `cargo nextest run --workspace` (337 pass)
- [x] `cargo nextest run --package triad-runner --features integration` (259 pass, 1 ignored — EOS flaky, needs real Kafka broker)
- [x] `cargo llvm-cov nextest --workspace --fail-under-lines 80` (86.72%)
- [x] `cargo llvm-cov nextest -p triad-runner --fail-under-lines 90` (90.91%)
- [x] `cargo semver-checks` (N/A for v0.1.0 first release — baseline established)
- [x] Stale worktree cleanup (all Phase 0–4 worktrees removed)
- [x] Tag `v0.1.0`

---

## Pre-Phase 10/11 introspective review

> Generated after Phase 9 completion and SDLC automation improvements.
> Work through before launching `/zellij-launch phase 5`.

### 1. SDLC automation improvements (done during Phase 9 → 10 transition)

- [x] `PostToolUse` hook: `post-edit-cargo-check.sh` — runs `cargo check --workspace` after every Edit/Write
- [x] `PreToolUse` hook: `pre-push-gate.sh` — blocks `git push` if `cargo fmt --check` or `cargo clippy -D warnings` fails
- [x] `phase-worker.md` agent with `isolation: worktree` and quality gates documented
- [x] GitHub MCP server added to `.mcp.json` (reads `GITHUB_TOKEN` from `.env`)
- [x] `zellij-launch` skill: added `disable-model-invocation: true` + `allowed-tools` frontmatter
- [x] `project-status` skill: added `allowed-tools` frontmatter
- [x] `session-report` plugin installed (observability for multi-agent token usage)

### 2. Phase 10/11 setup checklist

- [x] `maturin` installed globally via `uv tool install maturin` (required for triad-py build)
- [x] Worktree `triad-worktrees/triad-py` created on `feat/triad-py`
- [x] Worktree `triad-worktrees/triad-tui` created on `feat/triad-tui`
- [x] Prompt file `scripts/prompts/triad-py.md` written (PyO3 bindings, uv tooling, async bridge)
- [x] Prompt file `scripts/prompts/triad-tui.md` written (Ratatui + Tachyonfx, 6 screens)
- [ ] `.claude/settings.json` written to each worktree (done by `zellij-launch` skill automatically)
- [ ] Launch: `/zellij-launch phase 5`

### 3. Known deferred items (v0.2.0 scope)

- [ ] `triad migrate` / `triad version` / `triad lag` CLI subcommands
- [ ] gRPC admin server (`admin/grpc.rs`) — proto definitions ready
- [ ] k6 load test scripts (`tests/load/`) — deferred from Phase 9
- [ ] EOS flaky test (`test_eos_kafka_txn_aborted_on_pg_commit_failure`) — needs real Kafka broker with transaction coordinator; currently `#[ignore]`

### 4. Tooling notes for Phase 10/11 agents

- Python tooling: `uv` for all package ops (NOT pip), `uv run pytest`, `uv run mypy`, `uv run maturin develop`
- TUI crate must NOT import `triad-runner` or `triad-sdk` — only `triad-core` + HTTP via `reqwest`
- Both phases are independent — can run in parallel (no shared crates being mutated)

---

## Phase 10/11 end-of-phase review (2026-04-25)

### Findings fixed in this session

- [x] **`cargo fmt` drift** — 14 long `assert!` lines in test_admin_api, test_checkpoint, test_circuit_breaker, test_inbox, test_backends_postgres needed reformatting. Root cause: tests added in Phase 9 gate without a fmt check. Fix: `cargo fmt --all`. **Effort: XS**
- [x] **Unused deps — triad-py** — `serde`, `thiserror`, `tracing`, `uuid` added as scaffold but not used; binding layer delegates everything to triad-sdk/core. Removed. **Effort: XS**
- [x] **Unused deps — triad-tui** — `serde_json` added directly but reqwest handles JSON internally; only `serde` needed for `#[derive(Deserialize)]`. Removed. **Effort: XS**
- [x] **Physical design §1.1 stale** — triad-py, triad-tui, tui.rs command, test_inbox.rs, test_circuit_breaker.rs, test_checkpoint.rs, test_backends_postgres.rs all missing from file tree. Updated. **Effort: S**
- [x] **Phase 7 plan checkbox drift** — test_inbox.rs and test_circuit_breaker.rs marked `[ ]` in Phase 7 but completed in Phase 9; boxes not synced back. Fixed. **Effort: XS**
- [x] **`cargo deny` advisory — paste** — RUSTSEC-2024-0436 transitive via ratatui→tachyonfx; no safe upgrade. Added exemption with explanation to deny.toml. **Effort: XS**

### Open findings (not fixed — tracked as v0.2.0 items)

- [x] **`app.rs` oversized** — `crates/triad-tui/src/app.rs` split into `app/{mod,input,tests}.rs` in Phase 11 refactor commit. **Effort: M** (resolved)

---

## Phase 12/13 end-of-phase review (2026-04-25)

### Background agent permission model — lessons learned

- [x] **`bypassPermissions` mode does NOT override project settings.json allow list** — Background agents spawned via the `Agent` tool cannot use `Edit`, `Write`, or any `Bash(...)` pattern not listed in the worktree's `.claude/settings.json`. Symptom: agents pivot to running the `fewer-permission-prompts` skill instead of their assigned task. Fix: pre-populate all four settings.json files with the complete allow list. **Effort: S per batch**

- [x] **Agent prompt hijacking via skill discovery** — When real tools are blocked, background agents discover the `fewer-permission-prompts` skill and run it, producing permissions-analysis output instead of feature work. This wastes a full agent invocation. Pre-flight: verify settings.json before launching any batch. **Effort: XS (checklist)**

- [x] **Parallel squash-merge conflicts in `claude-best-practices-learned.md`** — When multiple parallel agents each append to `claude-best-practices-learned.md`, their PRs will conflict on rebase. Pattern: always keep both sections (HEAD + incoming), renumber colliding phase numbers, remove conflict markers. **Effort: XS per merge**

- [x] **`project-plan.md` phase numbering collision** — Parallel agents working simultaneously both assigned themselves "Phase 12". On merge, rename the later one (tui-coverage → Phase 13). **Effort: XS per merge**

### Process improvements applied

- [x] Switched load-test framework from k6 (JavaScript) to goose 0.17 (Rust-native, Tokio-based)
- [x] Added `step_done` event granularity between milestones and line-by-line logging in `/project-status`
- [x] Expanded all `.claude/settings.json` allow lists from 18 → 37+ entries (Edit, Write, full Bash set)
- [x] Added agent prompt files: `scripts/prompts/{load-tests,proptest,tui-coverage}.md`

### Open findings

- [ ] **goose Prometheus assertion pattern needs a running server** — Load tests assert `triad_outbox_relay_published_total` increments by scraping `/metrics`. These tests only work against a live triad-runner instance. They should be documented as "manual / CI with service" tests, not unit tests. **Effort: S**
- [ ] **tui-coverage Phase 13 items not in Agent Launch Configuration table** — The batch table in `project-plan.md` only covers through Phase 11. Phases 12/13 were launched manually via background agents. Update the table for documentation completeness. **Effort: XS**
- [ ] **`paste` unmaintained dep** — transitive via tachyonfx 0.7. No action until tachyonfx releases an update. Re-evaluate when tachyonfx cuts a new release. **Effort: XS when unblocked**
