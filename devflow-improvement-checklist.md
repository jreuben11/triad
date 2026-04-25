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

## Phase 9 gate (after all above are done)

- [ ] `cargo deny check`
- [ ] `cargo machete`
- [ ] `cargo fmt --check`
- [ ] `cargo clippy --workspace -- -D warnings`
- [ ] `cargo check --workspace`
- [ ] `cargo nextest run --workspace`
- [ ] `cargo nextest run --workspace --features integration`
- [ ] `cargo llvm-cov nextest --workspace --fail-under-lines 80`
- [ ] `cargo llvm-cov nextest -p triad-runner --fail-under-lines 90`
- [ ] `cargo semver-checks` (establishes v0.1.0 baseline)
- [ ] Stale worktree cleanup
- [ ] Tag `v0.1.0`
