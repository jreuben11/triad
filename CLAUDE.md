# Triad — Claude Code Instructions

## Table of Contents

- [Project summary](#project-summary)
- [Workspace layout](#workspace-layout)
- [Common commands](#common-commands)
- [Security and quality tooling](#security-and-quality-tooling)
- [Rust best practices for this project](#rust-best-practices-for-this-project)
  - [Error handling](#error-handling)
  - [Async](#async)
  - [Traits and generics](#traits-and-generics)
  - [Configuration](#configuration)
  - [Observability](#observability)
  - [Safety invariants — do not violate](#safety-invariants--do-not-violate)
  - [Kubernetes feature flag](#kubernetes-feature-flag)
- [Quality gate](#quality-gate--run-this-sequence-before-every-cargo-nextest-invocation-and-before-every-commit)
- [Post-merge / post-rebase gate](#post-merge--post-rebase-gate--mandatory)
- [Git workflow in this repo](#git-workflow-in-this-repo)
- [Testing requirements](#testing-requirements)
  - [Coverage target](#coverage-target)
  - [Test organisation](#test-organisation)
  - [Unit tests](#unit-tests)
  - [Integration tests](#integration-tests)
  - [Property-based and fuzz targets](#property-based-and-fuzz-targets)
- [Definition of done — pattern module](#definition-of-done--pattern-module)
- [Key design documents](#key-design-documents)
- [Deployment modes (summary)](#deployment-modes-summary)
- [Admin API endpoints (port 8080)](#admin-api-endpoints-port-8080)
- [Launching agents](#launching-agents)
  - [Starting a phase](#starting-a-phase)
  - [When to use /loop vs parallel agents](#when-to-use-loop-vs-parallel-agents)
  - [Checking progress](#checking-progress)
  - [Agent prompts](#agent-prompts)
- [PR checklist](#pr-checklist)
- [Self-optimisation instructions](#self-optimisation-instructions)
- [End-of-phase introspective review (mandatory)](#end-of-phase-introspective-review-mandatory)
  - [Process review](#process-review)
  - [Retrospective — process improvement](#retrospective--process-improvement)
  - [Design doc review](#design-doc-review)
  - [Design/code/plan sync review](#designcodeplan-sync-review-run-at-end-of-every-phase)
  - [Design improvement signals](#design-improvement-signals)
  - [Code improvement signals](#code-improvement-signals)
  - [API ergonomics check](#api-ergonomics-check)
  - [Test quality audit](#test-quality-audit)
  - [Codebase review](#codebase-review)
  - [Tooling review](#tooling-review)
  - [Output](#output)

---

## Project summary

Triad is a Rust library + runner implementing all PostgreSQL × Kafka × Redis integration patterns as composable, observable, exactly-once primitives. See `triad-system-design.md` for the conceptual design and `triad-physical-design.md` for the full implementation plan with code sketches.

## Workspace layout

```
crates/
  triad-proto/    # protobuf definitions compiled by tonic-build
  triad-core/     # shared types, traits, config, error hierarchy, metric names
  triad-sdk/      # Mode 1: application-facing SDK (embed in-process)
  triad-runner/   # Mode 2/3: pattern engine, backend clients, admin HTTP server
  triad-cli/      # Mode 2/3: `triad` binary + admin CLI client
```

Dependency direction: `cli → runner → core ← sdk`, `runner → proto`.

## Common commands

```bash
cargo check --workspace                                          # fast compile check
cargo nextest run --workspace                                    # parallelised unit tests (preferred over cargo test)
cargo nextest run --workspace --features integration             # + testcontainers integration tests
cargo nextest run --workspace --test-threads 8                   # explicit parallelism cap
cargo clippy --workspace -- -D warnings                          # lints — all warnings are errors
cargo fmt --check                                                # formatting gate
cargo llvm-cov nextest --workspace --html                        # coverage report (HTML)
cargo llvm-cov nextest --workspace --fail-under-lines 80         # coverage gate (CI)
cargo llvm-cov nextest -p triad-runner --fail-under-lines 90     # runner coverage gate
```

Install `cargo-nextest` if not present: `cargo install cargo-nextest cargo-llvm-cov`

## Security and quality tooling

```bash
cargo deny check                    # license compliance + CVE advisories (deny.toml config)
cargo machete                       # unused dependency detection
cargo semver-checks                 # API compatibility check (run before every version bump)
```

Install once: `cargo install cargo-deny cargo-machete cargo-semver-checks`

To run the async task inspector (requires `--features tokio-console`):
```bash
cargo run --features tokio-console  # start runner with console-subscriber enabled
tokio-console                       # connect inspector in a separate terminal
```
Install tokio-console: `cargo install tokio-console`

## Rust best practices for this project

### Error handling
- Library crates (`triad-core`, `triad-sdk`, `triad-runner`, `triad-proto`): use `thiserror` with typed error enums. Never use `anyhow` in library crates.
- Binary crate (`triad-cli`): use `anyhow` for top-level error propagation.
- Never use `unwrap()` or `expect()` in non-test code. Use `?` propagation.
- All public functions return `Result<T, TriadError>` or a domain-specific error type.
- Error messages must be actionable: include the field name, the bad value, and what is expected. `"postgres connection failed: pool exhausted (max_connections=10) — increase backends.postgres.max_connections"` not `"connection error"`.
- Config validation errors must be caught at startup with the full YAML field path — not at the first runtime use of that config value.

### Async
- All async code uses `tokio`. No `async-std`, no `futures::executor`.
- Use `tokio::task::JoinSet` for supervising concurrent pattern modules in `engine.rs`.
- Use `tokio_util::sync::CancellationToken` for shutdown propagation — not channels or booleans.
- Cancellation tokens flow top-down: `Runner → PatternEngine → each module`.

### Traits and generics
- Core traits (`Source`, `Sink`, `Transform`, `PatternModule`, `CheckpointStore`, `LeaderElector`) live in `triad-core/src/traits.rs`.
- All traits are `#[async_trait]` and object-safe. Prefer `Box<dyn Trait>` over generics at module boundaries.
- `#[automock]` annotations on traits are gated behind `#[cfg(test)]` to avoid mockall pulling into prod builds.

### Configuration
- All config is loaded via the `config` crate (YAML + env layering). No hardcoded values.
- Config structs live in `triad-core/src/config.rs` and derive `serde::Deserialize`.
- Environment variable overrides use `TRIAD_` prefix and `__` as separator (e.g. `TRIAD_BACKENDS__KAFKA__BROKERS`).

### Observability
- Use `tracing` macros everywhere (`trace!`, `debug!`, `info!`, `warn!`, `error!`). Never `println!` or `eprintln!` in library/runner code.
- Metric name constants live in `triad-core/src/metrics.rs`. Never inline metric name strings.
- Every span must include `pattern_name` and `pipeline_name` fields.
- Audit events go to the `triad.audit` Kafka topic, not stdout.

### Safety invariants — do not violate
- **EOS**: the Kafka producer transaction must wrap both the message send and `send_offsets_to_transaction`. Never commit a Kafka transaction without the offset.
- **Optimistic locking**: checkpoint updates must use `WHERE id = $1 AND version = $2`. Never update without the version check.
- **DLQ naming**: always use the template `triad.dlq.{source_topic}`. Never hardcode or use `{source_topic}.dlq`.
- **Inbox deduplication**: insert into `triad_inbox` must happen inside the same PG transaction as the business write.
- **Redis fallback**: when the Redis circuit breaker is open, inbox `INSERT` falls back to a PG transaction — never skip deduplication entirely.
- **WAL replication**: uses `tokio-postgres` replication connection (not the `sqlx` pool). These are separate connection types and must not be mixed.

### Kubernetes feature flag
- K8s leader election code (`K8sLeaseLeader`, kube client) is gated behind `#[cfg(feature = "kubernetes")]` in `triad-runner`.
- The `NoopLeader` (always-leader) is the default and must always compile without the feature.

## Quality gate — run this sequence before every `cargo nextest` invocation and before every commit

Always run in order. Never skip to nextest without the preceding checks passing first.

```bash
cargo fmt --check                                            # 1. formatting
cargo clippy --workspace -- -D warnings                      # 2. lints
cargo check --workspace                                      # 3. compile
cargo nextest run --workspace                                # 4. all unit tests
cargo llvm-cov nextest --workspace --fail-under-lines 80    # 5. coverage threshold
```

Never commit with failing tests. Never commit with clippy warnings. If tests fail, fix them before committing — do not skip or `#[ignore]` tests without adding a tracking comment with the reason.

## Post-merge / post-rebase gate — MANDATORY

After every `git merge`, `git rebase`, or conflict resolution, run the full quality gate before creating any commit or pushing:

```bash
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo check --workspace
cargo nextest run --workspace
```

This is non-negotiable. A rebase that compiles but breaks tests must be fixed before the branch is pushed or merged. Never assume a clean rebase is a correct rebase.

## Git workflow in this repo

This repo uses git worktrees for parallel agent development. Each feature has its own worktree:
```
/home/jreuben1/Code/triad-worktrees/<feature>/   ← each agent works here
/home/jreuben1/Code/triad/                        ← main branch, integration point
```

When working in a worktree, set `CARGO_TARGET_DIR` to avoid conflicts with other worktrees:
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-$(git branch --show-current)
```

Commit within the worktree branch. Do not push directly to `main`.

## Testing requirements

### Coverage target
- **Minimum 80% line coverage** per crate, measured with `cargo-llvm-cov`.
- **Critical modules** (`checkpoint.rs`, `engine.rs`, `backends/kafka.rs`, `backends/postgres.rs`): target 90%+.
- Coverage is checked with: `cargo llvm-cov --workspace --fail-under-lines 80`.

### Test organisation
```
src/
  module.rs
  module/
    tests.rs        # unit tests for module (in same crate)
tests/
  integration/
    test_*.rs       # testcontainers integration tests
```

### Unit tests
- Use `#[rstest]` for parameterised cases. Avoid copy-pasting test bodies.
- Mock all external I/O with `mockall` `#[automock]` traits. Unit tests must not touch the network, filesystem, or real backend processes.
- Async unit tests use `#[tokio::test]`.
- Test function names: `test_<unit>_<scenario>_<expected_outcome>`.

### Integration tests
- Use `testcontainers-modules` (Kafka, PostgreSQL, Redis) — not docker-compose.
- Share a single `TestStack` helper (defined in `tests/integration/helpers.rs`) that boots all three containers once per test binary.
- Webhook delivery tests use `wiremock`.
- Integration tests are gated behind `#[cfg(feature = "integration")]` and the `integration` feature flag.

### Property-based and fuzz targets
- For parsing code (protobuf, config, WAL event decoding): add `proptest` or `cargo-fuzz` targets.

## Definition of done — pattern module

A pattern module is not done until all of the following pass:

- [ ] Unit tests ≥ 90% line coverage for the module file
- [ ] Integration test covers: happy path + at least one failure/retry path
- [ ] All metrics emitted using constants from `triad-core/src/metrics.rs` (no inline strings)
- [ ] Every `tracing` span includes `pattern_name` and `pipeline_name` fields
- [ ] DLQ routing implemented for poison pills (uses `triad.dlq.{source_topic}` template)
- [ ] Config struct added to the `PatternConfig` enum in `triad-core/src/config.rs`
- [ ] Example YAML stanza added to `triad-physical-design.md` §7 or the config reference
- [ ] Admin API route wired if the pattern has inspectable state (saga, dlq, checkpoint)
- [ ] Cancellation token respected: module exits cleanly within drain timeout
- [ ] `cargo clippy` clean, `cargo fmt` applied

## Key design documents

- `triad-system-design.md` — conceptual design, all patterns, deployment modes, observability SLOs, durability model
- `triad-physical-design.md` — Rust implementation plan: all structs, traits, SQL DDL, test matrix

## Deployment modes (summary)

| Mode | How | Leader |
|---|---|---|
| 1 | `TriadInstance::start()` embedded in app | `NoopLeader` (always leader) |
| 2 | `triad run` standalone binary | `NoopLeader` |
| 3 | K8s Deployment + `kubernetes` feature | `K8sLeaseLeader` (15s lease) |

## Admin API endpoints (port 8080)

```
GET  /health/live               liveness probe
GET  /health/ready              readiness probe (checks PG + Kafka + Redis)
GET  /health/started            startup probe
GET  /metrics                   Prometheus text format
GET  /patterns                  list all pattern modules + state
POST /patterns/{name}/pause
POST /patterns/{name}/resume
POST /patterns/{name}/replay
GET  /registry                  pattern module registry
GET  /checkpoints               list checkpoint offsets
POST /pipelines/{name}/reload
GET  /lag                       Kafka consumer group lag per topic/partition
GET  /dlq/{topic}               list DLQ messages
POST /dlq/{topic}/replay
DELETE /dlq/{topic}             purge DLQ
GET  /saga                      list in-flight sagas
GET  /saga/{id}                 inspect saga state + step history
POST /saga/{id}/cancel
POST /config/reload             re-read triad.yaml
GET  /metrics/cardinality       metric label cardinality report
```

## Launching agents

This project uses the `/zellij-launch` skill (must be inside a zellij session).

### Starting a phase

```
/zellij-launch phase N    # opens tabs for batch N as defined in project-plan.md
```

The skill reads the **Agent Launch Configuration** table in `project-plan.md` — that table is the single source of truth for which worktrees, targets, prompts, and `/loop` flags apply to each batch. Do not hardcode phase numbers here; check the table for what is currently unstarted.

### When to use /loop vs parallel agents

| Agent strategy | When to use |
|---|---|
| **Parallel agent** (default) | Module is self-contained with a clear spec — agent runs once, commits, done |
| **`/loop`** | Complex concurrency or state machines (saga, EOS, engine FSM) — needs iterative TDD cycles |

After `/zellij-launch`, check the skill output for "Run /loop in: ..." and switch to those tabs to start the loop.

### Checking progress
Run `/project-status` in any Claude Code tab (or the dedicated `status` tab opened by `/zellij-launch`) to see: git branches, worktree list, plan checklist, cargo check result, and pass/fail per worktree.

### Agent prompts
Per-agent task descriptions live in `scripts/prompts/<name>.md`. Each prompt specifies the worktree, done criteria, and key invariants. Read these before modifying agent behaviour.

Before starting any agent task, also read `claude-best-practices-learned.md` — it contains accumulated patterns for avoiding permission prompts, git pitfalls, and Cargo anti-patterns discovered across all sessions.

## PR checklist

Run through this before marking any branch ready for merge:

```
[ ] cargo fmt --check passes
[ ] cargo clippy --workspace -- -D warnings passes (zero warnings)
[ ] cargo nextest run --workspace passes (zero failures, zero ignored without comment)
[ ] cargo llvm-cov nextest --workspace --fail-under-lines 80 passes
[ ] No todo!(), unimplemented!(), or bail!("not implemented") left in non-stub code
[ ] No inline metric name strings (all use constants from metrics.rs)
[ ] project-plan.md items checked off for work done in this branch
[ ] CLAUDE.md updated if a new safety invariant was discovered
[ ] claude-best-practices-learned.md updated if a new pitfall was discovered
[ ] triad-physical-design.md §1.1 still matches the file tree
[ ] Any non-obvious design choice recorded in triad-system-design.md §14
```

If opening a PR that adds or changes a public API: also run `cargo semver-checks`.

## Self-optimisation instructions

**Before starting work**, read `claude-best-practices-learned.md` for accumulated invariants from previous sessions. Apply them immediately — do not rediscover known pitfalls.

**After completing each module or phase**, do all four steps before opening a PR:

1. **Update `project-plan.md`**: check off completed items `[x]`. Add any sub-tasks discovered.

2. **Update `claude-best-practices-learned.md`** when you discover:
   - A new permission/allowlist pitfall (e.g. a command pattern that triggers prompts)
   - A Bash call pattern that causes unexpected behaviour (pipes, expansion, exit codes)
   - A Cargo/nextest/git gotcha not already documented
   Keep entries concise: **Rule** + **Fix** only. No prose.

3. **Update this file (`CLAUDE.md`)** when you discover:
   - A new safety invariant that must hold across all modules (add to "Safety invariants")
   - A Rust anti-pattern that caused test failures or clippy errors
   - A crate API gotcha from compiler errors
   Do not add anything derivable from reading the code. Only things that would surprise a future agent.

4. **Commit all three files together** (`project-plan.md`, `CLAUDE.md`, `claude-best-practices-learned.md`) with the implementation commit.

**Recording architecture decisions**: when you make a non-obvious design choice — picking one library over another, choosing a data structure, modelling something as a trait vs enum — record it in `triad-system-design.md` §14 as a short paragraph: *decision made*, *alternatives considered*, *reason chosen*. This prevents the same debate recurring in a future session.

## End-of-phase introspective review (mandatory)

At the end of every phase — before tagging or opening the next phase's PR — run this review. It takes 10–15 minutes and prevents invisible technical debt. The goal is not only to verify compliance but to actively propose improvements to process, design, and code quality.

### Process review
- Does `/project-status` show a clear, phase-grouped TODO list? If the skill output was confusing, improve `~/.claude/commands/project-status.md`.
- Are all new phases added to the **Agent Launch Configuration** table in `project-plan.md`?
- Do all worktree `.claude/settings.json` Stop hooks still point to a valid tab name (`status`)?
- Are any merged branches still present as local worktrees? Run `git worktree list` and clean up.
- Did every agent commit the three-file discipline (`project-plan.md` + `CLAUDE.md` + `claude-best-practices-learned.md`)? Check with `git log --name-only main..HEAD` per worktree.

### Retrospective — process improvement
Answer these before moving to the next phase. Capture anything non-obvious in `devflow-improvement-checklist.md`.

- **What slowed this phase down?** Any task that took 2× longer than expected? Could a better prompt, a pre-written scaffold, or a skill have helped?
- **Any repeated manual steps?** If you ran the same sequence of commands more than twice, write a script or update a skill.
- **Did any agent produce wrong output requiring hand-correction?** If so, update the agent's prompt in `scripts/prompts/<name>.md` to prevent recurrence.
- **Was the phase scope right?** If the batch took more than one full session, split it next time. If it was trivial, merge it with an adjacent phase.
- **Did the `/project-status` skill give accurate information?** If not, fix the skill.

### Design doc review
- Does `triad-system-design.md` still accurately describe the implementation? Flag any sections that use Go terminology, pseudocode, or describe unimplemented patterns without a "v0.x.0 scope" note.
- Are newly implemented patterns added to §13 and marked in the v0.1.0 status block?
- Are any §14 open questions now resolved? Update with the decision taken.
- Do §15.3 metric names match constants in `triad-core/src/metrics.rs`?

### Design/code/plan sync review (run at end of every phase)

Verify all three artefacts are consistent **before** opening a PR or tagging a release:

1. **Directory tree** — does `triad-physical-design.md` §1.1 match reality?
   ```bash
   find crates/ -type f -name "*.rs" | sort   # compare against §1.1
   find tests/  -type f | sort
   ```
   Update §1.1 for any files added, renamed, or removed.

2. **Schema** — do `triad-physical-design.md` §7 DDL blocks match the actual migration files in `crates/triad-runner/migrations/`? Any column added/removed in a migration must be reflected.

3. **CLI command tree** — does §6.1 in the design doc match `triad-cli/src/main.rs`? Update for new subcommands.

4. **Plan truthfulness** — no `[x]` in `project-plan.md` for an artefact not present on disk:
   ```bash
   # spot-check: pick five [x] file items and run: ls <path>
   # uncheck any item whose artefact is missing; move to current phase as [ ]
   ```

5. **Admin routes** — does the `CLAUDE.md` "Admin API endpoints" table match the routes in `admin.rs`? Add any new routes added this phase.

### Design improvement signals
Actively look for opportunities to improve the architecture — not just verify it is still correct.

- **Module size**: any source file (excluding tests) exceeding ~400 lines? Consider splitting at a natural seam.
- **Cross-cutting duplication**: if the same pattern (retry loop, circuit-breaker check, span creation) appears in 3+ modules, it should be a shared utility or middleware.
- **Trait object friction**: if callers are doing `Box::new(...)` in many places or fighting object safety, consider making the trait generic with an associated type.
- **Unnecessary `Arc<Mutex<T>>`**: could it be `Arc<RwLock<T>>`? Could it be replaced with a message-passing channel? Lock contention in async code is a common performance trap.
- **Unrecorded design decisions**: any choice made this phase that a future agent would find surprising? Add it to `triad-system-design.md` §14.

### Code improvement signals
Look for specific code-quality opportunities, not just "does it compile".

- **Gratuitous clones**: search for `.clone()` on `Vec`, `String`, or large structs in hot paths (poll loops, per-message processing). Are any avoidable with borrows or `Arc`?
- **Performance lints**: run `cargo clippy -- -W clippy::perf` explicitly (in addition to the default set) and fix anything flagged.
- **`// TODO` / `// FIXME` accumulation**: `grep -rn 'TODO\|FIXME' crates/`. List them. Fix any that are easy; file the rest as tracked plan items with a `[ ]` in `project-plan.md`.
- **Stub methods**: `grep -rn 'todo!\|unimplemented!\|not yet implemented' crates/`. Any that are not gated behind a feature flag or explicitly deferred must be completed before merge.
- **Unused error variants**: do all `thiserror` enum variants have at least one call site? Dead variants are dead code and mislead readers.

### API ergonomics check
Run this when any phase adds or changes public API (SDK, CLI, or admin HTTP).

- **First-use test**: read the SDK public surface (`triad-sdk/src/lib.rs`) as if you wrote none of it. Is the entry point (`TriadInstance::start`) obvious? Are the pattern facades discoverable?
- **Error message quality**: are user-visible errors actionable? Each error should include the affected field/resource, the bad value, and what to do about it.
- **Config validation at startup**: every required config field should be validated in `TriadConfig::validate()` with a clear field path in the error — not discovered at runtime when the first query fails.
- **CLI help text**: run `triad --help` and `triad <subcommand> --help`. Are descriptions clear to someone who has never read the design docs?

### Test quality audit
- **Behaviour vs implementation**: are tests asserting outcomes ("the message appears in Kafka") or implementation details ("function X was called with argument Y")? Tests that assert internal call order break on refactors without catching regressions.
- **Test independence**: do integration tests share mutable state? Each test must be runnable in isolation and in any order.
- **`#[ignore]` hygiene**: `grep -rn '#\[ignore\]' crates/`. Every ignored test must have a comment explaining why and a plan item to re-enable it.
- **Coverage blind spots**: run `cargo llvm-cov nextest --workspace --html` and open the report. Look for uncovered branches in error handling paths — these are where bugs hide.
- **Brittle assertions**: tests asserting exact error message strings or exact log output will break on wording changes. Prefer asserting error *type* and *key fields*.

### Codebase review
- Run `cargo machete` — remove any unused dependencies that accumulated.
- Run `cargo deny check` — verify no new CVEs or license violations.
- Run `cargo semver-checks` — verify no accidental public API breaks (before any version bump).
- Check `cargo clippy --workspace -- -D warnings` for new lint categories that emerged.

### Tooling review
- Are all dev-dependencies actually used? If a crate was added for a planned feature that wasn't implemented, remove it or add a placeholder test.
- Would any marketplace plugin (`/ultrareview`, `/schedule`) improve the next phase's workflow?
- Are the agent prompts in `scripts/prompts/` still accurate? Update any that drifted from what agents actually need.

### Output
Capture non-obvious findings in `claude-best-practices-learned.md` and update `devflow-improvement-checklist.md` with any new action items discovered. For each item: state the finding, the proposed improvement, and the effort estimate (S/M/L).
