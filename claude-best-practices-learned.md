# Claude Code — Best Practices Learned

> Accumulated from triad development sessions. Each agent should read this before starting work
> and update it when discovering new invariants or anti-patterns.

---

## Permission Allowlist

### Pipes break allowlist pattern matching
**Rule:** Never use `|` in a Bash tool call when the command needs to match an allowlist entry.
A piped command (e.g. `git log ... | head -20`) is evaluated as a shell pipeline; Claude Code
sees the full string and the `git -C *` pattern no longer matches.
**Fix:** Use native flags (`git log -n 20`) or separate sequential Bash calls.

### cd before git triggers a hardcoded hook-safety prompt
**Rule:** Never write `cd <path> && git <subcommand>`. Claude Code specifically blocks this
pattern to prevent untrusted git hook execution. **This cannot be suppressed via the allowlist.**
**Fix:** Use `git -C <path> <subcommand>` for every git operation in a worktree.

### Env-var prefix breaks allowlist prefix matching
**Rule:** `CARGO_TARGET_DIR=... cargo nextest run` does NOT match `Bash(cargo nextest *)` because
pattern matching is prefix-based and the command starts with `CARGO_TARGET_DIR=`.
**Fix:** Add `Bash(CARGO_TARGET_DIR=/tmp/<project>-target-*)` to the project allowlist, or pass
the target dir via `--target-dir` so the command starts with `cargo`.

### Shell variable expansion triggers "Contains expansion" prompt
**Rule:** For-loops with `${VAR}` or `$(cmd)` in the command string cause a blanket prompt
regardless of what the expanded values are.
**Fix:** Have Claude iterate in memory and make one concrete Bash call per item with literal paths.

### ls outside the project root triggers a directory-scope check
**Rule:** Even though `ls` is auto-allowed, accessing paths outside the project root prompts for
directory-level permission.
**Fix:** Use `git -C <REPO> worktree list` to enumerate worktrees; no external path access needed.

### grep -c with || echo 0 produces double output
**Rule:** `grep -c 'pattern' file || echo 0` outputs `0\n0` when there are no matches (grep
exits 1 and prints "0", then echo also prints "0"), breaking arithmetic expressions.
**Fix:** Use `grep 'pattern' file | wc -l` (always exits 0, always outputs a number) — but this
is itself a pipe, so do it in a separate Bash call or use the Read tool and count in memory.
**In skills:** Read the file with the Read tool and count matching lines in Claude's memory.

---

## Skills Architecture

### Generic mechanism → skill; project data → project file
**Rule:** Claude skills (`~/.claude/commands/*.md`) contain the reusable *how* (zellij tab
mechanics, merge/rebase logic, status display logic). Project-specific *what* (which worktrees,
which prompts, which phases) lives in `project-plan.md` and is read at runtime by the skill.

### Skills must not use bash watch; use interactive invocation instead
**Rule:** `watch -n N /skill` is not possible — skills require an interactive Claude session.
**Fix:** Open the status tab as `cd <repo> && claude`, then run `/project-status` on demand.

### Skill Bash calls: one allowlist-safe pattern per call
**Rule:** Every Bash call in a skill must begin with a token that matches an allowlist pattern
(or is auto-allowed). Check the allowlist before writing a skill step. If no pattern covers it,
either add one or restructure the call.

### Skills read project-plan.md for config — never hardcode paths
**Rule:** Worktree names, CARGO_TARGET_DIR values, tab names, prompt paths, and /loop flags
all live in the "Agent Launch Configuration" table in `project-plan.md`. Skills parse this at
runtime so changing the project config requires only one edit.

---

## Zellij Automation

### Tab lifecycle
- Tabs do not auto-close when Claude finishes — this is expected. Review output, then close manually
  or let `/project-status` close it after a successful merge.
- To close a tab from another tab: `zellij action go-to-tab-name <name>` → `zellij action close-tab`
  → `zellij action go-to-tab-name status` to return.
- KDL layout files only work for *new* sessions. For in-session tab management use `zellij action`.

### Tab commands must use concrete paths
**Rule:** Write-chars commands typed into a zellij tab run in a shell. Use absolute paths and
`git -C` style — no `cd && git` patterns, no `${VAR}` expansion.

---

## Cargo / Testing

### nextest exit codes
- Exit 0: all tests passed
- Exit 4: no tests to run (scaffold crate) — treat as neutral, not failure
- Other non-zero: compilation error or test failure

### nextest parallel cancellation
**Rule:** When running multiple nextest calls in parallel and one exits with code 4, Claude Code
cancels all sibling calls. **Run worktrees sequentially** in status checks.

### --manifest-path instead of cd
**Rule:** `cargo nextest run --manifest-path <abs-path>/Cargo.toml` avoids the `cd && cargo`
pattern. Combine with the allowlist entry `Bash(cargo nextest *)`.

### Isolated CARGO_TARGET_DIR per worktree
**Rule:** Each worktree must export `CARGO_TARGET_DIR=/tmp/<project>-target-<branch>` to prevent
build artifact conflicts between parallel agents. This is set in the Agent Launch Configuration
table and passed through by the `/zellij-launch` skill.

---

## Git Worktree Workflow

### Detect scaffold vs worked-on worktree without external ls
```
git -C <REPO> rev-parse main          # get main SHA
git -C <wt> rev-list <SHA>..HEAD --count   # 0 = scaffold, >0 = has work
```

### Merge strategy: ff-only first, rebase on divergence
1. `git -C <REPO> merge --ff-only <branch>` — safe, no merge commit
2. On failure: `git -C <wt> rebase main` → retry ff-only
3. On rebase conflict: `git -C <wt> rebase --abort` + warn user
Never force-merge or use `--no-ff` without explicit user request.

### Merge gate: plan complete + tests pass
**Rule:** Auto-merge only when **all** plan checklist items for a branch are `[x]` AND
`cargo nextest run` exits 0. If tests pass but plan items remain, the agent needs more work.

---

## Project Plan as Source of Truth

### Plan checklist drives skill behaviour
- `not-started` (all `[ ]`): skip testing — no commits to evaluate
- `in-progress` (some `[x]`): run tests, report results, do not auto-merge
- `plan-complete` (all `[x]`): run tests; if passing, auto-merge + close tab
- `merged`: skip entirely

### Update plan and CLAUDE.md together at end of each phase
After completing a phase, the agent must:
1. Mark all completed items `[x]` in `project-plan.md`
2. Add any new invariants discovered to `claude-best-practices-learned.md`
3. Update `CLAUDE.md` if a new safety invariant or anti-pattern was found
4. Commit all three files together before opening the PR

---

## Rust Crate API Pitfalls (triad-specific)

### tokio-postgres 0.7: no `replication_mode()` on `Config`
**Rule:** `tokio_postgres::Config` in version 0.7.x does NOT have a `replication_mode()` setter
or `config::ReplicationMode` type — the compiler suggests `application_name` as the closest
method. The `replication=database` startup parameter must be embedded in the DSN string itself
or added as a key-value pair before parsing.
**Fix:** Parse the base DSN, then try re-parsing a modified DSN with `replication=database`
appended; fall back to the base config if tokio-postgres doesn't recognise the parameter.

### deadpool-redis 0.16: no `sentinel` feature
**Rule:** `deadpool-redis = { version = "0.16", features = ["sentinel"] }` fails with
"package does not have that feature". Version 0.16.0 only ships `cluster`, `rt_tokio_1`,
`rt_async-std_1`, and `serde` features.
**Fix:** For Sentinel mode, use `redis::sentinel::SentinelClient::build()` directly (from the
`redis` crate with `sentinel` feature). The enum variant stores a `SentinelClient` instead of
a `deadpool_redis::Pool`; callers obtain connections via `get_async_master_connection()`.

### deadpool-redis cluster::Config: `connections` field
**Rule:** `deadpool_redis::cluster::Config` has three fields: `urls`, `connections`, `pool`.
Struct literals with only `urls` and `pool` fail with "missing field `connections`".
**Fix:** Include `connections: None` in the struct literal.

### redis 0.26: `SentinelServerType`, not `SentinelNodeType`
**Rule:** In redis 0.26, the sentinel node type enum is `redis::sentinel::SentinelServerType`,
not `SentinelNodeType`. The compiler suggests the correct name.

### tokio-util 0.7: no `sync` feature gate needed
**Rule:** `tokio-util = { version = "0.7", features = ["sync"] }` fails — `sync` is not a
recognised feature. The `sync` module (`CancellationToken`) is included by default.
**Fix:** Omit `features` entirely: `tokio-util = { version = "0.7" }`.

### sqlx::migrate!() requires migrations/ directory at compile time
**Rule:** `sqlx::migrate!("./migrations")` is a compile-time macro. If the directory does not
exist, the build fails. An empty directory (with a `.gitkeep`) compiles and runs as a no-op.
**Fix:** Create `crates/<crate>/migrations/.gitkeep` before using the macro.

### Clippy: prefer `io::Error::other()` over `io::Error::new(ErrorKind::Other, _)`
**Rule:** `clippy` (with `-D warnings`) rejects `std::io::Error::new(std::io::ErrorKind::Other, e)`
because `io::Error::other(e)` is more idiomatic in Rust 1.74+.
**Fix:** Use `std::io::Error::other(e)` for generic I/O error wrapping.

### Worktree rebase before feature work: pull main first
**Rule:** Feature worktrees are created from the scaffold commit and do NOT automatically
receive commits merged to main (e.g., triad-core). Always rebase the worktree onto main
before starting implementation: `git -C <worktree> rebase main`.
**Symptom:** triad-core source files appear empty (0 bytes) in the worktree.

---

## Inter-Agent Eventing

### Stop hook triggers /project-status automatically
**Rule:** Each worktree's `.claude/settings.json` has a `Stop` hook that fires when the Claude
session exits, switching to the `status` tab and typing `/project-status\n` into it.
**Format:**
```json
{"hooks": {"Stop": [{"matcher": "", "hooks": [{"type": "command",
  "command": "zellij action go-to-tab-name status; zellij action write-chars $'/project-status\\n'"}]}]}}
```
**Fix:** The `$'...'` ANSI-C quoting sends a literal newline; JSON needs `\\n` (double-escaped).
`go-to-tab-name` must precede `write-chars` — the latter types into whichever pane is focused.

### /project-status auto-launches next batch after merge
**Rule:** After a successful merge in step 9, `/project-status` step 10 reads the Agent Launch
Configuration table, determines which batch is newly unblocked, waits 10 seconds, then opens
the next batch's tabs directly via `zellij action new-tab / rename-tab / write-chars` — no
manual `/zellij-launch` call needed.
**Fix:** The 10-second `sleep` is the abort window (Ctrl-C) before irreversible tab creation.

### zellij tab-close needs sleep between actions
**Rule:** `go-to-tab-name` → `close-tab` → `go-to-tab-name status` must have `sleep 0.3`
between each pair; without it, `close-tab` may fire before the tab switch completes.

---

## PR Workflow

### Agents open PRs; /project-status merges via gh pr merge
**Rule:** Each agent's Done criteria ends with `gh pr create --title ... --body ...`.
`/project-status` step 9c runs `gh pr list --head <branch> --state open` to detect the PR;
if found, merges via `gh pr merge <branch> --merge --delete-branch` (CI-gated).
Branches without a PR fall back to the direct `git merge --ff-only` path (legacy/manual).
**Fix:** Add `Bash(gh pr *)` to the project allowlist so `gh pr list` and `gh pr merge` don't prompt.

---

## mockall Trait Design Pitfalls

### mockall cannot handle `&[&str]` (doubly-borrowed slice) in trait methods
**Rule:** `mockall::automock` cannot generate mock expectations for methods with `&[&str]`
parameters — the compiler raises "missing lifetime specifier" and "`&` without an explicit
lifetime name cannot be used here" errors in the test binary.
**Fix:** Change trait methods to use `Vec<String>` instead of `&[&str]`. Convert at the
call site inside the production implementation: `features.iter().map(|s| s.to_string()).collect()`.
The public API (e.g. `FeatureServer::lookup`) can still accept `&[&str]` — only the narrow
mock-targeted trait needs owned types.

### mockall cannot handle `Option<&str>` or `&str` in trait method signatures
**Rule:** Methods with `event_id: &str` or `error: Option<&str>` parameters cause lifetime
errors when mockall generates the mock. Affects any trait annotated with `#[automock]`.
**Fix:** Use owned types throughout: `String`, `Option<String>`. Update all call sites to
pass `.clone()` or `.to_string()` values.

### MockCheckpointStore from triad-core is not available in runner tests
**Rule:** `#[cfg_attr(test, mockall::automock)]` in a library crate generates the mock only
when *that crate* is compiled with `cfg(test)`. When used as a dependency, `cfg(test)` is
false, so `MockCheckpointStore` does not exist in the downstream crate's test binary.
**Fix:** Define a local `NoopCheckpointStore` struct in the test module:
```rust
struct NoopCheckpointStore;
#[async_trait::async_trait]
impl CheckpointStore for NoopCheckpointStore {
    async fn load(&self, _: &PatternName, _: &PipelineName) -> Result<Option<CheckpointRow>, CheckpointError> { Ok(None) }
    async fn save(&self, _: &CheckpointRow, _: i64) -> Result<(), CheckpointError> { Ok(()) }
}
```

### mockall::mock::MockFoo does not exist — use MockFoo directly
**Rule:** The path `mockall::mock::MockFooBar` is invalid — `mock` is not a module in mockall.
The generated mock type is named `MockFooBar` in the same module scope where the trait is defined.
**Fix:** Use `MockFooBar::new()` directly (imported via `use super::*` in the test module).
