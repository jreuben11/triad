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

### Atomic tab launch — use `new-tab -- command`, never `write-chars`
**Rule:** `zellij action new-tab --name <name> -- <command>` creates the tab with the command
already running. This is atomic — no focus timing, no race condition, no `write-chars` needed.
**Fix:**
```bash
zellij action new-tab --name "phase3-cli" -- /tmp/run-phase3-cli.sh
```
The returned value is the new tab's integer ID. Use `go-to-tab-by-id <id>` (not `go-to-tab-name`)
for reliable navigation after creation, since rename may not have propagated yet.
**Never use** the old four-step sequence (`new-tab` → `sleep` → `rename-tab` → `write-chars`) —
it is a race condition and will send keystrokes to the wrong pane when focus shifts.

### Agent wrapper scripts prevent markdown shell-expansion
**Rule:** Never inline `$(cat prompt.md)` in a `write-chars` call or in the parent shell.
Markdown backtick-quoted text is treated as shell subcommands.
**Fix:** Write a `/tmp/run-<name>.sh` wrapper:
```bash
#!/bin/bash
cd /home/.../worktree
export CARGO_TARGET_DIR=/tmp/triad-target-<name>
exec claude --dangerously-skip-permissions "$(cat /path/to/prompt.md)"
```
Then launch atomically: `zellij action new-tab --name "<name>" -- /tmp/run-<name>.sh`

### Tab lifecycle
- Tabs do not auto-close when Claude finishes — this is expected. Review output, then close manually
  or let `/project-status` close it after a successful merge.
- To close a specific tab by ID: `zellij action go-to-tab-by-id <id>` then `zellij action close-tab`
  then `zellij action go-to-tab-by-id <status-id>` to return.
- Use `zellij action list-tabs` to get current tab IDs — always prefer ID-based navigation over
  name-based navigation (`go-to-tab-name` is unreliable when tabs share names or rename is pending).
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

### Stale worktrees linger after squash-merge — clean up before tagging
**Rule:** `gh pr merge --squash --delete-branch` deletes the *remote* branch but leaves the local
worktree directory and local branch intact. After a successful merge, explicitly remove:
```bash
git worktree remove --force /path/to/<worktree>
git branch -D <branch>
```
Run `git worktree list` to audit. A stale worktree is harmless but pollutes `/project-status` output
(it still appears in the list with no commits ahead of main, classified as `scaffold`).

### Agent Launch Config table must be updated for new phases
**Rule:** `/project-status` step 10 (auto-launch) and `/zellij-launch phase N` both read the
**Agent Launch Configuration** table in `project-plan.md`. If a new phase's worktree is not in
the table, neither skill can create its tabs automatically.
**Fix:** Before starting a new phase, add its rows to the table. Phases 9 (manual gate),
10 (`feat/triad-py`), and 11 (`feat/triad-tui`) need entries before their auto-launch works.
Phase 9 has no new worktree — it runs in the main repo. Add a note in the table rather than a row.

### TOML subtable insertion must come AFTER all fields of the parent table
**Rule:** `[package.metadata.cargo-machete]` is a subtable of `[package]`. If you insert it
before all `[package]` key-value pairs, the fields after the subtable header (e.g.
`edition.workspace = true`) are parsed as part of `package.metadata.cargo-machete`, not
`package`. This causes `edition` to be missing from `[package]`, silently reverting to
Rust 2015 and breaking compilation with E0670 errors.
**Fix:** Always place all `[package]` fields (`name`, `description`, `version`, `edition`, etc.)
BEFORE any `[package.metadata.*]` sections.

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

## mockall + async_trait attribute ordering

### Put #[automock] BEFORE #[async_trait] — always
**Rule:** With `#[async_trait]` before `#[cfg_attr(test, mockall::automock)]`, the proc-macro
expansion desugars async methods into `fn() -> Pin<Box<dyn Future<...>>>` BEFORE mockall sees them.
mockall then generates expectations where `returning` must return the pinned-boxed-future type,
making tests verbose and error-prone.
**Fix:** Always put `#[cfg_attr(test, mockall::automock)]` BEFORE `#[async_trait]`:
```rust
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait Foo: Send + Sync {
    async fn bar(&self, x: &str) -> Result<bool, PatternError>;
}
```
With this order, mockall sees the async method and generates expectations that accept a plain
`returning(|arg| Ok(value))` closure — no `Box::pin(async { ... })` needed.

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

### zellij write-chars: always go-to-tab-name + sleep before write-chars
**Rule:** After `zellij action new-tab` and `rename-tab`, the pane focus may not have moved to
the new tab by the time `write-chars` fires. All subsequent commands land in the previously
active tab instead of the new one.
**Fix:** Always run `zellij action go-to-tab-name "<name>"` (with `sleep 0.4`) immediately before
every `write-chars` call — even when you just opened and renamed the tab.

### zellij write-chars: never interpolate multi-line strings — use wrapper scripts
**Rule:** Passing `$(cat prompt.md)` inside a `write-chars` argument causes the parent shell to
expand the markdown (which contains backticks, quotes, newlines) before zellij sees it. The
result is garbled text echoed into the tab rather than a working command.
**Fix:** Write a small `/tmp/run-<tab>.sh` wrapper script that does the `cd`, `export`, and
`exec claude --dangerously-skip-permissions "$(cat prompt.md)"`. Then `write-chars` only sends
the short string `bash /tmp/run-<tab>.sh` — the `$(cat ...)` is evaluated by the tab's shell
at runtime, not the parent shell.

### zellij: never use sleep + write-chars to send a follow-up message to claude
**Rule:** `sleep N && zellij action write-chars "prompt"` is unreliable — focus shifts during
the sleep and the text lands in the wrong pane (often the shell, not claude's TUI input).
**Fix:** Do NOT automate the "wait for TUI then send prompt" sequence. Send only the
`claude --dangerously-skip-permissions` command via write-chars, then tell the user to switch
to the tab manually and paste the prompt once the TUI is visible.

### claude CLI: start with no args to guarantee interactive TUI
**Rule:** `claude --dangerously-skip-permissions "message"` sometimes runs in non-interactive
print mode (processes the prompt and exits) rather than showing the interactive TUI.
**Fix:** Always start `claude --dangerously-skip-permissions` with no arguments to guarantee the
TUI, then type or paste the prompt once the UI has rendered.

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
if found, merges via `gh pr merge <branch> --squash --delete-branch` (CI-gated).
Branches without a PR fall back to the direct `git merge --ff-only` path (legacy/manual).
**Fix:** Add `Bash(gh pr *)` to the project allowlist so `gh pr list` and `gh pr merge` don't prompt.

### `--delete-branch` fails when branch is checked out in a worktree
**Rule:** `gh pr merge <branch> --squash --delete-branch` fails with
"failed to delete local branch: used by worktree" when the branch is still active in a worktree.
**Fix:** Omit `--delete-branch` from the merge command. Clean up the worktree manually after merge:
```bash
git worktree remove --force /path/to/worktree
git branch -D <branch>
```

### Always squash-merge feature branches — never --merge
**Rule:** Use `gh pr merge <branch> --squash --delete-branch`, never `--merge`.
`--merge` preserves every intermediate commit from the feature branch plus adds a merge commit,
making `git log` on main very noisy (one agent session = 5–10 commits + a merge commit).
`--squash` collapses the branch to a single clean commit on main — one entry per phase.
**Why:** Discovered after Phases 0–3 were merged with `--merge`; retroactive fix would require
rewriting pushed history. Applied from Phase 7 (feat/tests) onward.

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

---

## Engine / Supervisor Testing

### Engine drain() cancels tokens — don't use it to assert restart counts
**Rule:** `PatternEngine::drain()` calls `self.cancel.cancel()` before waiting for the JoinSet.
If a module is in a backoff sleep between restarts, the cancel fires via `tokio::select!` and
breaks the loop early. A test that calls `drain()` then asserts `run_count >= N` will flake.
**Fix:** Poll `run_count` via a short `tokio::time::sleep` loop with a timeout, THEN drain:
```rust
tokio::time::timeout(Duration::from_secs(5), async move {
    loop {
        if rc.load(Ordering::SeqCst) >= N { break; }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}).await.unwrap();
engine.drain(Duration::from_secs(1)).await;
```

### ExponentialBackoff default interval is 500ms — too slow for unit tests
**Rule:** `backoff::ExponentialBackoff::default()` starts at 500ms. Tests that assert
"module was restarted 3 times" will timeout or flake.
**Fix:** Set `initial_interval: Duration::from_millis(50)` in the engine's supervisor task so
retries are fast enough for unit tests (total ~150ms for 2 retries).

### sqlx::query (runtime) vs sqlx::query! (compile-time) in the runner
**Rule:** `sqlx::query!` needs `DATABASE_URL` at compile time (or a `.sqlx/` directory for
offline mode). Since neither exists in this repo, all SQL in `checkpoint.rs` and patterns
uses `sqlx::query` (runtime) with `.bind()`.
**Fix:** Never use `sqlx::query!` in triad-runner. Use `sqlx::query` for all runtime queries.

---

## SDK Crate Pitfalls (triad-sdk)

### sqlx PgPool::connect_lazy requires a Tokio runtime even in sync tests
**Rule:** `sqlx::PgPool::connect_lazy(...)` panics with "this functionality requires a Tokio context"
when called from a non-async test, even though it is nominally "lazy" (no connection is made).
**Fix:** Mark all tests that construct a `PgPool` (even via `connect_lazy`) as `#[tokio::test]`.

### clippy: `from_str` clashes with `std::str::FromStr::from_str`
**Rule:** `clippy::should_implement_trait` rejects any method named `from_str` that doesn't
implement the `std::str::FromStr` trait. This applies even to associated functions on newtypes.
**Fix:** Rename to something less ambiguous (e.g., `wrap`, `parse_key`, `of`) or implement the
`FromStr` trait properly.

### CbConfig construction: use `From<&CircuitBreakerConfig>` instead of field literals
**Rule:** `CbConfig` in triad-runner exposes `half_open_after: Duration`, NOT `timeout_ms: u64`.
Constructing the struct with `timeout_ms: u64` fails to compile.
**Fix:** Use `CbConfig::from(&config.circuit_breakers)` — a `From<&CircuitBreakerConfig>` impl
is already provided and converts `timeout_ms → Duration::from_millis(timeout_ms)`.

### `git rebase main` fails when `.claude/settings.json` is untracked
**Rule:** Worktree rebase fails with "untracked working tree files would be overwritten" when
`.claude/settings.json` exists but is not tracked by git. The file must be temporarily removed
before rebase and restored after.
**Fix:** `cp .claude/settings.json /tmp/backup.json && rm .claude/settings.json && git rebase main && cp /tmp/backup.json .claude/settings.json`

### triad-runner patterns: `FlagStore` / `FlagEvaluation` not re-exported at top level
**Rule:** `triad_runner::patterns::FlagStore` does not exist. Only a subset of types are
re-exported at `triad_runner::patterns::*` (see patterns/mod.rs `pub use` statements).
`FlagStore` and `FeatureFlag` live at `triad_runner::patterns::feature_flag::{FlagStore, FeatureFlag}`.
**Fix:** Import directly from the sub-module: `use triad_runner::patterns::feature_flag::FlagStore;`

### Tower middleware: use type alias for complex `Arc<dyn Fn(...)>` types
**Rule:** `clippy::type_complexity` fires on struct fields with type
`Arc<dyn Fn(&Request<Body>) -> String + Send + Sync>`.
**Fix:** Define a module-level type alias: `pub type KeyFn = Arc<dyn Fn(&Request<Body>) -> String + Send + Sync>;`
and use it in both the struct definition and the `layer()` method.

---

## Integration Test Pitfalls (Phase 7)

### PostgreSQL `gen_random_uuid()` requires pgcrypto extension
**Rule:** `gen_random_uuid()` is not available by default in PostgreSQL — it requires the
`pgcrypto` extension. Testcontainers starts a plain Postgres image without it.
**Fix:** Add `CREATE EXTENSION IF NOT EXISTS pgcrypto;` to `0001_outbox.sql` (must run before
any table that uses `DEFAULT gen_random_uuid()`).

### `sqlx::query_as::<_, (i64,)>("SELECT 1")` fails with type mismatch
**Rule:** PostgreSQL's literal `1` is INT4, not INT8. Decoding into `(i64,)` fails at runtime
with a type mismatch error.
**Fix:** Use `"SELECT 1::bigint"` to cast explicitly.

### Saga version semantics: INSERT must store version=1, not version=0
**Rule:** `SagaOrchestrator` calls `get_or_insert_with()` on the checkpoint (creating version=0)
then calls `persist_checkpoint(cp, expected_version=0)`. After persist it increments `cp.version`
to 1. On the next `persist_checkpoint(cp, expected_version=1)` the DB UPDATE checks `WHERE version=1`.
If INSERT stored version=0, the UPDATE WHERE version=1 finds nothing and fails with optimistic lock.
**Fix:** In `SagaRepository::persist`, when `expected_version <= 0`, INSERT the row with `version=1`
(not 0). Use `if expected_version <= 0` (not `< 0`) since first call passes expected=0.

### AdminServer metrics endpoint returns 503 without PrometheusHandle
**Rule:** `AdminServer` returns HTTP 503 for `GET /metrics` when no `PrometheusHandle` is
configured. Tests that assert 200 will fail.
**Fix:** Test that `/metrics` returns 503 (expected behavior) rather than 200.
The `/checkpoints` route does not exist in the admin server — do not test it.

### wiremock header matchers: `HeaderValue: From<AnyMatcher>` not satisfied
**Rule:** `Mock::given(header("X-Foo", wiremock::matchers::any()))` fails to compile because
`HeaderValue` cannot be constructed from `AnyMatcher`.
**Fix:** Use `Mock::given(header_exists("X-Foo"))` or match only on method/path and verify
the header separately. Alternatively, use `.and(header("X-Foo", "expected-value"))` with a literal.

### `redis::Client::get_async_connection()` is deprecated in redis 0.26
**Rule:** `get_async_connection()` returns a deprecated `aio::Connection`. Clippy `-D warnings`
will fail.
**Fix:** Use `client.get_multiplexed_async_connection().await?` instead.

### `&'a str` lifetime annotations on method parameters trigger `needless_lifetimes` clippy
**Rule:** Explicit lifetime parameters like `pub async fn f<'a>(&self, s: &'a str)` trigger
`clippy::needless_lifetimes` when the lifetime can be elided.
**Fix:** Remove the explicit lifetime: `pub async fn f(&self, s: &str)`.

### Axum handler without `State` extractor compiles fine even when router uses `with_state`
**Rule:** Axum handlers don't require a `State` extractor even when the router is constructed
with `.with_state(state)`. Handlers that don't need state simply omit the extractor.
**Fix:** Only add `State(s): State<AdminState>` to handlers that actually use `s`. Unused
extractors trigger clippy `unused_variables` warnings.

### `TriadConfig` does not derive `Clone` — cannot be cloned in `run.rs`
**Rule:** `TriadConfig` and all nested config structs only derive `Debug + Deserialize + Serialize`,
not `Clone`. Attempting to clone the config to pass to both `Runner::new` and a shared config
`Arc<RwLock<TriadConfig>>` will fail to compile.
**Fix:** Load the config twice via `TriadConfig::load(path)?` — once for `Runner::new` (moved)
and once for the `shared_config` Arc.

### Kafka transactional producer calls are blocking — must use `spawn_blocking`
**Rule:** `BaseProducer::init_transactions`, `begin_transaction`, `commit_transaction`, and
`abort_transaction` are all synchronous blocking calls in rdkafka 0.36. Calling them directly
in an async context blocks the tokio runtime.
**Fix:** Wrap all transaction calls in `tokio::task::spawn_blocking(|| { ... })`.

### `PatternControl` variants: use separate `Replay` and `Reload`, not one variant for both
**Rule:** Using a single `Reload` variant for both `/patterns/:name/replay` and
`/pipelines/:name/reload` confuses pattern replay (re-consume from earliest offset) with
pipeline reload (re-read config). These are semantically distinct operations.
**Fix:** Define four variants: `Pause(String)`, `Resume(String)`, `Replay(String)` for pattern
replay, and `Reload(String)` for pipeline/config reload.


---

## TUI Crate (triad-tui, Phase 11)

### tachyonfx 0.7 requires ratatui 0.28.1 exactly — not 0.29
**Rule:** `tachyonfx = "0.7"` depends on `ratatui = "0.28.1"`. If `ratatui = "0.29"` is also
in workspace.dependencies, cargo resolves two separate ratatui versions in the dep graph.
`ratatui::style::Color` from 0.29 is a *different type* than `ratatui::style::Color` from
0.28.1, so `C: Into<Color>` bounds in tachyonfx (using 0.28.1's Color) reject values of
the 0.29 Color type. Compile errors manifest as trait-bound failures on `fade_from_fg`,
`slide_in`, etc.
**Fix:** Set `ratatui = { version = "0.28.1" }` in workspace.dependencies to unify versions.
Check `cargo tree -p tachyonfx` to confirm what version tachyonfx pulls before choosing.

### tachyonfx: `running()` requires `Shader` trait in scope; `render_effect` requires `EffectRenderer`
**Rule:** `Effect::running()` is defined on the `Shader` trait. `frame.render_effect(...)` is
defined on the `EffectRenderer` trait. Both must be imported explicitly.
**Fix:** Add `use tachyonfx::{Shader, EffectRenderer};` in any module that calls these methods,
including inside `#[cfg(test)]` modules that assert `effect.running()`.

### Dead-code lints on deserialization-only struct fields: use field-level `#[allow(dead_code)]`
**Rule:** Structs that are populated via `serde::Deserialize` from API responses often have
fields that are not read in the UI. Clippy `-D warnings` treats these as dead code.
**Fix:** Add `#[allow(dead_code)]` to individual unused fields. Avoid `#[allow(dead_code)]`
on the whole struct — that suppresses legitimate warnings on future fields. Alternatively,
add `#[allow(dead_code)]` to effect/API constructors that are reserved for future wiring.

---

## PyO3 0.28 Bindings (triad-py)

### pyo3 0.28: `Python::with_gil` renamed to `Python::attach`
**Rule:** `Python::with_gil(|py| ...)` does not exist in pyo3 0.28. The method was renamed.
**Fix:** Use `Python::attach(|py| ...)` everywhere. The closure semantics are identical.

### pyo3 0.28: `PyObject` removed from prelude — use `Py<PyAny>`
**Rule:** `PyObject` is not exported by `pyo3::prelude::*` in version 0.28. It was an alias
for `Py<PyAny>` and is no longer available by that name.
**Fix:** Replace all `PyObject` with `Py<PyAny>`. Importing `use pyo3::ffi::PyObject` gives the
raw FFI type — not what you want for high-level code.

### pyo3 0.28: `&PyModule` → `&Bound<'_, PyModule>` in `#[pymodule]`
**Rule:** The `#[pymodule]` fn signature must use `&Bound<'_, PyModule>` not `&PyModule`.
**Fix:** `fn _triad(m: &Bound<'_, PyModule>) -> PyResult<()>`

### pyo3 0.28: `experimental-async` feature enables `async fn` in `#[pymethods]`
**Rule:** To use `async fn` in `#[pymethods]`, add `features = ["experimental-async"]` to the
pyo3 dependency. Async methods become Python coroutines polled by asyncio — no separate
`pyo3-asyncio` crate needed.
**Fix:** The async fn body polls on the asyncio event loop. For Tokio-based futures (sqlx, redis,
etc.), bridge with `OnceLock<Runtime>` + `oneshot` channels:
```rust
static RT: OnceLock<Runtime> = OnceLock::new();
// In async #[pymethods]:
let (send, recv) = futures::channel::oneshot::channel();
RT.get().unwrap().spawn(async move { let _ = send.send(tokio_op().await); });
recv.await.map_err(|e| PyRuntimeError::new_err(e.to_string()))?
```

### pyo3 0.28: `#[pyclass]` with `#[derive(Clone)]` deprecated `FromPyObject` impl
**Rule:** `#[pyclass]` + `#[derive(Clone)]` implicitly generated a `FromPyObject` impl; in 0.28
this is deprecated and triggers `-D warnings` failures.
**Fix:** Add `skip_from_py_object` to the pyclass attribute: `#[pyclass(skip_from_py_object)]`.

### pyo3 0.28: `Option<T>` args in `#[pyfunction]` are NOT automatically optional
**Rule:** A `#[pyfunction]` with `foo: Option<u64>` requires explicit `#[pyo3(signature = (...))]`
to make the argument optional at the Python call site. Without it, Python must explicitly pass
the argument.
**Fix:** Add `#[pyo3(signature = (arg1, optional_arg=None))]` above the function.

### pyo3 0.28: `Arc::clone` pattern for extracting from `Py<T>` in async methods
**Rule:** `async fn` in `#[pymethods]` with `slf: Py<Self>` cannot hold a `PyRef` across await
points (GIL-tied). Extract what you need before the first await.
**Fix:** Use `Python::attach` to extract values before spawning the tokio task:
```rust
async fn my_method(slf: Py<Self>) -> PyResult<()> {
    let arc_val = Python::attach(|py| Arc::clone(&slf.borrow(py).inner_arc));
    // Now arc_val is 'static-safe, use in async block
    ...
}
```

### maturin: run from the crate directory, not the workspace root
**Rule:** `maturin develop` fails with "the manifest-path must be a path to a Cargo.toml file"
if you pass `--manifest-path path/to/pyproject.toml`. maturin expects to be run from the
directory that contains `pyproject.toml`.
**Fix:** `cd crates/triad-py && maturin develop --uv` (or use the directory form).
