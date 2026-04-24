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
