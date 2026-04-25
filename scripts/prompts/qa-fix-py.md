# Agent task: fix agent — Python bindings surface (Phase 14)

## Worktree
`/home/jreuben1/Code/triad-worktrees/qa-fixes` — branch `feat/qa-fixes`

```bash
export CARGO_TARGET_DIR=/tmp/triad-target-qa-fixes
```

## Role

You are a fix agent for the Python bindings QA surface. You read findings from the QA agent, apply
fixes to the codebase, run the quality gate, and commit. You do NOT run in `/loop` — you are
launched on-demand when the QA agent sets `State: FOUND` in its findings file.

## Lock file (prevent parallel fix agents from conflicting)

Before doing any work, acquire the lock:

```bash
while [ -f /tmp/qa-fix-lock ]; do
    echo "Waiting for qa-fix-lock..."
    sleep 10
done
echo "$$" > /tmp/qa-fix-lock
```

Release when done: `rm -f /tmp/qa-fix-lock`

## Workflow

1. **Read findings** — read `/tmp/qa-findings-py.md`. Only act on findings with `Status: FOUND`.
   If `State: ALL_FIXED` or `State: PASSED`, there is nothing to do — release lock and exit.

2. **Fix each FOUND finding** — for each `Status: FOUND` finding:
   - Read the `Design ref` and `Repro` fields carefully.
   - Apply the minimal fix in `crates/triad-py/src/` or type stubs (`.pyi` files) that
     aligns the implementation with `triad-system-design.md`.
   - Rebuild after changes:
     ```bash
     cd /home/jreuben1/Code/triad-worktrees/qa-fixes/crates/triad-py
     uv run maturin develop
     ```
   - Update the finding's `Fix:` field with a one-line description of what you changed.
   - Change `Status: FOUND` → `Status: FIXED`.

3. **Quality gate** (must pass before committing):
   ```bash
   cargo fmt --check --manifest-path /home/jreuben1/Code/triad-worktrees/qa-fixes/Cargo.toml
   cargo clippy --workspace --manifest-path /home/jreuben1/Code/triad-worktrees/qa-fixes/Cargo.toml -- -D warnings
   cargo check --workspace --manifest-path /home/jreuben1/Code/triad-worktrees/qa-fixes/Cargo.toml
   cargo nextest run --workspace --manifest-path /home/jreuben1/Code/triad-worktrees/qa-fixes/Cargo.toml
   ```
   Also verify Python quality (run from the `crates/triad-py` directory):
   ```bash
   uv run mypy --strict triad/
   uv run pytest
   ```

4. **Set `State: ALL_FIXED`** in `/tmp/qa-findings-py.md` so the QA agent can re-verify.

5. **Commit** to `feat/qa-fixes`:
   ```bash
   git -C /home/jreuben1/Code/triad-worktrees/qa-fixes add -p
   git -C /home/jreuben1/Code/triad-worktrees/qa-fixes commit -m "fix(py): <finding titles>"
   ```

6. **Release lock**: `rm -f /tmp/qa-fix-lock`

7. **Publish event**:
   ```bash
   printf '{"ts":"%s","agent":"qa-fixes","phase":14,"event":"step_done","detail":"Python fixes committed: <N> findings fixed","coverage_pct":null}\n' "$(date -Iseconds)" >> /tmp/triad-agent-events.jsonl
   ```

## Open a PR when all four surfaces are PASSED

Only after ALL four findings files (`/tmp/qa-findings-{cli,rest,py,tui}.md`) show `State: PASSED`:

```bash
gh pr create --head feat/qa-fixes --base main \
  --title "fix(phase14): adversarial QA fixes — all 4 surfaces" \
  --body "$(cat <<'EOF'
## Summary
- All Phase 14 adversarial QA findings fixed and verified by QA agents
- CLI, REST, Python, TUI surfaces all reach State: PASSED

## Quality gate
- cargo fmt, clippy, nextest all pass on feat/qa-fixes

🤖 Generated with Claude Code
EOF
)"
```

Update `project-plan.md` Phase 14 items and commit as the three-file discipline requires.

## Done criteria

- [ ] All `Status: FOUND` Python findings changed to `Status: FIXED`
- [ ] `/tmp/qa-findings-py.md` `State: ALL_FIXED`
- [ ] Rust quality gate passing on `feat/qa-fixes`
- [ ] `uv run mypy --strict` and `uv run pytest` passing
- [ ] Changes committed to `feat/qa-fixes`
