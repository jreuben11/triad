#!/usr/bin/env bash
# Triad development status — run anytime to see where things stand
# Usage: ./scripts/status.sh

REPO=/home/jreuben1/Code/triad
WORKTREES=/home/jreuben1/Code/triad-worktrees

echo "════════════════════════════════════════════════════"
echo " Triad — Development Status  $(date '+%Y-%m-%d %H:%M')"
echo "════════════════════════════════════════════════════"

echo ""
echo "── Git branches ────────────────────────────────────"
git -C "$REPO" log --oneline --all --graph --decorate 2>/dev/null | head -20

echo ""
echo "── Worktree branches ───────────────────────────────"
git -C "$REPO" worktree list 2>/dev/null

echo ""
echo "── Project plan checklist ──────────────────────────"
TOTAL=$(grep -c '^\- \[' "$REPO/project-plan.md" 2>/dev/null || echo 0)
DONE=$(grep -c '^\- \[x\]' "$REPO/project-plan.md" 2>/dev/null || echo 0)
TODO=$((TOTAL - DONE))
echo "  Completed: $DONE / $TOTAL   Remaining: $TODO"
echo ""
grep '^\- \[x\]' "$REPO/project-plan.md" 2>/dev/null | sed 's/^/  ✓ /' || true
grep '^\- \[ \]' "$REPO/project-plan.md" 2>/dev/null | head -10 | sed 's/^/  · /' || true

echo ""
echo "── Cargo check (workspace) ─────────────────────────"
cargo check --workspace --manifest-path "$REPO/Cargo.toml" -q 2>&1 \
  | grep -E "^error|Finished" || echo "  (not checked)"

echo ""
echo "── Test results per worktree ───────────────────────"
for wt in "$WORKTREES"/*/; do
  branch=$(git -C "$wt" branch --show-current 2>/dev/null || echo "?")
  result=$(CARGO_TARGET_DIR="/tmp/triad-target-$(basename $wt)" \
    cargo nextest run --manifest-path "$wt/Cargo.toml" -q 2>&1 | tail -1)
  printf "  %-45s %s\n" "$branch" "$result"
done

echo ""
echo "════════════════════════════════════════════════════"
