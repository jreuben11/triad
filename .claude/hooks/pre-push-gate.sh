#!/usr/bin/env bash
# PreToolUse hook: blocks `git push` unless cargo fmt and clippy are clean.
# Exit 2 to block with message; exit 0 to allow.
set -euo pipefail

export CARGO_TARGET_DIR=/tmp/triad-target-main
MANIFEST="$CLAUDE_PROJECT_DIR/Cargo.toml"

# fmt check
FMT=$(cargo fmt --check --manifest-path "$MANIFEST" 2>&1 || true)
if [[ -n "$FMT" ]]; then
    echo "⛔ git push blocked: cargo fmt --check failed. Run 'cargo fmt' first."
    echo "$FMT" | head -10
    exit 2
fi

# clippy check
CLIPPY=$(cargo clippy --workspace --manifest-path "$MANIFEST" -- -D warnings 2>&1 || true)
CLIPPY_ERRORS=$(echo "$CLIPPY" | grep "^error" | head -5)
if [[ -n "$CLIPPY_ERRORS" ]]; then
    echo "⛔ git push blocked: cargo clippy found errors:"
    echo "$CLIPPY_ERRORS"
    exit 2
fi

exit 0
