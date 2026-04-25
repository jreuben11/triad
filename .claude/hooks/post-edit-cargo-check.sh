#!/usr/bin/env bash
# PostToolUse hook: runs `cargo check` after any Edit or Write to a Rust file.
# Outputs compiler errors to Claude's context so it can fix them immediately.
# Always exits 0 (post-tool hooks are advisory, not blocking).
set -euo pipefail

INPUT=$(cat)
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // ""' 2>/dev/null || echo "")

# Only trigger for Rust source or Cargo manifests
if [[ "$FILE" != *.rs ]] && [[ "$FILE" != */Cargo.toml ]]; then
    exit 0
fi

export CARGO_TARGET_DIR=/tmp/triad-target-main
RESULT=$(cargo check --workspace --manifest-path "$CLAUDE_PROJECT_DIR/Cargo.toml" 2>&1 || true)
ERRORS=$(echo "$RESULT" | grep "^error" | head -5)

if [[ -n "$ERRORS" ]]; then
    echo "⚠ cargo check errors after edit:"
    echo "$ERRORS"
fi
exit 0
