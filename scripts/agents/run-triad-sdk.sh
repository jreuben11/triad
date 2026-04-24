#!/usr/bin/env bash
export CARGO_TARGET_DIR=/tmp/triad-target-sdk
cd /home/jreuben1/Code/triad-worktrees/triad-sdk
exec claude --dangerously-skip-permissions "$(cat /home/jreuben1/Code/triad/scripts/prompts/triad-sdk.md)"
