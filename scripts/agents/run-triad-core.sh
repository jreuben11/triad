#!/usr/bin/env bash
export CARGO_TARGET_DIR=/tmp/triad-target-core
cd /home/jreuben1/Code/triad-worktrees/triad-core
exec claude --dangerously-skip-permissions "$(cat /home/jreuben1/Code/triad/scripts/prompts/triad-core.md)"
