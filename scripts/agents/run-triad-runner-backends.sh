#!/usr/bin/env bash
export CARGO_TARGET_DIR=/tmp/triad-target-runner-backends
cd /home/jreuben1/Code/triad-worktrees/triad-runner-backends
exec claude --dangerously-skip-permissions "$(cat /home/jreuben1/Code/triad/scripts/prompts/triad-runner-backends.md)"
