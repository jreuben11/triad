#!/usr/bin/env bash
export CARGO_TARGET_DIR=/tmp/triad-target-cli
cd /home/jreuben1/Code/triad-worktrees/triad-cli
exec claude --dangerously-skip-permissions "$(cat /home/jreuben1/Code/triad/scripts/prompts/triad-cli.md)"
