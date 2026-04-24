#!/usr/bin/env bash
export CARGO_TARGET_DIR=/tmp/triad-target-proto
cd /home/jreuben1/Code/triad-worktrees/triad-proto
exec claude --dangerously-skip-permissions "$(cat /home/jreuben1/Code/triad/scripts/prompts/triad-proto.md)"
