#!/usr/bin/env bash
export CARGO_TARGET_DIR=/tmp/triad-target-tests
cd /home/jreuben1/Code/triad-worktrees/tests
exec claude --dangerously-skip-permissions "$(cat /home/jreuben1/Code/triad/scripts/prompts/tests.md)"
