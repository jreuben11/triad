#!/usr/bin/env bash
export CARGO_TARGET_DIR=/tmp/triad-target-patterns-1
cd /home/jreuben1/Code/triad-worktrees/triad-runner-patterns-cdc-outbox
exec claude --dangerously-skip-permissions "$(cat /home/jreuben1/Code/triad/scripts/prompts/triad-runner-patterns-cdc-outbox.md)"
