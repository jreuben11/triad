#!/usr/bin/env bash
# Engine + runner + shutdown + admin — iterative TDD.
# After launching, run:  /loop
# Claude will self-pace iterations until cargo test passes clean.
export CARGO_TARGET_DIR=/tmp/triad-target-engine
cd /home/jreuben1/Code/triad-worktrees/triad-runner-engine
exec claude --dangerously-skip-permissions "$(cat /home/jreuben1/Code/triad/scripts/prompts/triad-runner-engine.md)"
