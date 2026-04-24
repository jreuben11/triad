#!/usr/bin/env bash
# Saga + EOS patterns — iterative TDD.
# After launching, run:  /loop
# Claude will self-pace iterations until cargo test passes with 90%+ coverage.
export CARGO_TARGET_DIR=/tmp/triad-target-saga-eos
cd /home/jreuben1/Code/triad-worktrees/triad-runner-patterns-saga-eos
exec claude --dangerously-skip-permissions "$(cat /home/jreuben1/Code/triad/scripts/prompts/triad-runner-patterns-saga-eos.md)"
