Implement `triad-cli` per §6 of triad-physical-design.md.
Runs after triad-runner-engine is merged.

## Before starting
Read `/home/jreuben1/Code/triad/claude-best-practices-learned.md` and apply all documented invariants immediately — do not rediscover known pitfalls.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/triad-cli`

## Setup
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-cli
```

## Tasks
1. `src/main.rs` — Clap `Cli` derive struct with all subcommands: `run`, `status`, `pattern list`, `pattern pause/resume`, `checkpoint list`, `dlq list/replay/purge`, `pipeline reload`, `config validate` (§6.1)
2. `src/commands/run.rs` — load config via `TriadConfig::load()`, start `Runner`, block on SIGTERM
3. `src/commands/admin/mod.rs` — `AdminClient`: GET/POST/DELETE via `reqwest` pointing at admin HTTP server (§6.2)
4. Wire all subcommands to `AdminClient` methods

## Done criteria
- `cargo build -p triad-cli` produces the `triad` binary
- `./target/debug/triad --help` prints usage for all subcommands
- `cargo clippy -p triad-cli -- -D warnings` clean
- Mark all completed items `[x]` in `project-plan.md` (at `/home/jreuben1/Code/triad/project-plan.md`)
- If you discovered any new pitfalls (permission prompts, cargo/git gotchas), add them to `claude-best-practices-learned.md`
- Commit implementation **together with** `project-plan.md` and `claude-best-practices-learned.md` in a single commit on branch `feat/triad-cli`
- Open a pull request: `gh pr create --title "feat(cli): implement triad CLI — all subcommands + AdminClient" --body "Implements §6 of triad-physical-design.md. triad --help shows all subcommands."`

## Constraints
- Use `anyhow` for top-level error propagation in the binary
- All output to stdout via `println!` or a formatting library — no `tracing` for CLI output
- `triad run` must handle SIGTERM gracefully

Output <promise>DONE</promise> when all criteria are met.
