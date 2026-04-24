Implement `triad-cli` per §6 of triad-physical-design.md.
Runs after triad-runner-engine is merged.

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
- Commit on branch `feat/triad-cli`

## Constraints
- Use `anyhow` for top-level error propagation in the binary
- All output to stdout via `println!` or a formatting library — no `tracing` for CLI output
- `triad run` must handle SIGTERM gracefully

Output <promise>DONE</promise> when all criteria are met.
