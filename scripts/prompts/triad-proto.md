Implement `triad-proto` per §2 of triad-physical-design.md.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/triad-proto`

## Tasks
1. Create `proto/triad_admin.proto` with all message types and the `TriadAdmin` gRPC service (§2.1 of triad-physical-design.md)
2. Create `build.rs` using `tonic_build::configure()` (§2.2 of triad-physical-design.md)
3. Ensure `src/lib.rs` re-exports generated types via `tonic::include_proto!`

## Done criteria
- `cargo build -p triad-proto` compiles cleanly with zero errors and zero warnings
- `cargo clippy -p triad-proto -- -D warnings` is clean
- Commit all changes on branch `feat/triad-proto`

## Constraints
- Read triad-physical-design.md §2 for the exact proto schema — do not invent fields
- Do not modify any other crate
- Commit message format: `feat(proto): <description>`

Output <promise>DONE</promise> when all criteria are met.
