Implement `triad-tui` — Ratatui terminal dashboard for triad — per `stage2-design.md` §"Stage 2b" and Phase 11 of `project-plan.md`.

## Before starting
Read `/home/jreuben1/Code/triad/claude-best-practices-learned.md` and apply all documented invariants immediately — do not rediscover known pitfalls.
Read `/home/jreuben1/Code/triad/stage2-design.md` §"Stage 2b" for screen layouts, Tachyonfx effect plan, widget specs, and crate structure.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/triad-tui`

## Setup
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-tui
```

## Tasks

All files live under `crates/triad-tui/` (create the crate from scratch):

**Dependencies** (add to `crates/triad-tui/Cargo.toml`):
```toml
ratatui       = "0.29"
tachyonfx     = { version = "0.7", features = ["sendable"] }
crossterm     = "0.28"
tokio         = { version = "1", features = ["full"] }
reqwest       = { version = "0.12", features = ["json"] }
serde_json    = "1"
triad-core    = { path = "../triad-core" }
```

1. **`src/app.rs`** — `App` struct: active `Screen` enum, last-fetch data, `effect_queue: Vec<(Rect, Effect)>`, action dispatch; screen switching via number keys `1`–`6`, `q` to quit

2. **`src/client.rs`** — `AdminClient`: polls all admin endpoints (`/health/ready`, `/health/live`, `/patterns`, `/lag`, `/checkpoints`, `/sagas`) every `--poll-ms` ms; returns typed structs

3. **`src/effects.rs`** — named Tachyonfx constructors for all 10 trigger/effect pairs from the design doc (startup glitch, screen slide forward/back, pattern status fade, confirm popup coalesce, config validate OK/error, DLQ count alert, action success, data refresh pulse)

4. **`src/screens/dashboard.rs`** — Screen 1: health badge + patterns summary panel + consumer lag bar chart + backends panel; layout matches design doc ASCII art

5. **`src/screens/patterns.rs`** — Screen 2: scrollable list with ● Running / ◌ Paused / ✗ Error badges; `[p]`ause, `[r]`esume, `[x]`replay call admin endpoints; row fade on status change via `fade_from_fg`

6. **`src/screens/dlq.rs`** — Screen 3: per-topic DLQ counts; `[R]`eplay and `[P]`urge with confirm popup (Tachyonfx `coalesce` on popup appearance)

7. **`src/screens/checkpoints.rs`** — Screen 4: checkpoint offsets table (Pattern / Pipeline / Offset / Updated)

8. **`src/screens/sagas.rs`** — Screen 5: saga list with `Enter` to expand step detail inline; `[c]`ancel calls `POST /saga/:id/cancel`

9. **`src/screens/config.rs`** — Screen 6: collapsible `triad.yaml` tree; `[v]`alidate calls `TriadConfig::load()` live, shows pass/fail with Tachyonfx effect

10. **`src/widgets/status_badge.rs`** — coloured ● / ◌ / ✗ `Widget` impl

11. **`src/widgets/lag_bar.rs`** — mini horizontal bar chart `Widget` impl for consumer lag

12. **`src/widgets/key_help.rs`** — context-sensitive key-binding bar at bottom of each screen

13. **`src/main.rs`** — arg parse (`--admin-url`, `--poll-ms`), spawn poller task, run crossterm event loop; shows "connecting…" state when admin server unreachable

14. **Wire CLI subcommand** in `crates/triad-cli/src/main.rs` and `crates/triad-cli/src/commands/tui.rs`:
    ```rust
    #[derive(Args)]
    pub struct TuiArgs {
        #[arg(long, env = "TRIAD_ADMIN_URL", default_value = "http://localhost:8080")]
        pub admin_url: String,
        #[arg(long, default_value = "1000")]
        pub poll_ms: u64,
    }
    ```

15. **Unit tests** for `App` state transitions: screen switching, action dispatch, effect queue management

## Done criteria
- `cargo clippy -p triad-tui -- -D warnings` clean
- `cargo nextest run -p triad-tui` — all App state transition tests pass
- `triad tui --help` works without panic
- `triad tui` opens without panic when admin server unreachable (shows "connecting…")
- All 6 screens render without layout overflow at 80×24 and 220×50
- All Tachyonfx effects run without terminal corruption
- All Phase 11 checklist items marked `[x]` in `/home/jreuben1/Code/triad/project-plan.md`
- `claude-best-practices-learned.md` updated with any new Ratatui/Tachyonfx pitfalls
- Commit implementation **together with** `project-plan.md` and `claude-best-practices-learned.md` in a single commit on branch `feat/triad-tui`
- Open PR: `gh pr create --title "feat(tui): Ratatui terminal dashboard with Tachyonfx effects" --body "Implements Stage 2b of stage2-design.md. Six-screen TUI: dashboard, patterns, DLQ, checkpoints, sagas, config. Tachyonfx effects on all state transitions."`

## Constraints
- Must compile with `CARGO_TARGET_DIR=/tmp/triad-target-tui`
- `cargo clippy --workspace -- -D warnings` must remain clean after adding the triad-tui crate and CLI subcommand wiring
- Do NOT import `triad-runner` or `triad-sdk` directly — only `triad-core` (for `TriadConfig`) and HTTP via `reqwest`
- All admin API calls go through `AdminClient` — screens never call `reqwest` directly

Output <promise>DONE</promise> when all criteria are met.
