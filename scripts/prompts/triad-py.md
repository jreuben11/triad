Implement `triad-py` — PyO3 Python bindings for triad-sdk — per `stage2-design.md` §"Stage 2a" and Phase 10 of `project-plan.md`.

## Before starting
Read `/home/jreuben1/Code/triad/claude-best-practices-learned.md` and apply all documented invariants immediately — do not rediscover known pitfalls.
Read `/home/jreuben1/Code/triad/stage2-design.md` §"Stage 2a" for the full API surface, async bridging notes, and file layout.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/triad-py`

## Setup
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-py
```

## Tasks

All files live under `crates/triad-py/` (create the crate from scratch):

1. **Scaffold** — `Cargo.toml` (crate-type `cdylib`, deps: `pyo3 = { version = "0.22", features = ["extension-module"] }`, `pyo3-async-runtimes = { version = "0.22", features = ["tokio-runtime"] }`, `triad-sdk`, `triad-core`), `pyproject.toml` (maturin), `src/lib.rs` (`#[pymodule]` root registering all classes)

2. **`src/instance.rs`** — `PyTriadInstance`: `start(config_path)` async classmethod, `shutdown(timeout_secs)` async, `transaction()` async context manager yielding `PyTransaction`

3. **`src/outbox.rs`** — `PyTransaction` (wraps `Mutex<Option<sqlx::Transaction<'_, sqlx::Postgres>>>`, exposes `execute`/`fetch_one`/`fetch_all`), `PyOutboxPublisher` (`publish(tx, aggregate_type, aggregate_id, event_type, payload)` returns EventId str)

4. **`src/flags.rs`** — `PyFlagEvaluator`: `is_enabled(flag)` async, Redis hot path with PG fallback

5. **`src/saga.rs`** — `PySagaBuilder` fluent builder (`step`, `with_compensation`, `with_step_timeout`, `build`), `PySagaConfig` + `PySagaStepConfig` dataclasses

6. **`src/idempotency.rs`** — `PyIdempotencyKey` (`generate`, `wrap`, `__str__`), `PyIdempotencyRecord` dataclass, module-level async `lookup` and `store_result` functions

7. **`src/aggregate.rs`** — `PyAggregateRoot` (`#[pyclass]`, holds state as `serde_json::Value`, dispatches `apply` back to Python), `Aggregate` abstract base class exposed as `PyAggregate`

8. **`python/triad/__init__.py`** — re-exports all public classes, `py.typed` marker

9. **`python/triad/__init__.pyi`** — type stubs for all public API

10. **`tests/`** — pytest suite using `pytest-asyncio` and testcontainers:
    - `test_instance.py` — start/shutdown lifecycle
    - `test_outbox.py` — `publish()` inserts row in testcontainers PG
    - `test_flags.py` — `is_enabled()` correct values from seeded Redis
    - `test_saga.py` — `SagaBuilder` round-trips correctly
    - `test_idempotency.py` — `lookup`/`store_result` dedup semantics
    - `test_aggregate.py` — `apply_new`, `rehydrate`, `persist_snapshot` round-trip

Async bridging: all `async def` Python methods use `pyo3_async_runtimes::tokio::future_into_py`. Init Tokio runtime once at module import via `pyo3_async_runtimes::tokio::init_once()`. `PyTransaction` uses `Mutex<Option<...>>` to satisfy `Send` across await points.

Error mapping: `TriadError` → `triad.TriadError(RuntimeError)`; `ShutdownError::Timeout` → `asyncio.TimeoutError`.

## Done criteria
- `cargo clippy -p triad-py -- -D warnings` clean
- `maturin develop` installs `triad` into the local virtualenv (`uv run maturin develop`)
- `python -c "import triad; print(triad.__version__)"` works
- `uv run pytest crates/triad-py/tests/` — all tests pass
- `uv run mypy crates/triad-py/python/` clean
- All Phase 10 checklist items marked `[x]` in `/home/jreuben1/Code/triad/project-plan.md`
- `claude-best-practices-learned.md` updated with any new PyO3/async-bridge pitfalls discovered
- Commit implementation **together with** `project-plan.md` and `claude-best-practices-learned.md` in a single commit on branch `feat/triad-py`
- Open PR: `gh pr create --title "feat(py): PyO3 Python bindings for triad-sdk" --body "Implements Stage 2a of stage2-design.md. PyO3 extension module with full async bridge to tokio runtime."`

## Constraints
- Do NOT use `pip` — use `uv` for all Python package operations
- `maturin develop` for local install, `uv run pytest` for tests, `uv run mypy` for type checking
- `PyTransaction` must use `Mutex<Option<...>>` — never hold a raw reference across await points
- All async Rust futures bridged via `pyo3_async_runtimes::tokio::future_into_py`
- The crate must compile with `CARGO_TARGET_DIR=/tmp/triad-target-py`

Output <promise>DONE</promise> when all criteria are met.
