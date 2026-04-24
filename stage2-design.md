# Triad — Stage 2 Design: PyO3 Python Bindings

## Goal

Expose the `triad-sdk` surface as a native Python extension module (`triad`) so that
Python applications can use the same PG × Kafka × Redis patterns without running a
separate sidecar process.

---

## New crate: `crates/triad-py`

```
crates/triad-py/
  Cargo.toml        # cdylib + pyo3 dep
  src/
    lib.rs          # #[pymodule] root — registers all classes
    instance.rs     # PyTriadInstance
    outbox.rs       # PyOutboxPublisher + PyTransaction
    flags.rs        # PyFlagEvaluator
    saga.rs         # PySagaBuilder + PySagaConfig
    idempotency.rs  # PyIdempotencyKey, PyIdempotencyRecord, lookup/store_result
    aggregate.rs    # PyAggregateRoot
  pyproject.toml    # maturin metadata
  python/
    triad/__init__.py   # re-exports, type stubs
    triad/py.typed
```

Dependency direction: `triad-py → triad-sdk → triad-runner → triad-core`.

---

## Build tooling

| Tool | Purpose |
|------|---------|
| `maturin` | Build + publish `.whl`; `maturin develop` for local install |
| `pyo3 = { version = "0.22", features = ["extension-module"] }` | Rust ↔ Python FFI |
| `pyo3-async-runtimes = { version = "0.22", features = ["tokio-runtime"] }` | Bridge Tokio futures → Python `asyncio` coroutines |

All async methods use:
```rust
pyo3_async_runtimes::tokio::future_into_py(py, async move { ... })
```

The Tokio runtime is started once at module import via `pyo3_async_runtimes::tokio::init_once()`.

---

## Python package layout

```python
import triad

triad.TriadInstance      # entry point
triad.OutboxPublisher
triad.FlagEvaluator
triad.SagaBuilder
triad.SagaConfig         # dataclass returned by SagaBuilder.build()
triad.IdempotencyKey
triad.IdempotencyRecord
triad.lookup             # async fn
triad.store_result       # async fn
triad.AggregateRoot
```

---

## API surface

### `TriadInstance`

```python
class TriadInstance:
    @staticmethod
    async def start(config_path: str) -> "TriadInstance":
        """Load triad.yaml, connect PG + Redis backends."""

    async def shutdown(self, timeout_secs: float = 30.0) -> None:
        """Cancel all tasks; raises TimeoutError if drain exceeds timeout."""

    async def transaction(self) -> "AsyncContextManager[PyTransaction]":
        """
        Async context manager yielding a live PG transaction.
        Commits on clean exit, rolls back on exception.

        async with instance.transaction() as tx:
            event_id = await publisher.publish(tx, ...)
            await pool.execute("INSERT INTO orders ...", tx=tx)
        """
```

`PyTransaction` wraps `sqlx::Transaction<'_, sqlx::Postgres>` and exposes
`execute(sql, *args)` / `fetch_one(sql, *args)` / `fetch_all(sql, *args)` so Python
callers can do business writes in the same transaction as `OutboxPublisher.publish`.

---

### `OutboxPublisher`

```python
class OutboxPublisher:
    def __init__(self, instance: TriadInstance, table: str, kafka_topic: str) -> None: ...

    async def publish(
        self,
        tx: PyTransaction,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: dict,          # serialised to JSON
    ) -> str:                   # returns EventId as str(UUID)
        """
        Insert into triad_outbox inside the caller-supplied transaction.
        Safety invariant preserved: same txn as business write.
        """
```

---

### `FlagEvaluator`

```python
class FlagEvaluator:
    def __init__(self, instance: TriadInstance) -> None: ...

    async def is_enabled(self, flag: str) -> bool:
        """Redis hot path, PG fallback when circuit breaker open."""
```

Internally constructs a `FlagStore` backed by the instance's PG pool + Redis handle.

---

### `SagaBuilder`

Config-only, no async. Returns a `SagaConfig` dataclass.

```python
class SagaBuilder:
    def __init__(
        self,
        name: str,
        trigger_topic: str,
        trigger_event: str,
        timeout: str,           # "30s", "5m"
    ) -> None: ...

    def step(
        self,
        name: str,
        command_topic: str,
        reply_topic: str,
    ) -> "SagaBuilder": ...     # fluent — returns self

    def with_compensation(self, topic: str) -> "SagaBuilder": ...
    def with_step_timeout(self, timeout: str) -> "SagaBuilder": ...
    def build(self) -> "SagaConfig": ...

@dataclass
class SagaConfig:
    name: str
    trigger_topic: str
    trigger_event: str
    timeout: str
    steps: list[SagaStepConfig]

@dataclass
class SagaStepConfig:
    name: str
    command_topic: str
    reply_topic: str
    compensation: str | None
    timeout: str | None
```

---

### `IdempotencyKey` / `IdempotencyRecord`

```python
class IdempotencyKey:
    @classmethod
    def generate(cls, scope: str = "") -> "IdempotencyKey": ...
    @classmethod
    def wrap(cls, s: str) -> "IdempotencyKey": ...
    def __str__(self) -> str: ...

@dataclass
class IdempotencyRecord:
    key: str
    status_code: int
    body: dict
    created_at: datetime

async def lookup(
    instance: TriadInstance,
    key: IdempotencyKey,
) -> IdempotencyRecord | None: ...

async def store_result(
    instance: TriadInstance,
    key: IdempotencyKey,
    record: IdempotencyRecord,
    ttl_secs: float = 3600.0,
) -> bool:
    """True = stored (new key); False = lost race (key already existed)."""
```

Backed by the instance's PG pool (`triad.idempotency_keys` table).

---

### `AggregateRoot`

The Rust `Aggregate` trait becomes a Python abstract base class. Users subclass it.

```python
class Aggregate(ABC):
    @classmethod
    @abstractmethod
    def aggregate_type(cls) -> str: ...

    @abstractmethod
    def apply(self, event: dict) -> None: ...

    version: int = 0


class AggregateRoot:
    def __init__(self, aggregate_cls: type[Aggregate], aggregate_id: str) -> None: ...

    @classmethod
    def rehydrate(
        cls,
        aggregate_cls: type[Aggregate],
        aggregate_id: str,
        events: list[dict],
    ) -> "AggregateRoot": ...

    def apply_new(self, event: dict) -> None:
        """Apply event to state and stage it for persistence."""

    def take_pending_events(self) -> list[dict]: ...

    async def persist_snapshot(
        self,
        instance: TriadInstance,
        snapshot_table: str = "triad_snapshots",
    ) -> None: ...

    @staticmethod
    async def load_snapshot(
        instance: TriadInstance,
        aggregate_cls: type[Aggregate],
        aggregate_id: str,
        snapshot_table: str = "triad_snapshots",
    ) -> dict | None:
        """Returns {"aggregate_id", "version", "snapshot"} or None."""
```

The `apply` method receives events as plain Python `dict` (deserialised from JSON).
`AggregateRoot` is implemented entirely in Rust via `#[pyclass]`, holding the aggregate state
as `serde_json::Value` and dispatching `apply` calls back into Python via `PyObject::call1`.

---

## What is NOT wrapped

| Rust surface | Reason excluded |
|---|---|
| `IdempotencyLayer` (Tower) | Axum-specific; Python has its own middleware stacks (FastAPI, Starlette) |
| `RateLimitLayer` (Tower) | Same; expose `RateLimiter.check(key)` directly if needed in Stage 3 |
| `PgBackend` / `RedisBackend` raw handles | Too low-level; leak Rust connection pool internals |
| `CancellationToken` | Not meaningful in Python; `shutdown()` handles it |
| `PatternEngine` / `Runner` | Mode 2/3 only — Python uses Mode 1 via `TriadInstance` |

---

## Async bridging notes

- All `async def` Python methods call `pyo3_async_runtimes::tokio::future_into_py`.
- The Tokio runtime runs on a dedicated OS thread pool; Python's event loop is not blocked.
- `PyTransaction` holds a `Mutex<Option<sqlx::Transaction<'_, sqlx::Postgres>>>` to satisfy
  `Send` across the `await` points. The `Option` is taken on `commit`/`rollback` to prevent
  double-use.
- Errors map: `TriadError` → `triad.TriadError(RuntimeError)`;
  `ShutdownError::Timeout` → `asyncio.TimeoutError`.

---

## Done criteria

- [ ] `maturin develop` installs `triad` into the local virtualenv
- [ ] `python -c "import triad; print(triad.__version__)"` works
- [ ] `pytest crates/triad-py/tests/` — unit tests using `pytest-asyncio` pass:
  - `test_instance.py` — start/shutdown lifecycle (mocked backends via `TriadConfig` pointing at testcontainers)
  - `test_outbox.py` — `publish()` inserts row in testcontainers PG
  - `test_flags.py` — `is_enabled()` returns correct values from seeded Redis
  - `test_saga.py` — `SagaBuilder` round-trips correctly
  - `test_idempotency.py` — `lookup`/`store_result` dedup semantics
  - `test_aggregate.py` — `apply_new`, `rehydrate`, `persist_snapshot` round-trip
- [ ] Type stubs (`triad/*.pyi`) generated via `maturin`'s stub generator or hand-written
- [ ] `cargo clippy -p triad-py -- -D warnings` clean
- [ ] `mypy crates/triad-py/python/` clean
- [ ] Commit `project-plan.md` + `CLAUDE.md` + `claude-best-practices-learned.md` together
- [ ] Open PR: `feat: PyO3 Python bindings for triad-sdk`

---

---

## Stage 2b — Terminal UI (`crates/triad-tui`)

### Goal

A `triad tui` subcommand that opens a full-screen Ratatui dashboard connected to the admin
HTTP API. Exercises every CLI option interactively, displays live config and runtime state,
and uses Tachyonfx for polished screen transitions and status animations.

---

### New crate: `crates/triad-tui`

```
crates/triad-tui/
  Cargo.toml
  src/
    main.rs           # arg parse, spawn poller task, run event loop
    app.rs            # App state: active screen, last-fetch data, effect queue
    client.rs         # AdminClient poller — fetches all endpoints on a 1s tick
    effects.rs        # named Tachyonfx effect constructors (startup, transition, alert)
    screens/
      mod.rs          # Screen enum + render dispatch
      dashboard.rs    # overview: health, patterns summary, lag, backends
      patterns.rs     # full pattern list + pause/resume/replay actions
      dlq.rs          # DLQ topics, message counts, replay/purge
      checkpoints.rs  # checkpoint offsets per pipeline
      sagas.rs        # saga list + step inspector popup
      config.rs       # parsed triad.yaml pretty-printed as a tree
    widgets/
      status_badge.rs # coloured ● Running / ◌ Paused / ✗ Error badge
      lag_bar.rs      # consumer-lag mini bar chart
      key_help.rs     # bottom key-binding bar, context-sensitive
```

**Dependencies:**
```toml
ratatui       = "0.29"
tachyonfx     = { version = "0.7", features = ["sendable"] }
crossterm     = "0.28"
tokio         = { version = "1", features = ["full"] }
reqwest       = { version = "0.12", features = ["json"] }
serde_json    = "1"
triad-core    = { path = "../triad-core" }   # TriadConfig for the Config screen
```

`triad-tui` is added as a new binary in `triad-cli/Cargo.toml` OR as a standalone
`crates/triad-tui/` binary that `triad-cli/main.rs` invokes via `Command::Tui`.

---

### Screen layouts

#### 1. Dashboard (default, key `1`)

```
╔══════════════════════════════════════════════════════════════════════╗
║  ▸ triad  ■ RUNNING  ↑ 2h 34m  inst: a1b2c3d  leader: ✓  v0.1.0   ║
╠═══════════════════════════════════╦══════════════════════════════════╣
║ PATTERNS  8 / 8 running           ║ BACKENDS                         ║
║                                   ║  ● postgres   ok  (pool: 8/16)   ║
║  ● outbox          Running        ║  ● kafka      ok                  ║
║  ● inbox           Running        ║  ● redis      ok  (standalone)   ║
║  ● cdc             Running        ╠══════════════════════════════════╣
║  ● cache           Running        ║ CONSUMER LAG                     ║
║  ● webhook         Running        ║  orders.events      ████░░  240  ║
║  ● feature_flag    Running        ║  payments.events    ░░░░░░    0  ║
║  ● rate_limit      Running        ║  saga.commands      ░░░░░░    0  ║
║  ● saga            Running        ║                                  ║
╠═══════════════════════════════════╩══════════════════════════════════╣
║ [1]Dashboard [2]Patterns [3]DLQ [4]Checkpoints [5]Sagas [6]Config  ║
║ [r]efresh  [q]uit  [?]help                                           ║
╚══════════════════════════════════════════════════════════════════════╝
```

#### 2. Patterns (key `2`)

```
╔══════════════════════════════════════════════════════════════════════╗
║  PATTERNS                                              [ESC] back    ║
╠══════════════════════════════════════════════════════════════════════╣
║  Name             Type          Status      Actions                  ║
║  ─────────────────────────────────────────────────────────────────  ║
║▶ outbox           outbox        ● Running   [p]ause   [x]replay      ║
║  inbox            inbox         ● Running   [p]ause   [x]replay      ║
║  cdc              cdc           ● Running   [p]ause                  ║
║  cache            cache         ◌ Paused    [r]esume  [x]replay      ║
║  webhook          webhook       ● Running   [p]ause   [x]replay      ║
║  feature_flag     feature_flag  ● Running   [p]ause                  ║
║  rate_limit       rate_limit    ● Running   [p]ause                  ║
║  saga             saga          ● Running   [p]ause   [x]replay      ║
╠══════════════════════════════════════════════════════════════════════╣
║  [↑/↓] select  [p] pause  [r] resume  [x] replay  [ESC] dashboard   ║
╚══════════════════════════════════════════════════════════════════════╝
```

Status changes (Running → Paused) trigger a `tachyonfx::fade_from_fg(Color::Yellow, 400ms)`
on the affected row so the eye catches it immediately.

#### 3. DLQ (key `3`)

```
╔══════════════════════════════════════════════════════════════════════╗
║  DEAD LETTER QUEUES                                    [ESC] back    ║
╠════════════════════════════════╦═════════════╦════════╦═════════════╣
║  Topic                         ║  Messages   ║        ║             ║
║  ──────────────────────────────╬─────────────╬────────╬─────────────║
║▶ triad.dlq.orders.events       ║          42 ║ [R]epl ║ [P]urge     ║
║  triad.dlq.payments.events     ║           0 ║        ║             ║
║  triad.dlq.webhook.deliveries  ║           7 ║ [R]epl ║ [P]urge     ║
╠════════════════════════════════╩═════════════╩════════╩═════════════╣
║  [↑/↓] select  [R] replay topic  [P] purge topic  [ESC] dashboard   ║
╚══════════════════════════════════════════════════════════════════════╝
```

Confirm popup (styled with `tachyonfx::coalesce(300ms)`) before destructive Purge action.

#### 4. Checkpoints (key `4`)

```
╔══════════════════════════════════════════════════════════════════════╗
║  CHECKPOINTS                                           [ESC] back    ║
╠═══════════════════════╦═════════════════╦══════════╦════════════════╣
║  Pattern              ║  Pipeline       ║  Offset  ║  Updated       ║
║  ─────────────────────╬─────────────────╬──────────╬────────────────║
║  outbox               ║  orders         ║  1048234 ║  2s ago        ║
║  inbox                ║  orders         ║  1048220 ║  3s ago        ║
║  cdc                  ║  inventory      ║    99012 ║  1s ago        ║
║  saga                 ║  order-saga     ║      441 ║  12s ago       ║
╠═══════════════════════╩═════════════════╩══════════╩════════════════╣
║  [↑/↓] select  [ESC] dashboard                                       ║
╚══════════════════════════════════════════════════════════════════════╝
```

#### 5. Sagas (key `5`)

```
╔══════════════════════════════════════════════════════════════════════╗
║  SAGAS                                                 [ESC] back    ║
╠══════════════════════════════╦═══════════╦════════════╦═════════════╣
║  Saga ID                     ║  Name     ║  Status    ║  Step       ║
║  ────────────────────────────╬───────────╬────────────╬─────────────║
║▶ 3f2a…c19d                   ║  order    ║  Running   ║  2/3        ║
║  8b1e…44fa                   ║  order    ║  Completed ║  3/3        ║
║  0d9c…8812                   ║  payment  ║  RolledBack║  1/2        ║
╠══════════════════════════════╧═══════════╧════════════╧═════════════╣
║ ▼ SAGA DETAIL: 3f2a…c19d — order-saga                               ║
║   Step 1: reserve-inventory  ✓ Success   48ms                       ║
║   Step 2: charge-payment     ● Running   …                          ║
║   Step 3: confirm-shipment   ○ Pending                              ║
║   [c]ancel saga                                                      ║
╠══════════════════════════════════════════════════════════════════════╣
║  [↑/↓] select  [Enter] expand  [c] cancel  [ESC] dashboard          ║
╚══════════════════════════════════════════════════════════════════════╝
```

#### 6. Config (key `6`)

```
╔══════════════════════════════════════════════════════════════════════╗
║  CONFIG  triad.yaml                       [v]alidate  [ESC] back    ║
╠══════════════════════════════════════════════════════════════════════╣
║  ▼ backends                                                          ║
║    ▼ postgres                                                        ║
║        url:          postgres://localhost/triad                      ║
║        pool_size:    16                                              ║
║        min_idle:     2                                               ║
║    ▼ kafka                                                           ║
║        brokers:      ["localhost:9092"]                              ║
║        group_id:     triad-runner                                    ║
║    ▼ redis                                                           ║
║        mode:         standalone                                      ║
║        url:          redis://localhost:6379                          ║
║  ▼ patterns  (8 configured)                                          ║
║    ● outbox / feature_flag / rate_limit / webhook / …               ║
╠══════════════════════════════════════════════════════════════════════╣
║  [↑/↓] scroll  [v] validate  [ESC] dashboard                        ║
╚══════════════════════════════════════════════════════════════════════╝
```

`[v]alidate` calls `TriadConfig::load() + validate()` live and shows result with a
`tachyonfx::fade_from_fg(Color::Green, 500ms)` flash on success or
`tachyonfx::glitch_in(400ms)` on error.

---

### Tachyonfx effect plan

| Trigger | Effect | Duration |
|---------|--------|----------|
| TUI startup | `glitch_in` on header bar | 600ms |
| Screen switch (forward) | `slide_in(Direction::Left)` on content area | 250ms |
| Screen switch (back) | `slide_in(Direction::Right)` on content area | 250ms |
| Pattern status change | `fade_from_fg(status_color)` on changed row | 400ms |
| Confirm popup appears | `coalesce` on popup text | 300ms |
| Config validate OK | `fade_from_fg(Color::Green)` on status line | 500ms |
| Config validate error | `glitch_in` on error message | 400ms |
| DLQ message count increases | `fade_from_fg(Color::Red)` on count cell | 300ms |
| Successful action (pause/replay) | `fade_from_fg(Color::Cyan)` on row | 250ms |
| Data refresh tick | `fade_from_fg(Color::DarkGray, 150ms)` pulse on stale cells | 150ms |

All effects are fire-and-forget: registered in `App::effect_queue: Vec<(Rect, Effect)>`,
rendered each frame via `fx::effect_renderer`, expired effects removed automatically.

---

### New CLI subcommand

Add to `triad-cli/src/main.rs`:
```rust
/// Launch the interactive terminal dashboard
Tui(commands::tui::TuiArgs),
```

```rust
// commands/tui.rs
#[derive(Args)]
pub struct TuiArgs {
    #[arg(long, env = "TRIAD_ADMIN_URL", default_value = "http://localhost:8080")]
    pub admin_url: String,

    #[arg(long, default_value = "1000")]
    pub poll_ms: u64,   // admin API polling interval
}
```

---

### Done criteria (TUI)

- [ ] `triad tui` opens without panic when admin server is unreachable (shows "connecting…" state)
- [ ] Dashboard polls admin API every `--poll-ms` ms and refreshes all panels
- [ ] All 6 screens render without layout overflow on 80×24 and 220×50 terminals
- [ ] Patterns screen: pause/resume/replay calls correct admin endpoints and refreshes state
- [ ] DLQ screen: replay and purge work; purge shows confirm popup before executing
- [ ] Checkpoints screen: displays all checkpoint rows
- [ ] Sagas screen: list renders; `Enter` expands detail; `c` triggers cancel
- [ ] Config screen: displays parsed `triad.yaml`; `v` validate shows pass/fail
- [ ] All Tachyonfx effects run without terminal corruption; effects clean up on completion
- [ ] `cargo clippy -p triad-tui -- -D warnings` clean
- [ ] `cargo nextest run -p triad-tui` — unit tests for App state transitions pass
- [ ] Commit and open PR → `main`

---

## Stage 2 project-plan additions

Add to `project-plan.md`:

### Phase 8 — Bug Fixes (`feat/bugfixes`)

Three bugs identified during CLI/HTTP surface audit:

**Bug 1 — `triad run` bails unconditionally** (`commands/run.rs`)
- `Runner` is already merged; the TODO comment is stale
- Fix: replace `anyhow::bail!` stub with `Runner::new(&config, ...).run().await`

**Bug 2 — `triad checkpoint list` calls `/checkpoints` which doesn't exist**
- HTTP server (`admin.rs`) has no `/checkpoints` route
- Fix: add `GET /checkpoints` to `admin_router`, backed by `PgCheckpointStore::list_all()`
  (new method returning all checkpoint rows)

**Bug 3 — `triad pipeline reload` calls `/pipelines/:name/reload` which doesn't exist**
- HTTP server has `/config/reload` (global), not per-pipeline
- Fix: add `POST /pipelines/:name/reload` to `admin_router`; handler logs the pipeline name
  and delegates to config hot-reload (per-pipeline reload is future work)

Checklist:
- [ ] `commands/run.rs` — wire `Runner::new` + `runner.run().await`; SIGTERM via `ShutdownCoordinator`
- [ ] `admin.rs` — add `GET /checkpoints` route + handler returning checkpoint rows from shared state
- [ ] `admin.rs` — add `POST /pipelines/:name/reload` route + handler
- [ ] Unit tests for the two new admin routes
- [ ] `cargo clippy --workspace -- -D warnings` clean after changes
- [ ] Commit and open PR → `main`

---

### Phase 9 — Python bindings (`feat/triad-py`)

- [ ] `crates/triad-py/` scaffold: `Cargo.toml` (`cdylib`), `pyproject.toml` (maturin), `src/lib.rs`
- [ ] `PyTriadInstance`: `start()`, `shutdown()`, `transaction()` context manager
- [ ] `PyTransaction`: `execute()`, `fetch_one()`, `fetch_all()` backed by `sqlx::Transaction`
- [ ] `PyOutboxPublisher`: `publish()` inside caller transaction
- [ ] `PyFlagEvaluator`: `is_enabled()` with Redis/PG fallback
- [ ] `PySagaBuilder`: fluent builder → `PySagaConfig` dataclass
- [ ] `PyIdempotencyKey` / `PyIdempotencyRecord` / `lookup` / `store_result`
- [ ] `PyAggregateRoot` + Python `Aggregate` ABC
- [ ] `pytest` test suite (all patterns, testcontainers PG + Redis)
- [ ] Type stubs + `mypy` clean
- [ ] `maturin build --release` produces a valid `.whl`
- [ ] Commit and open PR → `main`

---

### Phase 10 — Terminal UI (`feat/triad-tui`)

- [ ] `crates/triad-tui/` scaffold: `Cargo.toml`, `src/main.rs`, `src/app.rs`
- [ ] `client.rs` — polling `AdminClient` wrapping all admin endpoints on a configurable tick
- [ ] `effects.rs` — named Tachyonfx constructors for all 10 trigger/effect pairs
- [ ] Dashboard screen (screen 1): health + pattern summary + lag bars + backend status
- [ ] Patterns screen (screen 2): list with pause/resume/replay actions + row fade on status change
- [ ] DLQ screen (screen 3): per-topic counts + replay/purge with confirm popup
- [ ] Checkpoints screen (screen 4): checkpoint offsets table
- [ ] Sagas screen (screen 5): list + expandable step detail + cancel action
- [ ] Config screen (screen 6): collapsible tree view of `triad.yaml` + live validate
- [ ] `key_help` widget: context-sensitive key-binding bar
- [ ] `status_badge` widget: coloured ● / ◌ / ✗ badges
- [ ] `triad tui` CLI subcommand wired in `triad-cli/src/main.rs`
- [ ] Unit tests for `App` state transitions (screen switching, action dispatch)
- [ ] Renders correctly at 80×24 and 220×50
- [ ] `cargo clippy -p triad-tui -- -D warnings` clean
- [ ] Commit and open PR → `main`
