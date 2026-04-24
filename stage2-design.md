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

## Stage 2 project-plan additions

Add to `project-plan.md`:

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
