# Triad

[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20%2F%20Apache--2.0-blue.svg)](#license)
[![Rust: 1.85+](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)

A Rust library and runner that implements every integration pattern across **PostgreSQL × Kafka × Redis** as composable, observable primitives — so teams stop hand-rolling outbox relays, CDC consumers, saga state machines, and cache-warming scripts, and start declaring what they need.

## Table of Contents

- [What it does](#what-it-does)
- [Quick start](#quick-start)
- [Deployment modes](#deployment-modes)
- [Workspace layout](#workspace-layout)
  - [Crates](#crates)
  - [Crate dependency graph](#crate-dependency-graph)
- [Configuration](#configuration)
- [Database migrations](#database-migrations)
- [Observability](#observability)
- [Development](#development)
  - [Agent-driven development](#agent-driven-development)
  - [Features](#features)
- [Key technology choices](#key-technology-choices)
- [Documentation](#documentation)
- [License](#license)

## What it does

Triad provides pre-built, exactly-once implementations of all golden-triangle patterns:

| Pattern | Description | v0.1.0 |
|---|---|---|
| **Transactional Outbox** | Atomic PG write + Kafka publish via WAL relay | ✓ |
| **Inbox / Deduplication** | Idempotent consumer with PG-backed dedup table | ✓ |
| **Saga Orchestrator** | Durable multi-step workflows with compensations | ✓ |
| **CDC Pipeline** | PG logical replication → Kafka → Redis read models | ✓ |
| **Cache Sync** | Write-through, write-behind, read-through, and cold-start | ✓ |
| **Exactly-Once Coordinator** | Kafka transactions + Redis NX + PG outbox composed automatically | ✓ |
| **Webhook Dispatcher** | Reliable HTTP delivery with retries, DLQ, and circuit breaker | ✓ |
| **Feature Flags** | PG-backed flag store distributed to Redis with hot reload | ✓ |
| **Feature Store** | Online/offline feature serving from Redis + PG | ✓ |
| **Stream Enricher** | Real-time Kafka event enrichment from Redis/PG lookups | v0.2.0 |
| **Dual Read** | Parallel reads with consistent merge (LSN-ordered wins) | v0.2.0 |

## Quick start

**1. Add the SDK dependency:**
```toml
[dependencies]
triad-sdk = { git = "https://github.com/your-org/triad" }
```

**2. Apply database migrations** (or let `TriadInstance::start` apply them automatically):
```bash
sqlx migrate run --source crates/triad-runner/migrations
```

**3. Write `triad.yaml`** — see [Configuration](#configuration).

**4. Embed in your application (Mode 1):**
```rust
use triad_sdk::TriadInstance;

let triad = TriadInstance::start(&config).await?;
triad.outbox().publish("orders", &event).await?;
```

**5. Or run as a standalone binary (Mode 2):**
```bash
triad run --config triad.yaml
```

## Deployment modes

**Mode 1 — In-process SDK** (`triad-sdk`): embed directly in your application, zero network hops.

```rust
let triad = TriadInstance::start(&config).await?;
triad.outbox().publish("orders", &event).await?;
```

**Mode 2 — Standalone binary** (`triad` CLI): run as a sidecar or system service, managed via `triad run`.

```
triad run --config triad.yaml
triad status
triad pattern list
```

> **Deferred to v0.2.0:** `triad migrate`, `triad version`, and `triad lag` are planned but not yet implemented.

**Mode 3 — Kubernetes worker fleet**: multiple replicas with K8s Lease leader election, Prometheus ServiceMonitor, HPA, and PodDisruptionBudget included. Enable with the `kubernetes` feature flag.

## Workspace layout

```
triad/
├── Cargo.toml                  # workspace root
├── triad-system-design.md      # conceptual design
├── triad-physical-design.md    # implementation plan with code sketches
└── crates/
    ├── triad-proto/            # protobuf definitions (tonic-build)
    ├── triad-core/             # shared types, traits, config, error, metrics
    ├── triad-sdk/              # application-facing SDK (Mode 1)
    ├── triad-runner/           # pattern engine, backends, admin HTTP (Mode 2/3)
    └── triad-cli/              # `triad` binary + admin client (Mode 2/3)
```

### Crates

| Crate | Role |
|---|---|
| **`triad-proto`** | Protobuf definitions compiled by `tonic-build`. Defines gRPC service and message types used by the admin server. No business logic. |
| **`triad-core`** | Shared types, core traits (`Source`, `Sink`, `PatternModule`, `CheckpointStore`, `LeaderElector`), error hierarchy, metric name constants, and all configuration structs. Zero external I/O — pure definitions. |
| **`triad-sdk`** | Mode 1 API. `TriadInstance::start()` boots the runner in-process. Provides `OutboxPublisher`, `FlagEvaluator`, `SagaBuilder`, and Tower middleware for HTTP correlation. Depends on `triad-runner`. |
| **`triad-runner`** | The engine. Owns backend clients (PostgreSQL, Kafka, Redis), the circuit-breaker layer, all pattern module implementations, `PatternEngine` (tokio `JoinSet` supervisor), and the Axum admin HTTP server. Used by both the SDK and the CLI. |
| **`triad-cli`** | The `triad` binary. Thin Clap command tree that either starts the runner or calls the admin HTTP API for inspection and control. Uses `anyhow` for top-level error handling. |

### Crate dependency graph

```
triad-cli ──► triad-runner ──► triad-core ──► (tokio, tracing, metrics, …)
                     │
              triad-proto ──────────────────► (prost, tonic)
triad-sdk ──► triad-runner
```

## Configuration

Triad is configured via `triad.yaml`. Any field can be overridden with an environment variable using the `TRIAD_` prefix and `__` as the path separator (e.g. `TRIAD_BACKENDS__POSTGRES__URL`).

```yaml
backends:
  postgres:
    url: "postgresql://user:pass@localhost/mydb"
    replication_url: "postgresql://user:pass@localhost/mydb?replication=database"
    max_connections: 10
    connection_timeout_ms: 5000
    ssl_mode: "disable"
    replication_slot: "triad_slot"   # required for CDC
    publication: "triad_pub"         # required for CDC
  kafka:
    brokers: ["localhost:9092"]
    security:
      protocol: "PLAINTEXT"
    producer:
      transactional_id_prefix: "triad"
      acks: "all"
      compression_type: "snappy"
      transaction_timeout_ms: 60000
      enable_idempotence: true
      max_in_flight_requests: 5
      batch_size: 16384
      linger_ms: 5
      request_timeout_ms: 30000
    consumer:
      group_id: "triad-runner"
      isolation_level: "read_committed"
      auto_offset_reset: "earliest"
      max_poll_interval_ms: 300000
      max_poll_records: 500
      fetch_max_bytes: 52428800
      session_timeout_ms: 30000
      heartbeat_interval_ms: 3000
  redis:
    mode: standalone        # standalone | sentinel | cluster
    url: "redis://localhost:6379"
    pool_size: 10
    connection_timeout_ms: 2000
    read_timeout_ms: 2000
    write_timeout_ms: 2000
    max_retries: 3

patterns:
  - type: outbox
    name: "orders-out"
    table: "outbox"
    kafka_topic: "orders"
  - type: cdc
    name: "products-cdc"
    tables: ["products", "inventory"]
    kafka_topic_prefix: "cdc"

shutdown:
  drain_timeout_seconds: 30
  warn_on_forced_exit: true

admin:
  port: 8080
  auth:
    type: "none"
```

## Database migrations

Migrations live in `crates/triad-runner/migrations/` and are applied automatically on startup via `sqlx`:

```
0001_outbox.sql
0002_inbox.sql
0003_checkpoints.sql
0004_saga.sql
0005_webhooks.sql
0006_feature_flags.sql
0007_idempotency.sql
```

## Observability

Every pipeline emits:

- **Metrics** — Prometheus-compatible via `/metrics` (pattern throughput, lag, error rates, circuit breaker state)
- **Traces** — OpenTelemetry OTLP (per-event spans with correlation IDs)
- **Audit log** — structured events on the `triad.audit` Kafka topic (SIEM-ready)
- **Health checks** — `/health/live`, `/health/ready`, `/health/started`

## Development

Prerequisites: Rust 1.85+, Docker (for integration tests), [Claude Code](https://claude.ai/code).

```bash
# check the workspace
cargo check --workspace

# run unit tests (parallelised)
cargo nextest run --workspace

# run integration tests (starts Kafka, PG, Redis via testcontainers)
cargo nextest run --workspace --features integration

# coverage gate
cargo llvm-cov nextest --workspace --fail-under-lines 80
```

### Agent-driven development

This project uses Claude Code agents running in parallel git worktrees. Each crate has its own worktree under `../triad-worktrees/` so agents build in isolation without sharing a `target/` directory.

The `/zellij-launch` skill automates tab setup inside a running [zellij](https://zellij.dev) session. It reads the **Agent Launch Configuration** table in `project-plan.md` to know which worktrees, build targets, and agent prompts to use for each batch.

#### Starting a development batch

```
# inside a zellij session, from any tab:
/zellij-launch phase 0    # triad-proto + triad-core  (parallel agents)
/zellij-launch phase 1    # triad-runner backends     (parallel agent)
/zellij-launch phase 2    # cdc-outbox (parallel) + saga-eos (/loop TDD)
/zellij-launch phase 3    # engine (/loop TDD) + sdk + cli (parallel)
/zellij-launch phase 4    # integration tests          (parallel agent)
```

Each command opens named tabs in the current session, navigates each tab to its worktree, sets `CARGO_TARGET_DIR` to an isolated `/tmp/` path, and starts `claude --dangerously-skip-permissions` with the per-agent prompt from `scripts/prompts/`.

#### Iterative TDD with /loop

Two modules — `saga-eos` (Phase 2) and `engine` (Phase 3) — use `/loop` instead of a one-shot agent because they require iterative TDD cycles over complex concurrency and transaction semantics. After `/zellij-launch` opens the tab, switch to it and type `/loop`.

Claude self-paces: write failing test → implement → `cargo nextest` → fix → repeat, until coverage exceeds 90%.

#### Agent prompts

Per-agent task descriptions live in `scripts/prompts/<name>.md`. To customise what an agent does for a phase, edit the corresponding prompt file — the Agent Launch Configuration table in `project-plan.md` maps each batch row to its prompt.

### Features

| Feature | Crate | Description |
|---|---|---|
| `kubernetes` | `triad-runner`, `triad-cli` | K8s Lease leader election (Mode 3) |
| `integration` | `triad-runner` | testcontainers-based integration tests |

## Key technology choices

| Concern | Crate | Why |
|---|---|---|
| Async runtime | `tokio` | De-facto standard; best `epoll`/`io_uring` support in the ecosystem |
| gRPC | `tonic` + `prost` | First-class async, streaming, code-generated from `.proto` |
| HTTP / admin | `axum` + `tower` | Composable middleware stack; reuses the tokio runtime |
| Kafka | `rdkafka` (librdkafka) | Only Rust client with full EOS transaction support |
| PostgreSQL | `sqlx` + `tokio-postgres` | `sqlx` for pool + migrations; `tokio-postgres` for WAL replication (separate connection type, not interchangeable) |
| Redis | `redis` + `deadpool-redis` | Async-native client; `deadpool` for connection pooling with health checks |
| Observability | `tracing` + `opentelemetry-otlp` + `metrics` | Unified span/log/metric emission from the same instrumentation points |
| Config | `config` | YAML + env layering in a single builder call; no custom parsing code |
| Error handling | `thiserror` + `anyhow` | Typed errors in library crates; ergonomic `?` propagation in the binary |
| Testing | `mockall` + `testcontainers-modules` + `rstest` + `wiremock` | Mock traits for fast unit tests; real containers for integration; parameterised cases |

## Documentation

| Document | Audience | Description |
|---|---|---|
| [triad-system-design.md](triad-system-design.md) | Architects & contributors | Conceptual design: all patterns, deployment modes, observability SLOs, durability model |
| [triad-physical-design.md](triad-physical-design.md) | Contributors | Rust implementation plan: all structs, traits, SQL DDL, test matrix |
| [stage2-design.md](stage2-design.md) | Contributors | Stage 2 roadmap: Python bindings (`triad-py`) and Terminal UI (`triad-tui`) |
| [golden-triad-research.md](golden-triad-research.md) | Curious readers | Deep-dive on every PG × Kafka × Redis integration pattern with code examples |
| [project-plan.md](project-plan.md) | Agent operators | Phase-by-phase build plan with worktree layout and launch configuration |
| [CLAUDE.md](CLAUDE.md) | AI agents | Invariants, quality gates, and self-optimisation instructions for Claude Code |

## License

MIT OR Apache-2.0
