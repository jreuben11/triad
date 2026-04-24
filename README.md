# Triad

A Rust library and runner that implements every integration pattern across **PostgreSQL × Kafka × Redis** as composable, observable primitives — so teams stop hand-rolling outbox relays, CDC consumers, saga state machines, and cache-warming scripts, and start declaring what they need.

## What it does

Triad provides pre-built, exactly-once implementations of all golden-triangle patterns:

| Pattern | Description |
|---|---|
| **Transactional Outbox** | Atomic PG write + Kafka publish via WAL relay |
| **Inbox / Deduplication** | Idempotent consumer with PG-backed dedup table |
| **Saga Orchestrator** | Durable multi-step workflows with compensations |
| **CDC Pipeline** | PG logical replication → Kafka → Redis read models |
| **Cache Sync** | Write-through, write-behind, read-through, and cold-start |
| **Stream Enricher** | Real-time Kafka event enrichment from Redis/PG lookups |
| **Exactly-Once Coordinator** | Kafka transactions + Redis NX + PG outbox composed automatically |
| **Webhook Dispatcher** | Reliable HTTP delivery with retries, DLQ, and circuit breaker |
| **Feature Flags** | PG-backed flag store distributed to Redis with hot reload |
| **Feature Store** | Online/offline feature serving from Redis + PG |
| **Dual Read** | Parallel reads with consistent merge (LSN-ordered wins) |

## Deployment modes

**Mode 1 — In-process SDK** (`triad-sdk`): embed directly in your application, zero network hops.

```rust
let triad = Triad::start(config).await?;
triad.outbox().publish("orders", &event).await?;
```

**Mode 2 — Standalone binary** (`triad` CLI): run as a sidecar or system service, managed via `triad run`.

```
triad run --config triad.yaml
triad status
triad pattern list
```

**Mode 3 — Kubernetes worker fleet**: multiple replicas with K8s Lease leader election, Prometheus ServiceMonitor, HPA, and PodDisruptionBudget included.

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

### Crate dependency graph

```
triad-cli ──► triad-runner ──► triad-core ──► (tokio, tracing, metrics, …)
                     │
              triad-proto ──────────────────► (prost, tonic)
triad-sdk ──► triad-runner
```

## Configuration

Triad is configured via `triad.yaml` with optional environment variable overrides:

```yaml
runner:
  drain_timeout_seconds: 30

postgres:
  primary_url: "postgresql://user:pass@localhost/mydb"
  replication_url: "postgresql://user:pass@localhost/mydb?replication=database"
  pool_size: 10

kafka:
  brokers: ["localhost:9092"]
  consumer_group: "triad-runner"
  dlq_topic_template: "triad.dlq.{source_topic}"

redis:
  mode: standalone          # standalone | cluster | sentinel
  url: "redis://localhost:6379"

patterns:
  outbox:
    enabled: true
    poll_interval_ms: 100
    batch_size: 500
  cdc:
    enabled: true
    slot_name: "triad_cdc"
    publication: "triad_pub"

admin:
  port: 8080
  health:
    liveness_path:   /health/live
    readiness_path:  /health/ready
    startup_path:    /health/started
```

## Database migrations

Migrations live in `crates/triad-runner/migrations/` and are applied automatically on startup:

```
001_triad_outbox.sql
002_triad_inbox.sql
003_triad_checkpoints.sql
004_triad_saga.sql
005_webhook_subscriptions.sql
006_feature_flags.sql
007_idempotency_keys.sql
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

This project uses Claude Code agents running in parallel git worktrees. Each crate has its own worktree under `../triad-worktrees/` so agents build in isolation.

The `/zellij-launch` skill (located at `~/.claude/commands/zellij-launch.md`) automates tab setup inside a running [zellij](https://zellij.dev) session. It reads the **Agent Launch Configuration** table in `project-plan.md` to know which worktrees, build targets, and agent prompts to use for each batch.

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

Two modules — `saga-eos` (Phase 2) and `engine` (Phase 3) — use `/loop` instead of a one-shot agent because they require iterative TDD cycles over complex concurrency and transaction semantics. After `/zellij-launch` opens the tab, switch to it and type:

```
/loop
```

Claude self-paces: write failing test → implement → `cargo nextest` → fix → repeat, until coverage exceeds 90%.

#### Agent prompts

Per-agent task descriptions live in `scripts/prompts/<name>.md`. To customise what an agent does for a phase, edit the corresponding prompt file — the Agent Launch Configuration table in `project-plan.md` maps each batch row to its prompt.

### Features

| Feature | Crate | Description |
|---|---|---|
| `kubernetes` | `triad-runner`, `triad-cli` | K8s Lease leader election (Mode 3) |
| `integration` | `triad-runner` | testcontainers-based integration tests |

## Key technology choices

| Concern | Crate |
|---|---|
| Async runtime | `tokio` |
| gRPC | `tonic` + `prost` |
| HTTP / admin | `axum` + `tower` |
| Kafka | `rdkafka` (librdkafka, EOS transactions) |
| PostgreSQL | `sqlx` (pool + migrations) + `tokio-postgres` (WAL replication) |
| Redis | `redis` + `deadpool-redis` |
| Observability | `tracing` + `opentelemetry-otlp` + `metrics` |
| Config | `config` (YAML + env layering) |
| Error handling | `thiserror` (library) + `anyhow` (binary) |
| Testing | `mockall` + `testcontainers-modules` + `rstest` + `wiremock` |

## License

MIT OR Apache-2.0
