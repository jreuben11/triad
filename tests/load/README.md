# Triad Load Tests

Rust-native load tests using the [`goose`](https://docs.rs/goose) crate.
Requires a running Triad stack (Postgres + Kafka + Redis + `triad run`).

## Run

```bash
cargo run --manifest-path tests/load/Cargo.toml --bin outbox_throughput
cargo run --manifest-path tests/load/Cargo.toml --bin saga_throughput
cargo run --manifest-path tests/load/Cargo.toml --bin cache_read
```

## Override target URL

```bash
TRIAD_BASE_URL=http://prod:8080 cargo run --manifest-path tests/load/Cargo.toml --bin cache_read
```

## Goose flags (appended after --)

```bash
cargo run --bin outbox_throughput -- --users 10 --run-time 30s --report-file report.html
```

## What each binary tests

| Binary | Pattern | Prometheus counter asserted |
|---|---|---|
| `outbox_throughput` | Transactional outbox relay | `triad_outbox_relay_published_total` |
| `saga_throughput` | Saga orchestration | `triad_saga_completed_total` |
| `cache_read` | Redis cache-aside hit rate | `triad_cache_hits_total` / `triad_cache_misses_total` |

Before each run the binary scrapes `{TRIAD_BASE_URL}/metrics` for baseline counter values
and asserts they increased (or computes the hit rate) after the test completes.
