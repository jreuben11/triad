Implement the CDC/Outbox/Inbox/Cache/Webhook/FeatureFlag/RateLimit/DLQ/FeatureStore pattern modules
per §4.5 of triad-physical-design.md.
This runs after backends are merged to main.

## Your working directory
`/home/jreuben1/Code/triad-worktrees/triad-runner-patterns-cdc-outbox`

## Setup
```bash
export CARGO_TARGET_DIR=/tmp/triad-target-patterns-1
```

## Tasks (implement one at a time, cargo check after each)
1. `src/patterns/outbox.rs` — poll `triad_outbox` WHERE relay_status='pending', produce to Kafka inside EOS transaction, UPDATE relay_status='published'
2. `src/patterns/inbox.rs` — consume Kafka topic, check `triad_inbox` for dedup, invoke handler inside same PG transaction as inbox INSERT
3. `src/patterns/cdc.rs` — connect via tokio-postgres replication, decode pgoutput messages into `ChangeEvent` stream
4. `src/patterns/cache.rs` — write-through / write-behind / read-through / cold-start modes; Redis as primary, PG as source of truth
5. `src/patterns/webhook.rs` — HTTP delivery via reqwest, exponential backoff retry, DLQ to `triad.dlq.{source_topic}` on max retries, circuit breaker per endpoint
6. `src/patterns/feature_flag.rs` — poll PG `feature_flags` table, push to Redis with hot reload on change
7. `src/patterns/rate_limit.rs` — Redis sliding window (ZADD/ZREMRANGEBYSCORE) and token bucket (INCR + EXPIRE)
8. `src/patterns/dlq.rs` — `DlqRouter`: write to `triad.dlq.{source_topic}`, replay messages, purge topic
9. `src/patterns/feature_store.rs` — online feature serving from Redis, offline from PG, freshness tracking
10. Update `src/patterns.rs` → `src/patterns/mod.rs` and wire all modules

## Testing requirements
- Unit tests per pattern with mocked backends (no real I/O)
- Use `#[rstest]` for parameterised scenarios

## Done criteria
- `cargo check -p triad-runner` clean
- `cargo test -p triad-runner` unit tests pass
- `cargo clippy -p triad-runner -- -D warnings` clean
- Commit on branch `feat/triad-runner-patterns-cdc-outbox`

## Key invariants (from CLAUDE.md — do not violate)
- DLQ topic: always `triad.dlq.{source_topic}` — never `{source_topic}.dlq`
- Inbox INSERT must be inside same PG transaction as business write
- When Redis CB open, inbox dedup falls back to PG SELECT — never skip dedup

Output <promise>DONE</promise> when all criteria are met.
