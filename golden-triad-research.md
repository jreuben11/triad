# The Golden Triangle: PostgreSQL, Kafka, and Redis Integration Patterns

The three technologies form a complementary triad that covers the core requirements of modern backend systems: **durable relational storage** (PostgreSQL), **distributed event streaming** (Kafka), and **high-speed in-memory data structures** (Redis). Mastering their integration patterns is foundational to building scalable, resilient distributed systems.

---

## Technology Roles at a Glance

| Technology | Primary Role | Guarantees | Weakness |
|------------|-------------|------------|---------|
| PostgreSQL | Source of truth, relational queries, ACID transactions | Durability, consistency, strong integrity | Latency under high write throughput |
| Kafka | Asynchronous event backbone, durable log | Ordering within partition, at-least-once delivery, replay | Not a database; no random access |
| Redis | Low-latency reads/writes, ephemeral state, coordination | Sub-millisecond access, atomic operations | Memory-bound, not the source of truth |

---

## Part 1: PostgreSQL ↔ Kafka

### 1.1 Change Data Capture (CDC) with Debezium

The most important PostgreSQL–Kafka integration. Debezium reads PostgreSQL's Write-Ahead Log (WAL) and publishes every row-level change as a Kafka event.

```
PostgreSQL WAL → Debezium Connector → Kafka Topic (per table)
```

**Setup:**
- Enable logical replication in PostgreSQL: `wal_level = logical`
- Create a replication slot: `SELECT pg_create_logical_replication_slot('debezium', 'pgoutput')`
- Debezium publishes to topics named `<server>.<schema>.<table>`

**Event envelope:**
```json
{
  "op": "u",
  "before": { "id": 1, "status": "pending" },
  "after":  { "id": 1, "status": "shipped" },
  "source": { "lsn": "0/16B2450", "txId": 499, "ts_ms": 1714000000000 }
}
```

**Operations:** `c` (create), `u` (update), `d` (delete), `r` (read/snapshot)

**Use cases:**
- Propagate state changes to downstream microservices without polling
- Sync PostgreSQL data to search indexes (Elasticsearch, OpenSearch)
- Drive cache invalidation in Redis
- Build audit logs and event archives

**Pitfalls:**
- Replication slot lag can cause WAL disk bloat if consumer falls behind
- Schema changes require coordinated connector restarts or schema registry updates
- Exactly-once semantics requires idempotent consumers

---

### 1.2 Transactional Outbox Pattern

Solves the dual-write problem: atomically write to PostgreSQL and guarantee Kafka delivery without distributed transactions.

```
Application → PostgreSQL (data + outbox table in same transaction)
                          ↓
               Relay Process (poll or CDC) → Kafka
```

**Outbox table schema:**
```sql
CREATE TABLE outbox (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregate   TEXT NOT NULL,
  aggregate_id UUID NOT NULL,
  event_type  TEXT NOT NULL,
  payload     JSONB NOT NULL,
  created_at  TIMESTAMPTZ DEFAULT now(),
  published_at TIMESTAMPTZ
);
```

**Application write (single transaction):**
```sql
BEGIN;
  UPDATE orders SET status = 'shipped' WHERE id = $1;
  INSERT INTO outbox (aggregate, aggregate_id, event_type, payload)
    VALUES ('order', $1, 'OrderShipped', $2);
COMMIT;
```

**Relay strategies:**
- **Polling relay:** SELECT unpublished rows, publish to Kafka, UPDATE published_at. Simple but introduces latency.
- **CDC relay (preferred):** Run Debezium on the outbox table itself. Zero-polling, low latency, inherits WAL ordering.

**Why this beats dual-write:** If the application crashes after writing to PostgreSQL but before Kafka, the outbox relay retries. If it crashes after Kafka but before marking published, the relay re-publishes (idempotent consumers handle duplicates).

---

### 1.3 Kafka → PostgreSQL: Event-Driven Writes

Consumers write Kafka events back into PostgreSQL — the reverse direction. Critical pattern for event sourcing and CQRS read models.

**Idempotent consumer pattern:**
```sql
CREATE TABLE processed_events (
  event_id    TEXT PRIMARY KEY,
  processed_at TIMESTAMPTZ DEFAULT now()
);

-- In consumer, within a transaction:
INSERT INTO processed_events (event_id) VALUES ($event_id)
  ON CONFLICT DO NOTHING
  RETURNING event_id;
-- Only apply the side effect if the row was inserted (not a duplicate)
```

**Bulk loading with COPY:**
For high-throughput consumers, accumulate a batch and use PostgreSQL's `COPY` protocol instead of individual INSERTs:
```python
cursor.copy_expert(
    "COPY events (id, type, payload, occurred_at) FROM STDIN WITH (FORMAT binary)",
    binary_buffer
)
```

**Consumer offset tracking in PostgreSQL:**
```sql
CREATE TABLE kafka_offsets (
  consumer_group TEXT,
  topic          TEXT,
  partition      INT,
  offset         BIGINT,
  PRIMARY KEY (consumer_group, topic, partition)
);
```
Store offsets in PostgreSQL inside the same transaction as business data — atomic offset commit eliminates duplicate processing on crash recovery.

---

### 1.4 Event Sourcing with PostgreSQL as Event Store

PostgreSQL is a capable event store for bounded aggregates where Kafka handles inter-aggregate communication.

```sql
CREATE TABLE events (
  seq         BIGSERIAL PRIMARY KEY,
  stream_id   UUID NOT NULL,
  stream_type TEXT NOT NULL,
  version     INT NOT NULL,
  event_type  TEXT NOT NULL,
  payload     JSONB NOT NULL,
  metadata    JSONB,
  occurred_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (stream_id, version)  -- optimistic concurrency
);
```

**Optimistic concurrency:**
```sql
INSERT INTO events (stream_id, stream_type, version, event_type, payload)
  VALUES ($id, $type, $expected_version + 1, $event_type, $payload);
-- Fails with unique violation if another writer incremented version first
```

**Publishing to Kafka:** Use Debezium CDC on the events table so every committed event is automatically streamed.

---

## Part 2: PostgreSQL ↔ Redis

### 2.1 Cache-Aside (Lazy Loading)

The most common pattern. Application reads Redis first; on miss, reads PostgreSQL and populates the cache.

```
Read:  App → Redis (hit? return) → PostgreSQL → write to Redis → return
Write: App → PostgreSQL → invalidate Redis key
```

```python
def get_user(user_id: str) -> dict:
    key = f"user:{user_id}"
    cached = redis.get(key)
    if cached:
        return json.loads(cached)
    row = db.execute("SELECT * FROM users WHERE id = %s", user_id).fetchone()
    redis.setex(key, 300, json.dumps(row))  # TTL: 5 minutes
    return row

def update_user(user_id: str, data: dict):
    db.execute("UPDATE users SET ... WHERE id = %s", user_id)
    redis.delete(f"user:{user_id}")  # invalidate
```

**Thundering herd mitigation:** On cache miss under high concurrency, use a Redis lock to ensure only one process queries PostgreSQL:
```python
with redis.lock(f"lock:user:{user_id}", timeout=5):
    if not redis.exists(key):
        row = db.fetch(...)
        redis.setex(key, 300, json.dumps(row))
```

---

### 2.2 Write-Through Cache

Write to Redis and PostgreSQL simultaneously. Keeps cache always warm.

```
Write: App → Redis (sync) → PostgreSQL (sync)
Read:  App → Redis (always warm)
```

```python
def save_product(product: dict):
    with db.transaction():
        db.execute("INSERT INTO products ... ON CONFLICT DO UPDATE SET ...")
        redis.setex(f"product:{product['id']}", 600, json.dumps(product))
```

**Trade-off:** Higher write latency (two round trips); consistent reads. Best when read:write ratio is low.

---

### 2.3 Write-Behind (Write-Back) Cache

Write to Redis immediately, flush to PostgreSQL asynchronously. Minimizes write latency at the cost of durability window.

```
Write: App → Redis (fast)
                ↓ async
       Flush process → PostgreSQL (batched)
```

```python
# Write
redis.hset(f"counter:{id}", mapping=delta_dict)
redis.rpush("flush_queue", id)

# Flush worker (runs every N seconds or on queue threshold)
def flush_worker():
    ids = redis.lrange("flush_queue", 0, -1)
    for id in ids:
        data = redis.hgetall(f"counter:{id}")
        db.execute("INSERT INTO counters ... ON CONFLICT DO UPDATE SET ...")
    redis.ltrim("flush_queue", len(ids), -1)
```

**Risk:** Data in Redis but not yet in PostgreSQL is lost on Redis crash. Mitigate with Redis persistence (AOF) or accept the window as acceptable (analytics counters, view counts).

---

### 2.4 Redis as PostgreSQL Read Replica Accelerator

For complex queries too expensive to run repeatedly, materialize results in Redis and refresh on schedule or on PostgreSQL change.

```
Schedule/CDC trigger → Run PostgreSQL query → Store result in Redis hash/sorted set
```

```python
# Materialized leaderboard refreshed every 60 seconds
def refresh_leaderboard():
    rows = db.execute("""
        SELECT user_id, SUM(points) as total
        FROM scores
        WHERE created_at > now() - interval '7 days'
        GROUP BY user_id
        ORDER BY total DESC
        LIMIT 100
    """).fetchall()
    pipe = redis.pipeline()
    pipe.delete("leaderboard:weekly")
    for rank, row in enumerate(rows):
        pipe.zadd("leaderboard:weekly", {row.user_id: row.total})
    pipe.execute()
```

---

### 2.5 Distributed Locking with Redlock

Use Redis to coordinate access to PostgreSQL resources across multiple application instances.

```python
from redlock import Redlock

dlm = Redlock([redis_client])

def process_job(job_id: str):
    lock = dlm.lock(f"job:{job_id}", 10000)  # 10s TTL
    if not lock:
        return  # another instance is processing
    try:
        db.execute("UPDATE jobs SET status='processing' WHERE id=%s", job_id)
        do_work(job_id)
        db.execute("UPDATE jobs SET status='done' WHERE id=%s", job_id)
    finally:
        dlm.unlock(lock)
```

**Caution:** Redlock provides best-effort locking. For strict mutual exclusion over PostgreSQL rows, use `SELECT ... FOR UPDATE` (advisory locks) instead and reserve Redlock for cross-service coordination.

---

### 2.6 Session Storage

Store HTTP sessions in Redis; persist important session data to PostgreSQL for durability.

```
Login  → Create session → Redis (fast reads) + PostgreSQL (audit/recovery)
Request → Redis lookup only
Logout  → Delete from Redis + mark in PostgreSQL
```

```python
# On login
session_id = secrets.token_urlsafe(32)
session = {"user_id": user_id, "roles": roles, "created_at": now()}
redis.setex(f"session:{session_id}", 3600, json.dumps(session))
db.execute("INSERT INTO sessions (id, user_id, created_at) VALUES (%s, %s, now())", session_id, user_id)
```

---

## Part 3: Kafka ↔ Redis

### 3.1 Real-Time Stream Enrichment

Kafka Streams or consumer applications enrich events with reference data stored in Redis before forwarding.

```
Kafka topic (raw events)
       ↓ consumer
Redis lookup (enrich: user profile, geo data, product metadata)
       ↓
Kafka topic (enriched events)
```

```python
def enrich_event(event: dict) -> dict:
    user = redis.hgetall(f"user:{event['user_id']}")
    if not user:
        user = db.fetch_user(event['user_id'])
        redis.hmset(f"user:{event['user_id']}", user)
        redis.expire(f"user:{event['user_id']}", 600)
    return {**event, "user_segment": user["segment"], "country": user["country"]}
```

---

### 3.2 Rate Limiting with Redis, Enforcement via Kafka

Use Redis sliding-window counters to rate-limit event producers; emit violation events to Kafka.

```python
def check_rate_limit(user_id: str, event_type: str) -> bool:
    key = f"rate:{user_id}:{event_type}"
    pipe = redis.pipeline()
    now = time.time()
    window = 60  # 1 minute
    pipe.zremrangebyscore(key, 0, now - window)
    pipe.zadd(key, {now: now})
    pipe.zcard(key)
    pipe.expire(key, window)
    _, _, count, _ = pipe.execute()

    if count > LIMIT:
        kafka_producer.send("rate-limit-violations", {"user_id": user_id, "count": count})
        return False
    return True
```

---

### 3.3 Redis as Kafka Consumer State Store

Kafka consumers maintain windowed aggregations or lookup tables in Redis rather than in-process memory, enabling stateful processing across consumer restarts and multiple replicas.

```
Kafka events → Consumer group → Aggregate in Redis
                                      ↓
                              Periodic flush to PostgreSQL
```

```python
def handle_order_event(event: dict):
    seller_id = event["seller_id"]
    amount = event["amount"]
    # Accumulate daily revenue in Redis
    today = datetime.date.today().isoformat()
    redis.hincrbyfloat(f"revenue:{seller_id}:{today}", "total", amount)
    redis.hincrby(f"revenue:{seller_id}:{today}", "count", 1)
    redis.expire(f"revenue:{seller_id}:{today}", 86400 * 2)
```

---

### 3.4 Redis Pub/Sub as Kafka Fan-Out for Low-Latency Delivery

Kafka guarantees durability and ordering; Redis Pub/Sub provides sub-millisecond fan-out to WebSocket clients or internal services that need push notifications.

```
Kafka consumer → Redis PUBLISH → WebSocket servers (subscribed) → browsers
```

```python
# Kafka consumer bridges to Redis Pub/Sub
def on_event(event):
    channel = f"user:{event['user_id']}:notifications"
    redis.publish(channel, json.dumps(event))

# WebSocket handler subscribes
pubsub = redis.pubsub()
pubsub.subscribe(f"user:{user_id}:notifications")
for message in pubsub.listen():
    websocket.send(message['data'])
```

**Note:** Redis Pub/Sub has no persistence and no delivery guarantees. Missed events must be recovered from Kafka directly.

---

### 3.5 Kafka Dead Letter Queue + Redis Retry Tracker

Track retry attempts in Redis; route failed events to a Kafka DLQ after threshold.

```python
def process_with_retry(event: dict, topic: str):
    event_id = event["id"]
    retry_key = f"retry:{topic}:{event_id}"
    attempts = redis.incr(retry_key)
    redis.expire(retry_key, 86400)

    try:
        process(event)
        redis.delete(retry_key)
    except RetryableError:
        if attempts >= 3:
            kafka_producer.send(f"{topic}.dlq", event)
            redis.delete(retry_key)
        else:
            raise  # let Kafka consumer retry
```

---

## Part 4: All Three Together — Full Triangle Patterns

### 4.1 CQRS + Event Sourcing

Commands write to PostgreSQL (event store); Kafka propagates events; Redis serves the read model.

```
Command → PostgreSQL event store
               ↓ Debezium CDC
           Kafka topic (domain events)
               ↓ consumer
           Redis (materialized read model)
               ↑
           Query (reads served from Redis)
```

**Example: E-commerce order system**
- `orders` aggregate stored as events in PostgreSQL
- CDC publishes `OrderPlaced`, `OrderShipped`, `OrderCancelled` to Kafka
- Consumer builds Redis hashes: `order:{id}` → current state
- Consumer updates Redis sorted sets: `user:{id}:orders` → sorted by date
- REST API reads exclusively from Redis; writes go to PostgreSQL

**Benefits:** Commands and queries are fully decoupled. Read model scales independently. Historical event replay rebuilds Redis from scratch via Kafka.

---

### 4.2 Saga Pattern (Distributed Transactions)

Coordinate multi-step business transactions across services using Kafka for messaging and Redis for saga state, with PostgreSQL as durable record.

```
Step 1: Reserve inventory   → PostgreSQL + publish ReservationCreated to Kafka
Step 2: Charge payment      → PostgreSQL + publish PaymentCharged to Kafka
Step 3: Confirm order       → PostgreSQL + publish OrderConfirmed to Kafka

On failure: compensating events flow in reverse
```

**Redis saga state tracker:**
```python
def start_saga(saga_id: str, steps: list):
    redis.hmset(f"saga:{saga_id}", {
        "status": "running",
        "current_step": 0,
        "steps": json.dumps(steps),
        "started_at": time.time()
    })
    redis.expire(f"saga:{saga_id}", 3600)

def advance_saga(saga_id: str, completed_step: int):
    redis.hset(f"saga:{saga_id}", "current_step", completed_step + 1)

def fail_saga(saga_id: str, reason: str):
    redis.hmset(f"saga:{saga_id}", {"status": "compensating", "reason": reason})
    # Publish compensating commands to Kafka
```

**PostgreSQL records** the final committed/rolled-back saga state for auditability.

---

### 4.3 Real-Time Data Pipeline

Ingest high-velocity events, aggregate in-flight, persist durably.

```
External events → Kafka (ingestion topic)
                      ↓ stream processor
                  Redis (rolling window aggregates)
                      ↓ periodic flush (every N seconds or M events)
                  PostgreSQL (time-series or analytical tables)
                      ↓ batch job or dbt
                  PostgreSQL (aggregated reporting tables)
```

**Example: IoT sensor pipeline**
```python
# Consumer: accumulate in Redis
def on_sensor_reading(reading):
    device_id = reading["device_id"]
    value = reading["value"]
    ts = reading["timestamp"]
    
    # 1-minute rolling window
    window_key = f"sensor:{device_id}:{ts // 60}"
    redis.hincrbyfloat(window_key, "sum", value)
    redis.hincrby(window_key, "count", 1)
    redis.expire(window_key, 120)

# Flush worker: PostgreSQL persistence
def flush_windows():
    pattern = "sensor:*"
    for key in redis.scan_iter(pattern):
        data = redis.hgetall(key)
        _, device_id, window_minute = key.split(":")
        db.execute("""
            INSERT INTO sensor_aggregates (device_id, window_start, avg_value, reading_count)
            VALUES (%s, to_timestamp(%s * 60), %s, %s)
            ON CONFLICT DO NOTHING
        """, device_id, window_minute, float(data["sum"]) / int(data["count"]), data["count"])
```

---

### 4.4 Multi-Tenant SaaS: Per-Tenant Isolation

```
API request → Redis (tenant config + rate limit) → fast-path config
           → Kafka (tenant-namespaced events)
           → PostgreSQL (tenant schema or row-level isolation)
```

**Redis tenant config cache:**
```python
def get_tenant_config(tenant_id: str) -> dict:
    key = f"tenant:{tenant_id}:config"
    config = redis.get(key)
    if config:
        return json.loads(config)
    config = db.execute("SELECT * FROM tenants WHERE id=%s", tenant_id).fetchone()
    redis.setex(key, 600, json.dumps(config))
    return config
```

**Kafka topic naming:** `{env}.{tenant_id}.{domain}.{event_type}` enables per-tenant consumer groups and independent scaling.

**PostgreSQL isolation:** Row-level security (RLS) with `SET LOCAL app.tenant_id = $1` per connection, enforcing tenant boundaries at the database level.

---

### 4.5 Search-and-Write Pattern

Combine Redis for autocomplete, Kafka for async indexing, and PostgreSQL as the authoritative store.

```
Write:  App → PostgreSQL (save entity)
                   ↓ Debezium
              Kafka → [Consumer A] Redis ZADD (autocomplete)
                    → [Consumer B] Elasticsearch (full-text index)

Read:   Autocomplete → Redis ZRANGEBYLEX (sub-millisecond)
        Full search  → Elasticsearch
        Entity fetch → Redis cache → PostgreSQL
```

**Redis autocomplete index:**
```python
# Index: sorted set with score=0, member="entity_name:entity_id"
def index_entity(name: str, entity_id: str):
    for i in range(1, len(name) + 1):
        redis.zadd("autocomplete:entities", {f"{name[:i]}": 0})
    redis.zadd("autocomplete:entities", {f"{name}:{entity_id}*": 0})

def autocomplete(prefix: str, limit: int = 10) -> list:
    start = prefix.lower()
    stop = start[:-1] + chr(ord(start[-1]) + 1)
    results = redis.zrangebylex("autocomplete:entities", f"[{start}", f"({stop}", start=0, num=limit)
    return [r for r in results if r.endswith("*")]
```

---

### 4.6 Event-Driven Cache Invalidation via CDC

The cleanest approach to cache coherence: let Debezium drive Redis invalidation from PostgreSQL changes.

```
PostgreSQL write (any source)
    ↓ WAL → Debezium → Kafka (changes topic)
                            ↓ invalidation consumer
                        Redis DEL / HSET (precise invalidation)
```

```python
# Kafka consumer processing CDC events
def on_cdc_event(event):
    table = event["source"]["table"]
    op = event["op"]
    after = event.get("after", {})
    before = event.get("before", {})

    if table == "users":
        user_id = (after or before)["id"]
        if op == "d":
            redis.delete(f"user:{user_id}")
        else:
            # Optionally re-populate rather than just invalidate
            redis.setex(f"user:{user_id}", 300, json.dumps(after))

    elif table == "products":
        product_id = (after or before)["id"]
        redis.delete(f"product:{product_id}")
        redis.delete("product:list:*")  # use SCAN for wildcard invalidation in prod
```

**Advantage:** Any write path (direct SQL, ORM, migration, admin tool) automatically triggers correct invalidation. No application-level coupling.

---

## Part 5: Operational Patterns

### 5.1 Health Check Cascade

```python
async def health_check() -> dict:
    results = {}
    # PostgreSQL: simple query
    try:
        db.execute("SELECT 1")
        results["postgres"] = "ok"
    except Exception as e:
        results["postgres"] = str(e)

    # Redis: PING
    try:
        redis.ping()
        results["redis"] = "ok"
    except Exception as e:
        results["redis"] = str(e)

    # Kafka: check consumer group lag
    try:
        lag = get_consumer_lag("my-group", "my-topic")
        results["kafka"] = "ok" if lag < LAG_THRESHOLD else f"high lag: {lag}"
    except Exception as e:
        results["kafka"] = str(e)

    return results
```

---

### 5.2 Backpressure and Flow Control

When PostgreSQL write throughput is saturated, use Kafka as a buffer and Redis as a traffic signal.

```
High-throughput source
    ↓
Kafka (absorbs spikes, durable buffer)
    ↓ consumer (controlled rate)
PostgreSQL (writes at its own pace)

Redis role: store current lag metric; application checks before accepting new work
```

```python
def can_accept_request(topic: str) -> bool:
    lag_key = f"kafka_lag:{topic}"
    lag = redis.get(lag_key)
    if lag and int(lag) > HIGH_WATERMARK:
        return False  # shed load
    return True
```

---

### 5.3 Graceful Degradation

Design each leg of the triangle to fail independently:

| Component Down | Behavior |
|----------------|----------|
| Redis down | Serve reads from PostgreSQL directly (higher latency); writes continue normally |
| Kafka down | Use outbox table; relay resumes when Kafka recovers |
| PostgreSQL down | Serve cached reads from Redis; writes buffered in Kafka |

```python
def get_user(user_id: str) -> dict:
    try:
        cached = redis.get(f"user:{user_id}")
        if cached:
            return json.loads(cached)
    except RedisError:
        logger.warning("Redis unavailable, falling back to PostgreSQL")
    return db.execute("SELECT * FROM users WHERE id=%s", user_id).fetchone()
```

---

### 5.4 Schema Evolution

The three technologies have different schema evolution constraints:

| | PostgreSQL | Kafka | Redis |
|--|------------|-------|-------|
| Schema | Migrations (ALTER TABLE) | Schema Registry (Avro/Protobuf) | Schemaless (application handles) |
| Backward compat | Column additions safe; type changes risky | Forward/backward compatibility enforced by registry | Version key prefixes (`v2:user:{id}`) |
| Zero-downtime | Blue/green, expand-contract pattern | Consumer version negotiation | Dual-write old+new keys during migration |

**Expand-contract pattern for PostgreSQL + Kafka:**
1. **Expand:** Add new column to PostgreSQL. Update CDC consumer to populate Redis with new field.
2. **Migrate:** Backfill existing PostgreSQL rows.
3. **Switch:** Deploy application code using new field. Drop old field from Kafka events and Redis keys.
4. **Contract:** Remove old column from PostgreSQL.

---

## Part 6: Anti-Patterns to Avoid

### 6.1 Using Redis as Primary Database
Redis data is ephemeral by default. Never store data in Redis that doesn't exist in PostgreSQL. Redis is a cache and computation layer, not a source of truth.

### 6.2 Synchronous Kafka Writes in Hot Paths
Kafka's async commit model adds latency. Never wait for Kafka broker acknowledgment on a user-facing HTTP request critical path — use fire-and-forget with the outbox pattern as the safety net.

### 6.3 Polling PostgreSQL Instead of Using CDC
Polling for changes with `SELECT ... WHERE updated_at > last_run` misses deletes, adds polling load, and has unbounded latency. Use CDC (Debezium) for reactive propagation.

### 6.4 Unbounded Redis Keys Without TTLs
Every Redis key written from Kafka events or application code must have a TTL or an explicit deletion strategy. Unbounded growth exhausts memory.

### 6.5 Wildcard Redis KEYS in Production
`KEYS pattern*` is O(N) and blocks the Redis event loop. Always use `SCAN` with a cursor for key iteration.

### 6.6 Storing Large Payloads in Kafka
Kafka is optimized for small-to-medium messages (< 1 MB). For large payloads (images, documents), store in object storage (S3/GCS) and put only the reference pointer in the Kafka event.

### 6.7 Skipping Idempotency on Kafka Consumers
Kafka guarantees at-least-once delivery. Every consumer that writes to PostgreSQL or Redis must be idempotent — use `ON CONFLICT DO NOTHING`, event ID deduplication tables, or Redis `SET NX`.

### 6.8 Tight Coupling via Synchronous DB Reads in Kafka Consumers
Kafka consumers that query PostgreSQL on every event create a direct coupling that limits throughput. Enrich from Redis instead; refresh the Redis data via CDC.

---

## Part 7: Technology Selection Guide

| Requirement | Solution |
|-------------|----------|
| Durable state with ACID guarantees | PostgreSQL |
| Sub-millisecond reads | Redis |
| Fan-out to multiple consumers | Kafka |
| Distributed locking | Redis (Redlock) or PostgreSQL advisory locks |
| Event replay / time travel | Kafka (log compaction) or PostgreSQL event table |
| Autocomplete / leaderboards | Redis (sorted sets / ZRANGEBYLEX) |
| Complex relational queries | PostgreSQL |
| Real-time pub/sub (fire-and-forget) | Redis Pub/Sub |
| Guaranteed async message delivery | Kafka |
| Rate limiting | Redis (sliding window with ZADD) |
| Distributed counters | Redis (INCR, HINCR) |
| Audit trail | PostgreSQL (append-only events table) |
| Cache coherence driven by data changes | Debezium CDC → Kafka → Redis invalidation |

---

## Part 8: FOSS Frameworks That Do the Heavy Lifting

Rolling your own outbox relay, CDC pipeline, or saga orchestrator is feasible but expensive to maintain. The ecosystem has matured to the point where purpose-built frameworks cover most of the patterns described above. This section maps each pattern to the best available open-source options.

---

### 8.1 Change Data Capture Frameworks

#### Debezium
**Language:** Java | **License:** Apache 2.0 | **Connects:** PostgreSQL → Kafka

The reference implementation for CDC. Runs as a Kafka Connect plugin and reads PostgreSQL's WAL via logical replication. Ships connectors for MySQL, MongoDB, Oracle, SQL Server, and more.

```
PostgreSQL WAL → Debezium (Kafka Connect worker) → Kafka topics
```

- Produces structured change events with `before`/`after` snapshots and transaction metadata
- Handles schema evolution via Confluent Schema Registry (Avro/Protobuf/JSON Schema)
- Supports snapshot mode for initial backfill before streaming
- Production complexity is real: replication slot management, WAL disk bloat monitoring, connector restart coordination

**When to pick it:** You already run Kafka Connect infrastructure, or you need multi-database CDC from a single platform.

---

#### PeerDB
**Language:** Go | **License:** AGPL 3.0 | **Connects:** PostgreSQL → Kafka, Snowflake, BigQuery, S3, and more

Purpose-built for PostgreSQL. Exposes a SQL-like interface (`CREATE PEER`, `CREATE MIRROR`) that abstracts the underlying WAL replication. Production setup measured in days, not months.

```sql
CREATE PEER kafka_peer FROM KAFKA WITH (brokers = 'broker:9092');
CREATE MIRROR order_sync FROM postgres_peer TO kafka_peer
  FOR TABLE orders WITH (publication = 'orders_pub');
```

- No Kafka Connect overhead; runs as a standalone service
- Native support for `REPLICA IDENTITY FULL` and partial replication
- Built-in UI for monitoring mirror lag

**When to pick it:** PostgreSQL-only CDC without the Kafka Connect operational burden.

---

#### RisingWave
**Language:** Rust | **License:** AGPL 3.0 | **Connects:** Kafka → PostgreSQL (and many sinks)

A PostgreSQL-wire-compatible streaming database. Consume Kafka topics as streaming sources, define SQL materialized views, and sink results back to PostgreSQL or Redis.

```sql
CREATE SOURCE orders_stream FROM KAFKA TOPIC 'orders'
  FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW hourly_revenue AS
  SELECT seller_id, date_trunc('hour', event_time) AS hour, SUM(amount)
  FROM orders_stream GROUP BY 1, 2;

CREATE SINK pg_sink FROM hourly_revenue INTO POSTGRES ...;
```

- Sub-100ms end-to-end freshness; incremental view maintenance (no full recompute)
- Replaces bespoke Flink/Spark jobs for many streaming SQL use cases

**When to pick it:** You want streaming SQL materialized views without managing a Flink cluster.

---

### 8.2 Transactional Outbox Libraries

#### Spring Modulith — Event Externalization
**Language:** Java | **License:** Apache 2.0 | **Connects:** PostgreSQL → Kafka/RabbitMQ/AMQP

Spring Boot 3.0+ module that turns `ApplicationEvent` publishing into a transactional outbox automatically. An `event_publication` table is created by the framework; a background thread (or CDC relay) forwards events to the broker.

```java
@Service
@Transactional
public class OrderService {
    private final ApplicationEventPublisher events;

    public void placeOrder(Order order) {
        repo.save(order);
        events.publishEvent(new OrderPlaced(order.getId())); // written to event_publication table
    }
}
```

```yaml
spring.modulith.events.kafka.enable: true
```

- Zero boilerplate outbox table management
- Supports completion modes: archive (keep history) or delete (minimal storage)
- Integrates with Spring Kafka, Spring AMQP, JMS

**When to pick it:** Spring Boot application that needs guaranteed-delivery event publishing with minimal ceremony.

---

#### Eventuate Tram
**Language:** Java | **License:** Apache 2.0 | **Connects:** PostgreSQL/MySQL → Kafka/RabbitMQ/ActiveMQ/Redis Streams

Comprehensive transactional messaging library from the author of *Microservices Patterns*. Ships both a client library (writes to outbox) and a CDC service (reads outbox, publishes to broker).

```java
@Transactional
public Order createOrder(CreateOrderRequest request) {
    Order order = orderRepository.save(new Order(request));
    domainEventPublisher.publish(Order.class, order.getId(),
        List.of(new OrderCreated(order)));  // written to outbox in same transaction
    return order;
}
```

- The Eventuate CDC service uses Debezium under the hood for efficient WAL-based relay
- Also provides a saga framework (see §8.4)
- Supports multiple message brokers via a pluggable adapter layer

**When to pick it:** You want a complete outbox + saga solution in one library with multiple broker targets.

---

#### Quarkus Outbox Extension
**Language:** Java | **License:** Apache 2.0 | **Connects:** PostgreSQL → Kafka

First-class Quarkus extension for the outbox pattern using Debezium CDC or polling relay. Requires minimal configuration alongside `quarkus-smallrye-reactive-messaging-kafka`.

```java
@ApplicationScoped
public class OrderService {
    @Inject OutboxEvent outboxEvent;

    @Transactional
    public void placeOrder(Order order) {
        persist(order);
        outboxEvent.fire(new OrderPlaced(order.getId()));
    }
}
```

**When to pick it:** Quarkus-native application with GraalVM native image requirements.

---

### 8.3 Event Sourcing Frameworks

#### Axon Framework
**Language:** Java | **License:** Apache 2.0 (framework) + Axon Server OSS | **Connects:** PostgreSQL/MongoDB (event store) ↔ Kafka ↔ Redis

The dominant Java event sourcing framework. Provides aggregate roots, command handling, event handling, sagas, and snapshotting via annotations. Axon Server (open-source standard edition) acts as the message router and event store.

```java
@Aggregate
public class Order {
    @AggregateIdentifier private String orderId;

    @CommandHandler
    public Order(PlaceOrderCommand cmd) {
        apply(new OrderPlaced(cmd.getOrderId(), cmd.getAmount()));
    }

    @EventSourcingHandler
    public void on(OrderPlaced event) {
        this.orderId = event.getOrderId();
    }
}

@Component
public class OrderProjection {
    @EventHandler
    public void on(OrderPlaced event) {
        // write to Redis or PostgreSQL read model
        redis.hmset("order:" + event.getOrderId(), ...);
    }
}
```

- Axon Server provides distributed command routing and event streaming to Kafka
- Built-in snapshot support to cap aggregate replay time
- Replay projections from any point in history

**When to pick it:** Greenfield Java application where you want event sourcing + CQRS + sagas from a single, opinionated framework.

---

#### Marten
**Language:** C# / .NET | **License:** MIT | **Connects:** PostgreSQL (event store + document store)

Turns PostgreSQL into a full event store and document database via the `JSONB` column type. No separate event store process required.

```csharp
// Append events
await session.Events.AppendOptimistic(orderId, new OrderPlaced(amount));
await session.SaveChangesAsync();

// Rebuild projection
public class OrderProjection : SingleStreamProjection<OrderReadModel>
{
    public void Apply(OrderPlaced e, OrderReadModel model) =>
        model.Amount = e.Amount;
}
```

- Async daemon for background projection rebuilds
- Multi-tenancy via schema separation or row-level filtering
- Snapshotting and inline projections supported

**When to pick it:** .NET stack; you want event sourcing on PostgreSQL without adopting EventStoreDB as a separate service.

---

#### Commanded
**Language:** Elixir | **License:** MIT | **Connects:** PostgreSQL (EventStore library) or EventStoreDB

Elixir-native event sourcing + CQRS framework built on OTP. Aggregates are GenServers; commands are plain structs; events flow through the BEAM message-passing system.

```elixir
defmodule PlaceOrder do
  defstruct [:order_id, :amount]
end

defmodule Order do
  use Commanded.Aggregates.Aggregate

  def execute(%Order{}, %PlaceOrder{} = cmd) do
    %OrderPlaced{order_id: cmd.order_id, amount: cmd.amount}
  end

  def apply(%Order{} = order, %OrderPlaced{} = event) do
    %{order | amount: event.amount}
  end
end
```

**When to pick it:** Elixir/Phoenix stack where OTP's fault tolerance model is a first-class requirement.

---

#### EventStoreDB
**Language:** Polyglot clients (Go, .NET, Java, Python, Node.js, Rust) | **License:** BUSL 1.1 (OSS-friendly for most uses) | **Connects:** Standalone event store → Kafka (persistent subscriptions) → Redis

Purpose-built append-only event store created by Greg Young. Clients subscribe to streams and receive events in real time or via catch-up subscriptions.

```go
client, _ := esdb.NewClient(settings)
events := []esdb.EventData{
    esdb.CreateEventData("OrderPlaced", json.Marshal(orderPlaced)),
}
client.AppendToStream(ctx, "order-"+id, esdb.AppendToStreamOptions{}, events...)

// Subscribe and project to Redis
sub, _ := client.SubscribeToStream(ctx, "$all", esdb.SubscribeToStreamOptions{})
for event := range sub.Channel() {
    project(event, redis)
}
```

- Persistent subscriptions for competing consumer groups (similar to Kafka consumer groups)
- `$all` stream gives a global ordered log across all aggregate streams
- Server-side filtering by event type or stream prefix

**When to pick it:** You want a dedicated, purpose-optimized event store that isn't PostgreSQL.

---

### 8.4 Saga / Distributed Transaction Orchestrators

#### Temporal
**Language:** Polyglot (Go core; SDKs for Java, Python, Go, TypeScript, .NET, PHP) | **License:** MIT | **Connects:** PostgreSQL/Cassandra/MySQL (history store) ↔ Kafka (signals/events)

Durable execution engine. Workflow code is written as ordinary functions; Temporal replays the event history to reconstruct state after crashes. Compensating actions are just regular code paths.

```python
@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order_id: str) -> str:
        try:
            await workflow.execute_activity(reserve_inventory, order_id, ...)
            await workflow.execute_activity(charge_payment, order_id, ...)
            await workflow.execute_activity(fulfill_order, order_id, ...)
            return "completed"
        except ActivityError:
            await workflow.execute_activity(cancel_reservation, order_id, ...)
            return "compensated"
```

- Activity retries, timeouts, and heartbeats are first-class concepts
- Workflow history is an immutable event log (event sourcing under the hood)
- Kafka integration: emit Kafka events from activities; send signals to workflows from Kafka consumers
- Netflix, Stripe, Coinbase run Temporal at scale

**When to pick it:** Multi-step business workflows with complex retry/compensation logic across multiple services or languages.

---

#### Conductor (Netflix / Orkes OSS)
**Language:** Java server, REST API, polyglot workers | **License:** Apache 2.0 | **Connects:** Redis (primary state store) + PostgreSQL (secondary) ↔ Kafka

Netflix's original workflow engine, open-sourced and maintained by Orkes. Workflows are defined as JSON state machines; worker tasks are polled via REST. Kafka is a first-class event source/sink task type.

```json
{
  "name": "order_saga",
  "tasks": [
    { "name": "reserve_inventory", "taskReferenceName": "reserve", "type": "SIMPLE" },
    { "name": "kafka_publish",     "taskReferenceName": "notify",  "type": "KAFKA_PUBLISH",
      "inputParameters": { "topic": "order.confirmed", "value": "${reserve.output}" }},
    { "name": "charge_payment",    "taskReferenceName": "charge",  "type": "SIMPLE" }
  ]
}
```

- Redis stores workflow state natively (fast reads, horizontal scaling)
- PostgreSQL used for durability and indexing
- Built-in Kafka consumer and publisher task types

**When to pick it:** Microservices where workflows are defined declaratively by non-engineers, or you need REST-driven workflow management.

---

#### Camunda 8 (Zeebe)
**Language:** Java + BPMN | **License:** Apache 2.0 (Zeebe engine) | **Connects:** Kafka (internal distributed log) → PostgreSQL/Elasticsearch (exporter)

Business process orchestration using BPMN 2.0. The Zeebe engine uses a Kafka-like distributed log internally. Exporters push workflow state to PostgreSQL or Elasticsearch for querying.

```xml
<bpmn:serviceTask id="ReserveInventory" name="Reserve Inventory">
  <bpmn:extensionElements>
    <zeebe:taskDefinition type="reserve-inventory" />
  </bpmn:extensionElements>
</bpmn:serviceTask>
```

- Designed for business analysts + developers to co-own process definitions
- Handles millions of concurrent workflow instances
- Strong choice when BPMN compliance or process visibility is a requirement

**When to pick it:** Business process workflows where non-engineers need to read/edit the flow, or regulatory auditability is required.

---

#### Eventuate Tram Sagas
**Language:** Java | **License:** Apache 2.0 | **Connects:** PostgreSQL → Kafka

Saga framework built on top of Eventuate Tram messaging. Provides orchestration-based sagas where a coordinator class drives the state machine, sending commands and handling replies.

```java
public class CreateOrderSaga implements SimpleSaga<CreateOrderSagaState> {
    private SagaDefinition<CreateOrderSagaState> sagaDefinition = step()
        .invokeParticipant(inventoryService::reserve)
        .withCompensation(inventoryService::release)
        .step()
        .invokeParticipant(paymentService::charge)
        .withCompensation(paymentService::refund)
        .build();
}
```

**When to pick it:** Already using Eventuate Tram for messaging; want saga support without adopting a separate orchestration engine.

---

### 8.5 Stream Processing Frameworks

#### Kafka Streams
**Language:** Java | **License:** Apache 2.0 | **Connects:** Kafka ↔ PostgreSQL (via JDBC lookups or changelog topics) ↔ Redis (via Processor API)

Embedded stream processing library — no separate cluster. Runs inside your application process. State stores are backed by RocksDB locally and replicated to Kafka changelog topics for fault tolerance.

```java
StreamsBuilder builder = new StreamsBuilder();
KStream<String, Order> orders = builder.stream("orders");

KTable<String, UserProfile> users = builder.table("users");

orders.join(users, (order, user) -> enrich(order, user))
      .filter((k, v) -> v.isHighValue())
      .to("high-value-orders");
```

- Exactly-once semantics with `processing.guarantee=exactly_once_v2`
- Interactive queries: expose local state stores via REST for low-latency lookups
- No Flink/Spark cluster to operate

**When to pick it:** Moderate-scale stream processing tightly coupled to Kafka, deployed alongside your application.

---

#### Apache Flink
**Language:** Java, Scala, Python (PyFlink) | **License:** Apache 2.0 | **Connects:** Kafka (source/sink) ↔ PostgreSQL (JDBC sink/lookup) ↔ Redis (via flink-connector-redis)

Full distributed stream processing engine. Handles event-time semantics, watermarks, complex windowing, and CEP (complex event processing) at massive scale.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<Order> orders = env.fromSource(
    KafkaSource.<Order>builder().setTopics("orders").build(), ...);

orders
    .keyBy(Order::getSellerId)
    .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
    .aggregate(new RevenueAggregator())
    .addSink(new JdbcSink<>(INSERT_SQL, ...));   // → PostgreSQL
```

- Stateful operators with RocksDB backend; state checkpointed to S3/HDFS
- SQL API (`Table API`) allows declarative streaming queries
- Flink CDC connector can replace Debezium for some use cases

**When to pick it:** High-throughput stream processing with complex event-time windowing or CEP that outgrows Kafka Streams.

---

#### Apache Spark Structured Streaming
**Language:** Python, Scala, Java, R | **License:** Apache 2.0 | **Connects:** Kafka (source/sink) ↔ PostgreSQL (JDBC sink) ↔ Redis

Micro-batch streaming built on Spark's unified batch/stream API. Familiar to data engineers already using Spark for batch jobs.

```python
stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BROKERS)
    .option("subscribe", "orders")
    .load())

stream.groupBy(window("timestamp", "1 minute"), "seller_id") \
      .agg(sum("amount").alias("revenue")) \
      .writeStream \
      .foreachBatch(write_to_postgres) \
      .start()
```

- Checkpointing via HDFS/S3 for fault tolerance
- Inherits Spark's ML ecosystem (MLlib) for real-time inference pipelines
- Micro-batch latency (seconds) vs Flink's true streaming (milliseconds)

**When to pick it:** Teams already on Spark who want streaming with minimal platform change; ML inference on streams.

---

#### Quix Streams
**Language:** Python | **License:** Apache 2.0 | **Connects:** Kafka ↔ PostgreSQL/Redis

Python-native stream processing with a pandas-like API. Lower barrier to entry than PyFlink for Python data teams.

```python
app = Application(broker_address="localhost:9092")
orders = app.topic("orders", value_deserializer="json")
output = app.topic("enriched-orders", value_serializer="json")

sdf = app.dataframe(orders)
sdf["user_segment"] = sdf["user_id"].apply(lambda uid: redis.get(f"seg:{uid}"))
sdf.to_topic(output)

app.run(sdf)
```

**When to pick it:** Python-first team that needs Kafka stream processing without JVM complexity.

---

#### Bytewax
**Language:** Python (Rust core) | **License:** Apache 2.0 | **Connects:** Kafka ↔ PostgreSQL/Redis

Dataflow framework with Python API and Rust execution engine. Supports both streaming and batch in one model.

```python
flow = Dataflow("order_pipeline")
inp = op.input("kafka_in", flow, KafkaSource(["orders"]))
enrich = op.map("enrich", inp, lambda msg: enrich_from_redis(msg))
op.output("pg_out", enrich, PostgresSink(conn_str, "enriched_orders"))
```

**When to pick it:** Python teams that need better performance than pure-Python Quix Streams; Rust-backed execution without JVM.

---

### 8.6 Cache Synchronization Tools

#### Readyset
**Language:** Rust | **License:** Proprietary (community tier available) | **Connects:** PostgreSQL → Redis-compatible cache layer

Wire-compatible PostgreSQL proxy that automatically caches query results and invalidates them when the underlying data changes. Zero application code change required.

```
App → Readyset (postgres wire protocol) → PostgreSQL (on miss or write)
         ↑ serves cached results immediately for subsequent reads
```

```sql
-- One-time: tell Readyset to cache this query
CREATE CACHE FROM SELECT * FROM users WHERE id = $1;
-- App code is unchanged; Readyset intercepts and serves from cache
```

- Automatic cache invalidation on upstream writes (no TTL tuning)
- Query-level granularity: cache specific prepared statements
- Acts as a transparent drop-in between application and PostgreSQL

**When to pick it:** You want automatic cache coherence without adding Redis cache logic to application code.

---

#### PGEC (PostgreSQL Edge Cache)
**Language:** Erlang | **License:** Apache 2.0 | **Connects:** PostgreSQL → Redis / Memcached / REST API

Erlang service that subscribes to PostgreSQL logical replication and mirrors row changes to Redis, Memcached, or a REST API. Lightweight alternative to Debezium for simple cache sync use cases.

```
PostgreSQL (logical replication) → PGEC → Redis HSET
                                        → Memcached set
                                        → HTTP POST (webhook)
```

**When to pick it:** Simple PostgreSQL→Redis cache sync without Kafka in the path; Erlang/Elixir stack.

---

#### Debezium + Custom Redis Sink (most flexible)
Combines Debezium's WAL reading with a lightweight Kafka consumer that writes to Redis. Most teams already running Debezium use this rather than adopting a dedicated cache sync tool.

```
PostgreSQL WAL → Debezium → Kafka → Custom consumer → Redis
```

The consumer applies business logic to decide what goes in the cache, how it's keyed, and what TTL to use — something no off-the-shelf tool can fully abstract.

---

### 8.7 Kafka Connect Connectors

The Kafka Connect framework (shipped with Apache Kafka) provides a standard runtime for source and sink connectors. Key connectors for the golden triangle:

| Connector | Direction | Notes | License |
|-----------|-----------|-------|---------|
| **Debezium PostgreSQL** | PostgreSQL → Kafka | WAL-based CDC; low latency | Apache 2.0 |
| **Confluent JDBC Source** | PostgreSQL → Kafka | Poll-based; simpler than CDC; misses deletes | Confluent Community |
| **Confluent JDBC Sink** | Kafka → PostgreSQL | Upsert/insert/delete from Kafka to any JDBC DB | Confluent Community |
| **Redis Kafka Connector** (official) | Redis Streams ↔ Kafka | Bidirectional; maintained by Redis | Redis Developer |
| **kafka-connect-redis** (jcustenborder) | Kafka → Redis | Community sink; supports SET/HSET/ZADD/LPUSH | Apache 2.0 |
| **Aiven Redis Sink** | Kafka → Redis | Aiven-maintained; supports hash, string, list, set types | Apache 2.0 |

**Running connectors:**

```bash
# Deploy Debezium PostgreSQL source connector
curl -X POST http://connect:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "pg-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.dbname": "mydb",
    "plugin.name": "pgoutput",
    "publication.name": "dbz_pub",
    "topic.prefix": "prod"
  }
}'

# Deploy Redis sink connector
curl -X POST http://connect:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "redis-sink",
  "config": {
    "connector.class": "com.redis.kafka.connect.RedisSinkConnector",
    "topics": "prod.public.users",
    "redis.uri": "redis://redis:6379",
    "redis.command": "HSET"
  }
}'
```

---

### 8.8 Application Framework Integration

#### Spring Boot Ecosystem
The richest integration layer for Java. The relevant modules and what they provide:

| Module | What it provides |
|--------|-----------------|
| **Spring Data JPA** | PostgreSQL ORM, repositories, query DSL |
| **Spring Kafka** | `KafkaTemplate`, `@KafkaListener`, consumer offset management |
| **Spring Data Redis** | `RedisTemplate`, reactive Redis, repository abstraction |
| **Spring Integration** | Message channels, adapters, routing — wires all three together |
| **Spring Modulith** | Transactional outbox, modular event publishing |
| **Spring Cloud Data Flow** | Orchestrate streaming pipelines across all three technologies |

A typical Spring Boot service wiring all three:

```java
@Service
public class OrderService {
    private final OrderRepository db;         // → PostgreSQL
    private final KafkaTemplate<String, ?> kafka;
    private final RedisTemplate<String, ?> redis;
    private final ApplicationEventPublisher events; // → outbox (Spring Modulith)

    @Transactional
    public Order place(CreateOrderRequest req) {
        Order order = db.save(new Order(req));
        events.publishEvent(new OrderPlaced(order.getId())); // outbox handles Kafka
        redis.delete("user:" + req.getUserId() + ":orders"); // cache invalidation
        return order;
    }
}
```

---

#### Quarkus Ecosystem
Cloud-native alternative to Spring with native image support (GraalVM). Relevant extensions:

| Extension | What it provides |
|-----------|-----------------|
| `quarkus-hibernate-orm-panache` | PostgreSQL ORM |
| `quarkus-smallrye-reactive-messaging-kafka` | Reactive Kafka consumer/producer |
| `quarkus-redis-client` | Redis client (reactive + imperative) |
| `quarkus-debezium-outbox` | Transactional outbox via Debezium relay |
| `quarkus-kafka-streams` | Embedded Kafka Streams topology |

---

#### MicroProfile Reactive Messaging (language-agnostic specification)
Implemented by Quarkus (SmallRye), Open Liberty, Helidon, and KumuluzEE. Provides CDI-based channel bindings that decouple application code from the broker.

```java
@Incoming("orders-in")      // from Kafka
@Outgoing("orders-enriched") // to Kafka
public OrderEnriched enrich(Order order) {
    String segment = redis.get("user:" + order.getUserId() + ":segment");
    return new OrderEnriched(order, segment);
}
```

The `@Incoming`/`@Outgoing` annotations hide Kafka completely — switching broker requires only configuration changes, not code changes.

---

### 8.9 Framework Selection Decision Tree

```
What pattern do you need?
│
├─ CDC (stream PostgreSQL changes)
│   ├─ Already running Kafka Connect → Debezium PostgreSQL connector
│   ├─ PostgreSQL-only, want simplicity → PeerDB
│   └─ Want streaming SQL on top of CDC → RisingWave
│
├─ Transactional Outbox (guaranteed Kafka delivery)
│   ├─ Spring Boot → Spring Modulith event externalization
│   ├─ Quarkus → quarkus-debezium-outbox
│   └─ Need multi-broker support → Eventuate Tram
│
├─ Event Sourcing
│   ├─ Java → Axon Framework (+ Axon Server)
│   ├─ .NET on PostgreSQL → Marten
│   ├─ Dedicated event store → EventStoreDB
│   └─ Elixir → Commanded
│
├─ Saga / Distributed Transactions
│   ├─ Multi-language, complex retries → Temporal
│   ├─ REST-driven, Redis-backed state → Conductor (Orkes OSS)
│   ├─ BPMN / business analyst visibility → Camunda 8
│   └─ Already on Eventuate Tram → Eventuate Tram Sagas
│
├─ Stream Processing
│   ├─ Kafka-native, embedded in app → Kafka Streams
│   ├─ High-throughput, complex windowing → Apache Flink
│   ├─ Batch + streaming, ML pipelines → Spark Structured Streaming
│   └─ Python team → Quix Streams (simpler) or Bytewax (faster)
│
└─ Cache Synchronization (PostgreSQL → Redis)
    ├─ Zero code change, automatic → Readyset
    ├─ Already running Debezium → Debezium + Redis Kafka Connector
    └─ Lightweight, no Kafka → PGEC
```

---

### 8.10 Framework Coverage Matrix

| Framework | CDC | Outbox | Event Sourcing | CQRS | Saga | Streaming | PostgreSQL | Kafka | Redis | License |
|-----------|:---:|:------:|:--------------:|:----:|:----:|:---------:|:----------:|:-----:|:-----:|---------|
| Debezium | ✓✓ | | | | | | ✓ | ✓ | | Apache 2.0 |
| PeerDB | ✓✓ | | | | | | ✓ | ✓ | | AGPL 3.0 |
| RisingWave | ✓ | | | | | ✓✓ | ✓ | ✓ | | AGPL 3.0 |
| Spring Modulith | | ✓✓ | | | | | ✓ | ✓ | | Apache 2.0 |
| Eventuate Tram | | ✓✓ | | | ✓ | | ✓ | ✓ | ✓ | Apache 2.0 |
| Quarkus extensions | | ✓ | | | | ✓ | ✓ | ✓ | ✓ | Apache 2.0 |
| Axon Framework | | | ✓✓ | ✓✓ | ✓✓ | | ✓ | ✓ | ✓ | Apache 2.0 |
| Marten | | | ✓✓ | ✓ | | | ✓ | ✓ | | MIT |
| EventStoreDB | | | ✓✓ | ✓ | | | | ✓ | ✓ | BUSL 1.1 |
| Commanded | | | ✓✓ | ✓✓ | ✓ | | ✓ | | | MIT |
| Temporal | | | | | ✓✓ | | ✓ | ✓ | ✓ | MIT |
| Conductor (Orkes) | | | | | ✓✓ | | ✓ | ✓ | ✓ | Apache 2.0 |
| Camunda 8 / Zeebe | | | | | ✓✓ | | ✓ | ✓ | | Apache 2.0 |
| Kafka Streams | | | | | | ✓✓ | ✓ | ✓ | ✓ | Apache 2.0 |
| Apache Flink | | | | | | ✓✓ | ✓ | ✓ | ✓ | Apache 2.0 |
| Spark Structured Streaming | | | | | | ✓✓ | ✓ | ✓ | | Apache 2.0 |
| Quix Streams | | | | | | ✓ | ✓ | ✓ | ✓ | Apache 2.0 |
| Bytewax | | | | | | ✓ | ✓ | ✓ | ✓ | Apache 2.0 |
| Readyset | | | | | | | ✓ | | ✓ | Proprietary |
| PGEC | | | | | | | ✓ | | ✓ | Apache 2.0 |
| Spring Kafka | | | | | | | ✓ | ✓ | ✓ | Apache 2.0 |
| MicroProfile | | | | | | ✓ | ✓ | ✓ | ✓ | Apache 2.0 |

✓✓ = primary use case   ✓ = supported

---

## Part 9: Additional Integration Patterns

---

### 9.1 Inbox Pattern

The formal counterpart to the Outbox. Where the Outbox guarantees a service can *publish* to Kafka exactly once, the Inbox guarantees a consumer can *receive* and *process* a Kafka event exactly once — even when the broker delivers it more than once (at-least-once delivery) or the consumer crashes mid-processing.

The Outbox and Inbox together form a complete end-to-end exactly-once chain at the application layer, independent of Kafka's own transaction primitives.

```
Producer service                    Consumer service
──────────────                      ────────────────
PostgreSQL ← outbox table           inbox table → PostgreSQL
     ↓ relay                               ↑ deduplicate
  Kafka topic ─────────────────────────────┘
```

**Inbox table schema:**
```sql
CREATE TABLE inbox (
  event_id      TEXT PRIMARY KEY,           -- Kafka message key or payload UUID
  topic         TEXT NOT NULL,
  received_at   TIMESTAMPTZ DEFAULT now(),
  processed_at  TIMESTAMPTZ,
  payload       JSONB NOT NULL
);
```

**Consumer logic (single transaction):**
```python
def handle_kafka_message(msg):
    event_id = msg.key().decode()
    payload  = json.loads(msg.value())

    with db.transaction():
        # Atomically claim the event; duplicate delivery hits ON CONFLICT
        inserted = db.execute("""
            INSERT INTO inbox (event_id, topic, payload)
            VALUES (%s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
        """, event_id, msg.topic(), json.dumps(payload)).fetchone()

        if inserted:
            apply_business_logic(payload)          # side effects in same txn
            db.execute("UPDATE inbox SET processed_at = now() WHERE event_id = %s", event_id)

    consumer.commit()   # only commit offset after successful transaction
```

**Relation to Redis:** For high-throughput consumers where a PostgreSQL round trip per message is too expensive, a Redis `SET NX` check can act as a fast first-pass filter. Only events that pass the Redis gate pay the PostgreSQL transaction cost.

```python
def handle_kafka_message(msg):
    event_id = msg.key().decode()
    if not redis.set(f"inbox:{event_id}", "1", nx=True, ex=86400):
        consumer.commit()   # already processed; skip
        return
    # fallthrough to PostgreSQL transactional processing
    with db.transaction():
        apply_business_logic(json.loads(msg.value()))
    consumer.commit()
```

**Inbox vs idempotent consumer (§1.3):** §1.3 shows the deduplication table technique. "Inbox" is the named architectural pattern that formalises this: an inbox table is an explicit component with its own lifecycle (pruning, monitoring, backlog alerting) rather than just a dedup guard.

---

### 9.2 Kafka Transactions and End-to-End Exactly-Once

Kafka's producer transaction API enables atomic writes across multiple topics — and, crucially, atomic commit of offsets alongside business output. Combined with PostgreSQL offset tracking and Redis idempotency, this composes into a full exactly-once guarantee.

#### Kafka Producer Transactions

```python
producer = KafkaProducer(
    bootstrap_servers=BROKERS,
    transactional_id="order-processor-1",   # stable ID for fencing
    enable_idempotence=True,
)
producer.init_transactions()

def process(consumer, msg):
    result = transform(msg)

    producer.begin_transaction()
    try:
        # Publish to multiple output topics atomically
        producer.send("enriched-orders", key=result["id"], value=result)
        producer.send("order-metrics",   key=result["seller_id"], value=metrics(result))

        # Commit input offset inside the same transaction
        producer.send_offsets_to_transaction(
            {TopicPartition(msg.topic(), msg.partition()): OffsetAndMetadata(msg.offset() + 1)},
            consumer_group_id=CONSUMER_GROUP,
        )
        producer.commit_transaction()
    except Exception:
        producer.abort_transaction()
        raise
```

Consumers reading the output topics must set `isolation.level=read_committed` to see only committed data.

#### PostgreSQL Offset Commit in the Same Transaction

For consumers that write to PostgreSQL, storing the offset in the same database transaction as the business data gives exactly-once without Kafka transactions:

```python
def process_and_commit(msg, conn):
    with conn.transaction():
        apply_to_postgres(msg, conn)
        conn.execute("""
            INSERT INTO kafka_offsets (consumer_group, topic, partition, offset)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (consumer_group, topic, partition)
            DO UPDATE SET offset = EXCLUDED.offset
        """, GROUP, msg.topic(), msg.partition(), msg.offset() + 1)
    # Do NOT auto-commit Kafka offset; manage it manually from the PG table on restart
```

On restart, the consumer reads its last committed offset from PostgreSQL and seeks to it, discarding any Kafka auto-commit.

#### Redis as Idempotency Layer

Redis provides the sub-millisecond dedup check before the expensive PostgreSQL write:

```python
def process_event(event):
    key = f"processed:{event['id']}"
    if redis.set(key, "1", nx=True, ex=3600):   # NX = only set if not exists
        write_to_postgres(event)                  # guarded by Redis gate
    # else: duplicate, silently skip
```

#### Full Composition: End-to-End Exactly-Once

```
Kafka producer (transactional_id)
    ↓ begin_transaction
    ├─ send to output topic A
    ├─ send to output topic B
    └─ send_offsets_to_transaction
         ↓ commit_transaction

Consumer (isolation.level=read_committed)
    ↓ Redis NX check (fast dedup gate)
    ↓ PostgreSQL transaction
        ├─ apply business logic
        └─ store offset in kafka_offsets table
```

Each layer covers a different failure window:
- **Kafka transactions:** exactly-once across multiple output topics
- **PostgreSQL offset-in-transaction:** exactly-once for DB writes on consumer crash
- **Redis NX:** sub-millisecond fast path that avoids PG overhead for duplicates
- **Inbox table (§9.1):** exactly-once for consumers processing events from other services

---

### 9.3 Redis Streams as a Lightweight Kafka Alternative

Redis Streams (`XADD`, `XREAD`, `XREADGROUP`, `XACK`) provide a persistent, ordered, consumer-group-aware log that covers many of the same patterns as Kafka at much lower operational cost. Understanding when to use Redis Streams instead of Kafka — or alongside it — reshapes how you compose the triangle.

#### Redis Streams Primitives

```python
# Producer: append to stream
redis.xadd("orders", {"user_id": "u1", "amount": "99.00", "status": "placed"})

# Consumer group (mirrors Kafka consumer groups)
redis.xgroup_create("orders", "billing-service", id="0", mkstream=True)

# Consumer: read pending messages
messages = redis.xreadgroup("billing-service", "worker-1", {"orders": ">"}, count=10)
for stream, msgs in messages:
    for msg_id, fields in msgs:
        process(fields)
        redis.xack("orders", "billing-service", msg_id)   # explicit ack
```

`XACK` is the equivalent of committing a Kafka offset. Un-acked messages remain pending and are redeliverable via `XPENDING` / `XCLAIM`, enabling failed-consumer recovery.

#### Redis Streams vs Kafka: When to Choose

| Dimension | Redis Streams | Kafka |
|-----------|---------------|-------|
| Throughput | ~100K msg/s per stream | Millions/s per partition |
| Retention | Memory-bound (configurable `MAXLEN`) | Disk-based, terabytes |
| Replay | From any ID (while retained) | Configurable retention |
| Consumer groups | Native | Native |
| Multi-DC replication | Redis active-active (enterprise) or manual | MirrorMaker 2, Cluster Linking |
| Operational cost | Zero (already running Redis) | Significant (ZooKeeper/KRaft + brokers) |
| Ordering | Per stream (like per-partition) | Per partition |
| Schema registry | None (schemaless) | Confluent Schema Registry |

**Choose Redis Streams when:** you already have Redis, throughput is under ~100K/s, message retention fits in memory, and you don't need cross-DC replication or schema evolution tooling.

**Choose Kafka when:** you need replay across time windows longer than your Redis memory budget, regulated retention (GDPR, SOC2), cross-datacenter fan-out, or stream processing via Kafka Streams/Flink.

#### Redis Streams + PostgreSQL: Lightweight Outbox Alternative

For services that don't justify a full Kafka deployment:

```
Application → PostgreSQL (data) + Redis XADD (event)
                                         ↓ consumer group
                              Downstream service → PostgreSQL
```

This is not truly atomic (dual write), but for internal workloads where idempotent consumers are enforced and data loss on Redis failure is acceptable, it's a pragmatic trade-off.

For durability: enable Redis AOF persistence (`appendonly yes`, `appendfsync everysec`) so the stream survives Redis restarts.

#### Redis Streams as a Kafka Consumer Result Store

A common hybrid: Kafka handles durable ingestion; a consumer writes results to Redis Streams for immediate downstream real-time processing, while PostgreSQL receives the persisted form:

```
Kafka (durable ingestion)
    ↓ consumer
Redis XADD (real-time fan-out stream)   PostgreSQL (persisted record)
    ↓ XREADGROUP
WebSocket servers / notification workers
```

---

### 9.4 HTTP-Level Idempotency Keys

API-level idempotency is distinct from Kafka consumer deduplication. When a client retries an HTTP request (network timeout, mobile reconnect), the server must ensure the underlying PostgreSQL write happens exactly once and the same response is returned. Redis is the natural idempotency store: sub-millisecond lookup, configurable TTL, atomic `SET NX`.

```
Client → POST /orders (Idempotency-Key: <uuid>)
              ↓
         Redis NX check
              ├─ miss (first request): process → write PG → cache response in Redis
              └─ hit  (retry):         return cached response immediately (no PG write)
```

**Implementation:**

```python
def idempotent_handler(request):
    idem_key = request.headers.get("Idempotency-Key")
    if not idem_key:
        return process_request(request)   # non-idempotent path

    redis_key = f"idem:{idem_key}"
    lock_key  = f"idem:lock:{idem_key}"

    # Fast path: already completed
    cached = redis.get(redis_key)
    if cached:
        return json.loads(cached)

    # Slow path: acquire lock so concurrent retries don't double-write
    with redis.lock(lock_key, timeout=30):
        cached = redis.get(redis_key)    # re-check inside lock
        if cached:
            return json.loads(cached)

        # Execute business logic + PostgreSQL write
        result = process_and_persist(request)

        # Store response with TTL (24h is typical)
        redis.setex(redis_key, 86400, json.dumps(result))
        return result
```

**PostgreSQL as durable backup:** For idempotency keys that must survive a Redis flush (e.g., financial transactions), also write the key to PostgreSQL:

```sql
CREATE TABLE idempotency_keys (
  key          TEXT PRIMARY KEY,
  response     JSONB NOT NULL,
  created_at   TIMESTAMPTZ DEFAULT now(),
  expires_at   TIMESTAMPTZ NOT NULL
);
```

On Redis miss, check PostgreSQL before processing. On Redis warm-up (§9.5), replay recent keys from PostgreSQL into Redis.

**Kafka integration:** If the request triggers a Kafka event, include the idempotency key in the event payload. Downstream consumers can carry the key through the chain, enabling end-to-end deduplication without coordinating directly with the origin service.

---

### 9.5 Cold Start and State Rebuild

When a service starts for the first time, or Redis is flushed, or a new read model is deployed, you need a strategy for populating Redis before live traffic hits it. Getting this wrong causes a thundering herd against PostgreSQL. Three distinct strategies depending on context:

#### Strategy A: PostgreSQL Snapshot (for read models with bounded size)

Bulk-load the current state of PostgreSQL into Redis before opening traffic. Use a pipeline for batching.

```python
def warm_redis_from_postgres():
    pipe = redis.pipeline(transaction=False)
    batch_size = 500
    offset = 0

    while True:
        rows = db.execute(
            "SELECT id, data FROM users ORDER BY id LIMIT %s OFFSET %s",
            batch_size, offset
        ).fetchall()
        if not rows:
            break

        for row in rows:
            pipe.setex(f"user:{row.id}", 3600, json.dumps(row.data))

        pipe.execute()
        offset += batch_size

    pipe.execute()
```

**When to use:** Cache-aside or CQRS read models where the source of truth is PostgreSQL and the dataset fits in a reasonable warmup window.

#### Strategy B: Kafka Replay (for event-sourced read models)

Replay the Kafka topic from offset 0 (or from a known checkpoint). This rebuilds the read model from the event history.

```python
def rebuild_from_kafka(topic: str, target_redis_prefix: str):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BROKERS,
        auto_offset_reset="earliest",
        group_id=f"rebuild-{uuid.uuid4()}",   # unique group: no committed offsets
        enable_auto_commit=False,
    )
    high_watermarks = get_high_watermarks(consumer, topic)

    pipe = redis.pipeline(transaction=False)
    count = 0

    for msg in consumer:
        apply_event_to_redis(msg, pipe)
        count += 1
        if count % 500 == 0:
            pipe.execute()

        # Stop when we've consumed all events that existed at rebuild start
        tp = TopicPartition(msg.topic(), msg.partition())
        if msg.offset() >= high_watermarks[tp] - 1:
            break

    pipe.execute()
    consumer.close()
```

**When to use:** Event-sourced systems (§1.4, §4.1) where PostgreSQL holds events but Redis holds the materialised view. Kafka replay is authoritative and captures all intermediate states.

**Watermark snapshotting:** For very large topics, don't replay from 0 every time. Periodically snapshot the current Redis state to PostgreSQL (or S3), then replay only the delta from the snapshot's Kafka offset:

```sql
CREATE TABLE redis_snapshots (
  model_name    TEXT PRIMARY KEY,
  kafka_offset  BIGINT NOT NULL,
  snapshot_data JSONB NOT NULL,
  taken_at      TIMESTAMPTZ DEFAULT now()
);
```

#### Strategy C: Dual-Read (for gradual traffic migration)

During a migration or canary deploy, serve reads from both PostgreSQL and Redis. Redis is preferred when warm; PostgreSQL is the fallback. Track the hit rate to know when Redis is sufficiently warm before switching fully.

```python
def get_user(user_id: str) -> dict:
    cached = redis.get(f"user:{user_id}")
    if cached:
        metrics.incr("cache.hit")
        return json.loads(cached)
    metrics.incr("cache.miss")
    row = db.execute("SELECT * FROM users WHERE id = %s", user_id).fetchone()
    redis.setex(f"user:{user_id}", 3600, json.dumps(row))
    return row
```

Monitor the miss rate in real time (emit to Kafka, aggregate in Redis counters). Switch to Redis-primary mode when miss rate drops below a threshold.

---

### 9.6 Feature Flag Distribution

Feature flags need to be readable in sub-millisecond time on every request (Redis), changed rarely with full audit trail (PostgreSQL), and propagated to all service instances immediately when toggled (Kafka). This is one of the cleanest three-way integrations in the triangle.

```
Operator toggles flag
    ↓
PostgreSQL (authoritative flag config + audit log)
    ↓ CDC (Debezium) or outbox
Kafka (flag-changed topic)
    ↓ consumer (all service instances subscribed)
Redis (local runtime flag store, per-instance)
    ↑
Application hot path (sub-millisecond flag read)
```

**PostgreSQL schema:**
```sql
CREATE TABLE feature_flags (
  name         TEXT PRIMARY KEY,
  enabled      BOOLEAN NOT NULL DEFAULT false,
  rollout_pct  INT NOT NULL DEFAULT 0,           -- 0-100 percent rollout
  targeting    JSONB,                             -- user segment rules
  updated_by   TEXT,
  updated_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE flag_audit (
  id           BIGSERIAL PRIMARY KEY,
  flag_name    TEXT NOT NULL,
  old_value    JSONB,
  new_value    JSONB,
  changed_by   TEXT,
  changed_at   TIMESTAMPTZ DEFAULT now()
);
```

**Redis flag schema (hash per flag):**
```python
def cache_flag(flag: dict):
    redis.hset(f"flag:{flag['name']}", mapping={
        "enabled":     str(flag["enabled"]).lower(),
        "rollout_pct": flag["rollout_pct"],
        "targeting":   json.dumps(flag["targeting"] or {}),
        "version":     flag["updated_at"].isoformat(),
    })
    # No TTL: flags are invalidated by Kafka events, not expiry
```

**Flag evaluation (hot path, no network I/O on Redis hit):**
```python
def is_enabled(flag_name: str, user_id: str) -> bool:
    flag = redis.hgetall(f"flag:{flag_name}")
    if not flag or flag[b"enabled"] == b"false":
        return False

    rollout_pct = int(flag[b"rollout_pct"])
    if rollout_pct == 100:
        return True

    # Deterministic bucket assignment (consistent per user, not random)
    bucket = int(hashlib.md5(f"{flag_name}:{user_id}".encode()).hexdigest(), 16) % 100
    return bucket < rollout_pct
```

**Kafka consumer (all service instances):**
```python
@kafka_consumer("flag-changed")
def on_flag_changed(event):
    flag = event["after"]           # Debezium CDC envelope
    if event["op"] == "d":
        redis.delete(f"flag:{flag['name']}")
    else:
        cache_flag(flag)
```

**Cold start (§9.5 applied to flags):** On service startup, load all flags from PostgreSQL before processing traffic. The Kafka consumer then keeps them current without polling.

---

### 9.7 Multi-Region / Geo-Replication

Running the golden triangle across multiple geographic regions — for disaster recovery, latency reduction, or data sovereignty — requires a replication strategy for each leg. The three technologies have fundamentally different replication models that must be composed carefully.

```
Region A (primary)                 Region B (replica/active)
──────────────────                 ─────────────────────────
PostgreSQL (primary) ──────────→  PostgreSQL (logical replica)
     ↓ WAL                              ↓
  Kafka cluster ──── MirrorMaker 2 →  Kafka cluster
     ↓                                   ↓
  Redis (primary) ── active-passive →  Redis (replica)
              or
              ── active-active (CRDT) → Redis (peer)
```

#### PostgreSQL: Logical Replication Cross-Region

Physical streaming replication replicates everything to the replica. Logical replication (the same mechanism Debezium uses) lets you replicate specific tables at the row level — useful for partial replication or bidirectional sync.

```sql
-- On primary (Region A):
CREATE PUBLICATION cross_region FOR TABLE orders, users, products;

-- On replica (Region B):
CREATE SUBSCRIPTION region_b_sub
  CONNECTION 'host=pg-primary.region-a.internal port=5432 dbname=mydb'
  PUBLICATION cross_region;
```

**Conflict resolution for active-active PostgreSQL:** Use `generated always as identity` columns with region-prefixed sequences, or use UUIDs, to avoid primary key collisions when both regions accept writes. Application-level conflict resolution is required (last-write-wins, vector clocks, or CRDTs at the application layer).

#### Kafka: MirrorMaker 2

MirrorMaker 2 (MM2) replicates topics between Kafka clusters, including consumer group offsets, enabling consumer failover.

```properties
# mm2.properties
clusters = region-a, region-b
region-a.bootstrap.servers = kafka-a:9092
region-b.bootstrap.servers = kafka-b:9092

# Replicate all topics from A to B with region prefix
region-a->region-b.enabled = true
region-a->region-b.topics  = .*
# Topics appear in region-b as "region-a.orders", "region-a.users", etc.

# Sync consumer offsets for failover
region-a->region-b.sync.group.offsets.enabled = true
```

**Offset translation:** MM2 maintains an offset mapping topic so that a consumer that was at offset 1000 in region-a can continue from the equivalent position in region-b after failover.

**Active-active Kafka:** Both regions produce locally. MM2 replicates in both directions. Topic naming convention (`{region}.{topic}`) prevents infinite replication loops.

#### Redis: Active-Passive vs Active-Active

**Active-passive (simpler):** Region B runs Redis replicas pointing to Region A's primary. On failover, promote a replica to primary. Data is always eventually consistent (replication lag = risk window).

```
Region A: Redis primary  →  replication  →  Region B: Redis replica
                                                  ↑ promote on A failure
```

**Active-active with Redis Enterprise (CRDT):** Each region has a full primary. Writes in both regions are merged using conflict-free replicated data types. Available in Redis Enterprise (commercial) and approximated in OSS Redis with careful data structure choice.

For OSS Redis active-active, limit writes to commutative operations (INCR, ZADD with scores that don't conflict) and use Kafka as the replication log:

```
Region A writes → Redis A + Kafka A
                             ↓ MM2
                         Kafka B → consumer → Redis B (apply same ops)
```

#### Routing and Consistency Trade-offs

| Strategy | Write latency | Read staleness | Complexity |
|----------|--------------|----------------|------------|
| All writes → primary region | Low (local reads in replica region) | Replication lag | Low |
| Active-active, all data | Low per-region | Near-zero if CRDT | High |
| Active-active, partitioned by tenant | Low | Zero (tenant owns one region) | Medium |
| Read local, write primary | Low reads | Replication lag | Low |

**Recommended starting point:** Write to the primary region's PostgreSQL and Kafka; replicate to the secondary via logical replication and MM2; Redis in the secondary reads from a replica but serves reads locally. Failover is manual or DNS-based.

---

### 9.8 Webhook Delivery Pipeline

Webhooks require reliable outbound HTTP delivery to third-party endpoints — a pattern that combines the persistence of PostgreSQL, the buffering of Kafka, and the ephemeral state management of Redis. The challenge is durability (don't lose events), retries (endpoints are unreliable), and backpressure (a slow endpoint shouldn't block others).

```
Internal event (any source)
    ↓
PostgreSQL (webhook_subscriptions: who gets what)
    ↓ Kafka (webhook_dispatch topic)
    ↓ consumer (delivery worker pool)
    ├─ Redis (per-endpoint circuit breaker + retry state)
    ├─ HTTP POST → subscriber endpoint
    └─ PostgreSQL (webhook_deliveries: delivery log)
```

**PostgreSQL schema:**
```sql
CREATE TABLE webhook_subscriptions (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id    UUID NOT NULL,
  url          TEXT NOT NULL,
  event_types  TEXT[] NOT NULL,
  secret       TEXT NOT NULL,    -- HMAC signing secret
  active       BOOLEAN DEFAULT true
);

CREATE TABLE webhook_deliveries (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  subscription_id UUID REFERENCES webhook_subscriptions,
  event_id        TEXT NOT NULL,
  event_type      TEXT NOT NULL,
  payload         JSONB NOT NULL,
  attempt         INT DEFAULT 1,
  status          TEXT NOT NULL,  -- pending, delivered, failed, exhausted
  response_status INT,
  next_attempt_at TIMESTAMPTZ,
  delivered_at    TIMESTAMPTZ
);
```

**Fan-out: event → per-subscriber Kafka messages:**
```python
def fan_out_event(event: dict):
    subs = db.execute("""
        SELECT id, url, secret FROM webhook_subscriptions
        WHERE %s = ANY(event_types) AND active = true
    """, event["type"]).fetchall()

    for sub in subs:
        kafka_producer.send("webhook_dispatch", key=str(sub.id), value={
            "subscription_id": str(sub.id),
            "event_id":        event["id"],
            "event_type":      event["type"],
            "payload":         event,
            "url":             sub.url,
            "secret":          sub.secret,
        })
```

**Delivery worker with Redis circuit breaker:**
```python
def deliver_webhook(msg: dict):
    sub_id = msg["subscription_id"]
    url    = msg["url"]
    cb_key = f"webhook:cb:{sub_id}"

    # Circuit breaker: skip delivery if endpoint is known-bad
    cb_state = redis.hgetall(cb_key)
    if cb_state.get(b"state") == b"open":
        requeue_with_backoff(msg)
        return

    try:
        signature = hmac.new(msg["secret"].encode(), json.dumps(msg["payload"]).encode(), "sha256").hexdigest()
        resp = requests.post(url, json=msg["payload"],
                             headers={"X-Webhook-Signature": signature},
                             timeout=10)
        resp.raise_for_status()

        # Success: reset circuit breaker failure count
        redis.hset(cb_key, "failures", 0)
        redis.hset(cb_key, "state", "closed")
        log_delivery(msg, resp.status_code, "delivered")

    except Exception as e:
        failures = redis.hincrby(cb_key, "failures", 1)
        redis.expire(cb_key, 3600)

        if failures >= 5:
            redis.hset(cb_key, "state", "open")   # open circuit: skip for backoff window

        attempt = msg.get("attempt", 1)
        if attempt >= 10:
            log_delivery(msg, None, "exhausted")   # give up
        else:
            delay = min(2 ** attempt, 3600)        # exponential backoff, cap at 1h
            kafka_producer.send("webhook_dispatch",
                                value={**msg, "attempt": attempt + 1},
                                timestamp_ms=int((time.time() + delay) * 1000))
```

**Key properties:**
- Kafka partitioned by `subscription_id` ensures per-subscriber ordering
- Redis circuit breaker prevents hammering a dead endpoint
- PostgreSQL `webhook_deliveries` provides the audit trail and retry visibility
- Exponential backoff via delayed Kafka re-publish (or a dedicated delay topic)

---

### 9.9 Online ML Feature Store

Machine learning models at serving time need features in sub-millisecond latency (Redis), computed by pipelines that read raw events (Kafka), and registered in a central catalog with offline data for training (PostgreSQL). This three-way integration is the backbone of real-time ML inference in production.

```
Raw events → Kafka (feature pipeline input)
                 ↓ stream processor (Flink / Kafka Streams)
             Feature computation
                 ├─ Redis (online store: sub-ms serving)
                 └─ PostgreSQL (offline store: training data + feature registry)

Inference request → Model server
                         ↓ feature lookup
                     Redis (online features, <1ms)
                         ↓ join with request features
                     Model inference → response
```

**PostgreSQL: feature registry and offline store:**
```sql
CREATE TABLE feature_definitions (
  name            TEXT PRIMARY KEY,
  entity_type     TEXT NOT NULL,          -- 'user', 'product', 'session'
  description     TEXT,
  dtype           TEXT NOT NULL,          -- 'float', 'int', 'string', 'vector'
  ttl_seconds     INT,                    -- how long Redis should cache
  pipeline        TEXT,                   -- name of Kafka Streams topology that computes it
  created_at      TIMESTAMPTZ DEFAULT now()
);

-- Offline store: time-stamped feature values for training dataset generation
CREATE TABLE feature_values_offline (
  entity_type TEXT NOT NULL,
  entity_id   TEXT NOT NULL,
  feature     TEXT NOT NULL REFERENCES feature_definitions(name),
  value       JSONB NOT NULL,
  event_time  TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (entity_type, entity_id, feature, event_time)
);
```

**Kafka Streams feature pipeline:**
```java
StreamsBuilder builder = new StreamsBuilder();

// 30-day rolling purchase count per user
KStream<String, PurchaseEvent> purchases = builder.stream("purchases");

purchases
    .groupByKey()
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(30)))
    .count()
    .toStream()
    .foreach((windowedKey, count) -> {
        String userId = windowedKey.key();

        // Write to Redis online store
        jedis.setex("feat:user:" + userId + ":purchase_count_30d",
                    86400 * 7,         // TTL: 7 days
                    count.toString());

        // Write to PostgreSQL offline store for training
        db.execute("INSERT INTO feature_values_offline VALUES (?, ?, ?, ?, ?)",
                   "user", userId, "purchase_count_30d", count, Instant.now());
    });
```

**Redis online store layout:**

```
feat:{entity_type}:{entity_id}:{feature_name}  →  scalar value
feat:{entity_type}:{entity_id}:__vec__          →  binary-encoded embedding vector
feat:{entity_type}:{entity_id}:__meta__         →  hash of feature timestamps
```

**Batch feature retrieval at inference time:**
```python
def get_features(entity_type: str, entity_id: str, features: list[str]) -> dict:
    pipe = redis.pipeline()
    for feat in features:
        pipe.get(f"feat:{entity_type}:{entity_id}:{feat}")
    values = pipe.execute()

    result = {}
    for feat, val in zip(features, values):
        if val is None:
            # Fall back to PostgreSQL for cold entities
            val = fetch_from_offline_store(entity_type, entity_id, feat)
            if val:
                redis.setex(f"feat:{entity_type}:{entity_id}:{feat}", 3600, val)
        result[feat] = val
    return result
```

**Point-in-time correct training data:** When generating training datasets, query the offline PostgreSQL store with `AS OF` semantics — the feature value that was available at the time of the label event, not the current value:

```sql
SELECT
    l.entity_id,
    l.label,
    l.occurred_at,
    f.value AS purchase_count_30d
FROM training_labels l
JOIN LATERAL (
    SELECT value FROM feature_values_offline
    WHERE entity_type = 'user'
      AND entity_id    = l.entity_id
      AND feature      = 'purchase_count_30d'
      AND event_time  <= l.occurred_at          -- point-in-time correct
    ORDER BY event_time DESC LIMIT 1
) f ON true;
```

**Feature freshness monitoring via Kafka:**

Emit a metadata event every time a feature is written to Redis. A consumer tracks the lag between `event_time` and `write_time` per feature and publishes staleness alerts:

```python
kafka_producer.send("feature-freshness", {
    "feature": "purchase_count_30d",
    "entity_id": user_id,
    "event_time": event_time.isoformat(),
    "write_time": datetime.utcnow().isoformat(),
    "lag_ms": (datetime.utcnow() - event_time).total_seconds() * 1000,
})
```

---

The golden triangle works because the three technologies address orthogonal failure modes:

- **PostgreSQL** handles the hard problem of consistency — ACID transactions, foreign key integrity, complex joins.
- **Kafka** handles the hard problem of coupling — services communicate through durable events, decoupled in time and deployment.
- **Redis** handles the hard problem of latency — sub-millisecond data access, atomic counters, and coordination primitives.

The canonical integration flow is:

```
User action → Application
    ↓
PostgreSQL (authoritative write + outbox)
    ↓ CDC (Debezium)
Kafka (event backbone)
    ↓ consumers
Redis (low-latency read model + ephemeral state)
    ↑
User queries
```

Master the outbox pattern (PostgreSQL → Kafka), CDC-driven cache invalidation (Kafka → Redis), and the CQRS read model (Redis serving queries), and you have a solid foundation for any high-scale backend system.
