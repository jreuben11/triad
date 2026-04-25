# Triad: Conceptual Design for a Unified Golden-Triangle Integration System

Triad is a **library + runner** that implements every integration pattern across PostgreSQL, Kafka, and Redis as first-class, composable, operationally observable primitives — so teams stop hand-rolling outbox relays, CDC consumers, saga state machines, and cache-warming scripts, and start declaring what they need.

---

## 1. Design Goals

| Goal | Description |
|------|-------------|
| **Pattern completeness** | Every pattern in the golden-triangle doc is a named, tested, pre-built module |
| **Declarative first** | Common patterns configured in YAML; imperative SDK for custom logic |
| **Exactly-once by default** | Outbox + Inbox + Kafka transactions + Redis NX composed automatically |
| **Operationally observable** | Every pipeline emits structured metrics, traces, and lag alerts |
| **Graceful degradation** | Circuit breakers and fallback paths for each backend independently |
| **Multi-region aware** | Replication topology is a first-class concept, not an afterthought |
| **Extension points** | Custom sources, sinks, transforms, and pattern implementations plug in |

**Non-goals:** Triad does not replace Kafka, PostgreSQL, or Redis. It does not manage broker/cluster infrastructure. It is not an ORM, a migration tool, or a query builder.

---

## 2. System Context

```mermaid
C4Context
    title Triad — System Context

    Person(dev, "Application Developer", "Writes business logic using the Triad SDK")
    Person(ops, "Platform Engineer", "Configures pipelines, monitors health")

    System(triad, "Triad", "Library + Runner implementing all golden-triangle integration patterns")

    System_Ext(pg, "PostgreSQL", "Source of truth, event store, outbox, offline feature store")
    System_Ext(kafka, "Apache Kafka", "Durable event backbone, replay log")
    System_Ext(redis, "Redis", "Low-latency read model, state store, rate limiter, online feature store")
    System_Ext(app, "Application Services", "Microservices / APIs consuming Triad SDK")
    System_Ext(obs, "Observability Stack", "Prometheus, OpenTelemetry, Grafana")

    Rel(dev, app, "writes business logic with")
    Rel(dev, triad, "configures patterns via YAML + SDK")
    Rel(ops, triad, "monitors via admin API + dashboards")
    Rel(triad, pg, "WAL replication, transactional writes, offset tracking")
    Rel(triad, kafka, "produce/consume, transactions, MirrorMaker topology")
    Rel(triad, redis, "cache ops, streams, pub/sub, state store")
    Rel(app, triad, "SDK calls: publish, enrich, flag check, feature lookup")
    Rel(triad, obs, "metrics, traces, lag alerts")
```

---

## 3. Architecture Layers

```mermaid
block-beta
    columns 1

    block:APP["Application Layer"]:1
        A1["Domain Models"]
        A2["Business Logic Handlers"]
        A3["REST / gRPC APIs"]
    end

    block:SDK["Triad Library (SDK)"]:1
        S1["Aggregate API"]
        S2["Pipeline DSL"]
        S3["Pattern Facades\n(Saga, Cache, Flag, Webhook, FeatureStore)"]
        S4["Idempotency & EOS Helpers"]
    end

    block:RUNNER["Triad Runner (Daemon)"]:1
        block:ENGINE["Pattern Engine"]:1
            E1["CDC Module"]
            E2["Outbox/Inbox Relay"]
            E3["Saga Orchestrator"]
            E4["Cache Sync"]
            E5["Stream Enricher"]
            E6["Exactly-Once Coordinator"]
            E7["Webhook Dispatcher"]
            E8["Feature Store Pipeline"]
            E9["Cold Start Manager"]
            E10["Flag Distributor"]
        end
        block:CORE["Core Services"]:1
            C1["Registry"]
            C2["Internal Event Router"]
            C3["Scheduler"]
            C4["Health Monitor"]
            C5["Backpressure Controller"]
        end
    end

    block:CONN["Connection Manager"]:1
        CM1["PG Pool\n(primary + replicas)"]
        CM2["Kafka Client\n(producer + consumer groups)"]
        CM3["Redis Client\n(Cluster / Sentinel)"]
        CM4["Circuit Breakers"]
        CM5["Retry Budgets"]
    end

    block:BACKENDS["Backend Adapters"]:1
        B1["PostgreSQL\n(WAL, JDBC, COPY)"]
        B2["Kafka\n(Connect, Streams, Transactions)"]
        B3["Redis\n(Strings, Hashes, ZSets, Streams, Pub/Sub)"]
    end

    APP --> SDK
    SDK --> RUNNER
    RUNNER --> CONN
    CONN --> BACKENDS
```

---

## 4. Core Abstractions

### 4.1 The Five Primitives

Everything in Triad composes from five primitives:

```mermaid
classDiagram
    class Source {
        +String id
        +SourceType type
        +read() EventStream
        +checkpoint() Offset
    }
    class Sink {
        +String id
        +SinkType type
        +write(event Event)
        +flush()
    }
    class Transform {
        +String id
        +apply(Event) Event
        +filter(Event) bool
    }
    class Pipeline {
        +String name
        +Source source
        +List~Transform~ transforms
        +Sink sink
        +DeliveryGuarantee guarantee
        +start()
        +pause()
        +drain()
    }
    class Pattern {
        +String name
        +PatternType type
        +List~Pipeline~ pipelines
        +Map~String,Config~ config
        +deploy(Runner)
    }

    Pipeline "1" --> "1" Source
    Pipeline "1" --> "1" Sink
    Pipeline "1" --> "0..*" Transform
    Pattern "1" --> "1..*" Pipeline

    class SourceType {
        <<enumeration>>
        PG_WAL
        PG_TABLE_POLL
        KAFKA_TOPIC
        REDIS_STREAM
        HTTP_ENDPOINT
    }
    class SinkType {
        <<enumeration>>
        PG_TABLE
        KAFKA_TOPIC
        REDIS_KEY
        REDIS_STREAM
        HTTP_WEBHOOK
    }
    class DeliveryGuarantee {
        <<enumeration>>
        AT_MOST_ONCE
        AT_LEAST_ONCE
        EXACTLY_ONCE
    }
```

### 4.2 Registry

The Registry is the single source of truth for all named components at runtime. Every Pattern, Pipeline, Aggregate type, Saga definition, and Feature Flag schema registers here on startup.

```mermaid
classDiagram
    class Registry {
        +register(Pattern)
        +register(AggregateType)
        +register(SagaDefinition)
        +register(FeatureFlagSchema)
        +lookup(name) Pattern
        +listAll() List~Pattern~
        +healthOf(name) Health
    }
    class AggregateType {
        +String name
        +String eventStoreTable
        +List~CommandHandler~ commands
        +List~EventHandler~ events
        +SnapshotPolicy snapshotPolicy
    }
    class SagaDefinition {
        +String name
        +List~SagaStep~ steps
        +Map~String,Compensation~ compensations
        +Duration timeout
        +RetryPolicy retryPolicy
    }
    class FeatureFlagSchema {
        +String flagName
        +String pgTable
        +String redisKeyPattern
        +RolloutStrategy rollout
    }
    Registry "1" --> "0..*" Pattern
    Registry "1" --> "0..*" AggregateType
    Registry "1" --> "0..*" SagaDefinition
    Registry "1" --> "0..*" FeatureFlagSchema
```

### 4.3 Internal Event Router

The Router decouples pattern modules from each other. When the CDC module emits a change event, it doesn't know whether the Cache Sync module, the Saga module, or a user handler will consume it — the Router decides, based on subscriptions registered at startup.

```mermaid
flowchart LR
    CDC[CDC Module] -->|ChangeEvent| Router
    Outbox[Outbox Relay] -->|OutboxEvent| Router
    KafkaIn[Kafka Consumer] -->|DomainEvent| Router
    Router -->|route by type + topic| CacheSync[Cache Sync Module]
    Router -->|route by type + topic| SagaOrch[Saga Orchestrator]
    Router -->|route by type + topic| UserHandler[User Handler\nregistered via SDK]
    Router -->|route by type + topic| FeatureStore[Feature Store Pipeline]
    Router -->|route by type + topic| WebhookDisp[Webhook Dispatcher]
```

---

## 5. The Library (SDK)

The SDK is what application developers import. It provides high-level APIs that compile down to Pipeline + Pattern registrations with the Runner.

### 5.1 Aggregate and Event Sourcing API

```
AggregateRoot
    .command(handler)        → handles command, calls apply(event)
    .apply(event)            → writes event to PG event store (§1.4)
    .on(event, handler)      → reconstitutes state from event history
    .snapshot(policy)        → configures automatic snapshotting
    .publishTo(topic)        → CDC publishes events to Kafka after commit
```

### 5.2 Pipeline DSL

```
Triad.pipeline("name")
    .from(Source)            → define source (WAL, Kafka topic, Redis stream)
    .transform(fn)           → optional stateless transform
    .enrich(RedisLookup)     → enrich from Redis (§3.1) with PG fallback
    .filter(predicate)
    .to(Sink)                → define sink
    .withGuarantee(EOS)      → wrap in Kafka transaction + PG offset commit
    .withRetry(policy)
    .withDLQ(topic)          → dead-letter queue (§3.5)
    .build()
```

### 5.3 Pattern Facades

High-level sugar over the DSL for the named patterns:

```
Triad.outbox(table)          → Outbox pattern (§1.2) — returns OutboxPublisher
Triad.inbox(topic)           → Inbox pattern (§9.1) — returns InboxConsumer
Triad.saga("name")           → Saga builder (§4.2, §9.2)
Triad.cache("name")          → Cache strategy selector → CacheAside | WriteThrough | WriteBehind
Triad.featureFlag("name")    → Flag evaluator backed by Redis (§9.6)
Triad.webhook("name")        → Webhook dispatcher builder (§9.8)
Triad.featureStore("name")   → ML feature store builder (§9.9)
Triad.idempotent(key)        → Idempotency key wrapper (§9.4)
Triad.rateLimit("name")      → Rate limiter (§3.2)
```

### 5.4 Configuration vs Code

```mermaid
flowchart TD
    Config[triad.yaml\ndeclarative config]
    Code[SDK API calls\nimperative code]
    Loader[Config Loader]
    Builder[Pattern Builder]
    Registry[Registry]

    Config --> Loader --> Registry
    Code --> Builder --> Registry

    note1["YAML: standard patterns\nwith no custom logic"]
    note2["Code: custom transforms,\nbusiness rules, complex sagas"]

    Config -.-> note1
    Code -.-> note2
```

---

## 6. The Runner (Daemon)

The Runner is a long-lived process — deployed as a sidecar, a standalone service, or embedded in the application process. It owns the lifecycle of all Pipelines and Pattern modules.

### 6.1 Runner Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Initialising

    Initialising --> LoadingConfig : parse triad.yaml
    LoadingConfig --> ConnectingBackends : config valid
    LoadingConfig --> Failed : config invalid

    ConnectingBackends --> ColdStart : all backends healthy
    ConnectingBackends --> DegradedStart : partial backend availability

    ColdStart --> WarmingRedis : Strategy A — PG snapshot\nor Strategy B — Kafka replay\nor Strategy C — dual-read
    WarmingRedis --> Running : warm-up complete or threshold reached

    DegradedStart --> Running : start with available backends\n(graceful degradation §5.3)

    Running --> Draining : SIGTERM / graceful shutdown
    Running --> Paused : admin pause command
    Running --> Recovering : backend connection lost
    Recovering --> Running : reconnected + lag caught up
    Paused --> Running : admin resume command

    Draining --> [*] : all in-flight messages committed

    Failed --> [*]
```

### 6.2 Pattern Module Lifecycle

Each Pattern module runs as an independent tokio async task supervised by the Runner via `JoinSet`. Failure of one module does not crash others.

```mermaid
flowchart TD
    Runner[Runner Supervisor\nJoinSet]
    Runner --> CDC_M[CDC Module\ntokio task]
    Runner --> Outbox_M[Outbox Relay\ntokio task]
    Runner --> Inbox_M[Inbox Consumer\ntokio task]
    Runner --> Saga_M[Saga Orchestrator\ntokio task]
    Runner --> Cache_M[Cache Sync\ntokio task]
    Runner --> Flag_M[Flag Distributor\ntokio task]
    Runner --> Webhook_M[Webhook Dispatcher\ntask pool]
    Runner --> FeatureStore_M[Feature Store Pipeline\ntokio task]

    CDC_M -->|crash| Runner
    Runner -->|restart with backoff| CDC_M

    style Runner fill:#2d6a9f,color:#fff
```

### 6.3 Backpressure Controller

Monitors lag across all pipelines and applies backpressure when any backend is saturated.

```mermaid
flowchart LR
    Monitor[Lag Monitor\npoll every 5s]

    Monitor -->|Kafka consumer lag| KafkaBP[Kafka backpressure:\nreduce fetch.max.bytes\nslow poll interval]
    Monitor -->|Redis memory %| RedisBP["Redis backpressure:\nconsumer.Pause(partitions)\npause write-behind flush\nstop new cache writes"]
    Monitor -->|PG connection pool| PGBP[PG backpressure:\nqueue writes\nbatch smaller COPY blocks]

    KafkaBP --> Signal[Signal to\nproducing services\nvia Redis flag]
    RedisBP --> Signal
    PGBP --> Signal

    RedisBP -->|memory clears| Resume["consumer.Resume(partitions)\nresume normal poll rate"]

    Signal -->|HIGH_WATERMARK key| AppLayer[Application\nsheds new requests]
```

---

## 7. Pattern Modules

### 7.1 CDC Module

```mermaid
sequenceDiagram
    participant PG as PostgreSQL WAL
    participant CDC as CDC Module
    participant Schema as Schema Registry
    participant Router as Internal Router
    participant Kafka as Kafka

    PG->>CDC: WAL change event (LSN, table, op, before, after)
    CDC->>Schema: resolve schema for table version
    Schema-->>CDC: Avro/Protobuf schema
    CDC->>CDC: encode event
    CDC->>Kafka: produce(topic, key=PK, value=encoded_event)
    Kafka-->>CDC: ack (offset)
    CDC->>PG: advance replication slot LSN
    CDC->>Router: emit ChangeEvent internally (for Cache Sync, Saga, etc.)
```

### 7.2 Outbox + Inbox Pipeline (End-to-End Exactly-Once)

```mermaid
sequenceDiagram
    participant App as Application
    participant PG_A as PostgreSQL (Service A)
    participant Outbox as Outbox Relay
    participant Kafka as Kafka
    participant Inbox as Inbox Consumer
    participant Redis as Redis
    participant PG_B as PostgreSQL (Service B)

    App->>PG_A: BEGIN; UPDATE orders; INSERT outbox; COMMIT
    Outbox->>PG_A: SELECT unpublished FROM outbox (CDC or poll)
    Outbox->>Kafka: initTransaction(); produce(event); commitTransaction()
    Kafka-->>Outbox: committed
    Outbox->>PG_A: UPDATE outbox SET published_at = now()

    Kafka->>Inbox: poll(isolation.level=read_committed)
    Inbox->>Redis: SET NX "inbox:{event_id}" EX 86400
    alt Redis NX = acquired (first delivery)
        Inbox->>PG_B: BEGIN; apply business logic; INSERT inbox(event_id); COMMIT
        Inbox->>Kafka: commitOffset (manual)
    else Redis NX = rejected (duplicate)
        Inbox->>Kafka: commitOffset (skip)
    end
```

### 7.3 Saga Orchestrator

```mermaid
stateDiagram-v2
    [*] --> Started : SagaStarted event → Redis HMSET saga:{id}

    Started --> Step1_Pending : send command to Step 1 via Kafka
    Step1_Pending --> Step1_Complete : reply event received
    Step1_Pending --> Compensating : timeout or failure

    Step1_Complete --> Step2_Pending : send command to Step 2 via Kafka
    Step2_Pending --> Step2_Complete : reply event received
    Step2_Pending --> Compensating : timeout or failure

    Step2_Complete --> StepN_Pending : ... (N steps)
    StepN_Pending --> Completed : all steps complete

    Compensating --> Comp1_Pending : send compensation for Step N-1
    Comp1_Pending --> Comp2_Pending : compensation ack
    Comp2_Pending --> RolledBack : all compensations applied

    Completed --> [*] : write final state to PG; delete Redis saga key
    RolledBack --> [*] : write final state to PG; delete Redis saga key

    note right of Started : Redis holds in-flight saga state\nPG holds the durable record
    note right of Compensating : Kafka carries compensating commands\nto each participant service
```

**Saga step idempotency — required contract:** Every saga step handler registered with the SDK **must** be idempotent. The Saga Orchestrator re-executes the current step on restart (resuming from `triad_saga_checkpoints`), so a step may run more than once with the same inputs. Triad provides a `StepContext.IdempotencyKey()` derived from `(saga_id, step_name, attempt)` that step implementations must use for their own dedup:

```go
func ReserveInventory(ctx saga.StepContext, cmd ReserveCmd) error {
    key := ctx.IdempotencyKey()   // stable across retries
    return inventoryClient.Reserve(ctx, key, cmd.ItemID, cmd.Qty)
}
```

Step handlers that are not idempotent produce incorrect state on crash-recovery and are a design contract violation, not a bug Triad can detect automatically.

### 7.4 Cache Sync Module

```mermaid
flowchart TD
    WAL[PostgreSQL WAL] --> CDC_M[CDC Module]
    CDC_M --> Router[Internal Router]

    Router -->|ChangeEvent for table=users| CacheSync[Cache Sync Module]

    CacheSync --> DecideOp{operation?}
    DecideOp -->|INSERT or UPDATE| WriteRedis["Redis SET / HSET\nuser:{id} → serialised row\nwith configured TTL"]
    DecideOp -->|DELETE| DeleteRedis["Redis DEL\nuser:{id}"]
    DecideOp -->|schema change| Flush["Scan + DEL all\nuser:* keys\n(SCAN cursor, not KEYS)"]

    WriteRedis --> Metrics[emit cache_sync_lag_ms\ncache_writes_total]
    DeleteRedis --> Metrics
    Flush --> Metrics
```

### 7.5 Exactly-Once Coordinator

Wraps any Pipeline with the three-layer EOS guarantee described in §9.2.

**Redis unavailability:** When the Redis circuit breaker is open, the fast-path NX check falls through to the `triad_inbox` table in PostgreSQL. In this fallback path, the inbox row INSERT **must** occur inside the same PG transaction as the business-logic side effects — not as a separate statement — to preserve atomicity. The EOS coordinator enforces this: when Redis is unavailable it opens a PG transaction, runs business logic, inserts the inbox row, and commits; Kafka offset commit follows via `sendOffsetsToTransaction`. An implementation that defers the inbox INSERT to after the PG commit breaks the exactly-once guarantee under Redis failures.

```mermaid
flowchart TD
    Event[Incoming event\nfrom Kafka] --> RedisFast{Redis NX\ncheck}

    RedisFast -->|key exists: duplicate| Skip[Commit offset\nno-op]
    RedisFast -->|key absent: new| Txn["Begin Kafka\nproducer transaction\ninitTransactions()"]

    Txn --> AppLogic[Run user transform / business logic]
    AppLogic --> MultiWrite["Produce to N output topics\natomically in same transaction"]
    MultiWrite --> OffsetCommit["sendOffsetsToTransaction()\ncommit consumer offset\ninside same txn"]
    OffsetCommit --> KafkaCommit["commitTransaction()"]

    KafkaCommit --> PGCommit["BEGIN PG txn\napply side effects\nINSERT kafka_offsets(partition, offset)\nCOMMIT"]

    PGCommit --> Success[Done]

    AppLogic -->|exception| Abort["abortTransaction()\nRedis DEL idempotency key"]
    Abort --> Retry[Re-queue / DLQ]

    style RedisFast fill:#d93,color:#fff
    style KafkaCommit fill:#1a6,color:#fff
    style PGCommit fill:#259,color:#fff
```

---

## 8. Key Cross-Cutting Flows

### 8.1 Request → Outbox → Kafka → Cache Invalidation → Read

The canonical golden-triangle request lifecycle, showing all three legs working together:

```mermaid
sequenceDiagram
    participant Client
    participant API as API Service
    participant SDK as Triad SDK
    participant PG as PostgreSQL
    participant Relay as Outbox Relay
    participant Kafka as Kafka
    participant CacheSync as Cache Sync Module
    participant Redis as Redis

    Client->>API: POST /orders
    API->>SDK: Triad.idempotent(key).run(fn)
    SDK->>Redis: SET NX "idem:{key}" EX 86400
    Redis-->>SDK: OK (first request)
    SDK->>PG: BEGIN; INSERT orders; INSERT outbox; COMMIT
    SDK->>Redis: SET "idem:{key}" = serialised_response
    API-->>Client: 201 Created

    Relay->>PG: (CDC) detect outbox row
    Relay->>Kafka: produce(domain.events, OrderCreated)

    CacheSync->>Kafka: consume(db.public.orders)
    CacheSync->>Redis: HSET "order:{id}" ... / DEL "order:{id}"

    Client->>API: GET /orders/{id}
    API->>Redis: GET "order:{id}"
    Redis-->>API: cache hit → return immediately
```

### 8.2 Cold Start Sequence

```mermaid
sequenceDiagram
    participant Runner
    participant Config as Config Loader
    participant PG as PostgreSQL
    participant Kafka as Kafka
    participant Redis as Redis
    participant Health as Health Monitor

    Runner->>Config: parse triad.yaml
    Config-->>Runner: pattern definitions

    par Connect all backends
        Runner->>PG: check connection + WAL level
        Runner->>Kafka: check broker connectivity + list topics
        Runner->>Redis: PING
    end

    Runner->>Health: assess backend availability
    Health-->>Runner: all healthy → full start / partial → degraded start

    alt Strategy A: PG Snapshot warm-up
        Runner->>PG: SELECT * FROM users ORDER BY id (batched)
        PG-->>Runner: rows
        Runner->>Redis: PIPELINE SETEX user:{id} 3600 ... (500 per batch)
    else Strategy B: Kafka Replay
        Runner->>Kafka: assign partitions, seek to earliest
        loop until high watermark
            Kafka-->>Runner: events (batch)
            Runner->>Redis: apply events to read model
        end
    else Strategy C: Dual-read (lazy warm)
        Runner->>PG: snapshot base state (batched SELECT)
        Runner->>Kafka: replay events with offset > snapshot LSN watermark
        Note over Runner: Kafka events newer than snapshot overwrite PG base state.\nConflict rule: Kafka offset > snapshot LSN wins (Kafka is more recent).
        Runner->>Runner: open traffic immediately\nresidual cache misses populate Redis on first read
    end

    Runner->>Runner: spawn all Pattern module tokio tasks
    Runner->>Health: begin continuous health polling loop
    Runner-->>Runner: RUNNING
```

### 8.3 Multi-Region Failover

```mermaid
flowchart TD
    subgraph RegionA["Region A (Primary)"]
        PG_A[PostgreSQL Primary]
        Kafka_A[Kafka Cluster A]
        Redis_A[Redis Primary]
        Runner_A[Triad Runner A]
    end

    subgraph Replication["Replication Layer (managed by Triad)"]
        PG_Rep["PG Logical Replication\nCREATE SUBSCRIPTION"]
        MM2["Kafka MirrorMaker 2\n(topic + offset sync)"]
        Redis_Rep["Redis Replication\n(replica-of) or\nKafka-bridged active-active"]
    end

    subgraph RegionB["Region B (Standby / Active)"]
        PG_B[PostgreSQL Replica]
        Kafka_B[Kafka Cluster B]
        Redis_B[Redis Replica]
        Runner_B[Triad Runner B\n(standby mode)]
    end

    PG_A --> PG_Rep --> PG_B
    Kafka_A --> MM2 --> Kafka_B
    Redis_A --> Redis_Rep --> Redis_B

    Runner_A -->|heartbeat to| Runner_B
    Runner_B -->|failover trigger\nDNS cut / leader election| Runner_B

    note1["On failover:\n1. Promote PG_B to primary\n2. Runner B enters RUNNING state\n3. Kafka consumers re-point to Cluster B\n   (MM2 offset mapping preserves position)\n4. Redis_B promoted to primary"]

    style RegionA fill:#1a3a5c,color:#fff
    style RegionB fill:#2d6a2d,color:#fff
    style Replication fill:#5c4000,color:#fff
```

---

## 9. Configuration Model

A single `triad.yaml` manifest declares everything. The Runner parses it at startup and reconciles with what is currently deployed.

```yaml
triad:
  version: "1"

  # ── Backend connections ────────────────────────────────────────
  backends:
    postgres:
      primary:   "postgresql://user:pass@pg-primary:5432/mydb"
      replicas:  ["postgresql://user:pass@pg-replica:5432/mydb"]
      pool_size: 20
      wal_level: logical           # verified at startup
    kafka:
      brokers:         ["kafka-1:9092", "kafka-2:9092"]
      schema_registry: "http://schema-registry:8081"
      producer:
        transactional_id_prefix: "triad"
        enable_idempotence: true
      consumer:
        isolation_level: read_committed
        auto_offset_reset: earliest
    redis:
      url:  "redis://redis:6379"
      mode: cluster                 # cluster | sentinel | standalone

  # ── Multi-region (optional) ────────────────────────────────────
  regions:
    primary: region-a
    replicas:
      - id: region-b
        postgres_replica: "postgresql://user:pass@pg-b:5432/mydb"
        kafka_mirrormaker:
          source_cluster: region-a
          target_cluster: region-b
          topics: ".*"
          sync_offsets: true
        redis_replica: "redis://redis-b:6379"

  # ── Pattern declarations ───────────────────────────────────────
  patterns:

    # CDC: stream every table change to Kafka
    - name: orders_cdc
      type: cdc
      source_table: orders
      target_topic: "db.public.orders"
      snapshot_mode: initial        # backfill on first run

    # Outbox: guaranteed domain event publishing
    - name: domain_outbox
      type: outbox
      table: outbox
      relay: cdc                    # cdc | poll
      target_topic: "domain.events"

    # Inbox: exactly-once consumer on the receiving service
    - name: fulfillment_inbox
      type: inbox
      source_topic: "domain.events"
      dedup_backend: redis          # redis | postgres | both
      dedup_ttl: 86400
      handler: "com.example.FulfillmentHandler"

    # Cache sync: CDC → Redis invalidation/population
    - name: user_cache_sync
      type: cache_sync
      source_topic: "db.public.users"
      redis_key_pattern: "user:{id}"
      ttl: 300
      on_delete: del                # del | tombstone

    # Saga: multi-step distributed transaction
    - name: order_fulfillment_saga
      type: saga
      trigger_topic: "domain.events"
      trigger_event: "OrderPlaced"
      state_backend: redis          # redis for in-flight; postgres for durable record
      timeout: "10m"
      steps:
        - name: reserve_inventory
          command_topic: "inventory.commands"
          reply_topic:   "inventory.replies"
          compensation:  release_inventory
        - name: charge_payment
          command_topic: "payment.commands"
          reply_topic:   "payment.replies"
          compensation:  refund_payment
        - name: confirm_order
          command_topic: "order.commands"
          reply_topic:   "order.replies"

    # Feature flags: PG config + Kafka propagation + Redis hot path
    - name: feature_flags
      type: feature_flag
      table: feature_flags
      audit_table: flag_audit
      redis_key_pattern: "flag:{name}"
      propagation: cdc              # cdc | poll (interval: 30s)

    # Webhook delivery: fan-out with circuit breaker + retry
    - name: order_webhooks
      type: webhook
      source_topic: "domain.events"
      subscription_table: webhook_subscriptions
      delivery_log_table: webhook_deliveries
      signing: hmac-sha256
      retry:
        max_attempts: 10
        backoff: exponential
        max_delay: "1h"
      circuit_breaker:
        threshold: 5                # open after 5 consecutive failures
        half_open_after: "5m"

    # Rate limiter: Redis sliding window
    - name: api_rate_limit
      type: rate_limit
      algorithm: sliding_window
      redis_key_pattern: "rate:{user_id}:{endpoint}"
      window: "60s"
      limit: 1000
      violation_topic: "rate-limit-violations"

    # ML feature store — Triad is the serving + sync layer only.
    # Feature computation (aggregations, ML pipelines) runs externally (Kafka Streams, Flink, dbt).
    # Triad reads computed values from an external Kafka topic or PG table and syncs them to Redis.
    - name: user_features
      type: feature_store
      entity: user
      features:
        - name: purchase_count_30d
          source:
            type: external_topic          # external_topic | pg_table
            topic: "features.user.computed"   # written by external compute pipeline
          redis_key_pattern: "feat:user:{id}:purchase_count_30d"
          ttl: 604800               # 7 days
          offline_table: feature_values_offline   # PG fallback for Redis miss
      registry_table: feature_definitions

    # HTTP idempotency
    - name: idempotency
      type: idempotency
      redis_key_pattern: "idem:{key}"
      ttl: 86400
      pg_backup: true               # also write to idempotency_keys table for durability

  # ── Cold start strategy ────────────────────────────────────────
  cold_start:
    default_strategy: dual_read     # pg_snapshot | kafka_replay | dual_read
    overrides:
      user_cache_sync: pg_snapshot
      order_cqrs_view: kafka_replay

  # ── Exactly-once defaults ──────────────────────────────────────
  delivery:
    default_guarantee: exactly_once
    kafka_transaction_timeout: "60s"
    idempotency_dedup_window: "24h"

  # ── Observability ──────────────────────────────────────────────
  observability:
    metrics:
      provider: prometheus
      port: 9090
      labels: [region, pattern_name, pipeline_name]
    tracing:
      provider: opentelemetry
      endpoint: "http://otel-collector:4317"
      sample_rate: 0.1
    alerts:
      kafka_lag_threshold: 10000
      pg_replication_lag_threshold: "30s"
      redis_memory_threshold: 0.85

  # ── Admin API ─────────────────────────────────────────────────
  admin:
    port: 8080
    endpoints:
      - /health/live        # liveness probe
      - /health/ready       # readiness probe (503 during drain / cold start)
      - /health/started     # startup probe (503 until init complete)
      - /patterns          # list all registered patterns + status
      - /patterns/{name}/pause
      - /patterns/{name}/resume
      - /patterns/{name}/replay    # trigger cold-start replay for a pattern
      - /lag               # current consumer lag per pipeline
      - /registry          # dump full registry
```

---

## 10. Observability Model

Every pattern module emits a consistent set of signals. The same label schema applies across all three backends so dashboards compose naturally.

```mermaid
flowchart TD
    subgraph Signals["Signal Types"]
        M[Metrics\nPrometheus counters\nhistograms, gauges]
        T[Traces\nOpenTelemetry spans\nacross PG + Kafka + Redis]
        L[Logs\nStructured JSON\nwith trace_id correlation]
        A[Alerts\nlag thresholds\nerror rates\ncircuit breaker state]
    end

    subgraph Key_Metrics["Key Metrics per Pattern"]
        M1["triad_pipeline_events_total{pattern, pipeline, status}"]
        M2["triad_pipeline_lag_seconds{pattern, pipeline}"]
        M3["triad_cache_hit_ratio{pattern}"]
        M4["triad_saga_active_total{saga_name}"]
        M5["triad_saga_step_duration_seconds{saga_name, step}"]
        M6["triad_webhook_delivery_attempts{subscription_id, status}"]
        M7["triad_feature_flag_evaluations_total{flag_name, result}"]
        M8["triad_idempotency_dedup_total{pattern, outcome}"]
        M9["triad_cold_start_duration_seconds{strategy}"]
        M10["triad_eos_txn_total{pipeline_name, outcome}"]
    end

    Signals --> Grafana[Grafana Dashboards]
    Key_Metrics --> Grafana
    A --> PagerDuty[PagerDuty / Alertmanager]
```

### Trace Propagation Across Backends

A single trace spans all three backends for end-to-end visibility:

```mermaid
gantt
    title Example trace: POST /orders → Redis read model updated
    dateFormat x
    axisFormat %Lms

    section API Layer
    HTTP handler + idempotency check     :a1, 0, 5
    section PostgreSQL
    BEGIN + INSERT orders + INSERT outbox :a2, 5, 20
    section Outbox Relay
    detect outbox row (CDC)              :a3, 20, 25
    Kafka produce + commitTransaction     :a4, 25, 35
    section Cache Sync
    Kafka consume change event           :a5, 35, 42
    Redis HSET user cache                :a6, 42, 46
```

---

## 11. Extension Points

Triad is built on a plugin interface so custom patterns, sources, sinks, and transforms can be added without forking the core.

```mermaid
classDiagram
    class SourcePlugin {
        <<interface>>
        +String id()
        +EventStream open(Config)
        +Offset checkpoint()
        +void seekTo(Offset)
    }
    class SinkPlugin {
        <<interface>>
        +String id()
        +void write(Event, Config)
        +void flush()
    }
    class TransformPlugin {
        <<interface>>
        +String id()
        +Event apply(Event, Context)
    }
    class PatternPlugin {
        <<interface>>
        +String type()
        +List~Pipeline~ build(Config, Registry)
        +void onStart()
        +void onStop()
    }

    class BuiltinCDCPattern {
        +type() "cdc"
        +build() : WAL source → Kafka sink
    }
    class BuiltinOutboxPattern {
        +type() "outbox"
        +build() : PG poll/CDC source → Kafka sink
    }
    class CustomElasticsearchSink {
        +id() "elasticsearch"
        +write(Event, Config)
    }

    PatternPlugin <|-- BuiltinCDCPattern
    PatternPlugin <|-- BuiltinOutboxPattern
    SinkPlugin <|-- CustomElasticsearchSink
```

Custom plugins are loaded at runtime from a configured plugin directory. The admin API lists all registered plugins and their status.

---

## 12. Deployment Modes

Triad supports three first-class deployment modes. The mode is selected at startup and governs process topology, lifecycle management, leader election, and health signalling.

### 12.0 Mode Comparison

| Feature | Mode 1 — In-Process SDK | Mode 2 — Standalone Binary | Mode 3 — Kubernetes Fleet |
|---------|------------------------|---------------------------|--------------------------|
| Process boundary | Same process as app | Separate `triad` process | `triad-worker` Deployment |
| Invocation | `Triad.Start(ctx, cfg)` | `triad server` | K8s Deployment controller |
| Lifecycle owner | Application | systemd / OS | K8s Deployment + PDB |
| Leader election | Single instance (no election) | File lock / single process | `coordination.k8s.io/v1 Lease` |
| Restart on crash | App's own restart logic | `systemd Restart=on-failure` | K8s pod restart policy |
| Horizontal scale | No (tokio tasks scale vertically) | No (single process) | Yes — HPA on Kafka lag |
| Admin API | Embedded on app port | `localhost:8080` (default) | ClusterIP service |
| Config source | `triad.yaml` / env vars | `triad.yaml` / env vars | ConfigMap + Secret |
| Health probes | Exported via app's own server | `/health/{live,ready,started}` | `/health/{live,ready,started}` |
| Suitable for | Monoliths, lambdas, tests | Single-server deployments | Production microservices |

---

### 12.1 Mode 1 — In-Process SDK

The Triad engine runs as tokio async tasks inside the host application process. No network hop between application code and the Triad engine. The application is responsible for all lifecycle management.

**API surface:**

```rust
// Initialise and start all configured patterns inside the calling process.
let instance = TriadInstance::start(TriadConfig::load("triad.yaml")?).await?;
// instance.shutdown() blocks until drain completes or the token is cancelled
```

**Lifecycle contract:**
- `TriadInstance::start` returns after all backends are connected and cold-start is complete.
- `instance.shutdown()` triggers the graceful drain sequence (§20.3) and blocks until complete.
- If the application process crashes, Triad crashes with it. Durability relies on PG replication slot persistence, Kafka `__consumer_offsets`, and outbox rows (§20.1).
- SIGTERM handling is the application's responsibility. The application must call `instance.shutdown()` from its signal handler. A minimal correct signal handler using `tokio::signal`:

```rust
let mut sigterm = signal(SignalKind::terminate())?;
let mut sigint  = signal(SignalKind::interrupt())?;
tokio::select! {
    _ = sigterm.recv() => {},
    _ = sigint.recv()  => {},
}
tokio::time::timeout(
    Duration::from_secs(35),
    instance.shutdown(),
).await.unwrap_or_else(|_| warn!("triad: drain timed out"));
```

If the application exits without calling `instance.shutdown()`, Triad logs an error (`triad: instance not shut down cleanly`). In-flight messages may be redelivered after restart — inbox dedup handles duplicates, but uncommitted Kafka producer transactions are aborted by the broker after the transaction timeout expires.

```mermaid
flowchart TD
    subgraph Process["Application Process (single binary)"]
        App["Application Code"]
        SDK["TriadInstance::start()"]
        CDC["cdc tokio task"]
        Outbox["outbox tokio task"]
        Inbox["inbox tokio task"]
        Saga["saga tokio task"]
        Admin["Admin HTTP\n(optional, app port)"]
        App -->|"TriadInstance::start(cfg)"| SDK
        SDK --> CDC
        SDK --> Outbox
        SDK --> Inbox
        SDK --> Saga
        SDK --> Admin
    end
    CDC --> PG[(PostgreSQL)]
    Outbox --> PG
    Inbox --> Kafka[(Kafka)]
    Saga --> Redis[(Redis)]
```

**Metrics and tracing:** Metrics are registered in the process-global Prometheus default registry. OTel spans are emitted via whatever TracerProvider the application configures before calling `Triad.Start`.

---

### 12.2 Mode 2 — Standalone Binary + CLI

A single `triad` binary that can act as both a long-running server and a CLI management client. The server exposes an Admin HTTP API; CLI subcommands either run locally (no server required) or call the Admin API of a running server.

**Binary commands:**

*Local commands* (no running server required — operate on config files, binaries, or backends directly):

| Command | Description |
|---------|-------------|
| `triad server` | Start the Triad runner as a foreground process |
| `triad validate` | Parse and validate `triad.yaml`; print all startup checks |
| `triad migrate` | Apply DB schema migrations (outbox, inbox, saga, checkpoint tables) |
| `triad version` | Print build version, Rust version, and config schema version |

*Admin-client commands* (require a running `triad server`; connect to `TRIAD_ADMIN_URL`, default `http://localhost:8080`):

| Command | Description |
|---------|-------------|
| `triad status` | Print server health, mode, uptime, and active pattern count |
| `triad patterns list` | List all active pipelines with status and throughput |
| `triad patterns pause <name>` | Pause a pipeline (drains in-flight, holds position) |
| `triad patterns resume <name>` | Resume a paused pipeline |
| `triad dlq list` | List DLQ topics with message counts |
| `triad dlq replay <topic>` | Replay DLQ messages back to the source topic |
| `triad dlq drop <topic>` | Discard all DLQ messages for a topic (destructive) |
| `triad lag` | Print Kafka consumer group lag per topic/partition |
| `triad saga list` | List in-flight sagas with step and state |
| `triad saga inspect <id>` | Print full saga state including step history |
| `triad saga cancel <id>` | Trigger compensation for a running saga |

**systemd unit file:**

```ini
[Unit]
Description=Triad Integration Runner
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=simple
User=triad
ExecStart=/usr/local/bin/triad server --config /etc/triad/triad.yaml
Restart=on-failure
RestartSec=5s
Environment=TRIAD_ADMIN_URL=http://localhost:8080
# Secrets injected by systemd-creds or a secrets manager sidecar
EnvironmentFile=-/etc/triad/triad.env

# Graceful shutdown: SIGTERM → drain → exit
# TimeoutStopSec must be > shutdown.drain_timeout_seconds in triad.yaml (default 30s).
# The gap (60s - 30s = 30s) gives Triad time to exit cleanly before systemd sends SIGKILL.
# If you increase drain_timeout_seconds, increase TimeoutStopSec proportionally.
TimeoutStopSec=60s
KillMode=mixed
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
```

```mermaid
flowchart LR
    subgraph Host["Host / VM"]
        CLI["triad &lt;admin-cmd&gt;\n(CLI process)"]
        Server["triad server\n(long-running process)"]
        AdminAPI["Admin HTTP\n:8080"]
        Engine["Triad Engine\ntokio tasks"]
        CLI -->|"HTTP → TRIAD_ADMIN_URL"| AdminAPI
        AdminAPI --- Engine
        Server --> AdminAPI
        Server --> Engine
    end
    Engine --> PG[(PostgreSQL)]
    Engine --> Kafka[(Kafka)]
    Engine --> Redis[(Redis)]
```

---

### 12.3 Mode 3 — Kubernetes Worker Fleet

A horizontally scalable fleet of `triad-worker` pods managed by a K8s Deployment. Leader election uses the Kubernetes `coordination.k8s.io/v1 Lease` API. The leader owns the PG WAL replication slot and saga orchestration; all pods (including leader) own a share of Kafka consumer partitions as assigned by the Kafka group coordinator.

**Leader election:**

```yaml
# Lease resource — Triad creates and renews this automatically
apiVersion: coordination.k8s.io/v1
kind: Lease
metadata:
  name: triad-leader
  namespace: triad-system
spec:
  leaseDurationSeconds: 15
  # renewTime and holderIdentity are set at runtime by the leader pod; do not set statically
  holderIdentity: ""   # managed by triad-worker at runtime
```

The leader performs:
- Holding the PG logical replication slot (CDC source).
- Running the saga timeout watchdog.
- Coordinating cold-start Strategy A (sequential pattern activation).
- Updating `triad_checkpoints.owner_instance_id` for the slots it owns.

Followers perform:
- Consuming assigned Kafka partitions (inbox, EOS dedup).
- Serving the Admin HTTP API (all pods; K8s Service routes to any pod).
- Writing to Redis (write-behind, feature flag cache).

**Leader election gap:** If the leader pod crashes, the Lease expires after `leaseDurationSeconds` (default 15s) before a follower acquires it. During this window the WAL replication slot is idle (WAL slot lag grows), no saga timeouts are fired, and no cold-start coordination runs. Sagas with timeout windows shorter than 15s may silently miss their timeout; for time-sensitive sagas set `saga.timeout` well above `leaseDurationSeconds`. Reduce `leaseDurationSeconds` only if your saga timeouts require tighter bounds — the minimum practical value is 5s (below that, network jitter causes spurious leader changes).

**Kubernetes manifests:**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: triad-worker
  namespace: triad-system
spec:
  replicas: 3
  selector:
    matchLabels:
      app: triad-worker
  template:
    metadata:
      labels:
        app: triad-worker
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      serviceAccountName: triad-worker   # needs Lease get/create/update
      containers:
        - name: triad-worker
          image: ghcr.io/your-org/triad-worker:latest
          args: ["server", "--mode=kubernetes"]
          ports:
            - name: admin
              containerPort: 8080
            - name: metrics
              containerPort: 9090
          env:
            - name: TRIAD_POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: TRIAD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
          envFrom:
            - configMapRef:
                name: triad-config
            - secretRef:
                name: triad-secrets
          livenessProbe:
            httpGet:
              path: /health/live
              port: admin
            initialDelaySeconds: 10
            periodSeconds: 10
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /health/ready
              port: admin
            initialDelaySeconds: 15
            periodSeconds: 10
            failureThreshold: 3
          startupProbe:
            httpGet:
              path: /health/started
              port: admin
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 12   # 60s budget for cold start
          resources:
            requests:
              cpu: "500m"
              memory: "256Mi"
            limits:
              cpu: "2"
              memory: "1Gi"
---
apiVersion: v1
kind: Service
metadata:
  name: triad-admin
  namespace: triad-system
spec:
  selector:
    app: triad-worker
  ports:
    - name: admin
      port: 8080
    - name: metrics
      port: 9090
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: triad-worker-pdb
  namespace: triad-system
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      app: triad-worker
---
# HPA using Prometheus Adapter External metric (Kafka lag)
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: triad-worker-hpa
  namespace: triad-system
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: triad-worker
  minReplicas: 3
  maxReplicas: 20
  metrics:
    - type: External
      external:
        metric:
          name: triad_kafka_consumer_lag_messages   # exposed via Prometheus Adapter; matches §15.3 inventory
          selector:
            matchLabels:
              consumer_group: triad-worker
        target:
          type: AverageValue
          averageValue: "1000"   # scale up when average lag > 1000 messages/pod
---
# ServiceMonitor for Prometheus Operator
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: triad-worker
  namespace: triad-system
spec:
  selector:
    matchLabels:
      app: triad-worker
  endpoints:
    - port: metrics
      path: /metrics
      interval: 15s
```

**Prometheus Adapter ConfigMap** (required for HPA External metric):

The HPA references `triad_kafka_consumer_lag_messages` via the Prometheus Adapter. The Adapter must be told how to query Prometheus and how to expose the metric as a K8s External metric:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-adapter-config
  namespace: monitoring
data:
  config.yaml: |
    rules:
      - seriesQuery: 'triad_kafka_consumer_lag_messages{consumer_group="triad-worker"}'
        resources:
          overrides:
            namespace: { resource: namespace }
        name:
          matches: "triad_kafka_consumer_lag_messages"
          as: "triad_kafka_consumer_lag_messages"
        metricsQuery: >
          sum(triad_kafka_consumer_lag_messages{consumer_group="triad-worker"})
          by (consumer_group)
```

The HPA `averageValue: "1000"` divides this sum by the current replica count. At 3 replicas and total lag 6,000, each "virtual pod" has lag 2,000 > 1,000, so HPA scales up. At 6 replicas and lag 6,000, average = 1,000, stable.

**CRD admission validation** (optional, for GitOps `TriadPattern` CRD):

If you deploy the `TriadPattern` CRD, add CEL validation rules directly in the CRD to reject malformed patterns at `kubectl apply` time rather than at runtime:

```yaml
x-kubernetes-validations:
  - rule: "self.spec.type in ['outbox','inbox','cdc','saga','cache_sync','webhook','feature_flag','rate_limit','feature_store']"
    message: "spec.type must be a supported Triad pattern type"
  - rule: "self.spec.timeout == '' || duration(self.spec.timeout) >= duration('1s')"
    message: "spec.timeout must be >= 1s if set"
```

**RBAC for Lease API:**

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: triad-leader-election
  namespace: triad-system
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: triad-leader-election
  namespace: triad-system
subjects:
  - kind: ServiceAccount
    name: triad-worker
roleRef:
  kind: Role
  name: triad-leader-election
  apiGroup: rbac.authorization.k8s.io
```

**Kubernetes topology:**

```mermaid
flowchart TD
    subgraph K8s["Kubernetes Cluster — triad-system namespace"]
        subgraph Deployment["triad-worker Deployment (3–20 replicas)"]
            W1["Pod: triad-worker-aaa\n(LEADER)"]
            W2["Pod: triad-worker-bbb\n(follower)"]
            W3["Pod: triad-worker-ccc\n(follower)"]
        end
        Lease["coordination.k8s.io/v1\nLease: triad-leader"]
        SVC["Service: triad-admin\n:8080 / :9090"]
        PDB["PodDisruptionBudget\nmaxUnavailable=1"]
        HPA["HPA\nmin=3 max=20\nmetric: kafka lag avg 1000"]
        CM["ConfigMap: triad-config"]
        SEC["Secret: triad-secrets"]
        SM["ServiceMonitor"]
        W1 -->|"holds"| Lease
        W2 -.->|"watches"| Lease
        W3 -.->|"watches"| Lease
        SVC --> W1
        SVC --> W2
        SVC --> W3
        HPA --> Deployment
        PDB --- Deployment
        CM --> W1
        CM --> W2
        CM --> W3
        SEC --> W1
        SEC --> W2
        SEC --> W3
        SM --> SVC
    end
    W1 -->|"WAL slot\n(leader only)"| PG[(PostgreSQL)]
    W1 & W2 & W3 -->|"consumer group"| Kafka[(Kafka)]
    W1 & W2 & W3 --> Redis[(Redis)]
    Prom["Prometheus"] -->|scrape| SM
    HPA -->|"External metric\n(Prometheus Adapter)"| Prom
```

---

## 13. Pattern Module Summary

A full map of every golden-triangle pattern to the Triad module that implements it:

| Pattern (from golden-triangle.md) | Triad Module | Source | Sink | EOS? |
|-----------------------------------|-------------|--------|------|------|
| CDC (§1.1) | `cdc` | PG WAL | Kafka | at-least-once (ack on LSN advance) |
| Transactional Outbox (§1.2) | `outbox` | PG outbox table | Kafka | exactly-once |
| Inbox / idempotent consumer (§1.3, §9.1) | `inbox` | Kafka | PG + Redis | exactly-once |
| Event Sourcing (§1.4) | `aggregate` (SDK) | commands | PG events table | exactly-once |
| Cache-aside (§2.1) | `cache` mode=aside | PG | Redis | at-least-once + TTL |
| Write-through (§2.2) | `cache` mode=write_through | App | PG + Redis | at-least-once |
| Write-behind (§2.3) | `cache` mode=write_behind | App → Redis | PG flush | at-most-once (AOF optional) |
| Read replica accelerator (§2.4) | `cache` mode=materialise | PG | Redis | at-least-once |
| Distributed locking (§2.5) | `lock` (SDK helper) | Redis | Redis | atomic (Redlock) |
| Session storage (§2.6) | `session` (SDK helper) | Redis | Redis + PG audit | at-least-once |
| Stream enrichment (§3.1) | `enrich` transform | Kafka | Kafka | at-least-once |
| Rate limiting (§3.2) | `rate_limit` | Redis | Redis + Kafka violations | atomic |
| Consumer state store (§3.3) | `state_store` | Kafka | Redis → PG flush | at-least-once |
| Pub/Sub fan-out (§3.4) | `fanout` | Kafka | Redis Pub/Sub | fire-and-forget |
| DLQ + retry (§3.5) | `dlq` decorator | Kafka | Kafka DLQ + Redis counters | at-least-once |
| CQRS + Event Sourcing (§4.1) | `cqrs` | PG events → Kafka | Redis read model | exactly-once |
| Saga (§4.2) | `saga` | Kafka | Kafka + Redis state + PG audit | exactly-once |
| Real-time data pipeline (§4.3) | `pipeline` | Kafka | Redis window → PG flush | at-least-once |
| Multi-tenant SaaS (§4.4) | `tenant` decorator | any | any (tenant-scoped) | inherits |
| Search + autocomplete (§4.5) | `search_index` | Kafka | Redis ZSet + external index | at-least-once |
| CDC-driven cache invalidation (§4.6) | `cache_sync` | Kafka (CDC events) | Redis | at-least-once |
| Health checks (§5.1) | `health` (built-in) | all backends | admin API | n/a |
| Backpressure (§5.2) | `backpressure` (built-in) | lag metrics | rate signals | n/a |
| Graceful degradation (§5.3) | circuit breakers (built-in) | backend health | fallback paths | n/a |
| Schema evolution (§5.4) | Schema Registry integration | — | — | n/a |
| Kafka transactions + EOS (§9.2) | `exactly_once` coordinator | Kafka | Kafka + PG | exactly-once |
| Redis Streams alternative (§9.3) | `redis_stream` source/sink | Redis Stream | Redis Stream / PG | at-least-once + XACK |
| HTTP idempotency keys (§9.4) | `idempotency` | HTTP | Redis NX + PG backup | exactly-once |
| Cold start / state rebuild (§9.5) | `cold_start` manager | PG snapshot or Kafka replay | Redis | at-least-once |
| Feature flag distribution (§9.6) | `feature_flag` | PG → Kafka → Redis | Redis hot path | at-least-once |
| Multi-region (§9.7) | `replication` topology | PG logical + MM2 + Redis rep | replica backends | eventually consistent |
| Webhook delivery (§9.8) | `webhook` | Kafka | HTTP + PG log + Redis CB | at-least-once + dedup |
| ML feature store (§9.9) | `feature_store` | External topic or PG table → Redis | Redis online + PG offline | at-least-once |

### v0.1.0 Implementation Status

**Implemented in v0.1.0:** `cdc`, `outbox`, `inbox`, `aggregate` (SDK), `cache` (all four modes), `rate_limit`, `dlq`, `exactly_once`, `idempotency`, `cold_start` (Strategy C only — dual-read; see §8.2), `feature_flag`, `webhook`, `feature_store`, `saga`, built-in health/backpressure/circuit-breakers.

**Deferred to v0.2.0 (Stage 2):** `lock`, `session`, `enrich`, `state_store`, `fanout`, `cqrs`, `pipeline`, `tenant`, `search_index`, `redis_stream`, `replication` (multi-region). Schema Registry integration (§5.4) is also deferred — see §14 open question #4.

**Cold-start strategies:** Strategy C (dual-read / lazy warm) is the only strategy implemented in v0.1.0. Strategies A (PG snapshot) and B (Kafka replay) are designed but not yet built.

---

## 14. Design Trade-offs and Open Questions

```mermaid
quadrantChart
    title Pattern complexity vs implementation value
    x-axis "Low implementation effort" --> "High implementation effort"
    y-axis "Low operational value" --> "High operational value"
    quadrant-1 Build first
    quadrant-2 High value, invest
    quadrant-3 Low priority
    quadrant-4 Question ROI

    Outbox Relay: [0.15, 0.90]
    Inbox Dedup: [0.20, 0.85]
    CDC Module: [0.35, 0.92]
    Cache Aside: [0.10, 0.70]
    Cache Sync: [0.25, 0.80]
    Saga Orchestrator: [0.70, 0.88]
    Exactly Once EOS: [0.65, 0.85]
    Feature Flags: [0.30, 0.75]
    Webhook Pipeline: [0.45, 0.72]
    Cold Start Manager: [0.40, 0.78]
    Multi Region: [0.85, 0.82]
    ML Feature Store: [0.80, 0.65]
    Redis Streams Alt: [0.20, 0.55]
    Rate Limiter: [0.15, 0.65]
    HTTP Idempotency: [0.15, 0.80]
```

**Key open questions for any concrete implementation:**

1. **Language choice for the Runner core:** Rust was chosen — zero-cost async with tokio, no GC pauses, and the rich `rdkafka` / `sqlx` / `deadpool-redis` ecosystem covers all three backends. The SDK is a Rust library crate; Python bindings (Phase 10) are exposed via PyO3/maturin.

2. **WAL replication slot ownership:** In the centralised topology, only one Runner can own a replication slot. Leader election (via PostgreSQL advisory locks or Redis `SET NX`) is required. In the sidecar topology, each service owns its own slot — slot proliferation must be monitored.

3. **Saga state consistency:** The design stores in-flight saga state in Redis (fast) and commits the final outcome to PostgreSQL (durable). If Redis fails mid-saga, the orchestrator must replay from the last Kafka event in the saga's event sequence to reconstruct state. This requires the saga's Kafka events to be a complete journal — an event sourcing constraint on the Saga module itself.

4. **Schema Registry coupling:** The CDC module is tightly coupled to a Schema Registry for encoding. Teams not running Confluent Schema Registry must either run an OSS alternative (Apicurio Registry) or use JSON with schema evolution handled at the application level. **v0.1.0 decision:** Schema Registry integration is deferred. The `ChangeEvent` is serialized with `serde_json` without schema validation. Post-v0.1.0, use the `apache_avro` crate and wire `SchemaRegistryConverter` around the CDC encoder/decoder.

5. **Multi-region write conflicts:** The design acknowledges that active-active PostgreSQL requires application-level conflict resolution. Triad can provide the replication infrastructure but cannot define the business-level merge logic — that must be supplied as a plugin.

---

## 15. Metrics Capture (Logical Design)

### 15.1 Naming Convention

```
triad_{subsystem}_{measurement}_{unit_suffix}
```

| Field | Convention | Examples |
|-------|-----------|---------|
| Prefix | Always `triad_` | — |
| Subsystem | Matches pattern type or Runner component | `pipeline`, `cdc`, `outbox`, `inbox`, `saga`, `cache`, `webhook`, `flag`, `eos`, `conn` |
| Measurement | Verb-noun describing what is counted | `events`, `lag`, `hit`, `duration`, `active`, `pending` |
| Unit suffix | Prometheus convention | `_total` (counter), `_seconds` (histogram/gauge), `_bytes` (gauge), `_ratio` (gauge) |

### 15.2 Label Schema

Every metric carries a baseline label set. Subsystem-specific labels are additive.

| Label | Description | Cardinality bound |
|-------|-------------|------------------|
| `pattern_name` | Named pattern from `triad.yaml` (e.g., `orders_cdc`) | ≤ 50 per instance |
| `pipeline_name` | Named pipeline within a pattern | ≤ 200 per instance |
| `region` | Deployment region (e.g., `us-east-1`) | ≤ 10 |
| `backend` | Backend involved: `postgres`, `kafka`, `redis` | 3 |
| `status` | Outcome: `ok`, `error`, `duplicate`, `timeout`, `rejected` | ≤ 10 |

High-cardinality labels (e.g., `endpoint_id` for webhooks, `subscription_id`) are isolated to dedicated metric families with explicit allow-lists configured in `triad.yaml` under `observability.metrics.cardinality_limits`.

### 15.3 Full Metrics Inventory

> **Implementation cross-reference:** Metric name constants are defined in `crates/triad-core/src/metrics.rs`.
> Every name in this table must have a matching constant there. Phase 8b added counter assertions
> for outbox, eos, saga, and webhook patterns; remaining patterns are tracked by name only.

| Metric | Type | Key Additional Labels | Description |
|--------|------|----------------------|-------------|
| `triad_pipeline_events_total` | Counter | `pattern_name`, `pipeline_name`, `status` | All events processed |
| `triad_pipeline_processing_duration_seconds` | Histogram | `pattern_name`, `pipeline_name` | Per-event end-to-end processing time |
| `triad_pipeline_lag_seconds` | Gauge | `pattern_name`, `pipeline_name` | Age of the oldest unprocessed event |
| `triad_pg_replication_lag_bytes` | Gauge | `slot_name` | WAL bytes retained by the logical replication slot; replaces deprecated `triad_cdc_wal_lsn_lag_bytes` |
| `triad_cdc_events_total` | Counter | `table_name`, `operation` | CDC events by table and DML type (INSERT/UPDATE/DELETE) |
| `triad_outbox_pending_total` | Gauge | `table_name` | Unpublished outbox rows |
| `triad_outbox_relay_duration_seconds` | Histogram | `pattern_name` | Time from outbox INSERT to Kafka commit |
| `triad_inbox_dedup_total` | Counter | `pattern_name`, `outcome`, `dedup_backend` | Inbox dedup decisions: `accepted` or `rejected` |
| `triad_kafka_producer_txn_total` | Counter | `transactional_id`, `outcome` | Kafka producer transaction outcomes |
| `triad_kafka_consumer_lag_messages` | Gauge | `group_id`, `topic`, `partition` | Kafka consumer group lag in messages |
| `triad_redis_op_duration_seconds` | Histogram | `command`, `pattern_name` | Redis operation latency per command type |
| `triad_redis_memory_used_bytes` | Gauge | `instance` | Redis `used_memory` |
| `triad_cache_hit_total` | Counter | `pattern_name`, `key_prefix` | Cache hits |
| `triad_cache_miss_total` | Counter | `pattern_name`, `key_prefix` | Cache misses |
| `triad_cache_hit_ratio` | Gauge | `pattern_name` | Rolling hit ratio, updated every 15 s |
| `triad_saga_active_total` | Gauge | `saga_name` | In-flight saga instances |
| `triad_saga_step_duration_seconds` | Histogram | `saga_name`, `step_name` | Per-step latency |
| `triad_saga_completed_total` | Counter | `saga_name`, `outcome` | Saga completions: `completed`, `rolledback`, `timeout` |
| `triad_saga_compensation_total` | Counter | `saga_name`, `step_name` | Compensation commands sent |
| `triad_webhook_delivery_attempts_total` | Counter | `endpoint_id`, `status_class` | Delivery attempts by HTTP status class: `2xx`, `4xx`, `5xx`, `timeout` |
| `triad_webhook_delivery_duration_seconds` | Histogram | `endpoint_id` | HTTP delivery round-trip time |
| `triad_webhook_cb_state` | Gauge | `endpoint_id` | **Deprecated alias.** Use `triad_circuit_breaker_state{backend="http"}` with `name=endpoint_id` instead |
| `triad_feature_flag_evaluations_total` | Counter | `flag_name`, `result` | Flag evaluations: `enabled`, `disabled`, `rollout_bucket` |
| `triad_feature_flag_sync_lag_seconds` | Gauge | `flag_name` | Time since flag last synced from PG to Redis |
| `triad_feature_store_lookup_duration_seconds` | Histogram | `entity`, `feature_name`, `source` | Feature lookup latency; `source` = `redis` or `pg_fallback` |
| `triad_feature_store_freshness_seconds` | Gauge | `entity`, `feature_name` | Age of the most recent feature value in Redis |
| `triad_rate_limit_checks_total` | Counter | `limiter_name`, `outcome` | Rate limit decisions: `allowed` or `rejected` |
| `triad_eos_txn_total` | Counter | `pipeline_name`, `outcome` | EOS coordinator: `committed`, `aborted`, `noop_dup` |
| `triad_cold_start_duration_seconds` | Histogram | `pattern_name`, `strategy` | Full warm-up duration |
| `triad_cold_start_records_total` | Counter | `pattern_name`, `strategy` | Records loaded during warm-up |
| `triad_conn_pool_active` | Gauge | `backend`, `pool_name` | Active connections |
| `triad_conn_pool_idle` | Gauge | `backend`, `pool_name` | Idle connections |
| `triad_conn_pool_wait_seconds` | Histogram | `backend`, `pool_name` | Time waiting for a connection |
| `triad_circuit_breaker_state` | Gauge | `backend`, `name` | 0=CLOSED, 1=OPEN, 2=HALF\_OPEN |
| `triad_circuit_breaker_transitions_total` | Counter | `backend`, `name`, `from`, `to` | State transitions |
| `triad_error_total` | Counter | `pattern_name`, `error_type`, `retryable` | All errors by type |
| `triad_retry_attempts_total` | Counter | `pattern_name`, `attempt` | Retry attempt number distribution |
| `triad_dlq_messages_total` | Counter | `source_topic`, `dlq_topic`, `error_type` | Messages routed to DLQ |
| `triad_replication_lag_seconds` | Gauge | `region`, `replication_type` | Cross-region lag: `pg_logical`, `kafka_mm2`, `redis` |
| `triad_backpressure_active` | Gauge | `backend` | 1 when backpressure is active for this backend |
| `triad_redis_memory_max_bytes` | Gauge | `instance` | Redis `maxmemory` setting in bytes (0 = unlimited) |
| `triad_saga_step_total` | Counter | `saga_name`, `step_name`, `outcome` | Saga step completions: `success`, `timeout`, `failed`, `compensated` |
| `triad_db_operation_duration_seconds` | Histogram | `backend`, `operation`, `pattern_name` | Database operation latency (PG queries, Redis commands, Kafka produce) |

### 15.4 Collection Architecture

```mermaid
flowchart LR
    subgraph Runner["Triad Runner"]
        PM[Pattern Modules\nCDC, Outbox, Saga, Cache...]
        MR[MetricRegistry\nin-process, lock-free counters]
        EP["/metrics\nPrometheus text exposition\n:9090"]
        PM -->|record| MR
        MR --> EP
    end
    subgraph Obs["Observability Stack"]
        Prom[Prometheus\nscrapes every 15s]
        Grafana[Grafana Dashboards]
        AM[Alertmanager]
    end
    EP -->|HTTP GET /metrics| Prom
    Prom --> Grafana
    Prom --> AM
    AM -->|webhook| OnCall[PagerDuty / Slack]
```

### 15.5 Histogram Bucket Defaults

| Metric | Buckets |
|--------|---------|
| `triad_pipeline_processing_duration_seconds` | 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10 |
| `triad_outbox_relay_duration_seconds` | 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30 |
| `triad_redis_op_duration_seconds` | 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1 |
| `triad_conn_pool_wait_seconds` | 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5 |
| `triad_saga_step_duration_seconds` | 0.1, 0.5, 1, 5, 10, 30, 60, 120, 300 |
| `triad_webhook_delivery_duration_seconds` | 0.05, 0.1, 0.5, 1, 2, 5, 10, 30 |
| `triad_cold_start_duration_seconds` | 1, 5, 10, 30, 60, 120, 300, 600 |

---

## 16. Error Handling Model

### 16.1 Error Taxonomy

```mermaid
flowchart TD
    Err[Error Occurs] --> IsNet{Network or\nconnection error?}
    IsNet -->|Yes| Trans[TRANSIENT\nRetry with exponential backoff]
    IsNet -->|No| IsDeser{Deserialization\nor schema failure?}
    IsDeser -->|Yes| PermSchema[PERMANENT — Poison Pill\nWrite to DLQ, commit offset, alert]
    IsDeser -->|No| IsConstraint{Business constraint\nor auth violation?}
    IsConstraint -->|Yes| PermBiz[PERMANENT\nLog + DLQ, no retry]
    IsConstraint -->|No| IsAmbig{Timeout after\npartial network send?}
    IsAmbig -->|Yes| Ambig[AMBIGUOUS\nRetry with idempotency key\nto deduplicate if already applied]
    IsAmbig -->|No| IsResource{Resource exhausted?\nOOM or disk full}
    IsResource -->|Yes| PermRes[PERMANENT\nAlert + pause pipeline\nDo NOT retry]
    IsResource -->|No| Trans

    Trans --> Retry[Retry Engine\nexponential backoff + jitter]
    Ambig --> Retry
    Retry -->|max attempts exceeded| DLQ[Route to DLQ topic]
```

### 16.2 Retry Policy Matrix

| Backend | Base Delay | Max Delay | Max Attempts | Jitter | Notes |
|---------|-----------|-----------|-------------|--------|-------|
| PostgreSQL write | 100 ms | 30 s | 10 | ±20% | Pool exhaustion: wait for connection before first attempt |
| PostgreSQL WAL reconnect | 500 ms | 60 s | 20 | ±30% | Reconnect only; do not retry individual WAL reads |
| Kafka producer | 100 ms | 10 s | 5 | ±25% | Call `abortTransaction()` before each retry |
| Kafka consumer poll | — | — | — | — | No retry; re-poll is automatic on next tick |
| Redis read | 50 ms | 5 s | 8 | ±20% | On max exceeded, fall back to PostgreSQL |
| Redis write | 50 ms | 5 s | 8 | ±20% | On max exceeded, queue in write-behind buffer |
| HTTP webhook | 1 s | 3600 s | 10 | ±50% | Exponential: 1s → 2s → 4s → ... → 512s → 1 h cap |

```yaml
# Retry config snippet in triad.yaml
retry:
  default:
    base_delay: "100ms"
    max_delay: "30s"
    max_attempts: 10
    jitter_factor: 0.2
  per_backend:
    redis:
      base_delay: "50ms"
      max_delay: "5s"
      max_attempts: 8
    kafka_producer:
      base_delay: "100ms"
      max_delay: "10s"
      max_attempts: 5
    http_webhook:
      base_delay: "1s"
      max_delay: "3600s"
      max_attempts: 10
      jitter_factor: 0.5
  retryable_error_types:
    - network_timeout
    - connection_refused
    - resource_temporarily_unavailable
    - kafka_leader_not_available
  non_retryable_error_types:
    - schema_validation_failure
    - message_too_large
    - unauthorized
    - deserialization_error
```

### 16.3 Circuit Breaker State Machine

```mermaid
stateDiagram-v2
    [*] --> CLOSED : initial state

    CLOSED --> CLOSED : call succeeds — reset failure count in rolling window
    CLOSED --> OPEN : failure rate exceeds threshold within rolling window

    OPEN --> OPEN : incoming call — fail fast, no backend call made
    OPEN --> HALF_OPEN : half_open_after duration elapsed

    HALF_OPEN --> CLOSED : probe call succeeds — reset failure count
    HALF_OPEN --> OPEN : probe call fails — remain open

    note right of CLOSED
        Tracks total calls and failures
        in a sliding time window.
        triad_circuit_breaker_state = 0
    end note

    note right of OPEN
        All calls return CircuitBreakerOpenError.
        Downstream uses configured fallback path.
        triad_circuit_breaker_state = 1
    end note

    note right of HALF_OPEN
        One probe request allowed through.
        All other calls fail fast.
        triad_circuit_breaker_state = 2
    end note
```

```yaml
# Per-pattern circuit breaker config
circuit_breakers:
  enabled: true
  failure_threshold: 5        # failures in rolling_window before opening
  rolling_window: "60s"
  half_open_after: "30s"
  success_threshold: 2        # consecutive successes required to close
  per_backend:
    redis:
      fallback: postgres      # serve reads from PG while Redis CB is open
    kafka:
      fallback: outbox_table  # buffer writes to outbox table while Kafka CB is open
    postgres:
      fallback: readonly      # reject writes; allow reads from replica
```

### 16.4 DLQ Routing and Reprocessing

```mermaid
flowchart TD
    Msg[Message from Kafka] --> Process[Pattern Module — process attempt]
    Process -->|success| Ack[commitOffset]
    Process -->|retryable error| Retry[Retry Engine\nexponential backoff]
    Retry -->|success| Ack
    Retry -->|max attempts exceeded| DLQProd["Produce to DLQ topic\ntriad.dlq.{source_topic}"]

    DLQProd --> DLQMsg[DLQ Message\noriginal payload preserved]

    subgraph DLQHeaders["Kafka Headers added by Triad"]
        H1["triad-dlq-reason"]
        H2["triad-dlq-attempt-count"]
        H3["triad-dlq-original-topic"]
        H4["triad-dlq-original-offset"]
        H5["triad-dlq-error-type"]
        H6["triad-dlq-timestamp-iso8601"]
        H7["triad-traceparent"]
    end

    DLQMsg --> DLQHeaders
    DLQMsg --> Alert[triad_dlq_messages_total incremented\nAlert fired if rate exceeds threshold]
    DLQMsg --> AdminAPI["Admin API\nGET /dlq/triad.dlq.{topic} lists pending messages"]

    AdminAPI --> Decision{Operator decision}
    Decision -->|fix root cause then replay| Replay["POST /dlq/triad.dlq.{topic}/replay\nor seek consumer group to offset"]
    Decision -->|discard| Discard[Mark as dead\narchive to object storage]
    Replay --> Process
```

DLQ topic names follow the template `triad.dlq.{source_topic}` (e.g., source topic `orders` → DLQ topic `triad.dlq.orders`). The prefix `triad.dlq.` is a dedicated namespace that avoids collision with application-owned topics. The template is configurable:

```yaml
delivery:
  dlq_topic_template: "triad.dlq.{source_topic}"   # default; override if your Kafka naming policy differs
```

### 16.5 Poison Pill Handling

A **poison pill** is a message that can never be successfully processed: corrupt bytes, incompatible schema, or a missing required field. The correct response is to skip — not to retry indefinitely.

```mermaid
flowchart TD
    Msg[Kafka message received] --> Deser{Deserialize payload}
    Deser -->|success| Validate{Schema validate}
    Deser -->|DeserializationException| Pill[POISON PILL detected]

    Validate -->|valid| Process[Normal processing path]
    Validate -->|invalid| Pill

    Pill --> Log["Structured log: poison_pill\nfields: trace_id, topic, partition,\noffset, error, raw_bytes_preview"]
    Log --> DLQWrite["Write to DLQ topic\nraw bytes preserved\nheader: triad-dlq-reason=poison_pill"]
    DLQWrite --> Skip["commitOffset — skip the message\nDo NOT block the consumer"]
    Skip --> Metric["triad_dlq_messages_total{error_type=poison_pill}++\nAlert if rate > 0 in steady state"]
```

### 16.6 Error Budget Expressions

```promql
# Overall pipeline error rate (SLO target: < 0.1%)
sum(rate(triad_error_total[5m]))
  /
sum(rate(triad_pipeline_events_total[5m]))

# DLQ ingest rate (target: 0 in steady state)
sum(rate(triad_dlq_messages_total[5m]))

# Fraction of time any circuit breaker is open (target: < 0.1%)
avg_over_time(max(triad_circuit_breaker_state > 0)[1h:1m])

# P99 processing latency (target: < 100 ms)
histogram_quantile(0.99,
  sum by (le) (rate(triad_pipeline_processing_duration_seconds_bucket[5m]))
)

# EOS transaction abort rate (target: < 0.01%)
rate(triad_eos_txn_total{outcome="aborted"}[5m])
  /
rate(triad_eos_txn_total[5m])

# Poison pill rate (target: 0)
sum(rate(triad_dlq_messages_total{error_type="poison_pill"}[5m]))
```

---

## 17. OpenTelemetry End-to-End Tracing

### 17.1 Span Hierarchy

A single customer request generates a distributed trace that spans all three backends. The dotted arrows show async links propagated through Kafka message headers.

```
HTTP POST /orders                              [SERVER]  trace_id=a1b2c3...
├── triad.idempotency.check                   [INTERNAL]
│   └── redis.SET_NX  idem:{key}             [CLIENT]   db.system=redis
├── pg.transaction                            [CLIENT]   db.system=postgresql
│   ├── pg.INSERT orders
│   └── pg.INSERT outbox
│
╌╌ async: traceparent injected into outbox row and Kafka header ╌╌
│
├── triad.outbox.relay                        [PRODUCER]
│   └── kafka.produce  domain.events         [PRODUCER] messaging.system=kafka
│       │
│       ╌╌ traceparent extracted from Kafka header ╌╌
│       │
│       ├── triad.inbox.consume  [Service B] [CONSUMER]
│       │   ├── redis.SET_NX  inbox:{id}     [CLIENT]
│       │   └── pg.transaction              [CLIENT]
│       │       ├── pg.apply business logic
│       │       └── pg.INSERT inbox
│       │
│       └── triad.cache_sync                 [CONSUMER]
│           └── redis.HSET  order:{id}       [CLIENT]
```

```mermaid
flowchart TD
    Root["HTTP POST /orders\nSERVER span\ntrace_id=a1b2c3"]
    Idem["triad.idempotency.check\nINTERNAL"]
    RNX["redis.SET_NX idem-key\nCLIENT"]
    PGTxn["pg.transaction\nCLIENT — orders + outbox"]
    PGOrd["pg.INSERT orders"]
    PGOut["pg.INSERT outbox"]
    Relay["triad.outbox.relay\nPRODUCER"]
    KProd["kafka.produce domain.events\nPRODUCER"]
    Inbox["triad.inbox.consume\nCONSUMER — Service B"]
    CSync["triad.cache_sync\nCONSUMER"]
    IRedis["redis.SET_NX inbox-event-id\nCLIENT"]
    IPG["pg.transaction — apply + inbox\nCLIENT"]
    CRedis["redis.HSET order-id\nCLIENT"]

    Root --> Idem --> RNX
    Root --> PGTxn --> PGOrd
    PGTxn --> PGOut
    PGOut -.->|async — Kafka header| Relay --> KProd
    KProd -.->|traceparent in header| Inbox
    KProd -.->|traceparent in header| CSync
    Inbox --> IRedis
    Inbox --> IPG
    CSync --> CRedis

    style Root fill:#4a90d9,color:#fff
    style PGTxn fill:#259,color:#fff
    style PGOrd fill:#259,color:#fff
    style PGOut fill:#259,color:#fff
    style IPG fill:#259,color:#fff
    style RNX fill:#b03,color:#fff
    style IRedis fill:#b03,color:#fff
    style CRedis fill:#b03,color:#fff
    style KProd fill:#1a6,color:#fff
    style Relay fill:#1a6,color:#fff
    style Inbox fill:#1a6,color:#fff
    style CSync fill:#1a6,color:#fff
```

### 17.2 Context Propagation Across Backends

```mermaid
sequenceDiagram
    participant API as API Service
    participant SDK as Triad SDK
    participant PG as PostgreSQL
    participant KProd as Kafka Producer\n(Triad Runner)
    participant KCons as Kafka Consumer\n(Triad Runner)
    participant Redis as Redis

    Note over API: OTel: start SERVER span<br/>trace_id=abc, span_id=s001<br/>W3C traceparent header

    API->>SDK: Triad.idempotent(key).run(fn)
    SDK->>Redis: SET NX idem:{key} — child span s002
    Redis-->>SDK: OK

    SDK->>PG: BEGIN; INSERT orders; INSERT outbox;<br/>SET LOCAL application_name='triad:trace:abc:s003'<br/>child span s003
    PG-->>SDK: COMMIT

    Note over SDK: Store traceparent in outbox row<br/>column: trace_context VARCHAR

    KProd->>PG: Poll or CDC detect outbox row — read trace_context
    KProd->>KProd: Restore parent context from trace_context — child span s004
    KProd->>KProd: Inject traceparent into Kafka message header
    KProd->>KProd: kafka.produce(domain.events) — child span s004

    KCons->>KCons: kafka.consume — extract traceparent header
    KCons->>KCons: Start CONSUMER span s005 linked to s004
    KCons->>Redis: SET NX inbox:{event_id} — child span s006
    Redis-->>KCons: OK
    KCons->>PG: BEGIN; apply logic; INSERT inbox;<br/>SET LOCAL application_name='triad:trace:abc:s007'
    PG-->>KCons: COMMIT
    KCons->>Redis: HSET order:{id} — child span s008

    Note over KCons: All spans exported to OTel Collector<br/>→ Tempo / Jaeger
```

### 17.3 Standard Span Attributes

| Attribute | Example Value | Applied On |
|-----------|--------------|-----------|
| `triad.pattern.name` | `orders_cdc` | All Triad spans |
| `triad.pipeline.name` | `outbox_relay` | All Triad spans |
| `triad.backend` | `postgres` | Backend CLIENT spans |
| `db.system` | `postgresql` | PG CLIENT spans |
| `db.statement` | `INSERT INTO outbox ...` | PG CLIENT spans (bind params stripped) |
| `db.name` | `mydb` | PG CLIENT spans |
| `messaging.system` | `kafka` | Kafka PRODUCER/CONSUMER spans |
| `messaging.destination.name` | `domain.events` | Kafka spans |
| `messaging.kafka.partition` | `3` | Kafka CONSUMER spans |
| `messaging.kafka.offset` | `10482` | Kafka CONSUMER spans |
| `messaging.kafka.consumer.group` | `triad-inbox` | Kafka CONSUMER spans |
| `net.peer.name` | `kafka-1` | All CLIENT spans |
| `net.peer.port` | `9092` | All CLIENT spans |
| `triad.saga.id` | `saga-uuid-...` | Saga spans |
| `triad.saga.step` | `reserve_inventory` | Saga step spans |
| `triad.event.type` | `OrderCreated` | Event processing spans |
| `triad.event.id` | `evt-uuid-...` | Event processing spans |
| `error` | `true` | Error spans |
| `exception.type` | `ConnectionRefusedError` | Error spans |
| `exception.message` | `dial tcp: connection refused` | Error spans |

### 17.4 Sampling Strategy

```yaml
observability:
  tracing:
    provider: opentelemetry
    endpoint: "http://otel-collector:4317"
    exporter: otlp_grpc           # otlp_grpc | otlp_http | jaeger | zipkin
    propagators: [tracecontext, baggage]   # W3C standard
    sampling:
      default_rate: 0.1           # 10% of root spans in production
      overrides:
        - match: { pattern: saga }
          rate: 1.0               # always trace sagas — critical path
        - match: { pattern: eos }
          rate: 1.0               # always trace EOS coordinator
        - match: { status: error }
          rate: 1.0               # always trace error spans
        - match: { pattern: cdc }
          rate: 0.01              # 1% — very high volume, low per-event value
    resource_attributes:
      service.name: "triad-runner"
      service.version: "${TRIAD_VERSION}"
      deployment.environment: "${TRIAD_ENV}"
```

**Sampling approach:** Triad uses **parent-based + rule-based** composition. If an incoming Kafka message carries a `traceparent` header, the parent's sampling decision is honoured (parent-based). If no parent exists (e.g., a CDC event from an uninstrumented writer), the rule-based rates above apply. Error spans always override to 100% regardless of the parent decision.

### 17.5 Log–Trace Correlation

Every structured log line emitted by Triad includes `trace_id` and `span_id` from the active OTel context:

```json
{
  "level": "info",
  "ts": "2026-04-24T09:30:00.123Z",
  "msg": "outbox row relayed to Kafka",
  "pattern_name": "domain_outbox",
  "event_id": "evt-abc-123",
  "kafka_topic": "domain.events",
  "kafka_offset": 10482,
  "trace_id": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",
  "span_id": "0001020304050607",
  "region": "us-east-1"
}
```

Grafana Loki is configured with a derived field on `trace_id` that renders a link directly to the Tempo trace. Alertmanager notifications include `trace_id` in annotations when an alert fires from an error-rate rule, enabling one-click navigation from alert → trace.

Prometheus **exemplars** on histograms attach the current `trace_id` to each observation bucket, allowing Grafana to link from a P99 spike directly to the slowest trace in that window.

---

## 18. Configuration Reference (Full Logical Schema)

### 18.1 Complete `triad.yaml` Schema

Extends the configuration in §9 with all fields, types, defaults, and validation notes.

```yaml
triad:
  version: "1"                    # required; schema version — only "1" currently
  instance_id: "${HOSTNAME}"      # unique per Runner instance; appended to transactional_id

  # ── Backend connections ──────────────────────────────────────────────────────
  backends:

    postgres:
      primary: "${secret:env:PG_PRIMARY_DSN}"      # required
      replicas:                                      # optional; used for read-only queries
        - "${secret:env:PG_REPLICA_DSN}"
      pool_size: 20               # default 20; validated ≤ PG max_connections - 10
      pool_min_idle: 5            # default 5
      connection_timeout: "5s"
      statement_timeout: "30s"   # applied as SET LOCAL per transaction
      wal_level: logical          # verified at startup via SHOW wal_level
      max_replication_slots: 5   # validated against available PG replication slots
      ssl_mode: require           # disable | allow | prefer | require | verify-full
      ssl_root_cert: "/etc/ssl/pg-ca.crt"

    kafka:
      brokers:                    # required; at least 1 broker
        - "kafka-1:9092"
        - "kafka-2:9092"
      schema_registry: "http://schema-registry:8081"
      schema_registry_auth:
        user: "${secret:env:SR_USER}"
        password: "${secret:env:SR_PASSWORD}"

      producer:
        transactional_id_prefix: "triad"  # combined with instance_id for uniqueness
        enable_idempotence: true           # required when guarantee = exactly_once
        acks: all                          # required for EOS
        max_in_flight_requests: 5
        compression: lz4                   # none | gzip | snappy | lz4 | zstd
        batch_size: 65536                  # bytes (64 KB)
        linger_ms: 5
        request_timeout: "30s"
        transaction_timeout: "60s"

      consumer:
        group_id_prefix: "triad"          # appended with pattern_name per consumer group
        isolation_level: read_committed   # required for EOS; read_uncommitted for at-least-once
        auto_offset_reset: earliest       # earliest | latest | none
        max_poll_records: 500
        fetch_max_bytes: 52428800         # 50 MB
        session_timeout: "30s"
        heartbeat_interval: "10s"

      security:
        protocol: SASL_SSL                # PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
        sasl_mechanism: PLAIN             # PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512
        sasl_username: "${secret:env:KAFKA_USER}"
        sasl_password: "${secret:env:KAFKA_PASSWORD}"
        ssl_ca_cert: "/etc/ssl/kafka-ca.crt"

    redis:
      url: "${secret:env:REDIS_URL}"     # required
      mode: cluster                       # standalone | sentinel | cluster
      sentinel:                           # required when mode = sentinel
        master_name: "mymaster"
        nodes: ["sentinel-1:26379", "sentinel-2:26379"]
      pool_size: 50
      min_idle: 10
      dial_timeout: "5s"
      read_timeout: "3s"
      write_timeout: "3s"
      max_retries: 3
      tls:
        enabled: false
        cert: "/etc/ssl/redis-cert.pem"
        key:  "/etc/ssl/redis-key.pem"
        ca:   "/etc/ssl/redis-ca.pem"

  # ── Retry policies ───────────────────────────────────────────────────────────
  retry:
    default:
      base_delay: "100ms"
      max_delay: "30s"
      max_attempts: 10
      jitter_factor: 0.2
    per_backend:
      redis:
        base_delay: "50ms"
        max_delay: "5s"
        max_attempts: 8
      kafka_producer:
        base_delay: "100ms"
        max_delay: "10s"
        max_attempts: 5
      http_webhook:
        base_delay: "1s"
        max_delay: "3600s"
        max_attempts: 10
        jitter_factor: 0.5

  # ── Circuit breakers ─────────────────────────────────────────────────────────
  circuit_breakers:
    enabled: true
    failure_threshold: 5
    rolling_window: "60s"
    half_open_after: "30s"
    success_threshold: 2
    per_backend:
      redis:
        fallback: postgres
      kafka:
        fallback: outbox_table
      postgres:
        fallback: readonly

  # ── Cold start ───────────────────────────────────────────────────────────────
  cold_start:
    default_strategy: dual_read          # pg_snapshot | kafka_replay | dual_read
    pg_snapshot:
      batch_size: 500                    # rows per SELECT batch
      pipeline_size: 50                  # concurrent Redis SETEX writes per batch
      order_by: "id ASC"
    kafka_replay:
      stop_at_high_watermark: true
      consumer_group_suffix: "_coldstart"
    overrides:
      user_cache_sync: pg_snapshot
      order_cqrs_view: kafka_replay

  # ── Delivery guarantees ───────────────────────────────────────────────────────
  delivery:
    default_guarantee: exactly_once      # at_most_once | at_least_once | exactly_once
    kafka_transaction_timeout: "60s"
    idempotency_dedup_window: "24h"
    inbox_dedup_backend: both            # redis | postgres | both

  # ── Observability ─────────────────────────────────────────────────────────────
  observability:
    metrics:
      provider: prometheus
      port: 9090
      path: "/metrics"
      labels:                            # static labels on every metric
        region: "${TRIAD_REGION}"
        environment: "${TRIAD_ENV}"
      cardinality_limits:
        endpoint_id: 200                 # max distinct endpoint_id values
      histogram_buckets: {}              # override per-metric as needed (see §15.5)

    tracing:
      provider: opentelemetry
      endpoint: "http://otel-collector:4317"
      exporter: otlp_grpc
      sampling:
        default_rate: 0.1
        overrides:
          - match: { pattern: saga }
            rate: 1.0
          - match: { pattern: eos }
            rate: 1.0
          - match: { status: error }
            rate: 1.0
      propagators: [tracecontext, baggage]
      exemplars: true                    # attach trace_id to histogram observations
      resource_attributes:
        service.name: "triad-runner"
        service.version: "${TRIAD_VERSION}"

    logging:
      level: info                        # debug | info | warn | error
      format: json                       # json | text
      include_trace_context: true        # inject trace_id and span_id into every log line

    alerts:
      kafka_lag_threshold: 10000         # messages — fire alert
      pg_replication_lag_threshold: "30s"
      redis_memory_threshold: 0.85       # fraction of maxmemory
      dlq_rate_threshold: 1              # messages/min (alert if > 0 in steady state)
      saga_timeout_threshold: 10         # active sagas past timeout

  # ── Admin API ─────────────────────────────────────────────────────────────────
  admin:
    port: 8080
    auth:
      type: bearer                       # none | bearer | mtls
      token: "${secret:env:ADMIN_TOKEN}"
    endpoints:
      - GET  /health/live
      - GET  /health/ready
      - GET  /health/started
      - GET  /patterns
      - POST /patterns/{name}/pause
      - POST /patterns/{name}/resume
      - POST /patterns/{name}/replay
      - GET  /lag
      - GET  /registry
      - GET  /dlq/{topic}
      - POST /dlq/{topic}/replay
      - DELETE /dlq/{topic}
      - POST /config/reload
      - GET  /metrics/cardinality
```

### 18.2 Environment Variable Overrides

Any scalar YAML field can be overridden via an environment variable using the convention:

```
TRIAD_<YAML_PATH_UPPERCASED_WITH_UNDERSCORES>
```

| YAML Path | Environment Variable | Type |
|-----------|---------------------|------|
| `backends.postgres.primary` | `TRIAD_BACKENDS_POSTGRES_PRIMARY` | string |
| `backends.postgres.pool_size` | `TRIAD_BACKENDS_POSTGRES_POOL_SIZE` | int |
| `backends.kafka.brokers` | `TRIAD_BACKENDS_KAFKA_BROKERS` | JSON array |
| `backends.kafka.schema_registry` | `TRIAD_BACKENDS_KAFKA_SCHEMA_REGISTRY` | string |
| `backends.redis.url` | `TRIAD_BACKENDS_REDIS_URL` | string |
| `backends.redis.mode` | `TRIAD_BACKENDS_REDIS_MODE` | string |
| `delivery.default_guarantee` | `TRIAD_DELIVERY_DEFAULT_GUARANTEE` | string |
| `observability.tracing.sampling.default_rate` | `TRIAD_OBSERVABILITY_TRACING_SAMPLING_DEFAULT_RATE` | float |
| `observability.logging.level` | `TRIAD_OBSERVABILITY_LOGGING_LEVEL` | string |
| `admin.port` | `TRIAD_ADMIN_PORT` | int |
| `instance_id` | `TRIAD_INSTANCE_ID` | string |

Array and map fields provided via environment variables are parsed as JSON-encoded strings.

### 18.3 Secrets Management

Triad resolves `${secret:<provider>:<reference>}` tokens at startup. Values are never logged, exposed via metrics, or included in admin API responses.

| Provider | Syntax | Notes |
|----------|--------|-------|
| Environment variable | `${secret:env:PG_PASSWORD}` | K8s `Secret` mounted as env var |
| File | `${secret:file:/run/secrets/pg_password}` | K8s `Secret` mounted as file |
| HashiCorp Vault | `${secret:vault://secret/triad/pg#password}` | Requires `VAULT_ADDR` + `VAULT_TOKEN` |
| AWS Secrets Manager | `${secret:aws-ssm:/triad/prod/pg_password}` | Uses ambient AWS credential chain |

The admin API `/registry` endpoint redacts all config fields whose key contains `password`, `token`, `secret`, or `key`.

### 18.4 Validation Rules

Enforced at startup before any backend connections are attempted. Failures abort the Runner with a structured diagnostic message.

| Rule | Error |
|------|-------|
| `backends.postgres.wal_level` must equal `logical` (verified via `SHOW wal_level`) | `ConfigError: PG wal_level must be logical, got replica` |
| `backends.postgres.pool_size` ≤ PG `max_connections` − 10 | `ConfigError: pool_size exceeds PG max_connections budget` |
| `patterns[*].name` must be globally unique | `ConfigError: duplicate pattern name "orders_cdc"` |
| `patterns[type=saga].timeout` must be > sum of all step timeouts | `ConfigError: saga timeout must exceed total step timeouts` |
| `kafka.producer.transactional_id_prefix` must not be empty when `delivery.default_guarantee = exactly_once` | `ConfigError: transactional_id_prefix required for EOS` |
| `cold_start.default_strategy = kafka_replay` requires `kafka.consumer.auto_offset_reset = earliest` | `ConfigError: kafka_replay cold start requires auto_offset_reset=earliest` |
| `backends.redis.mode = cluster` requires ≥ 3 node URLs | `ConfigError: Redis cluster mode requires >= 3 nodes` |
| All `${secret:...}` references must resolve | `ConfigError: secret ${secret:env:PG_DSN} not found` |

### 18.5 Reloadable vs. Non-Reloadable Settings

`POST /config/reload` or `SIGHUP` triggers a hot reload. Only the following settings take effect without a Runner restart:

| Setting | Reloadable | Notes |
|---------|-----------|-------|
| `observability.logging.level` | ✅ | Takes effect on next log statement |
| `observability.tracing.sampling.*` | ✅ | Applied to new spans |
| `observability.alerts.*` thresholds | ✅ | Applied on next poll cycle |
| `circuit_breakers.failure_threshold` | ✅ | Resets rolling window on apply |
| `circuit_breakers.half_open_after` | ✅ | Applied on next state transition |
| `retry.*.max_attempts`, `max_delay` | ✅ | Applied to next retry chain |
| `patterns[type=webhook].retry.*` | ✅ | Applied to next delivery attempt |
| `patterns[type=feature_flag].propagation.interval` | ✅ | Applied to next poll tick |
| `backends.*.pool_size` (increase only) | ⚠️ | Increases immediate; decreases drain idle connections |
| `backends.postgres.primary` DSN | ❌ | Restart required (replication slot ownership) |
| `backends.kafka.brokers` | ❌ | Restart required (consumer group re-join) |
| `backends.redis.url` | ❌ | Restart required |
| `patterns[*].name` | ❌ | Restart required (Registry re-initialisation) |
| `delivery.default_guarantee` | ❌ | Restart required (transaction scope changes) |
| `cold_start.default_strategy` | ❌ | Only relevant at startup |

---

## 19. Test Suite Plan

### 19.1 Unit Tests — Mock Interfaces

All pattern modules accept dependencies through interfaces, enabling full mock injection with no real backends needed.

```mermaid
classDiagram
    class PGClient {
        <<interface>>
        +Query(ctx, sql, args) Rows
        +Exec(ctx, sql, args) Result
        +Begin(ctx) Tx
        +CopyFrom(ctx, table, cols, rows) int64
    }
    class KafkaProducer {
        <<interface>>
        +InitTransactions(ctx) error
        +BeginTransaction() error
        +Produce(msg) error
        +SendOffsetsToTransaction(offsets) error
        +CommitTransaction() error
        +AbortTransaction() error
    }
    class KafkaConsumer {
        <<interface>>
        +Poll(timeout) Message
        +CommitOffsets(offsets) error
        +Assign(partitions) error
        +Seek(partition, offset) error
    }
    class RedisClient {
        <<interface>>
        +SetNX(ctx, key, val, ttl) bool
        +Get(ctx, key) string
        +HSet(ctx, key, fields) error
        +Del(ctx, keys) error
        +ZAdd(ctx, key, members) error
        +Pipeline() RedisPipeline
    }
    class MockPGClient {
        +QueryFn Func
        +ExecFn Func
        +Calls map-string-int
        +AssertCalled(method, n) void
    }
    class MockKafkaProducer {
        +ProduceFn Func
        +CommitFn Func
        +AbortFn Func
        +ProducedMessages List
    }
    class MockRedisClient {
        +SetNXFn Func
        +Store map-string-string
        +SetNXCalls int
    }

    PGClient <|.. MockPGClient
    KafkaProducer <|.. MockKafkaProducer
    KafkaConsumer <|.. MockKafkaConsumer
    RedisClient <|.. MockRedisClient
```

**Example unit tests (Go pseudocode):**

```go
// OutboxRelay — happy path
func TestOutboxRelay_RelaysAllUnpublishedRows(t *testing.T) {
    pg    := &MockPGClient{rows: outboxRows(3)}  // 3 unpublished rows
    kafka := &MockKafkaProducer{produceErr: nil}
    relay := NewOutboxRelay(pg, kafka, defaultConfig())

    err := relay.Run(t.Context(), batchSize=10)

    require.NoError(t, err)
    assert.Equal(t, 3, kafka.Calls["Produce"])
    assert.Equal(t, 3, pg.Calls["MarkPublished"])
    assert.Equal(t, 1, kafka.Calls["CommitTransaction"])
    assert.Equal(t, 0, kafka.Calls["AbortTransaction"])
}

// InboxConsumer — duplicate skipped via Redis NX
func TestInboxConsumer_SkipsDuplicateEvent(t *testing.T) {
    redis := &MockRedisClient{setNXResult: false}  // NX=false → duplicate
    pg    := &MockPGClient{}
    kafka := &MockKafkaConsumer{}
    inbox := NewInboxConsumer(redis, pg, kafka, defaultConfig())

    err := inbox.Handle(t.Context(), testEvent("evt-123"))

    require.NoError(t, err)
    assert.Equal(t, 0, pg.Calls["Exec"])           // no PG write
    assert.Equal(t, 1, kafka.Calls["CommitOffsets"]) // offset committed (skip)
    assert.Equal(t, 1, redis.SetNXCalls)            // one dedup check
}

// SagaOrchestrator — step timeout triggers compensation
func TestSaga_StepTimeoutTriggersCompensation(t *testing.T) {
    redis := &MockRedisClient{
        hmgetResult: sagaStateMap("step1_pending", startedAt: 11*time.Minute ago),
    }
    kafka := &MockKafkaProducer{}
    pg    := &MockPGClient{}
    clock := &MockClock{now: startTime.Add(11 * time.Minute)}
    saga  := NewSagaOrchestrator(redis, kafka, pg, clock, sagaConfig(timeout: 10*min))

    err := saga.Tick(t.Context())

    require.NoError(t, err)
    assert.Equal(t, 1, kafka.Calls["Produce"])          // compensation command
    assert.Equal(t, "compensating", redis.Store["saga:test-id:state"])
}

// CircuitBreaker — transitions to OPEN after threshold
func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
    cb := NewCircuitBreaker(config(threshold: 5, window: 60*time.Second))

    for i := 0; i < 5; i++ {
        cb.RecordFailure()
    }

    assert.Equal(t, StateOpen, cb.State())
    assert.Equal(t, ErrCircuitOpen, cb.Allow())
}

// ExactlyOnceCoordinator — duplicate short-circuits via Redis NX
func TestEOSCoordinator_DeduplicatesDuplicate(t *testing.T) {
    redis := &MockRedisClient{setNXResult: false}
    kafka := &MockKafkaProducer{}
    pg    := &MockPGClient{}
    coord := NewEOSCoordinator(redis, kafka, pg, defaultConfig())

    err := coord.Process(t.Context(), testEvent("evt-dup-123"))

    require.NoError(t, err)
    assert.Equal(t, 0, kafka.Calls["BeginTransaction"])
    assert.Equal(t, 0, pg.Calls["Exec"])
    assert.Equal(t, 1, kafka.Calls["CommitOffsets"]) // skip: offset committed
}
```

### 19.2 Unit Test Matrix

| Module | Scenario | Mock Behaviour | Expected Outcome |
|--------|----------|---------------|-----------------|
| OutboxRelay | Happy path | PG returns 3 rows, Kafka OK | 3 produces, 3 marks, 1 transaction committed |
| OutboxRelay | Kafka produce fails once (transient) | Kafka errors then OK | Retry once, success, no DLQ |
| OutboxRelay | Kafka max retries exceeded | Kafka always errors | DLQ write, `triad_dlq_messages_total++` |
| OutboxRelay | No unpublished rows | PG returns empty set | No Kafka calls, no error |
| InboxConsumer | First delivery | Redis NX=true | PG write, offset committed |
| InboxConsumer | Duplicate delivery | Redis NX=false | No PG write, offset committed, `inbox_dedup_total{outcome=rejected}++` |
| InboxConsumer | PG write fails | PG tx returns error | Redis NX released, offset not committed, error returned |
| InboxConsumer | Redis NX unavailable | Redis returns error | Fall back to PG `INSERT ON CONFLICT DO NOTHING` |
| SagaOrchestrator | Happy path — 2 steps | All step replies arrive in time | `completed` state, PG saga audit row written |
| SagaOrchestrator | Step 1 timeout | No reply within timeout | `compensating` state, compensation command published |
| SagaOrchestrator | Compensation step fails | Compensation Kafka send errors | Retry compensation, error counter incremented |
| SagaOrchestrator | Redis state lost mid-saga | Redis HMGET returns nil | Replay from Kafka saga journal topic |
| CacheSync | INSERT event | — | Redis HSET with configured TTL |
| CacheSync | UPDATE event | — | Redis HSET (overwrites existing key) |
| CacheSync | DELETE event | — | Redis DEL |
| CacheSync | DDL schema change event | — | Redis SCAN + DEL all matching keys |
| CacheSync | Redis write fails | Redis HSET errors | Retry, `triad_error_total++` |
| EOS Coordinator | New message | Redis NX=true, Kafka=OK, PG=OK | All committed; `noop_dup` counter unchanged |
| EOS Coordinator | Duplicate message | Redis NX=false | Offset committed; no Kafka/PG writes; `noop_dup++` |
| EOS Coordinator | Kafka commitTransaction fails | `commitTransaction` returns error | `abortTransaction` called; re-queue for re-consume |
| EOS Coordinator | PG offset commit fails | PG tx returns error | Kafka `abortTransaction`; message re-consumed |
| CircuitBreaker | Threshold not yet reached | 4 failures / 60 s (threshold=5) | Stays CLOSED |
| CircuitBreaker | Threshold exceeded | 5 failures / 60 s | Transitions to OPEN; `cb_transitions_total++` |
| CircuitBreaker | Half-open probe succeeds | 1 success in HALF\_OPEN | Transitions to CLOSED |
| CircuitBreaker | Half-open probe fails | 1 failure in HALF\_OPEN | Transitions back to OPEN |
| FeatureFlagEvaluator | User in rollout bucket | `md5(flag:user) % 100 < rollout%` | Returns `true` |
| FeatureFlagEvaluator | User outside rollout bucket | `md5(flag:user) % 100 >= rollout%` | Returns `false` |
| FeatureFlagEvaluator | Redis cache miss | Redis HGET returns nil | Falls back to PG, repopulates Redis |
| RateLimiter | Under limit | Window count < limit | Returns allowed=true |
| RateLimiter | At limit | Window count = limit | Returns allowed=false, violation event emitted |
| RateLimiter | Sliding window expiry | Old entries pruned via ZREMRANGEBYSCORE | Only recent window entries counted |
| WebhookDispatcher | Successful delivery | HTTP 200 | Delivery log row: `status=delivered`, no retry |
| WebhookDispatcher | HTTP 500 (retryable) | HTTP 500 once | Retry scheduled with backoff delay |
| WebhookDispatcher | Circuit breaker open | CB state=OPEN | Fail fast, no HTTP call, `cb_state=1` |
| WebhookDispatcher | HMAC signature | Any payload | `X-Triad-Signature` header matches `HMAC-SHA256(secret, body)` |

### 19.3 Integration Tests — Docker Compose

**Test environment (`docker-compose.test.yml`):**

```yaml
version: "3.9"

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: triad_test
      POSTGRES_USER: triad
      POSTGRES_PASSWORD: triad_test_secret
    command: >
      postgres
        -c wal_level=logical
        -c max_replication_slots=10
        -c max_wal_senders=10
        -c log_min_duration_statement=100
    ports: ["5432:5432"]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U triad -d triad_test"]
      interval: 5s
      timeout: 5s
      retries: 10

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
      KAFKA_LOG_RETENTION_HOURS: 1
    ports: ["9092:9092"]
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 10

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    depends_on:
      kafka: { condition: service_healthy }
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: "kafka:9092"
    ports: ["8081:8081"]
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/subjects"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    command: redis-server --appendonly yes --loglevel notice
    ports: ["6379:6379"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 10

  triad-runner:
    build:
      context: .
      target: test
    depends_on:
      postgres:        { condition: service_healthy }
      kafka:           { condition: service_healthy }
      schema-registry: { condition: service_healthy }
      redis:           { condition: service_healthy }
    environment:
      TRIAD_BACKENDS_POSTGRES_PRIMARY: "postgresql://triad:triad_test_secret@postgres:5432/triad_test"
      TRIAD_BACKENDS_KAFKA_BROKERS: '["kafka:9092"]'
      TRIAD_BACKENDS_KAFKA_SCHEMA_REGISTRY: "http://schema-registry:8081"
      TRIAD_BACKENDS_REDIS_URL: "redis://redis:6379"
      TRIAD_BACKENDS_REDIS_MODE: standalone
      TRIAD_OBSERVABILITY_LOGGING_LEVEL: debug
      TRIAD_DELIVERY_DEFAULT_GUARANTEE: exactly_once
    volumes:
      - ./testdata/triad.test.yaml:/etc/triad/triad.yaml:ro
    ports:
      - "8080:8080"   # admin API
      - "9090:9090"   # Prometheus metrics
```

**Integration test scenarios:**

| # | Scenario | Precondition | Action | Assertion | Deadline |
|---|----------|-------------|--------|-----------|---------|
| 1 | Outbox → Kafka relay | Clean outbox table | INSERT row into `outbox` via PG | Kafka consumer receives matching `OrderCreated` on `domain.events` | 2 s |
| 2 | CDC → Kafka → Redis cache sync | Triad runner healthy | UPDATE row in `users` table | Redis `HGET user:{id} email` equals new value | 1 s |
| 3 | Inbox dedup — exactly-once | — | Produce `event-abc` to `domain.events` twice | PG `inbox` table has exactly 1 row for `event-abc`; Kafka lag = 0; `inbox_dedup_total{outcome=rejected}` = 1 | 3 s |
| 4 | Saga happy path — 2 steps | Saga definition registered | Publish `OrderPlaced` event; simulate both step replies | `saga_audits` row: `status=completed`; Redis saga key deleted; `triad_saga_completed_total{outcome=completed}` = 1 | 5 s |
| 5 | Saga compensation — step 2 fails | Saga definition registered | Publish `OrderPlaced`; step 2 reply: `FAILED` | Compensation event on `inventory.commands`; `saga_audits` row: `status=rolled_back` | 5 s |
| 6 | Cold start — Strategy A (PG snapshot) | Flush Redis; 1 000 rows in `users` PG table | `POST /patterns/user_cache_sync/replay` | 1 000 Redis keys `user:{id}` present; `triad_cold_start_records_total` = 1 000 | 30 s |
| 7 | Feature flag propagation | — | INSERT flag row in PG with `rollout_percent=50` | Within 2 s: Redis `HGET flag:my_flag rollout_percent` = `50`; evaluator returns `true` for ~50% of 1 000 deterministic test user IDs | 3 s |
| 8 | Webhook delivery with retry | Mock HTTP server: 500, 500, 200 | Produce `OrderCreated` event | Delivery log: 3 attempts, final `status=delivered`; `triad_webhook_delivery_attempts_total{status_class="5xx"}` = 2 | 30 s |
| 9 | Circuit breaker — Redis failure | — | Inject Redis connection errors (> threshold=5 in 60 s) | `triad_circuit_breaker_state{backend="redis"}` = 1 (OPEN); reads served from PG fallback | 10 s |
| 10 | Circuit breaker — Kafka failure | — | Stop Kafka mid-test; resume after 60 s | Events buffer in outbox table; CB transitions CLOSED → OPEN → CLOSED after recovery; zero data loss | 90 s |
| 11 | EOS end-to-end dedup | — | Process event `evt-dup-123` twice in same dedup window | PG `processed_events` has 1 row; `triad_eos_txn_total{outcome="noop_dup"}` = 1 | 3 s |
| 12 | Schema evolution — ADD COLUMN | — | `ALTER TABLE users ADD COLUMN phone VARCHAR` | Subsequent CDC events include `phone` field; consumers without schema awareness continue processing (forward compat) | 2 s |

**Test runner invocation:**

```bash
# Start test environment
docker compose -f docker-compose.test.yml up -d --wait

# Run integration tests
go test ./test/integration/... \
  -v -timeout 300s \
  -run TestIntegration \
  -tags integration

# Tear down (volumes removed to guarantee clean state)
docker compose -f docker-compose.test.yml down -v
```

### 19.4 Load Tests — Scenarios and PromQL Assertions

**Tool:** [k6](https://k6.io) with the `xk6-kafka` extension for direct Kafka message injection. Prometheus collects metrics during load; PromQL assertions are evaluated 2 minutes after the load window ends (allowing metric scrapes to settle).

**Scenario 1 — Sustained outbox throughput (10 K events/s for 60 s):**

```javascript
// k6: outbox_throughput.js
import { check } from 'k6';
import http from 'k6/http';

export const options = {
  scenarios: {
    outbox_load: {
      executor: 'constant-arrival-rate',
      rate: 10000,
      timeUnit: '1s',
      duration: '60s',
      preAllocatedVUs: 200,
      maxVUs: 500,
    },
  },
  thresholds: {
    http_req_duration: ['p(99)<100'],   // 99th percentile < 100 ms
    http_req_failed:   ['rate<0.001'],  // error rate < 0.1%
  },
};

export default function () {
  const res = http.post('http://app:8000/orders', JSON.stringify({
    order_id: `order-${__VU}-${__ITER}`,
    amount: 99.99,
    idempotency_key: `idem-${__VU}-${__ITER}`,
  }), { headers: { 'Content-Type': 'application/json' } });
  check(res, { 'status 201': (r) => r.status === 201 });
}
```

**Scenario 2 — Saga throughput (1 K concurrent sagas for 30 s):**

```javascript
// k6: saga_throughput.js
export const options = {
  scenarios: {
    saga_load: {
      executor: 'constant-arrival-rate',
      rate: 1000,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 100,
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<500'],
  },
};
```

**Scenario 3 — Cache hit ratio under read load (5 K reads/s for 60 s):**

```javascript
// k6: cache_read.js — issues GET /orders/{id} after outbox_throughput.js has run
export const options = {
  scenarios: {
    read_load: {
      executor: 'constant-arrival-rate',
      rate: 5000,
      timeUnit: '1s',
      duration: '60s',
      preAllocatedVUs: 100,
    },
  },
  thresholds: {
    http_req_duration: ['p(99)<10'],   // cache hits should be < 10 ms
  },
};
```

**PromQL assertions (evaluated at T+2 min after load end):**

```promql
# ── Throughput ────────────────────────────────────────────────────────────────

# Average throughput during load window (target: ≥ 9 500 events/s)
avg_over_time(
  sum(rate(triad_pipeline_events_total{status="ok"}[1m]))[60s:5s]
) >= 9500

# ── Latency ───────────────────────────────────────────────────────────────────

# P99 processing latency (target: < 100 ms)
histogram_quantile(0.99,
  sum by (le) (rate(triad_pipeline_processing_duration_seconds_bucket[5m]))
) < 0.1

# P99 outbox relay latency (target: < 500 ms)
histogram_quantile(0.99,
  sum by (le) (rate(triad_outbox_relay_duration_seconds_bucket[5m]))
) < 0.5

# P99 Redis operation latency (target: < 5 ms)
histogram_quantile(0.99,
  sum by (le) (rate(triad_redis_op_duration_seconds_bucket[5m]))
) < 0.005

# ── Correctness ───────────────────────────────────────────────────────────────

# Pipeline error rate (target: < 0.1%)
sum(rate(triad_error_total[5m]))
  /
sum(rate(triad_pipeline_events_total[5m]))
< 0.001

# EOS transaction abort rate (target: < 0.01%)
rate(triad_eos_txn_total{outcome="aborted"}[5m])
  /
rate(triad_eos_txn_total[5m])
< 0.0001

# DLQ messages during load (target: 0)
increase(triad_dlq_messages_total[5m]) == 0

# ── Consumer lag ──────────────────────────────────────────────────────────────

# Kafka consumer lag at end of load (target: < 1 000 messages)
max(triad_kafka_consumer_lag_messages) < 1000

# Outbox pending rows at end of load (target: < 100)
sum(triad_outbox_pending_total) < 100

# ── Cache efficiency ──────────────────────────────────────────────────────────

# Cache hit ratio under read load (target: > 95%)
min_over_time(triad_cache_hit_ratio[5m]) > 0.95

# Redis memory pressure (target: < 80% of maxmemory)
triad_redis_memory_used_bytes
  /
redis_memory_max_bytes
< 0.80

# ── Saga ──────────────────────────────────────────────────────────────────────

# P95 saga step latency (target: < 2 s per step)
histogram_quantile(0.95,
  sum by (le, saga_name) (rate(triad_saga_step_duration_seconds_bucket[5m]))
) < 2

# No saga timeouts during load (target: 0)
sum(rate(triad_saga_completed_total{outcome="timeout"}[5m])) == 0

# ── Webhook ───────────────────────────────────────────────────────────────────

# Webhook delivery success rate (target: > 99%)
sum(rate(triad_webhook_delivery_attempts_total{status_class="2xx"}[5m]))
  /
sum(rate(triad_webhook_delivery_attempts_total[5m]))
> 0.99

# ── Infrastructure health ─────────────────────────────────────────────────────

# No circuit breakers opened during load (target: all CLOSED)
max_over_time(triad_circuit_breaker_state[5m]) == 0

# No backpressure activated during load
max_over_time(triad_backpressure_active[5m]) == 0

# Connection pool not saturated (target: < 90% active)
max(
  triad_conn_pool_active
    /
  (triad_conn_pool_active + triad_conn_pool_idle)
) < 0.9
```

**Prometheus recording rules for standing dashboards:**

```yaml
groups:
  - name: triad_slo
    interval: 15s
    rules:
      - record: triad:pipeline_throughput:rate1m
        expr: sum by (pattern_name) (rate(triad_pipeline_events_total{status="ok"}[1m]))

      - record: triad:pipeline_error_rate:rate5m
        expr: |
          sum by (pattern_name) (rate(triad_error_total[5m]))
            /
          sum by (pattern_name) (rate(triad_pipeline_events_total[5m]))

      - record: triad:cache_hit_ratio:rate5m
        expr: |
          sum by (pattern_name) (rate(triad_cache_hit_total[5m]))
            /
          (
            sum by (pattern_name) (rate(triad_cache_hit_total[5m]))
              +
            sum by (pattern_name) (rate(triad_cache_miss_total[5m]))
          )

      - record: triad:kafka_lag:max
        expr: max by (pattern_name, topic) (triad_kafka_consumer_lag_messages)

      - record: triad:eos_abort_rate:rate5m
        expr: |
          rate(triad_eos_txn_total{outcome="aborted"}[5m])
            /
          rate(triad_eos_txn_total[5m])
```

---

## 20. Durability Model

Durability in Triad is not a single mechanism — it is the composition of backend-native persistence guarantees with per-mode crash recovery and a shared checkpoint table.

### 20.1 Durability Matrix

| Component | Durable store | Persistence mechanism | Recovery on restart |
|-----------|--------------|----------------------|---------------------|
| CDC / WAL position | PostgreSQL | Logical replication slot (`pg_replication_slots`) | Slot persists across restarts; resume from stored LSN |
| Outbox messages | PostgreSQL | `triad_outbox` table rows (`relay_status = 'pending'`) | Re-relay all `pending` rows on startup; published rows cleaned up by background reaper |
| Kafka consumer offsets | Kafka | `__consumer_offsets` internal topic | Consumer group resumes from last committed offset |
| Inbox dedup keys | Redis + PostgreSQL | Redis SETNX (hot) + `triad_inbox` table (durable) | Redis miss → fallback check `triad_inbox` table |
| Saga step state | Redis (hot) + PostgreSQL (drain) | In-flight: Redis HSET; on drain/crash: `triad_saga_checkpoints` | Resume from `triad_saga_checkpoints` on restart |
| Write-behind cache | Redis | AOF (`appendonly yes`) + async flush to PG | Redis AOF replay on Redis restart; PG flush retried |
| Feature flag state | Redis + PostgreSQL | Redis cache (TTL) + `feature_flags` PG table (source of truth) | Cache miss → reload from PG `feature_flags` table |
| Checkpoint metadata | PostgreSQL | `triad_checkpoints` table | Read on startup to restore LSN + Kafka offset watermarks |

**Outbox reaper:** A background goroutine (part of the Outbox module, runs on the leader in Mode 3) periodically deletes published outbox rows to prevent unbounded table growth:

```sql
DELETE FROM triad_outbox
 WHERE relay_status = 'published'
   AND published_at < now() - INTERVAL '7 days';
```

The retention window is configurable:

```yaml
patterns:
  - name: order-outbox
    type: outbox
    outbox_retention: "7d"   # default; set to 0 to disable reaper
```

### 20.2 Crash Recovery Per Mode

| Mode | Crash scenario | Recovery mechanism | Re-delivery risk |
|------|---------------|-------------------|-----------------|
| Mode 1 — In-Process | App process crash | Application's own restart (OS, supervisor, or PaaS) | At-least-once for CDC and outbox; exactly-once via inbox dedup |
| Mode 1 — In-Process | SIGTERM without drain | Incomplete drain; resume from last checkpoint | Duplicate delivery possible until inbox dedup catches |
| Mode 2 — Standalone | Process crash | `systemd Restart=on-failure` (default 5s backoff) | At-least-once; dedup on restart |
| Mode 2 — Standalone | Host reboot | systemd `WantedBy=multi-user.target` restores on boot | Same as crash |
| Mode 3 — Kubernetes | Pod crash | K8s Deployment controller restarts pod (backoff: 10s→20s→40s→…→5m) | At-least-once; leader re-elected within `leaseDurationSeconds` (15s) |
| Mode 3 — Kubernetes | Leader pod crash | Lease expires after 15s; fastest remaining pod acquires Lease | WAL slot reattached by new leader; no slot data lost |
| Mode 3 — Kubernetes | Node eviction | PDB `maxUnavailable=1` ensures ≥2 pods always running | Rolling replacement; Kafka rebalance triggers partition reassignment |
| Mode 3 — Kubernetes | Rolling deploy | PDB + `RollingUpdate` maxUnavailable=0 | Zero-downtime: new pods ready before old pods terminate |

### 20.3 Graceful Shutdown Sequence

On receipt of SIGTERM (or `instance.Shutdown(ctx)` in Mode 1), Triad transitions through a defined drain sequence before exiting:

```mermaid
sequenceDiagram
    participant OS as OS / K8s
    participant Main as Main goroutine
    participant CDC as CDC module
    participant Outbox as Outbox module
    participant EOS as EOS / Kafka producer
    participant WB as Write-behind module
    participant Saga as Saga module
    participant CP as Checkpoint writer

    OS->>Main: SIGTERM
    Main->>Main: state = DRAINING\n(readiness probe → 503)
    par Drain all modules in parallel
        Main->>CDC: Stop() — advance LSN to current, release WAL slot if follower
        Main->>Outbox: Stop() — flush remaining pending rows to Kafka
        Main->>EOS: Stop() — commitTransaction() for open batch
        Main->>WB: Stop() — flush write-behind buffer to PostgreSQL
        Main->>Saga: Stop() — persist in-flight step state to triad_saga_checkpoints
    end
    CDC-->>Main: drained
    Outbox-->>Main: drained
    EOS-->>Main: drained
    WB-->>Main: drained
    Saga-->>Main: drained
    Main->>CP: UPDATE triad_checkpoints SET pg_lsn, kafka_offsets, updated_at
    CP-->>Main: committed
    Main->>OS: exit 0
    Note over Main,OS: If drain exceeds timeout (default 30s): log warning, exit 1
```

Drain timeout is configurable:

```yaml
shutdown:
  drain_timeout_seconds: 30   # grace period before forced exit
  warn_on_forced_exit: true
```

The readiness probe (`/health/ready`) returns `503` as soon as `DRAINING` state is entered, ensuring K8s removes the pod from Service endpoints before connections are dropped.

### 20.4 Checkpoint Tracking

All modules write their durable position to two PostgreSQL tables that survive restarts.

**`triad_checkpoints`** — per-pipeline position tracking:

```sql
CREATE TABLE triad_checkpoints (
    pattern_name      TEXT        NOT NULL,
    pipeline_name     TEXT        NOT NULL,
    owner_instance_id TEXT        NOT NULL,  -- pod name / process ID
    version           BIGINT      NOT NULL DEFAULT 0,  -- optimistic locking counter
    pg_lsn            PG_LSN,                -- last processed WAL LSN (CDC pipelines)
    kafka_offsets     JSONB,                 -- {"topic:partition": offset, ...}
    redis_watermark   BIGINT,                -- monotonic sequence for write-behind
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pattern_name, pipeline_name)
);

CREATE INDEX ON triad_checkpoints (owner_instance_id);
```

On startup, each instance reads the row for its assigned pipelines and resumes from the stored position. All checkpoint writes use optimistic locking via the `version` column:

```sql
UPDATE triad_checkpoints
   SET pg_lsn = $new_lsn, kafka_offsets = $new_offsets, version = version + 1,
       owner_instance_id = $my_id, updated_at = now()
 WHERE pattern_name = $p AND pipeline_name = $pl AND version = $known_version;
-- If 0 rows updated: another instance raced; re-read and abort or retry once.
```

A new owner claims a row only after the previous owner's lease has expired. The `version` column ensures two racing instances cannot both commit a checkpoint without one detecting the conflict.

**`triad_saga_checkpoints`** — per-saga step tracking:

```sql
CREATE TABLE triad_saga_checkpoints (
    saga_id          UUID        PRIMARY KEY,
    saga_name        TEXT        NOT NULL,
    current_step     INT         NOT NULL DEFAULT 0,
    state            JSONB       NOT NULL DEFAULT '{}',
    compensation_mode BOOLEAN    NOT NULL DEFAULT false,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON triad_saga_checkpoints (saga_name, updated_at DESC);
```

On restart, the saga module loads all non-terminal sagas from this table and re-enters the step loop from `current_step`. Idempotent step handlers (required by contract — see §8) ensure safe re-execution.

### 20.5 Disaster Recovery

| Failure scenario | Data at risk | RTO | RPO | Recovery action |
|-----------------|-------------|-----|-----|-----------------|
| PG primary failure (single node) | Outbox rows, checkpoints, saga state | Minutes (manual failover) | RPO = replication lag at time of failure | Promote PG replica; Triad reconnects; resumes from last checkpoint |
| PG primary failure (HA with Patroni/RDS Multi-AZ) | None (synchronous replica) | < 30s (automatic failover) | 0 (synchronous replication) | Automatic; Triad reconnects via DSN that resolves to new primary |
| Kafka broker failure (replication factor ≥ 3) | None (replicated partitions) | < 10s (leader re-election) | 0 (committed offsets safe) | Kafka handles internally; consumer group rebalances |
| Redis failure (standalone) | Write-behind buffer, saga hot state | Minutes (restart or failover) | Write-behind: last AOF sync (default 1s); saga: `triad_saga_checkpoints` | Redis restart replays AOF; saga resumes from PG checkpoint |
| Redis failure (Sentinel / Cluster) | Write-behind buffer only | < 30s (Sentinel promotion) | Last AOF sync | Sentinel elects replica; Triad reconnects automatically |
| Full cluster outage (all three backends) | Depends on checkpoint recency | Hours (restore from backups) | Last checkpoint + backup age | Restore PG from backup; replay Kafka from retained offsets; rebuild Redis from PG |

---

## 21. Observability Completeness

This section defines the full observability contract: SLOs, alert rules, health check endpoints, and audit logging. Metrics and OTel span inventory are in §15 and §17 respectively.

### 21.1 SLO Definitions

| SLO | Target | Burn rate alert threshold | Measurement window |
|-----|--------|--------------------------|-------------------|
| Pipeline availability | 99.9% | > 5× burn (5m window) | 30-day rolling |
| P99 end-to-end latency | < 100ms | P99 > 150ms for 5m | Per pipeline |
| Throughput | ≥ 10,000 events/s (aggregate) | < 9,500 events/s for 2m | 1-minute rate |
| Cache hit ratio | > 95% | < 92% for 5m | 5-minute rate |
| EOS transaction abort rate | < 0.01% | > 0.05% for 5m | 5-minute rate |
| Webhook delivery success | > 99% | < 98% for 10m | 10-minute rate |
| Saga completion (no timeout) | > 99.5% | < 99% for 10m | 10-minute rate |
| DLQ message count | = 0 (no new DLQ entries) | Any new message | Instant |

### 21.2 Alertmanager Rules

```yaml
# alerting-rules.yaml — load alongside prometheus.rules.yaml from §19
groups:
  - name: triad.alerts
    rules:

      - alert: TriadKafkaLagHigh
        expr: triad:kafka_lag:max > 10000
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Kafka consumer lag > 10,000 messages"
          description: >
            Pattern {{ $labels.pattern_name }} topic {{ $labels.topic }}
            has lag {{ $value }}. Check consumer throughput and Kafka broker health.

      - alert: TriadKafkaLagCritical
        expr: triad:kafka_lag:max > 100000
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka consumer lag > 100,000 messages — SLO at risk"
          description: >
            Pattern {{ $labels.pattern_name }} topic {{ $labels.topic }}
            lag {{ $value }} exceeds critical threshold. Immediate investigation required.

      - alert: TriadDLQMessagesNonZero
        expr: sum by (pattern_name) (triad_dlq_messages_total) > 0
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "Messages routed to DLQ"
          description: >
            Pattern {{ $labels.pattern_name }} has {{ $value }} DLQ message(s).
            Run `triad dlq list` to inspect. Root cause must be resolved before replay.

      - alert: TriadCircuitBreakerOpen
        expr: triad_circuit_breaker_state == 1   # 1 = OPEN (numeric encoding: 0=CLOSED, 1=OPEN, 2=HALF_OPEN)
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Circuit breaker OPEN for {{ $labels.backend }}"
          description: >
            Circuit breaker for backend {{ $labels.backend }} in pattern
            {{ $labels.pattern_name }} has been OPEN for > 1m.
            All requests are failing fast. Check backend connectivity.

      - alert: TriadEOSAbortRateHigh
        expr: triad:eos_abort_rate:rate5m > 0.0005
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "EOS transaction abort rate > 0.05%"
          description: >
            Pattern {{ $labels.pattern_name }} EOS abort rate {{ $value | humanizePercentage }}.
            Check for Kafka producer fencing events or coordinator failures.

      - alert: TriadSagaTimeoutHigh
        expr: |
          rate(triad_saga_step_total{outcome="timeout"}[10m])
            /
          rate(triad_saga_step_total[10m]) > 0.01
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Saga step timeout rate > 1%"
          description: >
            Saga {{ $labels.saga_name }} step timeout rate {{ $value | humanizePercentage }}.
            Check downstream service latency and saga step timeout configuration.

      - alert: TriadPGReplicationLagHigh
        expr: triad_pg_replication_lag_bytes > 104857600   # 100 MiB
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "PG replication slot lag > 100 MiB"
          description: >
            Replication slot {{ $labels.slot_name }} has {{ $value | humanize1024 }}B of lag.
            CDC is falling behind. Check WAL sender throughput and Triad CDC module status.

      - alert: TriadRedisMemoryHigh
        expr: triad_redis_memory_used_bytes / triad_redis_memory_max_bytes > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Redis memory usage > 85%"
          description: >
            Redis instance {{ $labels.instance }} is at
            {{ $value | humanizePercentage }} memory capacity.
            Review write-behind buffer size and TTL settings.

      - alert: TriadPipelineDown
        expr: up{job="triad-worker"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Triad worker instance down"
          description: >
            Triad worker {{ $labels.instance }} has been unreachable for > 1m.
            Check pod status and node health.
```

### 21.3 Health Check Contract

Three probe endpoints are served on the Admin HTTP port (default `:8080`). All responses are `application/json`.

**`GET /health/live`** — liveness probe.

Returns `200 OK` as long as the process is running and not deadlocked. Never returns `503` (if the process is dead, there is no response). K8s uses this to decide whether to kill and restart a pod.

```json
{
  "status": "alive",
  "uptime_seconds": 3721
}
```

**`GET /health/ready`** — readiness probe.

Returns `200 OK` when the instance is ready to serve work. Returns `503 Service Unavailable` during startup (before cold start completes), during drain (SIGTERM received), or when a required backend is unhealthy.

```json
{
  "status": "ready",
  "backends": {
    "postgres":        { "status": "ok", "latency_ms": 2 },
    "kafka":           { "status": "ok", "latency_ms": 5 },
    "redis":           { "status": "ok", "latency_ms": 1 },
    "schema_registry": { "status": "ok", "latency_ms": 3 }
  },
  "patterns": {
    "order-outbox":  { "status": "running", "lag": 0 },
    "user-cdc":      { "status": "running", "lag": 12 },
    "payment-saga":  { "status": "running", "in_flight": 3 }
  },
  "cold_start_complete": true,
  "drain_mode": false,
  "leader": true
}
```

Returns `503` body example during drain:

```json
{
  "status": "draining",
  "cold_start_complete": true,
  "drain_mode": true,
  "drain_started_at": "2026-04-24T10:30:00Z"
}
```

**`GET /health/started`** — startup probe.

Returns `200 OK` once the initialisation sequence is complete (backends connected, schema validated, cold start done). Returns `503` before that point. K8s uses this to delay liveness/readiness checks during slow cold starts.

```json
{
  "status": "started",
  "cold_start_complete": true,
  "patterns_loaded": 5,
  "startup_duration_ms": 4210
}
```

**Probe routing summary:**

| Probe | K8s usage | 200 when | 503 when |
|-------|-----------|----------|----------|
| `/health/live` | `livenessProbe` | Process is running | Never (dead = no response) |
| `/health/ready` | `readinessProbe` | Backends OK + cold start done + not draining | Backend down, cold start in progress, or draining |
| `/health/started` | `startupProbe` | Initialisation complete | Before init completes |

### 21.4 Structured Audit Log Schema

Every pattern operation that mutates durable state emits a structured audit event. Audit events are written to a dedicated Kafka topic (`triad.audit` by default, configurable) using the same Kafka producer as the pattern modules. Writing directly to Kafka — rather than mixing audit lines into the stdout log stream — guarantees delivery order, enables SIEM consumption without log-collector filtering, and avoids the collector becoming a single point of failure for audit retention.

```yaml
observability:
  audit:
    topic: "triad.audit"          # dedicated Kafka topic
    retention_ms: 2592000000      # 30 days (set on the topic, not here)
    include_operations:           # omit to audit all operations
      - outbox.relay
      - saga.step.execute
      - saga.step.compensate
      - dlq.produce
      - circuit_breaker.opened
```

The audit event schema (JSON, written as Kafka message value):

```json
{
  "log_type":      "audit",
  "schema_version": "1",
  "timestamp":     "2026-04-24T10:30:01.123456789Z",
  "trace_id":      "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id":       "00f067aa0ba902b7",
  "pattern_name":  "order-outbox",
  "pipeline_name": "orders-to-kafka",
  "operation":     "outbox.relay",
  "outcome":       "success",
  "event_id":      "evt-9f3a1b2c",
  "event_type":    "order.created",
  "kafka_topic":   "orders",
  "kafka_offset":  19284,
  "duration_ms":   3,
  "instance_id":   "triad-worker-6d8f9b-xk2pq"
}
```

**Standard `operation` values:**

| Operation | Trigger |
|-----------|---------|
| `outbox.relay` | Outbox row relayed to Kafka |
| `inbox.consume` | Inbox message processed (dedup check + handler) |
| `inbox.duplicate_skip` | Message skipped as duplicate |
| `cdc.event` | WAL event forwarded to Kafka |
| `saga.step.execute` | Saga step executed |
| `saga.step.compensate` | Saga compensation step executed |
| `saga.complete` | Saga reached terminal success state |
| `saga.failed` | Saga reached terminal failure state |
| `dlq.produce` | Message routed to DLQ |
| `dlq.replay` | DLQ message replayed to source topic |
| `circuit_breaker.opened` | Circuit breaker transitioned CLOSED → OPEN |
| `circuit_breaker.closed` | Circuit breaker transitioned HALF_OPEN → CLOSED |
| `pattern.paused` | Pipeline paused via Admin API or CLI |
| `pattern.resumed` | Pipeline resumed via Admin API or CLI |

The `triad.audit` topic must be configured with appropriate retention (`retention.ms`) and replication factor for compliance. SIEM systems (Splunk, Elastic) consume directly from the topic. For object-store archival, deploy a Kafka Connect S3 sink on the `triad.audit` topic. Operational logs (`log_type: "info"/"error"`) remain on stdout and are collected by the normal log pipeline — audit events never appear in stdout.

### 21.5 Dashboard Layout

Recommended Grafana dashboard structure. Each row corresponds to a Grafana row panel.

| Row | Panels | Key metrics |
|-----|--------|-------------|
| **Overview** | Throughput (events/s), Error rate (%), Active patterns, Leader status | `triad:pipeline_throughput:rate1m`, `triad:pipeline_error_rate:rate5m` |
| **Kafka** | Consumer lag (heatmap by topic), Lag trend (line), Partition distribution | `triad_kafka_consumer_lag_messages`, `triad:kafka_lag:max` |
| **PostgreSQL** | WAL replication slot lag (bytes), Outbox pending count, DB latency P99 | `triad_pg_replication_lag_bytes`, `triad_db_operation_duration_seconds` |
| **Redis** | Memory usage %, Write-behind queue depth, Cache hit ratio | `triad_redis_memory_used_bytes`, `triad:cache_hit_ratio:rate5m` |
| **Sagas** | In-flight saga count, Step duration P95 (bar by saga_name), Timeout rate | `triad_saga_active_total`, `triad_saga_step_duration_seconds` |
| **EOS** | Transaction commit rate, Abort rate, Fencing events | `triad_eos_txn_total`, `triad:eos_abort_rate:rate5m` |
| **Webhooks** | Delivery success rate, Retry rate, Latency P99 (by endpoint) | `triad_webhook_delivery_attempts_total`, `triad_webhook_delivery_duration_seconds` |
| **Circuit Breakers** | State heatmap (CLOSED/HALF_OPEN/OPEN per backend), State change events | `triad_circuit_breaker_state` |
| **DLQ** | DLQ message count by topic, DLQ production rate, Last DLQ timestamp | `triad_dlq_messages_total` |
| **Errors & SLOs** | SLO burn rate (30d window), Error budget remaining, Alert firing table | SLO recording rules + Alertmanager API |
| **OTel Traces** | Trace panel (Grafana Tempo datasource) — P99 span duration, error spans | Linked from histogram exemplars in Kafka/DB panels |

---
