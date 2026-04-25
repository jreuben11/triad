use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::instrument;
use triad_core::error::PatternError;

/// Errors from aggregate persistence operations.
#[derive(Error, Debug)]
pub enum AggregateError {
    #[error("db: {0}")]
    Db(#[from] sqlx::Error),
    #[error("serialise: {0}")]
    Serialise(#[from] serde_json::Error),
    #[error("pattern: {0}")]
    Pattern(#[from] PatternError),
}

/// Core trait for event-sourced aggregates.
///
/// Implement this for your domain aggregate. State transitions happen only via
/// `apply` — the aggregate is the single source of truth for business rules.
pub trait Aggregate: Default + Clone + Send + Sync + 'static {
    /// The event type this aggregate handles.
    type Event: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static;

    /// The stable identifier for this aggregate type (used in the snapshot table).
    fn aggregate_type() -> &'static str;

    /// Apply a single event, mutating internal state.
    fn apply(&mut self, event: &Self::Event);

    /// Return the current version (number of events applied since creation).
    fn version(&self) -> u64;

    /// Set the version — called by `AggregateRoot` after replaying events.
    fn set_version(&mut self, version: u64);
}

/// Wrapper that tracks pending events and drives state rehydration.
///
/// ```ignore
/// let mut root = AggregateRoot::<MyAggregate>::new("order-123");
/// root.apply_new(MyEvent::OrderCreated { amount: 100 });
/// root.persist_snapshot(&pool, "triad_snapshots").await?;
/// ```
pub struct AggregateRoot<A: Aggregate> {
    pub id: String,
    pub state: A,
    pending_events: Vec<A::Event>,
}

impl<A: Aggregate> AggregateRoot<A> {
    /// Create a new root in its default (empty) state.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            state: A::default(),
            pending_events: Vec::new(),
        }
    }

    /// Reconstruct aggregate state by replaying a sequence of stored events.
    pub fn rehydrate(id: impl Into<String>, events: impl IntoIterator<Item = A::Event>) -> Self {
        let mut root = Self::new(id);
        for event in events {
            root.state.apply(&event);
        }
        root
    }

    /// Apply a new (not-yet-persisted) event to the aggregate and stage it.
    pub fn apply_new(&mut self, event: A::Event) {
        self.state.apply(&event);
        let v = self.state.version() + 1;
        self.state.set_version(v);
        self.pending_events.push(event);
    }

    /// Return the staged events and clear the internal queue.
    pub fn take_pending_events(&mut self) -> Vec<A::Event> {
        std::mem::take(&mut self.pending_events)
    }

    /// Persist a JSON snapshot of the full aggregate state to Postgres via UPSERT.
    ///
    /// Serializes the complete state (not just `{aggregate_id, version}`) so that
    /// `load_snapshot` can rehydrate the aggregate without replaying all events.
    #[instrument(skip(self, pool), fields(
        aggregate_type = A::aggregate_type(),
        aggregate_id = %self.id,
        version = self.state.version(),
    ))]
    pub async fn persist_snapshot(
        &self,
        pool: &sqlx::PgPool,
        snapshot_table: &str,
    ) -> Result<(), AggregateError>
    where
        A: serde::Serialize,
    {
        let snapshot_json = serde_json::to_value(&self.state)?;

        sqlx::query(&format!(
            "INSERT INTO {snapshot_table} \
             (aggregate_type, aggregate_id, version, snapshot, updated_at) \
             VALUES ($1, $2, $3, $4, NOW()) \
             ON CONFLICT (aggregate_type, aggregate_id) \
             DO UPDATE SET version = EXCLUDED.version, \
                           snapshot = EXCLUDED.snapshot, \
                           updated_at = EXCLUDED.updated_at"
        ))
        .bind(A::aggregate_type())
        .bind(&self.id)
        .bind(self.state.version() as i64)
        .bind(&snapshot_json)
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Load the most recent snapshot and rehydrate the aggregate state.
    ///
    /// Returns `None` if no snapshot exists for this aggregate. On success the
    /// returned root has its `state` fully deserialized and its version set from
    /// the stored row — no event replay is needed.
    pub async fn load_snapshot(
        pool: &sqlx::PgPool,
        snapshot_table: &str,
        aggregate_id: &str,
    ) -> Result<Option<Self>, AggregateError>
    where
        A: for<'de> serde::Deserialize<'de>,
    {
        let row: Option<(i64, serde_json::Value)> = sqlx::query_as(&format!(
            "SELECT version, snapshot FROM {snapshot_table} \
             WHERE aggregate_type = $1 AND aggregate_id = $2"
        ))
        .bind(A::aggregate_type())
        .bind(aggregate_id)
        .fetch_optional(pool)
        .await?;

        let Some((version, snapshot)) = row else {
            return Ok(None);
        };

        let mut state: A = serde_json::from_value(snapshot)?;
        state.set_version(version as u64);

        Ok(Some(Self {
            id: aggregate_id.to_string(),
            state,
            pending_events: Vec::new(),
        }))
    }
}

/// A snapshot row as loaded from Postgres.
#[derive(Debug, Clone)]
pub struct AggregateSnapshot {
    pub aggregate_id: String,
    pub version: u64,
    pub snapshot: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use serde::{Deserialize, Serialize};

    // ── Minimal test aggregate ───────────────────────────────────────────────

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct Counter {
        count: i64,
        version: u64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    enum CounterEvent {
        Incremented { by: i64 },
        Decremented { by: i64 },
        Reset,
    }

    impl Aggregate for Counter {
        type Event = CounterEvent;

        fn aggregate_type() -> &'static str {
            "counter"
        }

        fn apply(&mut self, event: &CounterEvent) {
            match event {
                CounterEvent::Incremented { by } => self.count += by,
                CounterEvent::Decremented { by } => self.count -= by,
                CounterEvent::Reset => self.count = 0,
            }
        }

        fn version(&self) -> u64 {
            self.version
        }

        fn set_version(&mut self, v: u64) {
            self.version = v;
        }
    }

    #[test]
    fn test_aggregate_root_starts_in_default_state() {
        let root = AggregateRoot::<Counter>::new("c1");
        assert_eq!(root.state.count, 0);
        assert_eq!(root.state.version(), 0);
        assert_eq!(root.id, "c1");
    }

    #[test]
    fn test_apply_new_updates_state_and_version() {
        let mut root = AggregateRoot::<Counter>::new("c1");
        root.apply_new(CounterEvent::Incremented { by: 5 });
        assert_eq!(root.state.count, 5);
        assert_eq!(root.state.version(), 1);
    }

    #[test]
    fn test_apply_new_stages_events() {
        let mut root = AggregateRoot::<Counter>::new("c1");
        root.apply_new(CounterEvent::Incremented { by: 3 });
        root.apply_new(CounterEvent::Incremented { by: 7 });

        let pending = root.take_pending_events();
        assert_eq!(pending.len(), 2);
        assert!(root.take_pending_events().is_empty());
    }

    #[rstest]
    #[case(vec![CounterEvent::Incremented { by: 10 }, CounterEvent::Decremented { by: 3 }], 7)]
    #[case(vec![CounterEvent::Incremented { by: 5 }, CounterEvent::Reset], 0)]
    #[case(vec![], 0)]
    fn test_rehydrate_replays_events(
        #[case] events: Vec<CounterEvent>,
        #[case] expected_count: i64,
    ) {
        let root = AggregateRoot::<Counter>::rehydrate("c1", events);
        assert_eq!(root.state.count, expected_count);
    }

    #[test]
    fn test_aggregate_type_name() {
        assert_eq!(Counter::aggregate_type(), "counter");
    }

    #[test]
    fn test_aggregate_snapshot_fields() {
        let snap = AggregateSnapshot {
            aggregate_id: "agg-1".to_string(),
            version: 42,
            snapshot: serde_json::json!({"count": 10}),
        };
        assert_eq!(snap.aggregate_id, "agg-1");
        assert_eq!(snap.version, 42);
    }

    #[test]
    fn test_apply_multiple_events_accumulate() {
        let mut root = AggregateRoot::<Counter>::new("c1");
        root.apply_new(CounterEvent::Incremented { by: 10 });
        root.apply_new(CounterEvent::Incremented { by: 5 });
        root.apply_new(CounterEvent::Decremented { by: 3 });
        assert_eq!(root.state.count, 12);
        assert_eq!(root.state.version(), 3);
    }

    #[test]
    fn test_reset_event_zeroes_count() {
        let mut root = AggregateRoot::<Counter>::new("c1");
        root.apply_new(CounterEvent::Incremented { by: 99 });
        root.apply_new(CounterEvent::Reset);
        assert_eq!(root.state.count, 0);
    }

    #[test]
    fn test_aggregate_error_from_serde_json_error() {
        let bad_json = "not valid";
        let err: Result<serde_json::Value, _> = serde_json::from_str(bad_json);
        let agg_err = AggregateError::from(err.unwrap_err());
        assert!(agg_err.to_string().contains("serialise"));
    }

    #[test]
    fn test_aggregate_error_from_pattern_error() {
        use triad_core::error::PatternError;
        let pe = PatternError::Cancelled;
        let agg_err = AggregateError::from(pe);
        assert!(agg_err.to_string().contains("pattern"));
    }

    #[test]
    fn test_rehydrate_sets_state_not_version() {
        // rehydrate applies events but does NOT set version (only apply_new does)
        let events = vec![
            CounterEvent::Incremented { by: 10 },
            CounterEvent::Incremented { by: 5 },
        ];
        let root = AggregateRoot::<Counter>::rehydrate("c1", events);
        assert_eq!(root.state.count, 15);
        // version stays 0 because rehydrate doesn't call set_version
        assert_eq!(root.state.version(), 0);
    }

    #[test]
    fn test_take_pending_events_clears_queue() {
        let mut root = AggregateRoot::<Counter>::new("c1");
        root.apply_new(CounterEvent::Incremented { by: 1 });
        root.apply_new(CounterEvent::Incremented { by: 2 });
        root.apply_new(CounterEvent::Incremented { by: 3 });
        let events = root.take_pending_events();
        assert_eq!(events.len(), 3);
        // Second call should return empty
        assert!(root.take_pending_events().is_empty());
    }

    #[test]
    fn test_apply_new_after_rehydrate() {
        let events = vec![CounterEvent::Incremented { by: 5 }];
        let mut root = AggregateRoot::<Counter>::rehydrate("c1", events);
        root.apply_new(CounterEvent::Incremented { by: 3 });
        assert_eq!(root.state.count, 8);
        assert_eq!(root.take_pending_events().len(), 1);
    }
}
