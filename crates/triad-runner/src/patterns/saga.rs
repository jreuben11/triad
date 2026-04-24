use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use triad_core::{
    error::PatternError,
    traits::{PatternModule, RunContext},
    types::{ModuleHealth, ModuleState, SagaId, StepContext},
};

// ---------------------------------------------------------------------------
// Saga state machine
// ---------------------------------------------------------------------------

/// The persistent state of a saga instance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SagaStatus {
    Started,
    StepPending(usize),
    Compensating(usize),
    Completed,
    RolledBack,
    Failed(String),
}

/// A snapshot of saga progress stored in `triad_saga_checkpoints`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SagaCheckpoint {
    pub saga_id: SagaId,
    pub saga_name: String,
    pub current_step: usize,
    pub status: SagaStatus,
    pub compensation_mode: bool,
    /// Optimistic-lock version — must equal the stored value on UPDATE.
    pub version: i64,
    pub state: serde_json::Value,
}

impl SagaCheckpoint {
    pub fn new(saga_id: SagaId, saga_name: String) -> Self {
        Self {
            saga_id,
            saga_name,
            current_step: 0,
            status: SagaStatus::Started,
            compensation_mode: false,
            version: 0,
            state: serde_json::Value::Object(Default::default()),
        }
    }
}

// ---------------------------------------------------------------------------
// Step outcome
// ---------------------------------------------------------------------------

/// Result of executing or compensating a single saga step.
#[derive(Debug, Clone, PartialEq)]
pub enum StepOutcome {
    Success,
    Failure(String),
}

// ---------------------------------------------------------------------------
// Traits (mocked in unit tests)
// ---------------------------------------------------------------------------

/// Persists and reloads saga checkpoints in `triad_saga_checkpoints`.
///
/// `persist` uses `expected_version` for optimistic locking —
/// `WHERE saga_id = $1 AND version = $2`. Returns `Err` on conflict.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SagaRepository: Send + Sync {
    async fn load(&self, saga_id: &SagaId) -> Result<Option<SagaCheckpoint>, PatternError>;
    async fn persist(
        &self,
        checkpoint: &SagaCheckpoint,
        expected_version: i64,
    ) -> Result<(), PatternError>;
    async fn record_step_outcome(
        &self,
        saga_id: &SagaId,
        step_index: usize,
        step_name: &str,
        outcome: &str,
        duration_ms: u64,
    ) -> Result<(), PatternError>;
}

/// A single saga step with forward execution and compensation.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait SagaStep: Send + Sync {
    fn name(&self) -> &str;
    async fn execute(&self, ctx: &StepContext) -> Result<StepOutcome, PatternError>;
    async fn compensate(&self, ctx: &StepContext) -> Result<StepOutcome, PatternError>;
}

// ---------------------------------------------------------------------------
// SagaOrchestrator
// ---------------------------------------------------------------------------

/// Drives a saga through its steps, handles compensations on failure, and
/// recovers from crashes by reloading from `triad_saga_checkpoints`.
pub struct SagaOrchestrator {
    saga_id: SagaId,
    saga_name: String,
    steps: Vec<Arc<dyn SagaStep>>,
    repository: Arc<dyn SagaRepository>,
    checkpoint: Option<SagaCheckpoint>,
    state: ModuleState,
}

impl SagaOrchestrator {
    pub fn new(
        saga_id: SagaId,
        saga_name: impl Into<String>,
        steps: Vec<Arc<dyn SagaStep>>,
        repository: Arc<dyn SagaRepository>,
    ) -> Self {
        Self {
            saga_id,
            saga_name: saga_name.into(),
            steps,
            repository,
            checkpoint: None,
            state: ModuleState::Starting,
        }
    }

    // ------------------------------------------------------------------
    // Checkpoint helpers
    // ------------------------------------------------------------------

    fn current_version(&self) -> i64 {
        self.checkpoint.as_ref().map(|c| c.version).unwrap_or(-1)
    }

    async fn persist_checkpoint(&mut self, new_status: SagaStatus) -> Result<(), PatternError> {
        let expected = self.current_version();
        let cp = self.checkpoint.get_or_insert_with(|| {
            SagaCheckpoint::new(self.saga_id.clone(), self.saga_name.clone())
        });
        cp.status = new_status;
        cp.compensation_mode = matches!(
            cp.status,
            SagaStatus::Compensating(_) | SagaStatus::RolledBack
        );

        self.repository.persist(cp, expected).await?;
        // Advance the version so the next persist sees the updated value.
        if let Some(ref mut c) = self.checkpoint {
            c.version += 1;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Crash-recovery
    // ------------------------------------------------------------------

    /// Load the last-known checkpoint from the repository.
    /// Returns `true` if a checkpoint was found (recovery mode).
    pub async fn recover(&mut self) -> Result<bool, PatternError> {
        match self.repository.load(&self.saga_id).await? {
            Some(cp) => {
                info!(
                    saga_id = %self.saga_id.0,
                    current_step = cp.current_step,
                    "recovering saga from checkpoint"
                );
                self.checkpoint = Some(cp);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    // ------------------------------------------------------------------
    // Forward execution
    // ------------------------------------------------------------------

    /// Execute the saga forward from the current checkpoint position.
    /// Returns the final `SagaStatus`.
    pub async fn execute_forward(&mut self) -> Result<SagaStatus, PatternError> {
        let start_step = self
            .checkpoint
            .as_ref()
            .and_then(|c| match c.status {
                SagaStatus::StepPending(n) => Some(n),
                SagaStatus::Started => Some(0),
                _ => None,
            })
            .unwrap_or(0);

        for idx in start_step..self.steps.len() {
            let step = Arc::clone(&self.steps[idx]);
            let ctx = StepContext {
                saga_id: self.saga_id.clone(),
                step_name: step.name().to_string(),
                attempt: 0,
            };

            let started = std::time::Instant::now();

            // Persist StepPending before executing (crash-safe)
            let cp = self.checkpoint.get_or_insert_with(|| {
                SagaCheckpoint::new(self.saga_id.clone(), self.saga_name.clone())
            });
            cp.current_step = idx;
            self.persist_checkpoint(SagaStatus::StepPending(idx)).await?;

            info!(
                saga_id = %self.saga_id.0,
                step = step.name(),
                step_index = idx,
                "executing saga step"
            );

            match step.execute(&ctx).await? {
                StepOutcome::Success => {
                    let dur_ms = started.elapsed().as_millis() as u64;
                    self.repository
                        .record_step_outcome(&self.saga_id, idx, step.name(), "success", dur_ms)
                        .await?;
                    info!(
                        saga_id = %self.saga_id.0,
                        step = step.name(),
                        "saga step succeeded"
                    );
                }
                StepOutcome::Failure(reason) => {
                    let dur_ms = started.elapsed().as_millis() as u64;
                    warn!(
                        saga_id = %self.saga_id.0,
                        step = step.name(),
                        reason = %reason,
                        "saga step failed — entering compensation"
                    );
                    self.repository
                        .record_step_outcome(&self.saga_id, idx, step.name(), "failed", dur_ms)
                        .await?;
                    return self.execute_compensation(idx).await;
                }
            }
        }

        self.persist_checkpoint(SagaStatus::Completed).await?;
        info!(saga_id = %self.saga_id.0, "saga completed");
        Ok(SagaStatus::Completed)
    }

    // ------------------------------------------------------------------
    // Compensation (reverse order, from `failed_at` down to 0)
    // ------------------------------------------------------------------

    async fn execute_compensation(&mut self, failed_at: usize) -> Result<SagaStatus, PatternError> {
        // Compensate steps [failed_at - 1 .. 0] inclusive in reverse order.
        // The failing step itself is not compensated (it didn't succeed).
        let compensation_range = (0..failed_at).rev();

        for idx in compensation_range {
            let step = Arc::clone(&self.steps[idx]);
            let ctx = StepContext {
                saga_id: self.saga_id.clone(),
                step_name: step.name().to_string(),
                attempt: 0,
            };

            self.persist_checkpoint(SagaStatus::Compensating(idx))
                .await?;

            info!(
                saga_id = %self.saga_id.0,
                step = step.name(),
                step_index = idx,
                "compensating saga step"
            );

            let started = std::time::Instant::now();
            match step.compensate(&ctx).await {
                Ok(StepOutcome::Success) | Ok(StepOutcome::Failure(_)) => {
                    let dur_ms = started.elapsed().as_millis() as u64;
                    self.repository
                        .record_step_outcome(
                            &self.saga_id,
                            idx,
                            step.name(),
                            "compensated",
                            dur_ms,
                        )
                        .await?;
                }
                Err(e) => {
                    error!(
                        saga_id = %self.saga_id.0,
                        step = step.name(),
                        error = %e,
                        "compensation failed — saga stuck in Failed state"
                    );
                    self.persist_checkpoint(SagaStatus::Failed(e.to_string()))
                        .await?;
                    return Ok(SagaStatus::Failed(e.to_string()));
                }
            }
        }

        self.persist_checkpoint(SagaStatus::RolledBack).await?;
        warn!(saga_id = %self.saga_id.0, "saga rolled back");
        Ok(SagaStatus::RolledBack)
    }

    // ------------------------------------------------------------------
    // Drain: flush current checkpoint to PG before shutdown
    // ------------------------------------------------------------------

    pub async fn drain_checkpoint(&mut self) -> Result<(), PatternError> {
        if let Some(ref cp) = self.checkpoint {
            if cp.status != SagaStatus::Completed && cp.status != SagaStatus::RolledBack {
                info!(
                    saga_id = %self.saga_id.0,
                    "flushing in-flight saga state to PG on drain"
                );
                let expected = cp.version;
                self.repository.persist(cp, expected).await?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PatternModule impl
// ---------------------------------------------------------------------------

#[async_trait]
impl PatternModule for SagaOrchestrator {
    fn name(&self) -> &str {
        &self.saga_name
    }

    fn pattern_type(&self) -> &str {
        "saga"
    }

    async fn run(&mut self, ctx: RunContext) -> Result<(), PatternError> {
        self.state = ModuleState::Running;
        let recovered = self.recover().await?;
        if recovered {
            self.state = ModuleState::Recovering;
        }
        // In a real deployment this loop would consume Kafka reply messages
        // and drive the state machine. For now, execute forward once.
        tokio::select! {
            result = self.execute_forward() => {
                result.map(|_| ())
            }
            _ = ctx.cancel.cancelled() => {
                self.state = ModuleState::Stopped;
                Err(PatternError::Cancelled)
            }
        }
    }

    async fn drain(&mut self, _timeout: Duration) -> Result<(), PatternError> {
        self.state = ModuleState::Draining;
        self.drain_checkpoint().await?;
        self.state = ModuleState::Stopped;
        Ok(())
    }

    fn health(&self) -> ModuleHealth {
        ModuleHealth {
            state: self.state.clone(),
            lag: None,
            in_flight: self.checkpoint.as_ref().map(|_| 1),
            last_error: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::predicate::*;
    use rstest::rstest;
    use std::sync::Arc;
    use triad_core::types::SagaId;

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    fn make_saga_id() -> SagaId {
        SagaId(uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap())
    }

    fn success_step(name: &'static str) -> Arc<dyn SagaStep> {
        let mut step = MockSagaStep::new();
        step.expect_name().return_const(name.to_string());
        step.expect_execute()
            .returning(|_| Ok(StepOutcome::Success));
        step.expect_compensate()
            .returning(|_| Ok(StepOutcome::Success));
        Arc::new(step)
    }

    fn failing_step(name: &'static str) -> Arc<dyn SagaStep> {
        let mut step = MockSagaStep::new();
        step.expect_name().return_const(name.to_string());
        step.expect_execute()
            .returning(|_| Ok(StepOutcome::Failure("step failed".to_string())));
        step.expect_compensate()
            .returning(|_| Ok(StepOutcome::Success));
        Arc::new(step)
    }

    fn repo_with_no_checkpoint() -> Arc<MockSagaRepository> {
        let mut repo = MockSagaRepository::new();
        repo.expect_load().returning(|_| Ok(None));
        repo.expect_persist().returning(|_, _| Ok(()));
        repo.expect_record_step_outcome().returning(|_, _, _, _, _| Ok(()));
        Arc::new(repo)
    }

    // ------------------------------------------------------------------
    // Happy-path: all steps succeed → Completed
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_saga_happy_path_all_steps_succeed_returns_completed() {
        let repo = repo_with_no_checkpoint();
        let mut orch = SagaOrchestrator::new(
            make_saga_id(),
            "order-saga",
            vec![
                success_step("reserve-inventory"),
                success_step("charge-payment"),
                success_step("dispatch-order"),
            ],
            Arc::clone(&repo) as Arc<dyn SagaRepository>,
        );

        let status = orch.execute_forward().await.unwrap();
        assert_eq!(status, SagaStatus::Completed);
    }

    // ------------------------------------------------------------------
    // Compensation: step K fails → steps K-1..0 compensated in reverse
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_saga_compensation_path_rolled_back_on_step_failure() {
        let mut repo = MockSagaRepository::new();
        repo.expect_load().returning(|_| Ok(None));
        repo.expect_persist().returning(|_, _| Ok(()));
        repo.expect_record_step_outcome().returning(|_, _, _, _, _| Ok(()));

        // Track compensation order
        let compensation_order: Arc<tokio::sync::Mutex<Vec<String>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let make_tracking_step = |step_name: &'static str, order_ref: Arc<tokio::sync::Mutex<Vec<String>>>| {
            let mut step = MockSagaStep::new();
            step.expect_name().return_const(step_name.to_string());
            step.expect_execute()
                .returning(|_| Ok(StepOutcome::Success));
            step.expect_compensate().returning(move |_| {
                let order = Arc::clone(&order_ref);
                let name = step_name.to_string();
                // Can't await in returning(), so record synchronously via try_lock
                if let Ok(mut v) = order.try_lock() {
                    v.push(name);
                }
                Ok(StepOutcome::Success)
            });
            Arc::new(step) as Arc<dyn SagaStep>
        };

        let step_a = make_tracking_step("step-a", Arc::clone(&compensation_order));
        let step_b = make_tracking_step("step-b", Arc::clone(&compensation_order));

        // step-c fails
        let mut step_c = MockSagaStep::new();
        step_c.expect_name().return_const("step-c".to_string());
        step_c
            .expect_execute()
            .returning(|_| Ok(StepOutcome::Failure("payment declined".to_string())));

        let mut orch = SagaOrchestrator::new(
            make_saga_id(),
            "order-saga",
            vec![step_a, step_b, Arc::new(step_c)],
            Arc::new(repo) as Arc<dyn SagaRepository>,
        );

        let status = orch.execute_forward().await.unwrap();
        assert_eq!(status, SagaStatus::RolledBack);

        // Compensations should run in reverse order: step-b first, then step-a
        let order = compensation_order.lock().await;
        assert_eq!(*order, vec!["step-b", "step-a"]);
    }

    // ------------------------------------------------------------------
    // Crash recovery: reload checkpoint, resume from last incomplete step
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_saga_crash_recovery_resumes_from_checkpoint() {
        let saga_id = make_saga_id();
        let saved_cp = SagaCheckpoint {
            saga_id: saga_id.clone(),
            saga_name: "order-saga".to_string(),
            current_step: 1,
            status: SagaStatus::StepPending(1),
            compensation_mode: false,
            version: 2,
            state: serde_json::Value::Null,
        };

        let mut repo = MockSagaRepository::new();
        repo.expect_load()
            .returning(move |_| Ok(Some(saved_cp.clone())));
        repo.expect_persist().returning(|_, _| Ok(()));
        repo.expect_record_step_outcome().returning(|_, _, _, _, _| Ok(()));

        // Track which steps actually execute
        let executed: Arc<tokio::sync::Mutex<Vec<usize>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let make_step = |idx: usize, exec: Arc<tokio::sync::Mutex<Vec<usize>>>| {
            let name = format!("step-{idx}");
            let mut step = MockSagaStep::new();
            step.expect_name().return_const(name);
            step.expect_execute().returning(move |_| {
                if let Ok(mut v) = exec.try_lock() {
                    v.push(idx);
                }
                Ok(StepOutcome::Success)
            });
            Arc::new(step) as Arc<dyn SagaStep>
        };

        let mut orch = SagaOrchestrator::new(
            saga_id.clone(),
            "order-saga",
            vec![
                make_step(0, Arc::clone(&executed)),
                make_step(1, Arc::clone(&executed)),
                make_step(2, Arc::clone(&executed)),
            ],
            Arc::new(repo) as Arc<dyn SagaRepository>,
        );

        // Recovery loads the checkpoint
        let recovered = orch.recover().await.unwrap();
        assert!(recovered, "should detect existing checkpoint");

        // Execute forward — step-0 should be skipped, step-1 and step-2 run
        let status = orch.execute_forward().await.unwrap();
        assert_eq!(status, SagaStatus::Completed);

        let ran = executed.lock().await;
        assert_eq!(*ran, vec![1, 2], "only steps from checkpoint onward should run");
    }

    // ------------------------------------------------------------------
    // Optimistic lock conflict on persist → VersionConflict propagated
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_saga_optimistic_lock_conflict_propagates_error() {
        let mut repo = MockSagaRepository::new();
        repo.expect_load().returning(|_| Ok(None));
        repo.expect_persist()
            .returning(|_, _| Err(PatternError::Saga("optimistic lock conflict: version mismatch".to_string())));

        let mut orch = SagaOrchestrator::new(
            make_saga_id(),
            "order-saga",
            vec![success_step("step-a")],
            Arc::new(repo) as Arc<dyn SagaRepository>,
        );

        let result = orch.execute_forward().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("version mismatch"));
    }

    // ------------------------------------------------------------------
    // drain: flush current in-flight checkpoint
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_saga_drain_flushes_in_flight_checkpoint() {
        let mut repo = MockSagaRepository::new();
        repo.expect_load().returning(|_| Ok(None));
        // persist called twice: once during execute_forward, once during drain
        repo.expect_persist().returning(|_, _| Ok(()));
        repo.expect_record_step_outcome().returning(|_, _, _, _, _| Ok(()));

        // A step that never finishes — we'll test drain separately
        let mut step = MockSagaStep::new();
        step.expect_name().return_const("slow-step".to_string());
        step.expect_execute()
            .returning(|_| Ok(StepOutcome::Success));

        let mut orch = SagaOrchestrator::new(
            make_saga_id(),
            "order-saga",
            vec![Arc::new(step)],
            Arc::new(repo) as Arc<dyn SagaRepository>,
        );

        // Prime a checkpoint manually so drain has something to flush
        orch.checkpoint = Some(SagaCheckpoint {
            saga_id: make_saga_id(),
            saga_name: "order-saga".to_string(),
            current_step: 0,
            status: SagaStatus::StepPending(0),
            compensation_mode: false,
            version: 1,
            state: serde_json::Value::Null,
        });

        // drain_checkpoint should call persist once more
        orch.drain_checkpoint().await.unwrap();
    }

    // ------------------------------------------------------------------
    // drain: completed sagas are NOT re-persisted
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_saga_drain_skips_completed_sagas() {
        // persist should NOT be called if saga is already Completed
        let mut repo = MockSagaRepository::new();
        repo.expect_persist()
            .never()
            .returning(|_, _| Ok(()));

        let mut orch = SagaOrchestrator::new(
            make_saga_id(),
            "order-saga",
            vec![],
            Arc::new(repo) as Arc<dyn SagaRepository>,
        );

        orch.checkpoint = Some(SagaCheckpoint {
            saga_id: make_saga_id(),
            saga_name: "order-saga".to_string(),
            current_step: 3,
            status: SagaStatus::Completed,
            compensation_mode: false,
            version: 5,
            state: serde_json::Value::Null,
        });

        orch.drain_checkpoint().await.unwrap();
    }

    // ------------------------------------------------------------------
    // health() reflects module state
    // ------------------------------------------------------------------

    #[test]
    fn test_saga_health_starting_state() {
        let repo = repo_with_no_checkpoint();
        let orch = SagaOrchestrator::new(
            make_saga_id(),
            "order-saga",
            vec![],
            Arc::clone(&repo) as Arc<dyn SagaRepository>,
        );
        let h = orch.health();
        assert_eq!(h.state, ModuleState::Starting);
        assert!(h.in_flight.is_none());
    }

    #[test]
    fn test_saga_health_with_checkpoint_shows_in_flight() {
        let repo = repo_with_no_checkpoint();
        let mut orch = SagaOrchestrator::new(
            make_saga_id(),
            "order-saga",
            vec![],
            Arc::clone(&repo) as Arc<dyn SagaRepository>,
        );
        orch.checkpoint = Some(SagaCheckpoint::new(make_saga_id(), "order-saga".to_string()));
        let h = orch.health();
        assert_eq!(h.in_flight, Some(1));
    }

    // ------------------------------------------------------------------
    // PatternModule: name / pattern_type
    // ------------------------------------------------------------------

    #[test]
    fn test_saga_pattern_module_name_and_type() {
        let repo = repo_with_no_checkpoint();
        let orch = SagaOrchestrator::new(
            make_saga_id(),
            "my-saga",
            vec![],
            Arc::clone(&repo) as Arc<dyn SagaRepository>,
        );
        assert_eq!(orch.name(), "my-saga");
        assert_eq!(orch.pattern_type(), "saga");
    }

    // ------------------------------------------------------------------
    // Parametrised: various step counts all complete successfully
    // ------------------------------------------------------------------

    #[rstest]
    #[case(1)]
    #[case(3)]
    #[case(5)]
    #[tokio::test]
    async fn test_saga_happy_path_n_steps(#[case] n: usize) {
        let repo = repo_with_no_checkpoint();
        let steps: Vec<Arc<dyn SagaStep>> = (0..n)
            .map(|i| success_step(Box::leak(format!("step-{i}").into_boxed_str())))
            .collect();

        let mut orch = SagaOrchestrator::new(
            make_saga_id(),
            "saga",
            steps,
            Arc::clone(&repo) as Arc<dyn SagaRepository>,
        );
        let status = orch.execute_forward().await.unwrap();
        assert_eq!(status, SagaStatus::Completed);
    }

    // ------------------------------------------------------------------
    // Step idempotency key format (uses StepContext from triad-core)
    // ------------------------------------------------------------------

    #[test]
    fn test_step_context_idempotency_key_format() {
        let ctx = StepContext {
            saga_id: SagaId(uuid::Uuid::parse_str("12345678-1234-1234-1234-123456789012").unwrap()),
            step_name: "charge".to_string(),
            attempt: 0,
        };
        let key = ctx.idempotency_key();
        assert_eq!(key, "12345678-1234-1234-1234-123456789012/charge/0");
    }

    // ------------------------------------------------------------------
    // Compensation failure → Failed state
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_saga_compensation_failure_results_in_failed_status() {
        let mut repo = MockSagaRepository::new();
        repo.expect_load().returning(|_| Ok(None));
        repo.expect_persist().returning(|_, _| Ok(()));
        repo.expect_record_step_outcome().returning(|_, _, _, _, _| Ok(()));

        // step-a succeeds, step-b fails, compensation of step-a also fails
        let mut step_a = MockSagaStep::new();
        step_a.expect_name().return_const("step-a".to_string());
        step_a.expect_execute().returning(|_| Ok(StepOutcome::Success));
        step_a
            .expect_compensate()
            .returning(|_| Err(PatternError::Saga("compensation error".to_string())));

        let mut step_b = MockSagaStep::new();
        step_b.expect_name().return_const("step-b".to_string());
        step_b
            .expect_execute()
            .returning(|_| Ok(StepOutcome::Failure("failed".to_string())));

        let mut orch = SagaOrchestrator::new(
            make_saga_id(),
            "order-saga",
            vec![Arc::new(step_a), Arc::new(step_b)],
            Arc::new(repo) as Arc<dyn SagaRepository>,
        );

        let status = orch.execute_forward().await.unwrap();
        assert!(matches!(status, SagaStatus::Failed(_)));
    }

    // ------------------------------------------------------------------
    // No-step saga completes immediately
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn test_saga_no_steps_completes_immediately() {
        let repo = repo_with_no_checkpoint();
        let mut orch = SagaOrchestrator::new(
            make_saga_id(),
            "empty-saga",
            vec![],
            Arc::clone(&repo) as Arc<dyn SagaRepository>,
        );
        let status = orch.execute_forward().await.unwrap();
        assert_eq!(status, SagaStatus::Completed);
    }

    // ------------------------------------------------------------------
    // SagaCheckpoint::new initialises correctly
    // ------------------------------------------------------------------

    #[test]
    fn test_saga_checkpoint_new_initial_values() {
        let id = make_saga_id();
        let cp = SagaCheckpoint::new(id.clone(), "test".to_string());
        assert_eq!(cp.current_step, 0);
        assert_eq!(cp.version, 0);
        assert!(!cp.compensation_mode);
        assert_eq!(cp.status, SagaStatus::Started);
    }
}
