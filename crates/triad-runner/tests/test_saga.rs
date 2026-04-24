#![cfg(feature = "integration")]

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use common::containers::PgStack;
use sqlx::PgPool;
use triad_core::{
    error::PatternError,
    types::{SagaId, StepContext},
};
use triad_runner::patterns::saga::{
    SagaCheckpoint, SagaOrchestrator, SagaRepository, SagaStatus, SagaStep, StepOutcome,
};
use uuid::Uuid;

// ---------- inline PG-backed SagaRepository for integration tests ----------

struct PgSagaRepo {
    pool: PgPool,
}

#[async_trait]
impl SagaRepository for PgSagaRepo {
    async fn load(&self, saga_id: &SagaId) -> Result<Option<SagaCheckpoint>, PatternError> {
        let row: Option<(Uuid, String, i32, String, serde_json::Value, bool, i64)> =
            sqlx::query_as(
                "SELECT saga_id, saga_name, current_step, status, state, compensation_mode, version
                 FROM triad.triad_saga_checkpoints WHERE saga_id = $1",
            )
            .bind(saga_id.0)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| PatternError::Saga(e.to_string()))?;

        Ok(row.map(|(id, name, step, status_str, state, comp, ver)| {
            let status = match status_str.as_str() {
                "Completed" => SagaStatus::Completed,
                "RolledBack" => SagaStatus::RolledBack,
                s if s.starts_with("StepPending(") => {
                    let n = s
                        .trim_start_matches("StepPending(")
                        .trim_end_matches(')')
                        .parse()
                        .unwrap_or(0);
                    SagaStatus::StepPending(n)
                }
                s if s.starts_with("Compensating(") => {
                    let n = s
                        .trim_start_matches("Compensating(")
                        .trim_end_matches(')')
                        .parse()
                        .unwrap_or(0);
                    SagaStatus::Compensating(n)
                }
                s if s.starts_with("Failed(") => SagaStatus::Failed(
                    s.trim_start_matches("Failed(")
                        .trim_end_matches(')')
                        .to_string(),
                ),
                _ => SagaStatus::Started,
            };
            SagaCheckpoint {
                saga_id: SagaId(id),
                saga_name: name,
                current_step: step as usize,
                status,
                compensation_mode: comp,
                version: ver,
                state,
            }
        }))
    }

    async fn persist(
        &self,
        cp: &SagaCheckpoint,
        expected_version: i64,
    ) -> Result<(), PatternError> {
        let status_str = match &cp.status {
            SagaStatus::Started => "Started".to_string(),
            SagaStatus::StepPending(n) => format!("StepPending({n})"),
            SagaStatus::Compensating(n) => format!("Compensating({n})"),
            SagaStatus::Completed => "Completed".to_string(),
            SagaStatus::RolledBack => "RolledBack".to_string(),
            SagaStatus::Failed(msg) => format!("Failed({msg})"),
        };

        let rows_affected = if expected_version <= 0 {
            // First persist: INSERT with version=1 so the next UPDATE WHERE version=1 succeeds.
            // SagaOrchestrator starts cp.version=0 and calls persist(cp, expected=0) first.
            // After persist, it increments cp.version to 1. DB must hold 1 for next CAS UPDATE.
            sqlx::query(
                "INSERT INTO triad.triad_saga_checkpoints
                 (saga_id, saga_name, current_step, status, state, compensation_mode, version)
                 VALUES ($1, $2, $3, $4, $5, $6, 1)
                 ON CONFLICT (saga_id) DO NOTHING",
            )
            .bind(cp.saga_id.0)
            .bind(&cp.saga_name)
            .bind(cp.current_step as i32)
            .bind(&status_str)
            .bind(&cp.state)
            .bind(cp.compensation_mode)
            .execute(&self.pool)
            .await
            .map_err(|e| PatternError::Saga(e.to_string()))?
            .rows_affected()
        } else {
            sqlx::query(
                "UPDATE triad.triad_saga_checkpoints
                 SET current_step = $1, status = $2, state = $3,
                     compensation_mode = $4, version = version + 1, updated_at = now()
                 WHERE saga_id = $5 AND version = $6",
            )
            .bind(cp.current_step as i32)
            .bind(&status_str)
            .bind(&cp.state)
            .bind(cp.compensation_mode)
            .bind(cp.saga_id.0)
            .bind(expected_version)
            .execute(&self.pool)
            .await
            .map_err(|e| PatternError::Saga(e.to_string()))?
            .rows_affected()
        };

        if rows_affected == 0 {
            return Err(PatternError::Saga(
                "optimistic lock conflict: version mismatch".to_string(),
            ));
        }
        Ok(())
    }

    async fn record_step_outcome(
        &self,
        saga_id: &SagaId,
        step_index: usize,
        step_name: &str,
        outcome: &str,
        duration_ms: u64,
    ) -> Result<(), PatternError> {
        sqlx::query(
            "INSERT INTO triad.triad_saga_steps
             (saga_id, step_index, step_name, outcome, duration_ms)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(saga_id.0)
        .bind(step_index as i32)
        .bind(step_name)
        .bind(outcome)
        .bind(duration_ms as i64)
        .execute(&self.pool)
        .await
        .map_err(|e| PatternError::Saga(e.to_string()))?;
        Ok(())
    }
}

// ---------- test steps ----------

struct SuccessStep {
    name: String,
}

#[async_trait]
impl SagaStep for SuccessStep {
    fn name(&self) -> &str {
        &self.name
    }
    async fn execute(&self, _ctx: &StepContext) -> Result<StepOutcome, PatternError> {
        Ok(StepOutcome::Success)
    }
    async fn compensate(&self, _ctx: &StepContext) -> Result<StepOutcome, PatternError> {
        Ok(StepOutcome::Success)
    }
}

struct FailStep {
    name: String,
}

#[async_trait]
impl SagaStep for FailStep {
    fn name(&self) -> &str {
        &self.name
    }
    async fn execute(&self, _ctx: &StepContext) -> Result<StepOutcome, PatternError> {
        Ok(StepOutcome::Failure(
            "step failed intentionally".to_string(),
        ))
    }
    async fn compensate(&self, _ctx: &StepContext) -> Result<StepOutcome, PatternError> {
        Ok(StepOutcome::Success)
    }
}

// ---------- tests ----------

#[tokio::test]
async fn test_saga_happy_path_all_steps_succeed() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url).await.expect("connect");

    let repo: Arc<dyn SagaRepository> = Arc::new(PgSagaRepo { pool });
    let saga_id = SagaId(Uuid::new_v4());

    let steps: Vec<Arc<dyn SagaStep>> = vec![
        Arc::new(SuccessStep {
            name: "step1".into(),
        }),
        Arc::new(SuccessStep {
            name: "step2".into(),
        }),
        Arc::new(SuccessStep {
            name: "step3".into(),
        }),
    ];

    let mut orch = SagaOrchestrator::new(saga_id.clone(), "order-saga".to_string(), steps, repo);

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), orch.execute_forward())
        .await
        .expect("timeout")
        .expect("saga failed");

    assert_eq!(result, SagaStatus::Completed);
}

#[tokio::test]
async fn test_saga_compensation_path_on_step_failure() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url).await.expect("connect");

    let repo: Arc<dyn SagaRepository> = Arc::new(PgSagaRepo { pool });
    let saga_id = SagaId(Uuid::new_v4());

    let steps: Vec<Arc<dyn SagaStep>> = vec![
        Arc::new(SuccessStep {
            name: "step1".into(),
        }),
        Arc::new(FailStep {
            name: "step2-fails".into(),
        }),
        Arc::new(SuccessStep {
            name: "step3".into(),
        }),
    ];

    let mut orch = SagaOrchestrator::new(saga_id.clone(), "order-saga".to_string(), steps, repo);

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), orch.execute_forward())
        .await
        .expect("timeout")
        .expect("saga error");

    assert_eq!(result, SagaStatus::RolledBack);
}
