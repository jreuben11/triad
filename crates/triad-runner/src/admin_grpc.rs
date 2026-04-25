use std::collections::HashMap;

use tonic::{Request, Response, Status};
use triad_proto::admin::{
    BackendHealth, DlqEntry, DlqListResponse, DlqRequest, LagEntry, LagResponse,
    ListPatternsResponse, LiveResponse, PatternHealth, PatternRequest, PatternSummary,
    ReadyResponse, SagaInspectResponse, SagaListResponse, SagaRequest, SagaSummary,
    StartedResponse,
    triad_admin_server::{TriadAdmin, TriadAdminServer},
};

use crate::admin::{
    AdminState, PatternControl, check_kafka_health, check_pg_health, check_redis_health,
};

pub struct TriadAdminService(pub AdminState);

pub fn grpc_service(state: AdminState) -> TriadAdminServer<TriadAdminService> {
    TriadAdminServer::new(TriadAdminService(state))
}

#[tonic::async_trait]
impl TriadAdmin for TriadAdminService {
    async fn live(&self, _request: Request<()>) -> Result<Response<LiveResponse>, Status> {
        Ok(Response::new(LiveResponse {
            status: "ok".to_string(),
            uptime_seconds: self.0.started_at.elapsed().as_secs(),
        }))
    }

    async fn ready(&self, _request: Request<()>) -> Result<Response<ReadyResponse>, Status> {
        let health = self.0.module_health.read().await;
        let all_running = health
            .values()
            .all(|h| h.state == triad_core::types::ModuleState::Running);
        let patterns: HashMap<String, PatternHealth> = health
            .iter()
            .map(|(name, h)| {
                (
                    name.clone(),
                    PatternHealth {
                        status: format!("{:?}", h.state),
                        lag: 0,
                        in_flight: 0,
                    },
                )
            })
            .collect();
        drop(health);

        let (pg_status, kafka_status, redis_status) = tokio::join!(
            check_pg_health(&self.0.pg_pool),
            check_kafka_health(self.0.kafka_brokers.clone()),
            check_redis_health(self.0.redis_url.clone()),
        );

        let has_degraded =
            pg_status == "degraded" || kafka_status == "degraded" || redis_status == "degraded";
        let status = if !has_degraded && all_running {
            "ok"
        } else {
            "degraded"
        };

        let mut backends = HashMap::new();
        backends.insert(
            "postgres".to_string(),
            BackendHealth {
                status: pg_status,
                latency_ms: 0,
            },
        );
        backends.insert(
            "kafka".to_string(),
            BackendHealth {
                status: kafka_status,
                latency_ms: 0,
            },
        );
        backends.insert(
            "redis".to_string(),
            BackendHealth {
                status: redis_status,
                latency_ms: 0,
            },
        );

        Ok(Response::new(ReadyResponse {
            status: status.to_string(),
            backends,
            patterns,
            cold_start_complete: true,
            drain_mode: false,
            leader: true,
        }))
    }

    async fn started(&self, _request: Request<()>) -> Result<Response<StartedResponse>, Status> {
        let health = self.0.module_health.read().await;
        Ok(Response::new(StartedResponse {
            status: "ok".to_string(),
            cold_start_complete: true,
            patterns_loaded: health.len() as u32,
            startup_duration_ms: self.0.started_at.elapsed().as_millis() as u64,
        }))
    }

    async fn list_patterns(
        &self,
        _request: Request<()>,
    ) -> Result<Response<ListPatternsResponse>, Status> {
        let health = self.0.module_health.read().await;
        let patterns: Vec<PatternSummary> = health
            .iter()
            .map(|(name, h)| PatternSummary {
                name: name.clone(),
                pattern_type: "unknown".to_string(),
                status: format!("{:?}", h.state),
                throughput_rps: 0.0,
            })
            .collect();
        Ok(Response::new(ListPatternsResponse { patterns }))
    }

    async fn pause_pattern(
        &self,
        request: Request<PatternRequest>,
    ) -> Result<Response<()>, Status> {
        let name = request.into_inner().name;
        if let Some(tx) = &self.0.control_tx {
            let _ = tx.try_send(PatternControl::Pause(name));
        }
        Ok(Response::new(()))
    }

    async fn resume_pattern(
        &self,
        request: Request<PatternRequest>,
    ) -> Result<Response<()>, Status> {
        let name = request.into_inner().name;
        if let Some(tx) = &self.0.control_tx {
            let _ = tx.try_send(PatternControl::Resume(name));
        }
        Ok(Response::new(()))
    }

    async fn replay_pattern(
        &self,
        request: Request<PatternRequest>,
    ) -> Result<Response<()>, Status> {
        let name = request.into_inner().name;
        if let Some(tx) = &self.0.control_tx {
            let _ = tx.try_send(PatternControl::Replay(name));
        }
        Ok(Response::new(()))
    }

    async fn get_lag(&self, _request: Request<()>) -> Result<Response<LagResponse>, Status> {
        let Some(brokers) = self.0.kafka_brokers.clone() else {
            return Ok(Response::new(LagResponse { entries: vec![] }));
        };

        let entries = tokio::task::spawn_blocking(move || -> Vec<LagEntry> {
            use rdkafka::{
                config::ClientConfig,
                consumer::{BaseConsumer, Consumer},
            };
            let Ok(consumer): Result<BaseConsumer, _> = ClientConfig::new()
                .set("bootstrap.servers", &brokers)
                .set("socket.connection.setup.timeout.ms", "2000")
                .create()
            else {
                return Vec::new();
            };
            let Ok(metadata) = consumer.fetch_metadata(None, std::time::Duration::from_secs(2))
            else {
                return Vec::new();
            };
            let mut result = Vec::new();
            for topic in metadata.topics() {
                for partition in topic.partitions() {
                    let (low, high) = consumer
                        .fetch_watermarks(
                            topic.name(),
                            partition.id(),
                            std::time::Duration::from_secs(2),
                        )
                        .unwrap_or((0, 0));
                    result.push(LagEntry {
                        pattern_name: "unknown".to_string(),
                        topic: topic.name().to_string(),
                        partition: partition.id(),
                        lag_messages: high - low,
                    });
                }
            }
            result
        })
        .await
        .unwrap_or_default();

        Ok(Response::new(LagResponse { entries }))
    }

    async fn list_dlq(
        &self,
        request: Request<DlqRequest>,
    ) -> Result<Response<DlqListResponse>, Status> {
        let topic = request.into_inner().topic;
        Ok(Response::new(DlqListResponse {
            entries: vec![DlqEntry {
                topic,
                message_count: 0,
                last_message_at: None,
            }],
        }))
    }

    async fn replay_dlq(&self, request: Request<DlqRequest>) -> Result<Response<()>, Status> {
        let topic = request.into_inner().topic;
        if let Some(replayer) = &self.0.dlq_replayer {
            replayer
                .replay(&topic)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(()))
    }

    async fn drop_dlq(&self, request: Request<DlqRequest>) -> Result<Response<()>, Status> {
        let topic = request.into_inner().topic;
        if let Some(replayer) = &self.0.dlq_replayer {
            replayer
                .purge(&topic)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }
        Ok(Response::new(()))
    }

    async fn list_sagas(
        &self,
        _request: Request<()>,
    ) -> Result<Response<SagaListResponse>, Status> {
        let Some(pool) = &self.0.pg_pool else {
            return Ok(Response::new(SagaListResponse { sagas: vec![] }));
        };
        let rows: Vec<(uuid::Uuid, String, i32, String)> = sqlx::query_as(
            "SELECT saga_id, saga_name, current_step, status \
             FROM triad.triad_saga_checkpoints ORDER BY updated_at DESC",
        )
        .fetch_all(pool)
        .await
        .unwrap_or_default();
        let sagas: Vec<SagaSummary> = rows
            .into_iter()
            .map(|(id, name, step, status)| SagaSummary {
                saga_id: id.to_string(),
                saga_name: name,
                current_step: step,
                status,
                started_at: None,
                updated_at: None,
            })
            .collect();
        Ok(Response::new(SagaListResponse { sagas }))
    }

    async fn inspect_saga(
        &self,
        request: Request<SagaRequest>,
    ) -> Result<Response<SagaInspectResponse>, Status> {
        let id = request.into_inner().id;
        let Some(pool) = &self.0.pg_pool else {
            return Err(Status::not_found(format!("saga {id} not found")));
        };
        let uuid = uuid::Uuid::parse_str(&id)
            .map_err(|_| Status::not_found(format!("saga {id} not found")))?;
        let row: Option<(
            uuid::Uuid,
            String,
            i32,
            String,
            i64,
            chrono::DateTime<chrono::Utc>,
        )> = sqlx::query_as(
            "SELECT saga_id, saga_name, current_step, status, version, updated_at \
                 FROM triad.triad_saga_checkpoints WHERE saga_id = $1",
        )
        .bind(uuid)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten();
        let Some((sid, name, step, status, _version, _updated_at)) = row else {
            return Err(Status::not_found(format!("saga {id} not found")));
        };
        Ok(Response::new(SagaInspectResponse {
            summary: Some(SagaSummary {
                saga_id: sid.to_string(),
                saga_name: name,
                current_step: step,
                status,
                started_at: None,
                updated_at: None,
            }),
            state: vec![],
            steps: vec![],
        }))
    }

    async fn cancel_saga(&self, request: Request<SagaRequest>) -> Result<Response<()>, Status> {
        let id = request.into_inner().id;
        let Some(pool) = &self.0.pg_pool else {
            return Ok(Response::new(()));
        };
        let uuid = uuid::Uuid::parse_str(&id)
            .map_err(|_| Status::not_found(format!("saga {id} not found")))?;
        let rows_affected = sqlx::query(
            "UPDATE triad.triad_saga_checkpoints \
             SET status = 'Cancelled', updated_at = now() WHERE saga_id = $1",
        )
        .bind(uuid)
        .execute(pool)
        .await
        .map(|r| r.rows_affected())
        .unwrap_or(0);
        if rows_affected > 0 {
            Ok(Response::new(()))
        } else {
            Err(Status::not_found(format!("saga {id} not found")))
        }
    }

    async fn reload_config(&self, _request: Request<()>) -> Result<Response<()>, Status> {
        let (Some(path), Some(shared_config)) = (&self.0.config_path, &self.0.shared_config) else {
            return Ok(Response::new(()));
        };
        let new_cfg = triad_core::config::TriadConfig::load(path)
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut cfg = shared_config.write().await;
        *cfg = new_cfg;
        Ok(Response::new(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::AdminState;

    fn svc() -> TriadAdminService {
        TriadAdminService(AdminState::new())
    }

    #[tokio::test]
    async fn test_grpc_live_returns_ok() {
        let resp = svc().live(Request::new(())).await.unwrap();
        assert_eq!(resp.get_ref().status, "ok");
    }

    #[tokio::test]
    async fn test_grpc_ready_returns_unconfigured_backends() {
        let resp = svc().ready(Request::new(())).await.unwrap();
        let r = resp.get_ref();
        assert_eq!(r.backends["postgres"].status, "unconfigured");
        assert_eq!(r.backends["kafka"].status, "unconfigured");
        assert_eq!(r.backends["redis"].status, "unconfigured");
    }

    #[tokio::test]
    async fn test_grpc_started_returns_ok() {
        let resp = svc().started(Request::new(())).await.unwrap();
        assert_eq!(resp.get_ref().status, "ok");
    }

    #[tokio::test]
    async fn test_grpc_list_patterns_empty() {
        let resp = svc().list_patterns(Request::new(())).await.unwrap();
        assert!(resp.get_ref().patterns.is_empty());
    }

    #[tokio::test]
    async fn test_grpc_pause_pattern_no_channel() {
        let resp = svc()
            .pause_pattern(Request::new(PatternRequest {
                name: "test".into(),
            }))
            .await;
        assert!(resp.is_ok());
    }

    #[tokio::test]
    async fn test_grpc_resume_pattern_no_channel() {
        let resp = svc()
            .resume_pattern(Request::new(PatternRequest {
                name: "test".into(),
            }))
            .await;
        assert!(resp.is_ok());
    }

    #[tokio::test]
    async fn test_grpc_replay_pattern_no_channel() {
        let resp = svc()
            .replay_pattern(Request::new(PatternRequest {
                name: "test".into(),
            }))
            .await;
        assert!(resp.is_ok());
    }

    #[tokio::test]
    async fn test_grpc_get_lag_no_kafka() {
        let resp = svc().get_lag(Request::new(())).await.unwrap();
        assert!(resp.get_ref().entries.is_empty());
    }

    #[tokio::test]
    async fn test_grpc_list_dlq_returns_entry() {
        let resp = svc()
            .list_dlq(Request::new(DlqRequest {
                topic: "test-topic".into(),
            }))
            .await
            .unwrap();
        assert_eq!(resp.get_ref().entries[0].topic, "test-topic");
    }

    #[tokio::test]
    async fn test_grpc_replay_dlq_no_replayer() {
        let resp = svc()
            .replay_dlq(Request::new(DlqRequest { topic: "t".into() }))
            .await;
        assert!(resp.is_ok());
    }

    #[tokio::test]
    async fn test_grpc_drop_dlq_no_replayer() {
        let resp = svc()
            .drop_dlq(Request::new(DlqRequest { topic: "t".into() }))
            .await;
        assert!(resp.is_ok());
    }

    #[tokio::test]
    async fn test_grpc_list_sagas_no_pool() {
        let resp = svc().list_sagas(Request::new(())).await.unwrap();
        assert!(resp.get_ref().sagas.is_empty());
    }

    #[tokio::test]
    async fn test_grpc_inspect_saga_no_pool() {
        let err = svc()
            .inspect_saga(Request::new(SagaRequest {
                id: "test-id".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_grpc_cancel_saga_no_pool() {
        let resp = svc()
            .cancel_saga(Request::new(SagaRequest {
                id: "test-id".into(),
            }))
            .await;
        assert!(resp.is_ok());
    }

    #[tokio::test]
    async fn test_grpc_reload_config_no_config_path() {
        let resp = svc().reload_config(Request::new(())).await;
        assert!(resp.is_ok());
    }

    #[test]
    fn test_grpc_service_builder() {
        let _svc = grpc_service(AdminState::new());
    }

    #[tokio::test]
    async fn test_grpc_ready_degraded_with_bad_redis() {
        let state = AdminState::new().with_redis_url("redis://127.0.0.1:1");
        let svc = TriadAdminService(state);
        let resp = svc.ready(Request::new(())).await.unwrap();
        assert_eq!(resp.get_ref().status, "degraded");
        assert_eq!(resp.get_ref().backends["redis"].status, "degraded");
    }

    #[tokio::test]
    async fn test_grpc_pause_pattern_with_channel() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<crate::admin::PatternControl>(1);
        let state = AdminState::new().with_control_tx(tx);
        let svc = TriadAdminService(state);
        let resp = svc
            .pause_pattern(Request::new(PatternRequest { name: "p".into() }))
            .await;
        assert!(resp.is_ok());
    }

    #[tokio::test]
    async fn test_grpc_resume_pattern_with_channel() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<crate::admin::PatternControl>(1);
        let state = AdminState::new().with_control_tx(tx);
        let svc = TriadAdminService(state);
        let resp = svc
            .resume_pattern(Request::new(PatternRequest { name: "p".into() }))
            .await;
        assert!(resp.is_ok());
    }

    #[tokio::test]
    async fn test_grpc_replay_pattern_with_channel() {
        let (tx, _rx) = tokio::sync::mpsc::channel::<crate::admin::PatternControl>(1);
        let state = AdminState::new().with_control_tx(tx);
        let svc = TriadAdminService(state);
        let resp = svc
            .replay_pattern(Request::new(PatternRequest { name: "p".into() }))
            .await;
        assert!(resp.is_ok());
    }

    #[tokio::test]
    async fn test_grpc_reload_config_internal_on_missing_file() {
        use std::sync::Arc;
        let yaml = r#"
backends:
  postgres:
    url: "postgres://localhost/testdb"
    max_connections: 5
    connection_timeout_ms: 5000
    ssl_mode: "disable"
  kafka:
    brokers:
      - "localhost:9092"
    security:
      protocol: "PLAINTEXT"
    producer:
      transactional_id_prefix: "triad"
      acks: "all"
      compression_type: "none"
      transaction_timeout_ms: 60000
      enable_idempotence: true
      max_in_flight_requests: 5
      batch_size: 16384
      linger_ms: 0
      request_timeout_ms: 30000
    consumer:
      group_id: "triad-group"
      isolation_level: "read_committed"
      auto_offset_reset: "earliest"
      max_poll_interval_ms: 300000
      max_poll_records: 500
      fetch_max_bytes: 52428800
      session_timeout_ms: 10000
      heartbeat_interval_ms: 3000
  redis:
    mode: "standalone"
    url: "redis://localhost:6379"
    pool_size: 5
    connection_timeout_ms: 5000
    read_timeout_ms: 1000
    write_timeout_ms: 1000
    max_retries: 3
patterns: []
cold_start:
  default_strategy: "pg_snapshot"
  overrides: {}
delivery:
  default_guarantee: "exactly_once"
  kafka_transaction_timeout: "60s"
  idempotency_dedup_window: "24h"
  dlq_topic_template: "triad.dlq.{source_topic}"
observability:
  metrics:
    provider: "prometheus"
    port: 9090
    labels: []
  tracing:
    provider: "otlp"
    endpoint: "http://localhost:4317"
    sample_rate: 1.0
  audit:
    topic: "triad.audit"
  alerts:
    kafka_lag_threshold: 1000
    pg_replication_lag_threshold: "30s"
    redis_memory_threshold: 0.9
shutdown:
  drain_timeout_seconds: 30
  warn_on_forced_exit: true
admin:
  port: 8080
  auth:
    type: "none"
retry:
  max_attempts: 3
  initial_delay_ms: 100
  max_delay_ms: 10000
  multiplier: 2.0
circuit_breakers:
  failure_threshold: 5
  success_threshold: 2
  timeout_ms: 60000
  half_open_max_calls: 3
"#;
        let tmp = std::env::temp_dir().join("triad_grpc_test.yaml");
        std::fs::write(&tmp, yaml).unwrap();
        let cfg = triad_core::config::TriadConfig::load(tmp.to_str().unwrap()).unwrap();
        let shared = Arc::new(tokio::sync::RwLock::new(cfg));
        let state = AdminState::new()
            .with_config_path("/nonexistent/path/that/does/not/exist.yaml")
            .with_shared_config(shared);
        let svc = TriadAdminService(state);
        let err = svc.reload_config(Request::new(())).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        std::fs::remove_file(&tmp).ok();
    }
}
