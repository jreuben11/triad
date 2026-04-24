/// All metric name constants — prevents string typos across crates.
pub mod names {
    pub const PIPELINE_EVENTS_TOTAL: &str = "triad_pipeline_events_total";
    pub const PIPELINE_PROCESSING_DURATION: &str = "triad_pipeline_processing_duration_seconds";
    pub const PIPELINE_LAG_SECONDS: &str = "triad_pipeline_lag_seconds";
    pub const CDC_EVENTS_TOTAL: &str = "triad_cdc_events_total";
    pub const PG_REPLICATION_LAG_BYTES: &str = "triad_pg_replication_lag_bytes";
    pub const OUTBOX_PENDING_TOTAL: &str = "triad_outbox_pending_total";
    pub const OUTBOX_RELAY_DURATION: &str = "triad_outbox_relay_duration_seconds";
    pub const INBOX_DEDUP_TOTAL: &str = "triad_inbox_dedup_total";
    pub const KAFKA_PRODUCER_TXN_TOTAL: &str = "triad_kafka_producer_txn_total";
    pub const KAFKA_CONSUMER_LAG: &str = "triad_kafka_consumer_lag_messages";
    pub const REDIS_OP_DURATION: &str = "triad_redis_op_duration_seconds";
    pub const REDIS_MEMORY_USED_BYTES: &str = "triad_redis_memory_used_bytes";
    pub const REDIS_MEMORY_MAX_BYTES: &str = "triad_redis_memory_max_bytes";
    pub const CACHE_HIT_TOTAL: &str = "triad_cache_hit_total";
    pub const CACHE_MISS_TOTAL: &str = "triad_cache_miss_total";
    pub const SAGA_ACTIVE_TOTAL: &str = "triad_saga_active_total";
    pub const SAGA_STEP_DURATION: &str = "triad_saga_step_duration_seconds";
    pub const SAGA_COMPLETED_TOTAL: &str = "triad_saga_completed_total";
    pub const SAGA_STEP_TOTAL: &str = "triad_saga_step_total";
    pub const WEBHOOK_DELIVERY_ATTEMPTS: &str = "triad_webhook_delivery_attempts_total";
    pub const WEBHOOK_DELIVERY_DURATION: &str = "triad_webhook_delivery_duration_seconds";
    pub const FEATURE_FLAG_EVALUATIONS: &str = "triad_feature_flag_evaluations_total";
    pub const RATE_LIMIT_CHECKS_TOTAL: &str = "triad_rate_limit_checks_total";
    pub const EOS_TXN_TOTAL: &str = "triad_eos_txn_total";
    pub const COLD_START_DURATION: &str = "triad_cold_start_duration_seconds";
    pub const CONN_POOL_ACTIVE: &str = "triad_conn_pool_active";
    pub const CIRCUIT_BREAKER_STATE: &str = "triad_circuit_breaker_state";
    pub const CIRCUIT_BREAKER_TRANSITIONS: &str = "triad_circuit_breaker_transitions_total";
    pub const ERROR_TOTAL: &str = "triad_error_total";
    pub const RETRY_ATTEMPTS_TOTAL: &str = "triad_retry_attempts_total";
    pub const DLQ_MESSAGES_TOTAL: &str = "triad_dlq_messages_total";
    pub const DB_OPERATION_DURATION: &str = "triad_db_operation_duration_seconds";
    pub const CACHE_HIT_RATIO: &str = "triad_cache_hit_ratio";
    pub const FEATURE_FLAG_SYNC_LAG: &str = "triad_feature_flag_sync_lag_seconds";
    pub const FEATURE_STORE_LOOKUP_DURATION: &str = "triad_feature_store_lookup_duration_seconds";
    pub const FEATURE_STORE_FRESHNESS: &str = "triad_feature_store_freshness_seconds";
    pub const COLD_START_RECORDS_TOTAL: &str = "triad_cold_start_records_total";
    pub const CONN_POOL_IDLE: &str = "triad_conn_pool_idle";
    pub const CONN_POOL_WAIT_SECONDS: &str = "triad_conn_pool_wait_seconds";
    pub const REPLICATION_LAG_SECONDS: &str = "triad_replication_lag_seconds";
    pub const BACKPRESSURE_ACTIVE: &str = "triad_backpressure_active";
    pub const SAGA_COMPENSATION_TOTAL: &str = "triad_saga_compensation_total";
}

/// Histogram bucket sets for each metric family.
pub mod buckets {
    pub const RELAY_DURATION_MS: &[f64] =
        &[1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0];
    pub const PROCESSING_DURATION_MS: &[f64] =
        &[0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0];
    pub const WEBHOOK_DURATION_MS: &[f64] =
        &[50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0];
    pub const SAGA_STEP_DURATION_S: &[f64] = &[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 300.0];
}

#[cfg(test)]
mod tests {
    use super::{buckets, names};
    use rstest::rstest;

    #[rstest]
    #[case(names::PIPELINE_EVENTS_TOTAL, "triad_pipeline_events_total")]
    #[case(
        names::PIPELINE_PROCESSING_DURATION,
        "triad_pipeline_processing_duration_seconds"
    )]
    #[case(names::PIPELINE_LAG_SECONDS, "triad_pipeline_lag_seconds")]
    #[case(names::CDC_EVENTS_TOTAL, "triad_cdc_events_total")]
    #[case(names::PG_REPLICATION_LAG_BYTES, "triad_pg_replication_lag_bytes")]
    #[case(names::OUTBOX_PENDING_TOTAL, "triad_outbox_pending_total")]
    #[case(names::OUTBOX_RELAY_DURATION, "triad_outbox_relay_duration_seconds")]
    #[case(names::INBOX_DEDUP_TOTAL, "triad_inbox_dedup_total")]
    #[case(names::KAFKA_PRODUCER_TXN_TOTAL, "triad_kafka_producer_txn_total")]
    #[case(names::KAFKA_CONSUMER_LAG, "triad_kafka_consumer_lag_messages")]
    #[case(names::REDIS_OP_DURATION, "triad_redis_op_duration_seconds")]
    #[case(names::REDIS_MEMORY_USED_BYTES, "triad_redis_memory_used_bytes")]
    #[case(names::REDIS_MEMORY_MAX_BYTES, "triad_redis_memory_max_bytes")]
    #[case(names::CACHE_HIT_TOTAL, "triad_cache_hit_total")]
    #[case(names::CACHE_MISS_TOTAL, "triad_cache_miss_total")]
    #[case(names::SAGA_ACTIVE_TOTAL, "triad_saga_active_total")]
    #[case(names::SAGA_STEP_DURATION, "triad_saga_step_duration_seconds")]
    #[case(names::SAGA_COMPLETED_TOTAL, "triad_saga_completed_total")]
    #[case(names::SAGA_STEP_TOTAL, "triad_saga_step_total")]
    #[case(
        names::WEBHOOK_DELIVERY_ATTEMPTS,
        "triad_webhook_delivery_attempts_total"
    )]
    #[case(
        names::WEBHOOK_DELIVERY_DURATION,
        "triad_webhook_delivery_duration_seconds"
    )]
    #[case(
        names::FEATURE_FLAG_EVALUATIONS,
        "triad_feature_flag_evaluations_total"
    )]
    #[case(names::RATE_LIMIT_CHECKS_TOTAL, "triad_rate_limit_checks_total")]
    #[case(names::EOS_TXN_TOTAL, "triad_eos_txn_total")]
    #[case(names::COLD_START_DURATION, "triad_cold_start_duration_seconds")]
    #[case(names::CONN_POOL_ACTIVE, "triad_conn_pool_active")]
    #[case(names::CIRCUIT_BREAKER_STATE, "triad_circuit_breaker_state")]
    #[case(
        names::CIRCUIT_BREAKER_TRANSITIONS,
        "triad_circuit_breaker_transitions_total"
    )]
    #[case(names::ERROR_TOTAL, "triad_error_total")]
    #[case(names::RETRY_ATTEMPTS_TOTAL, "triad_retry_attempts_total")]
    #[case(names::DLQ_MESSAGES_TOTAL, "triad_dlq_messages_total")]
    #[case(names::DB_OPERATION_DURATION, "triad_db_operation_duration_seconds")]
    #[case(names::CACHE_HIT_RATIO, "triad_cache_hit_ratio")]
    #[case(names::FEATURE_FLAG_SYNC_LAG, "triad_feature_flag_sync_lag_seconds")]
    #[case(
        names::FEATURE_STORE_LOOKUP_DURATION,
        "triad_feature_store_lookup_duration_seconds"
    )]
    #[case(
        names::FEATURE_STORE_FRESHNESS,
        "triad_feature_store_freshness_seconds"
    )]
    #[case(names::COLD_START_RECORDS_TOTAL, "triad_cold_start_records_total")]
    #[case(names::CONN_POOL_IDLE, "triad_conn_pool_idle")]
    #[case(names::CONN_POOL_WAIT_SECONDS, "triad_conn_pool_wait_seconds")]
    #[case(names::REPLICATION_LAG_SECONDS, "triad_replication_lag_seconds")]
    #[case(names::BACKPRESSURE_ACTIVE, "triad_backpressure_active")]
    #[case(names::SAGA_COMPENSATION_TOTAL, "triad_saga_compensation_total")]
    fn test_metric_name_values(#[case] actual: &str, #[case] expected: &str) {
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_metric_names_have_triad_prefix() {
        let all = [
            names::PIPELINE_EVENTS_TOTAL,
            names::PIPELINE_PROCESSING_DURATION,
            names::PIPELINE_LAG_SECONDS,
            names::CDC_EVENTS_TOTAL,
            names::PG_REPLICATION_LAG_BYTES,
            names::OUTBOX_PENDING_TOTAL,
            names::OUTBOX_RELAY_DURATION,
            names::INBOX_DEDUP_TOTAL,
            names::KAFKA_PRODUCER_TXN_TOTAL,
            names::KAFKA_CONSUMER_LAG,
            names::REDIS_OP_DURATION,
            names::REDIS_MEMORY_USED_BYTES,
            names::REDIS_MEMORY_MAX_BYTES,
            names::CACHE_HIT_TOTAL,
            names::CACHE_MISS_TOTAL,
            names::SAGA_ACTIVE_TOTAL,
            names::SAGA_STEP_DURATION,
            names::SAGA_COMPLETED_TOTAL,
            names::SAGA_STEP_TOTAL,
            names::WEBHOOK_DELIVERY_ATTEMPTS,
            names::WEBHOOK_DELIVERY_DURATION,
            names::FEATURE_FLAG_EVALUATIONS,
            names::RATE_LIMIT_CHECKS_TOTAL,
            names::EOS_TXN_TOTAL,
            names::COLD_START_DURATION,
            names::CONN_POOL_ACTIVE,
            names::CIRCUIT_BREAKER_STATE,
            names::CIRCUIT_BREAKER_TRANSITIONS,
            names::ERROR_TOTAL,
            names::RETRY_ATTEMPTS_TOTAL,
            names::DLQ_MESSAGES_TOTAL,
            names::DB_OPERATION_DURATION,
            names::CACHE_HIT_RATIO,
            names::FEATURE_FLAG_SYNC_LAG,
            names::FEATURE_STORE_LOOKUP_DURATION,
            names::FEATURE_STORE_FRESHNESS,
            names::COLD_START_RECORDS_TOTAL,
            names::CONN_POOL_IDLE,
            names::CONN_POOL_WAIT_SECONDS,
            names::REPLICATION_LAG_SECONDS,
            names::BACKPRESSURE_ACTIVE,
            names::SAGA_COMPENSATION_TOTAL,
        ];
        for name in all {
            assert!(name.starts_with("triad_"), "{name} missing triad_ prefix");
            assert!(!name.is_empty());
        }
    }

    #[test]
    fn test_bucket_arrays_are_sorted_and_non_empty() {
        for (label, buckets) in [
            ("relay", buckets::RELAY_DURATION_MS),
            ("processing", buckets::PROCESSING_DURATION_MS),
            ("webhook", buckets::WEBHOOK_DURATION_MS),
            ("saga_step", buckets::SAGA_STEP_DURATION_S),
        ] {
            assert!(!buckets.is_empty(), "{label} buckets empty");
            for window in buckets.windows(2) {
                assert!(
                    window[0] < window[1],
                    "{label} buckets not strictly ascending: {} >= {}",
                    window[0],
                    window[1]
                );
            }
        }
    }
}
