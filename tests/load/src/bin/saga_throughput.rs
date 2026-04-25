//! Saga throughput load test.
//!
//! Simulates load against the Triad saga orchestration pattern by
//! repeatedly querying the `/saga` endpoint (in-flight saga listing).
//!
//! Before the test, snapshots `triad_saga_completed_total` from Prometheus
//! and asserts it increased after the test completes. Also asserts error
//! rate < 2%.
//!
//! Run:
//!   cargo run --manifest-path tests/load/Cargo.toml --bin saga_throughput
//!
//! Override the target:
//!   TRIAD_BASE_URL=http://prod:8080 cargo run --bin saga_throughput

use goose::prelude::*;
use triad_loadtest::{
    assert_counter_increased, assert_p95, base_url, check_health, scrape_metrics,
};

async fn task_list_sagas(user: &mut GooseUser) -> TransactionResult {
    let _res = user.get("/saga").await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    // Snapshot counter before load test begins.
    let before = scrape_metrics();
    let counter_before = before
        .get("triad_saga_completed_total")
        .copied()
        .unwrap_or(0.0);
    println!("saga_throughput: triad_saga_completed_total before = {counter_before}");

    // Build and execute the load test.
    // Ramp: 20 users over 30 s (hatch_rate = 20/30 ≈ 0.67 users/s)
    // Hold: 60 s
    // Total run_time = 120 s (30s ramp + 60s hold + 30s ramp-down)
    let metrics = GooseAttack::initialize()?
        .set_default(GooseDefault::Host, base_url().as_str())?
        .set_default(GooseDefault::Users, 20_usize)?
        .set_default(GooseDefault::HatchRate, "0.67")?
        .set_default(GooseDefault::RunTime, 120_usize)?
        .register_scenario(
            scenario!("SagaThroughput")
                .register_transaction(transaction!(check_health).set_on_start())
                .register_transaction(transaction!(task_list_sagas)),
        )
        .execute()
        .await?;

    // Print summary.
    println!("{:#?}", metrics);

    // Assert p95 latency threshold.
    assert_p95(&metrics, "SagaThroughput", 500);

    // Assert error rate < 2%:
    // Use the aggregate request metrics keyed by "GET /saga".
    let total_requests: usize = metrics
        .requests
        .values()
        .map(|r| r.success_count + r.fail_count)
        .sum();
    let failed_requests: usize = metrics.requests.values().map(|r| r.fail_count).sum();
    if total_requests > 0 {
        let error_rate = failed_requests as f64 / total_requests as f64;
        assert!(
            error_rate < 0.02,
            "saga_throughput: error rate {:.2}% exceeds 2% threshold \
             (failed={failed_requests}, total={total_requests})",
            error_rate * 100.0
        );
        println!(
            "saga_throughput: error rate {:.2}% — OK (failed={failed_requests}, total={total_requests})",
            error_rate * 100.0
        );
    } else {
        eprintln!("saga_throughput: no requests recorded — skipping error-rate assertion");
    }

    // Assert counter increased.
    let after = scrape_metrics();
    let counter_after = after
        .get("triad_saga_completed_total")
        .copied()
        .unwrap_or(0.0);
    println!("saga_throughput: triad_saga_completed_total after  = {counter_after}");
    assert_counter_increased(counter_before, counter_after, "triad_saga_completed_total");

    Ok(())
}
