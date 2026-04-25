//! Outbox throughput load test.
//!
//! Simulates load against the Triad transactional outbox pattern by
//! repeatedly querying the `/lag` endpoint (Kafka consumer group lag).
//!
//! Before the test, snapshots `triad_outbox_relay_published_total` from
//! Prometheus and asserts it increased after the test completes.
//!
//! Run:
//!   cargo run --manifest-path tests/load/Cargo.toml --bin outbox_throughput
//!
//! Override the target:
//!   TRIAD_BASE_URL=http://prod:8080 cargo run --bin outbox_throughput

use goose::prelude::*;
use triad_loadtest::{
    assert_counter_increased, assert_p95, base_url, check_health, scrape_metrics,
};

async fn task_get_lag(user: &mut GooseUser) -> TransactionResult {
    let _res = user.get("/lag").await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    // Snapshot counter before load test begins.
    let before = scrape_metrics();
    let counter_before = before
        .get("triad_outbox_relay_published_total")
        .copied()
        .unwrap_or(0.0);
    println!("outbox_throughput: triad_outbox_relay_published_total before = {counter_before}");

    // Build and execute the load test.
    // Ramp: 50 users over 30 s (hatch_rate = 50/30 ≈ 1.67 users/s)
    // Hold: 60 s
    // Ramp-down: handled by goose shutdown (total run_time = 120 s)
    let metrics = GooseAttack::initialize()?
        .set_default(GooseDefault::Host, base_url().as_str())?
        .set_default(GooseDefault::Users, 50_usize)?
        .set_default(GooseDefault::HatchRate, "1.67")?
        .set_default(GooseDefault::RunTime, 120_usize)?
        .register_scenario(
            scenario!("OutboxThroughput")
                .register_transaction(transaction!(check_health).set_on_start())
                .register_transaction(transaction!(task_get_lag)),
        )
        .execute()
        .await?;

    // Print summary.
    println!("{:#?}", metrics);

    // Assert p95 latency threshold.
    assert_p95(&metrics, "OutboxThroughput", 200);

    // Assert counter increased.
    let after = scrape_metrics();
    let counter_after = after
        .get("triad_outbox_relay_published_total")
        .copied()
        .unwrap_or(0.0);
    println!("outbox_throughput: triad_outbox_relay_published_total after  = {counter_after}");
    assert_counter_increased(
        counter_before,
        counter_after,
        "triad_outbox_relay_published_total",
    );

    Ok(())
}
