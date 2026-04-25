//! Cache-aside read-path load test.
//!
//! Simulates load against the Triad Redis cache-aside pattern by
//! repeatedly hitting the `/health/ready` endpoint (which exercises the
//! Redis liveness check on every request).
//!
//! Before the test, snapshots `triad_cache_hits_total` and
//! `triad_cache_misses_total` from Prometheus. After the test, computes
//! and prints the cache hit rate for the run. Also asserts p95 ≤ 50 ms
//! and error rate < 0.5%.
//!
//! Run:
//!   cargo run --manifest-path tests/load/Cargo.toml --bin cache_read
//!
//! Override the target:
//!   TRIAD_BASE_URL=http://prod:8080 cargo run --bin cache_read

use goose::prelude::*;
use triad_loadtest::{assert_p95, base_url, check_health, scrape_metrics};

#[tokio::main]
async fn main() -> Result<(), GooseError> {
    // Snapshot counters before load test begins.
    let before = scrape_metrics();
    let hits_before = before.get("triad_cache_hits_total").copied().unwrap_or(0.0);
    let misses_before = before
        .get("triad_cache_misses_total")
        .copied()
        .unwrap_or(0.0);
    println!("cache_read: triad_cache_hits_total before   = {hits_before}");
    println!("cache_read: triad_cache_misses_total before = {misses_before}");

    // Build and execute the load test.
    // Ramp: 100 users over 20 s (hatch_rate = 100/20 = 5.0 users/s)
    // Hold: 60 s
    // Total run_time = 100 s (20s ramp + 60s hold + 20s ramp-down)
    let metrics = GooseAttack::initialize()?
        .set_default(GooseDefault::Host, base_url().as_str())?
        .set_default(GooseDefault::Users, 100_usize)?
        .set_default(GooseDefault::HatchRate, "5.0")?
        .set_default(GooseDefault::RunTime, 100_usize)?
        .register_scenario(scenario!("CacheRead").register_transaction(transaction!(check_health)))
        .execute()
        .await?;

    // Print summary.
    println!("{:#?}", metrics);

    // Assert p95 latency threshold (50 ms for a fast Redis liveness check).
    assert_p95(&metrics, "CacheRead", 50);

    // Assert error rate < 0.5%.
    let total_requests: usize = metrics
        .requests
        .values()
        .map(|r| r.success_count + r.fail_count)
        .sum();
    let failed_requests: usize = metrics.requests.values().map(|r| r.fail_count).sum();
    if total_requests > 0 {
        let error_rate = failed_requests as f64 / total_requests as f64;
        assert!(
            error_rate < 0.005,
            "cache_read: error rate {:.3}% exceeds 0.5% threshold \
             (failed={failed_requests}, total={total_requests})",
            error_rate * 100.0
        );
        println!(
            "cache_read: error rate {:.3}% — OK (failed={failed_requests}, total={total_requests})",
            error_rate * 100.0
        );
    } else {
        eprintln!("cache_read: no requests recorded — skipping error-rate assertion");
    }

    // Compute hit rate from counter deltas — print only, no minimum asserted
    // (the stack may be cold and all requests may be cache misses on first run).
    let after = scrape_metrics();
    let hits_after = after.get("triad_cache_hits_total").copied().unwrap_or(0.0);
    let misses_after = after
        .get("triad_cache_misses_total")
        .copied()
        .unwrap_or(0.0);

    let hits_delta = (hits_after - hits_before).max(0.0);
    let misses_delta = (misses_after - misses_before).max(0.0);
    let total_delta = hits_delta + misses_delta;

    println!("cache_read: triad_cache_hits_total after    = {hits_after}");
    println!("cache_read: triad_cache_misses_total after  = {misses_after}");

    if total_delta > 0.0 {
        let hit_rate = hits_delta / total_delta * 100.0;
        println!(
            "cache_read: hit rate for this run = {hit_rate:.1}% \
             (hits_delta={hits_delta}, misses_delta={misses_delta})"
        );
    } else {
        println!("cache_read: no cache operations observed during this run (stack may be cold)");
    }

    Ok(())
}
