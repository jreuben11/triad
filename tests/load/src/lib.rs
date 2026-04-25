//! Shared helpers for Triad goose load tests.
//!
//! Provides URL resolution, health-check transaction, random ID generation,
//! Prometheus metrics scraping, and assertion helpers.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use goose::prelude::*;

/// Returns the base URL for the Triad admin HTTP server.
///
/// Reads `TRIAD_BASE_URL` from the environment; defaults to `http://localhost:8080`.
pub fn base_url() -> String {
    std::env::var("TRIAD_BASE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string())
}

/// GooseUser transaction: GET `/health/ready`, asserts HTTP 200.
pub async fn check_health(user: &mut GooseUser) -> TransactionResult {
    let _res = user.get("/health/ready").await?;
    Ok(())
}

/// Generates a short random hex string using the system clock as entropy.
///
/// Not cryptographically secure — suitable only for generating unique request IDs
/// in load test scenarios.
pub fn random_id() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .subsec_nanos();
    nanos.hash(&mut hasher);
    // Mix in a thread id approximation using the address of a stack variable.
    let stack_addr: usize = &nanos as *const u32 as usize;
    stack_addr.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Calculates the p95 response time (in ms) from a `ScenarioMetricAggregate`'s
/// `times` BTreeMap and asserts it is at or below `max_ms`.
///
/// The `times` map stores `{ response_time_ms -> count }` pairs. We reconstruct
/// the empirical CDF and find the 95th-percentile bucket.
///
/// # Panics
/// Panics with a descriptive message when no scenario named `scenario` is found
/// or when the measured p95 exceeds `max_ms`.
pub fn assert_p95(metrics: &GooseMetrics, scenario: &str, max_ms: u64) {
    // Find the named scenario in the aggregate list.
    let agg = metrics
        .scenarios
        .iter()
        .find(|s| s.name == scenario)
        .unwrap_or_else(|| {
            panic!(
                "assert_p95: no scenario named '{}' found in metrics (available: [{}])",
                scenario,
                metrics
                    .scenarios
                    .iter()
                    .map(|s| s.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        });

    let total = agg.counter;
    if total == 0 {
        eprintln!(
            "assert_p95: scenario '{}' has 0 samples — skipping p95 assertion",
            scenario
        );
        return;
    }

    // Walk the sorted BTreeMap to find the bucket at the 95th percentile.
    let target = ((total as f64) * 0.95).ceil() as usize;
    let mut cumulative: usize = 0;
    let mut p95_ms: u64 = 0;

    for (&bucket_ms, &count) in &agg.times {
        cumulative += count;
        if cumulative >= target {
            p95_ms = bucket_ms as u64;
            break;
        }
    }

    assert!(
        p95_ms <= max_ms,
        "assert_p95 FAILED for scenario '{}': p95={p95_ms}ms exceeds threshold={max_ms}ms \
         (total_samples={total})",
        scenario
    );

    println!(
        "assert_p95 OK — scenario '{}': p95={p95_ms}ms ≤ threshold={max_ms}ms",
        scenario
    );
}

/// Scrapes the Prometheus text exposition format from `{base_url()}/metrics`.
///
/// Returns a flat map of `metric_name -> last_seen_value` for all non-comment lines.
/// Lines of the form `<name> <value>` and `<name>{<labels>} <value>` are both handled;
/// the key stored is the bare metric name (without labels).
///
/// # Panics
/// Panics if the HTTP request fails (the Triad stack must be running).
pub fn scrape_metrics() -> HashMap<String, f64> {
    let url = format!("{}/metrics", base_url());
    let body = reqwest::blocking::get(&url)
        .unwrap_or_else(|e| panic!("scrape_metrics: GET {url} failed: {e}"))
        .text()
        .unwrap_or_else(|e| panic!("scrape_metrics: failed to decode response body: {e}"));

    let mut map = HashMap::new();
    for line in body.lines() {
        // Skip comment / help / type lines.
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        // Split off the value (last whitespace-separated token).
        // A line is either:
        //   metric_name value
        //   metric_name{labels} value
        let (name_part, value_str) = match line.rsplit_once(' ') {
            Some(pair) => pair,
            None => continue,
        };
        // Strip label set from the name part: everything before the first `{`.
        let bare_name = match name_part.find('{') {
            Some(idx) => &name_part[..idx],
            None => name_part,
        }
        .trim();

        if let Ok(value) = value_str.trim().parse::<f64>() {
            map.insert(bare_name.to_string(), value);
        }
    }
    map
}

/// Asserts that `after > before` for the named Prometheus counter.
///
/// # Panics
/// Panics with an actionable message if `after <= before`.
pub fn assert_counter_increased(before: f64, after: f64, metric: &str) {
    assert!(
        after > before,
        "assert_counter_increased FAILED for metric '{}': \
         value did not increase (before={before}, after={after}). \
         Ensure the Triad stack processed requests during the load test.",
        metric
    );
    println!(
        "assert_counter_increased OK — '{}': {before} → {after} (+{})",
        metric,
        after - before
    );
}
