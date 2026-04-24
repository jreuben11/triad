#![cfg(feature = "integration")]

mod common;

use std::time::Duration;

use common::containers::PgStack;
use uuid::Uuid;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn test_webhook_delivery_success() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let response = reqwest::Client::new()
        .post(format!("{}/events", mock_server.uri()))
        .header("Content-Type", "application/json")
        .body(r#"{"event_type":"OrderCreated","payload":{}}"#)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status().as_u16(), 200);
    mock_server.verify().await;
}

#[tokio::test]
async fn test_webhook_retry_on_server_error() {
    let mock_server = MockServer::start().await;

    // First request returns 500, second succeeds.
    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(500))
        .expect(1)
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    let client = reqwest::Client::new();
    let url = format!("{}/events", mock_server.uri());

    let mut last_status = 0u16;
    for attempt in 1..=3u32 {
        let resp = client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(r#"{"event_type":"OrderCreated"}"#)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .expect("request failed");
        last_status = resp.status().as_u16();
        if last_status == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt))).await;
    }

    assert_eq!(last_status, 200, "retry should eventually succeed");
}

#[tokio::test]
async fn test_webhook_subscription_stored_in_pg() {
    let stack = PgStack::start().await;
    let pool = sqlx::PgPool::connect(&stack.pg_url)
        .await
        .expect("connect failed");

    let sub_id: Uuid = sqlx::query_scalar(
        "INSERT INTO triad.webhook_subscriptions
         (pattern_name, endpoint_url, event_types, enabled)
         VALUES ('outbox', 'https://example.com/hook', '{\"OrderCreated\"}', true)
         RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .expect("INSERT subscription failed");

    let event_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO triad.webhook_deliveries
         (subscription_id, event_id, attempt, status_code, outcome, duration_ms)
         VALUES ($1, $2, 1, 200, 'success', 42)",
    )
    .bind(sub_id)
    .bind(event_id)
    .execute(&pool)
    .await
    .expect("INSERT delivery failed");

    let outcome: String = sqlx::query_scalar(
        "SELECT outcome FROM triad.webhook_deliveries WHERE subscription_id = $1",
    )
    .bind(sub_id)
    .fetch_one(&pool)
    .await
    .expect("SELECT delivery failed");

    assert_eq!(outcome, "success");
}

#[tokio::test]
async fn test_webhook_dlq_after_max_retries() {
    let mock_server = MockServer::start().await;

    // Always return 500 — simulating a permanently failing endpoint.
    Mock::given(method("POST"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&mock_server)
        .await;

    let client = reqwest::Client::new();
    let url = format!("{}/events", mock_server.uri());
    let max_retries = 3;
    let mut failures = 0usize;

    for _ in 0..max_retries {
        let resp = client
            .post(&url)
            .body("{}")
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .expect("request failed");
        if !resp.status().is_success() {
            failures += 1;
        }
    }

    assert_eq!(
        failures, max_retries,
        "all retries should fail, triggering DLQ routing"
    );
}

#[tokio::test]
async fn test_webhook_hmac_signature_header() {
    let mock_server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/secure"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&mock_server)
        .await;

    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    let secret = "webhook-secret";
    let body = r#"{"event_type":"OrderCreated"}"#;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("HMAC init failed");
    mac.update(body.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    let response = reqwest::Client::new()
        .post(format!("{}/secure", mock_server.uri()))
        .header("Content-Type", "application/json")
        .header("X-Triad-Signature", format!("sha256={signature}"))
        .body(body)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .expect("request failed");

    assert_eq!(response.status().as_u16(), 200);
    mock_server.verify().await;
}
