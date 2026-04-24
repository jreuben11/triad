use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use axum::body::Body;
use http::{Request, Response, StatusCode};
use tower::{Layer, Service};
use tracing::debug;
use triad_runner::patterns::{RateLimitDecision, RateLimiter};

/// Type alias for the per-request key extraction function used by `RateLimitLayer`.
pub type KeyFn = Arc<dyn Fn(&Request<Body>) -> String + Send + Sync>;

// ── IdempotencyLayer ──────────────────────────────────────────────────────────

/// Header name carrying the idempotency key from the client.
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

/// Tower layer that rejects duplicate requests identified by an idempotency key.
///
/// On the first request the key is stored (SET NX) and the request passes
/// through. On a duplicate (key already stored) the layer returns 208 Already
/// Reported without replaying a cached body. Omitting the header bypasses
/// idempotency checking entirely.
pub struct IdempotencyLayer {
    store: Arc<dyn IdempotencyBackend>,
    ttl: Duration,
}

/// Narrow backend trait — mockable in tests, implemented over Redis in production.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait IdempotencyBackend: Send + Sync {
    /// Return `true` if the key was newly set (first request).
    /// Return `false` if the key already existed (duplicate).
    async fn set_if_absent(&self, key: &str, ttl: Duration) -> bool;
}

impl IdempotencyLayer {
    pub fn new(store: Arc<dyn IdempotencyBackend>, ttl: Duration) -> Self {
        Self { store, ttl }
    }
}

impl<S> Layer<S> for IdempotencyLayer {
    type Service = IdempotencyMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        IdempotencyMiddleware {
            inner,
            store: Arc::clone(&self.store),
            ttl: self.ttl,
        }
    }
}

/// Tower service produced by `IdempotencyLayer`.
#[derive(Clone)]
pub struct IdempotencyMiddleware<S> {
    inner: S,
    store: Arc<dyn IdempotencyBackend>,
    ttl: Duration,
}

impl<S> Service<Request<Body>> for IdempotencyMiddleware<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = Response<Body>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let key = req
            .headers()
            .get(IDEMPOTENCY_KEY_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let store = Arc::clone(&self.store);
        let ttl = self.ttl;
        let mut inner = self.inner.clone();

        Box::pin(async move {
            match key {
                None => inner.call(req).await.map_err(Into::into),
                Some(k) => {
                    debug!(idempotency_key = %k, "checking idempotency key");
                    let is_new = store.set_if_absent(&k, ttl).await;
                    if is_new {
                        inner.call(req).await.map_err(Into::into)
                    } else {
                        debug!(idempotency_key = %k, "duplicate request — returning 208");
                        Ok(Response::builder()
                            .status(StatusCode::ALREADY_REPORTED)
                            .body(Body::empty())
                            .unwrap())
                    }
                }
            }
        })
    }
}

// ── RateLimitLayer ────────────────────────────────────────────────────────────

/// Tower layer that enforces a rate limit, returning 429 on rejection.
///
/// The `key_fn` extracts a per-request identifier (e.g. IP address, API key)
/// for per-entity bucketing inside the underlying `RateLimiter`.
pub struct RateLimitLayer {
    limiter: Arc<RateLimiter>,
    key_fn: KeyFn,
}

impl RateLimitLayer {
    pub fn new(
        limiter: Arc<RateLimiter>,
        key_fn: impl Fn(&Request<Body>) -> String + Send + Sync + 'static,
    ) -> Self {
        Self {
            limiter,
            key_fn: Arc::new(key_fn) as KeyFn,
        }
    }
}

impl<S> Layer<S> for RateLimitLayer {
    type Service = RateLimitMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RateLimitMiddleware {
            inner,
            limiter: Arc::clone(&self.limiter),
            key_fn: Arc::clone(&self.key_fn),
        }
    }
}

/// Tower service produced by `RateLimitLayer`.
#[derive(Clone)]
pub struct RateLimitMiddleware<S> {
    inner: S,
    limiter: Arc<RateLimiter>,
    key_fn: KeyFn,
}

impl<S> Service<Request<Body>> for RateLimitMiddleware<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = Response<Body>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let key = (self.key_fn)(&req);
        let limiter = Arc::clone(&self.limiter);
        let mut inner = self.inner.clone();

        Box::pin(async move {
            match limiter.check(&key).await {
                Ok(RateLimitDecision::Allowed { .. }) => inner.call(req).await.map_err(Into::into),
                Ok(RateLimitDecision::Rejected { retry_after_ms }) => {
                    debug!(key = %key, "rate limit exceeded — returning 429");
                    let mut builder = Response::builder().status(StatusCode::TOO_MANY_REQUESTS);
                    if let Some(ms) = retry_after_ms {
                        let secs = (ms / 1000).max(1).to_string();
                        builder = builder.header("retry-after", secs);
                    }
                    Ok(builder.body(Body::empty()).unwrap())
                }
                Err(e) => {
                    // Fail open on backend error — let the request through.
                    tracing::warn!(error = %e, "rate limiter backend error — failing open");
                    inner.call(req).await.map_err(Into::into)
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;
    use mockall::predicate::*;
    use std::{
        convert::Infallible,
        future::{Ready, ready},
    };
    use tower::{Layer, Service, ServiceExt};
    use triad_runner::patterns::{RateLimitAlgorithm, RateLimiter};

    // ── Minimal inner service that always returns 200 ───────────────────────

    #[derive(Clone)]
    struct OkService;

    impl Service<Request<Body>> for OkService {
        type Response = Response<Body>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<Body>) -> Self::Future {
            ready(Ok(Response::new(Body::empty())))
        }
    }

    // ── IdempotencyLayer ────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_idempotency_passes_through_when_no_header() {
        let mut mock = MockIdempotencyBackend::new();
        mock.expect_set_if_absent().never();

        let layer = IdempotencyLayer::new(Arc::new(mock), Duration::from_secs(3600));
        let mut svc = layer.layer(OkService);

        let req = Request::builder().uri("/").body(Body::empty()).unwrap();
        let resp = svc.ready().await.unwrap().call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_idempotency_passes_through_new_key() {
        let mut mock = MockIdempotencyBackend::new();
        mock.expect_set_if_absent()
            .with(eq("key-123"), always())
            .returning(|_, _| true);

        let layer = IdempotencyLayer::new(Arc::new(mock), Duration::from_secs(3600));
        let mut svc = layer.layer(OkService);

        let req = Request::builder()
            .uri("/")
            .header(IDEMPOTENCY_KEY_HEADER, "key-123")
            .body(Body::empty())
            .unwrap();
        let resp = svc.ready().await.unwrap().call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_idempotency_rejects_duplicate_key() {
        let mut mock = MockIdempotencyBackend::new();
        mock.expect_set_if_absent().returning(|_, _| false);

        let layer = IdempotencyLayer::new(Arc::new(mock), Duration::from_secs(3600));
        let mut svc = layer.layer(OkService);

        let req = Request::builder()
            .uri("/")
            .header(IDEMPOTENCY_KEY_HEADER, "dup-key")
            .body(Body::empty())
            .unwrap();
        let resp = svc.ready().await.unwrap().call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ALREADY_REPORTED);
    }

    // ── RateLimitLayer ──────────────────────────────────────────────────────

    struct AllowedBackend;

    #[async_trait::async_trait]
    impl triad_runner::patterns::rate_limit::RateLimitBackend for AllowedBackend {
        async fn sliding_window_check(
            &self,
            _key: &str,
            _window_ms: u64,
            limit: u64,
            _now_ms: u64,
        ) -> Result<(bool, u64), triad_core::error::PatternError> {
            Ok((true, limit))
        }

        async fn token_bucket_check(
            &self,
            _key: &str,
            capacity: u64,
            _refill_window_secs: u64,
        ) -> Result<(bool, u64), triad_core::error::PatternError> {
            Ok((true, capacity))
        }
    }

    struct RejectedBackend;

    #[async_trait::async_trait]
    impl triad_runner::patterns::rate_limit::RateLimitBackend for RejectedBackend {
        async fn sliding_window_check(
            &self,
            _key: &str,
            _window_ms: u64,
            _limit: u64,
            _now_ms: u64,
        ) -> Result<(bool, u64), triad_core::error::PatternError> {
            Ok((false, 0))
        }

        async fn token_bucket_check(
            &self,
            _key: &str,
            _capacity: u64,
            _refill_window_secs: u64,
        ) -> Result<(bool, u64), triad_core::error::PatternError> {
            Ok((false, 0))
        }
    }

    #[tokio::test]
    async fn test_rate_limit_passes_allowed_request() {
        let limiter = Arc::new(RateLimiter::new(
            "test",
            Arc::new(AllowedBackend),
            100,
            RateLimitAlgorithm::SlidingWindow { window_ms: 60_000 },
        ));
        let layer = RateLimitLayer::new(limiter, |_req| "user-1".to_string());
        let mut svc = layer.layer(OkService);

        let req = Request::builder().uri("/").body(Body::empty()).unwrap();
        let resp = svc.ready().await.unwrap().call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_rate_limit_rejects_over_limit() {
        let limiter = Arc::new(RateLimiter::new(
            "test",
            Arc::new(RejectedBackend),
            100,
            RateLimitAlgorithm::SlidingWindow { window_ms: 60_000 },
        ));
        let layer = RateLimitLayer::new(limiter, |_req| "user-2".to_string());
        let mut svc = layer.layer(OkService);

        let req = Request::builder().uri("/").body(Body::empty()).unwrap();
        let resp = svc.ready().await.unwrap().call(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    }
}
