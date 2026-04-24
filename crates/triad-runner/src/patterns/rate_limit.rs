use std::sync::Arc;

use async_trait::async_trait;
use metrics::counter;
use tracing::{debug, instrument};
use triad_core::{error::PatternError, metrics::names};

/// Decision returned by a rate limiter check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RateLimitDecision {
    /// Request is within the limit.
    Allowed { remaining: u64 },
    /// Request exceeds the limit.
    Rejected { retry_after_ms: Option<u64> },
}

impl RateLimitDecision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed { .. })
    }
}

/// Narrow trait for Redis sliding-window and token-bucket operations.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait RateLimitBackend: Send + Sync {
    /// Sliding window check: ZADD + ZREMRANGEBYSCORE + ZCOUNT.
    /// Returns (allowed, current_count).
    async fn sliding_window_check(
        &self,
        key: &str,
        window_ms: u64,
        limit: u64,
        now_ms: u64,
    ) -> Result<(bool, u64), PatternError>;

    /// Token bucket check: INCR + EXPIRE approach.
    /// Returns (allowed, remaining_tokens).
    async fn token_bucket_check(
        &self,
        key: &str,
        capacity: u64,
        refill_window_secs: u64,
    ) -> Result<(bool, u64), PatternError>;
}

/// Rate limit algorithm selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RateLimitAlgorithm {
    SlidingWindow {
        window_ms: u64,
    },
    TokenBucket {
        capacity: u64,
        refill_window_secs: u64,
    },
}

/// Redis-backed rate limiter.
///
/// Uses two algorithms:
/// - `SlidingWindow`: ZADD/ZREMRANGEBYSCORE/ZCOUNT pattern for precise rate tracking.
/// - `TokenBucket`: INCR + EXPIRE for simple burst allowance.
pub struct RateLimiter {
    pub name: String,
    pub backend: Arc<dyn RateLimitBackend>,
    pub limit: u64,
    pub algorithm: RateLimitAlgorithm,
}

impl RateLimiter {
    pub fn new(
        name: impl Into<String>,
        backend: Arc<dyn RateLimitBackend>,
        limit: u64,
        algorithm: RateLimitAlgorithm,
    ) -> Self {
        Self {
            name: name.into(),
            backend,
            limit,
            algorithm,
        }
    }

    #[instrument(skip(self), fields(key, pattern_name = %self.name))]
    pub async fn check(&self, key: &str) -> Result<RateLimitDecision, PatternError> {
        counter!(names::RATE_LIMIT_CHECKS_TOTAL, "pattern_name" => self.name.clone()).increment(1);

        let decision = match &self.algorithm {
            RateLimitAlgorithm::SlidingWindow { window_ms } => {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let (allowed, count) = self
                    .backend
                    .sliding_window_check(key, *window_ms, self.limit, now_ms)
                    .await?;
                if allowed {
                    RateLimitDecision::Allowed {
                        remaining: self.limit.saturating_sub(count),
                    }
                } else {
                    RateLimitDecision::Rejected {
                        retry_after_ms: Some(*window_ms),
                    }
                }
            }
            RateLimitAlgorithm::TokenBucket {
                capacity,
                refill_window_secs,
            } => {
                let (allowed, remaining) = self
                    .backend
                    .token_bucket_check(key, *capacity, *refill_window_secs)
                    .await?;
                if allowed {
                    RateLimitDecision::Allowed { remaining }
                } else {
                    RateLimitDecision::Rejected {
                        retry_after_ms: None,
                    }
                }
            }
        };

        debug!(key, allowed = decision.is_allowed(), "rate limit check");
        Ok(decision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[tokio::test]
    async fn test_rate_limit_sliding_window_allowed() {
        let mut backend = MockRateLimitBackend::new();
        backend
            .expect_sliding_window_check()
            .returning(|_, _, _, _| Ok((true, 3)));

        let rl = RateLimiter::new(
            "test",
            Arc::new(backend),
            10,
            RateLimitAlgorithm::SlidingWindow { window_ms: 1000 },
        );
        let decision = rl.check("user:1").await.unwrap();
        assert!(decision.is_allowed());
        assert!(matches!(
            decision,
            RateLimitDecision::Allowed { remaining: 7 }
        ));
    }

    #[tokio::test]
    async fn test_rate_limit_sliding_window_rejected() {
        let mut backend = MockRateLimitBackend::new();
        backend
            .expect_sliding_window_check()
            .returning(|_, _, _, _| Ok((false, 10)));

        let rl = RateLimiter::new(
            "test",
            Arc::new(backend),
            10,
            RateLimitAlgorithm::SlidingWindow { window_ms: 5000 },
        );
        let decision = rl.check("user:2").await.unwrap();
        assert!(!decision.is_allowed());
        assert!(matches!(
            decision,
            RateLimitDecision::Rejected {
                retry_after_ms: Some(5000)
            }
        ));
    }

    #[tokio::test]
    async fn test_rate_limit_token_bucket_allowed() {
        let mut backend = MockRateLimitBackend::new();
        backend
            .expect_token_bucket_check()
            .returning(|_, _, _| Ok((true, 4)));

        let rl = RateLimiter::new(
            "test",
            Arc::new(backend),
            5,
            RateLimitAlgorithm::TokenBucket {
                capacity: 5,
                refill_window_secs: 60,
            },
        );
        let decision = rl.check("api:key123").await.unwrap();
        assert!(decision.is_allowed());
        assert!(matches!(
            decision,
            RateLimitDecision::Allowed { remaining: 4 }
        ));
    }

    #[tokio::test]
    async fn test_rate_limit_token_bucket_rejected() {
        let mut backend = MockRateLimitBackend::new();
        backend
            .expect_token_bucket_check()
            .returning(|_, _, _| Ok((false, 0)));

        let rl = RateLimiter::new(
            "test",
            Arc::new(backend),
            5,
            RateLimitAlgorithm::TokenBucket {
                capacity: 5,
                refill_window_secs: 60,
            },
        );
        let decision = rl.check("api:key456").await.unwrap();
        assert!(!decision.is_allowed());
    }

    #[tokio::test]
    async fn test_rate_limit_backend_error_propagates() {
        let mut backend = MockRateLimitBackend::new();
        backend
            .expect_sliding_window_check()
            .returning(|_, _, _, _| Err(PatternError::Saga("redis down".to_string())));

        let rl = RateLimiter::new(
            "test",
            Arc::new(backend),
            10,
            RateLimitAlgorithm::SlidingWindow { window_ms: 1000 },
        );
        assert!(rl.check("user:3").await.is_err());
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    fn test_rate_limit_decision_is_allowed(#[case] allowed: bool) {
        let decision = if allowed {
            RateLimitDecision::Allowed { remaining: 5 }
        } else {
            RateLimitDecision::Rejected {
                retry_after_ms: None,
            }
        };
        assert_eq!(decision.is_allowed(), allowed);
    }

    #[test]
    fn test_sliding_window_remaining_saturates_at_zero() {
        // When count >= limit, remaining = 0 (saturating_sub)
        let decision = RateLimitDecision::Allowed {
            remaining: 10u64.saturating_sub(15),
        };
        assert_eq!(decision, RateLimitDecision::Allowed { remaining: 0 });
    }
}
