use std::{
    future::Future,
    marker::PhantomData,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

use tokio::sync::watch;
use tracing::{info, warn};
use triad_core::error::BackendError;

/// Observable state of a circuit breaker.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CbState {
    /// Normal operation — all calls pass through.
    Closed,
    /// Too many failures — calls are rejected immediately.
    Open,
    /// Probing recovery — a limited number of calls are allowed through.
    HalfOpen,
}

/// Configuration for a `CircuitBreaker`.
#[derive(Clone, Debug)]
pub struct CbConfig {
    /// Consecutive failures in `Closed` state before tripping to `Open`.
    pub failure_threshold: u32,
    /// Consecutive successes in `HalfOpen` state before closing.
    pub success_threshold: u32,
    /// How long the breaker stays `Open` before probing with `HalfOpen`.
    pub half_open_after: Duration,
    /// Maximum concurrent probe calls allowed while in `HalfOpen`.
    pub half_open_max_calls: u32,
}

impl From<&triad_core::config::CircuitBreakerConfig> for CbConfig {
    fn from(cfg: &triad_core::config::CircuitBreakerConfig) -> Self {
        Self {
            failure_threshold: cfg.failure_threshold,
            success_threshold: cfg.success_threshold,
            half_open_after: Duration::from_millis(cfg.timeout_ms),
            half_open_max_calls: cfg.half_open_max_calls,
        }
    }
}

/// Tracks timing state requiring a `Mutex` (non-atomic).
struct CbTiming {
    /// Timestamp of the last transition to `Open`.
    opened_at: Option<Instant>,
}

/// Generic circuit breaker keyed by phantom type `S` (backend service marker).
///
/// State is broadcast via `watch::Sender<CbState>` so any number of tasks
/// can subscribe without polling. Failure/success counting uses `AtomicU32`
/// for lock-free fast-path reads.
pub struct CircuitBreaker<S = ()> {
    state: Arc<watch::Sender<CbState>>,
    failure_count: Arc<AtomicU32>,
    success_count: Arc<AtomicU32>,
    half_open_calls: Arc<AtomicU32>,
    config: CbConfig,
    backend_name: &'static str,
    timing: Arc<Mutex<CbTiming>>,
    _phantom: PhantomData<fn() -> S>,
}

impl<S> Clone for CircuitBreaker<S> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            failure_count: Arc::clone(&self.failure_count),
            success_count: Arc::clone(&self.success_count),
            half_open_calls: Arc::clone(&self.half_open_calls),
            config: self.config.clone(),
            backend_name: self.backend_name,
            timing: Arc::clone(&self.timing),
            _phantom: PhantomData,
        }
    }
}

impl<S> CircuitBreaker<S> {
    pub fn new(config: CbConfig, backend_name: &'static str) -> Self {
        let (tx, _rx) = watch::channel(CbState::Closed);
        Self {
            state: Arc::new(tx),
            failure_count: Arc::new(AtomicU32::new(0)),
            success_count: Arc::new(AtomicU32::new(0)),
            half_open_calls: Arc::new(AtomicU32::new(0)),
            config,
            backend_name,
            timing: Arc::new(Mutex::new(CbTiming { opened_at: None })),
            _phantom: PhantomData,
        }
    }

    /// Current circuit breaker state.
    pub fn state(&self) -> CbState {
        *self.state.borrow()
    }

    /// Subscribe to state change notifications (zero-cost watch receiver).
    pub fn subscribe(&self) -> watch::Receiver<CbState> {
        self.state.subscribe()
    }

    /// Execute a fallible async operation through the circuit breaker.
    ///
    /// Returns `Err(BackendError::CircuitBreakerOpen)` immediately when the
    /// breaker is `Open`. Records success/failure and drives state transitions.
    pub async fn call<F, T, E>(&self, f: F) -> Result<T, BackendError>
    where
        F: Future<Output = Result<T, E>>,
        E: Into<BackendError>,
    {
        // --- pre-call gate ---
        match self.state() {
            CbState::Open => {
                let elapsed = {
                    let t = self.timing.lock().expect("timing lock poisoned");
                    t.opened_at.map(|i| i.elapsed())
                };
                match elapsed {
                    Some(e) if e >= self.config.half_open_after => {
                        self.transition(CbState::HalfOpen);
                    }
                    _ => {
                        return Err(BackendError::CircuitBreakerOpen {
                            backend: self.backend_name.to_string(),
                        });
                    }
                }
            }
            CbState::Closed | CbState::HalfOpen => {}
        }

        // --- HalfOpen call-count gate ---
        let is_half_open = self.state() == CbState::HalfOpen;
        if is_half_open {
            let prev = self.half_open_calls.fetch_add(1, Ordering::AcqRel);
            if prev >= self.config.half_open_max_calls {
                self.half_open_calls.fetch_sub(1, Ordering::AcqRel);
                return Err(BackendError::CircuitBreakerOpen {
                    backend: self.backend_name.to_string(),
                });
            }
        }

        // --- execute ---
        let result = f.await;

        // --- post-call accounting ---
        match &result {
            Ok(_) => self.record_success(),
            Err(_) => self.record_failure(),
        }

        if is_half_open {
            self.half_open_calls.fetch_sub(1, Ordering::AcqRel);
        }

        result.map_err(|e| e.into())
    }

    fn record_success(&self) {
        match self.state() {
            CbState::Closed => {
                self.failure_count.store(0, Ordering::Release);
            }
            CbState::HalfOpen => {
                let successes = self.success_count.fetch_add(1, Ordering::AcqRel) + 1;
                if successes >= self.config.success_threshold {
                    info!(
                        backend = self.backend_name,
                        successes, "circuit breaker closing after probe successes"
                    );
                    self.success_count.store(0, Ordering::Release);
                    self.failure_count.store(0, Ordering::Release);
                    self.transition(CbState::Closed);
                }
            }
            CbState::Open => {}
        }
    }

    fn record_failure(&self) {
        match self.state() {
            CbState::Closed => {
                let failures = self.failure_count.fetch_add(1, Ordering::AcqRel) + 1;
                if failures >= self.config.failure_threshold {
                    warn!(
                        backend = self.backend_name,
                        failures, "circuit breaker opening after failure threshold"
                    );
                    self.transition(CbState::Open);
                }
            }
            CbState::HalfOpen => {
                warn!(
                    backend = self.backend_name,
                    "circuit breaker re-opening: probe call failed"
                );
                self.success_count.store(0, Ordering::Release);
                self.transition(CbState::Open);
            }
            CbState::Open => {}
        }
    }

    fn transition(&self, new_state: CbState) {
        if new_state == CbState::Open {
            let mut t = self.timing.lock().expect("timing lock poisoned");
            t.opened_at = Some(Instant::now());
            self.half_open_calls.store(0, Ordering::Release);
        }
        self.state.send_replace(new_state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    fn cb(failures: u32, successes: u32, half_open_after_ms: u64) -> CircuitBreaker {
        CircuitBreaker::new(
            CbConfig {
                failure_threshold: failures,
                success_threshold: successes,
                half_open_after: Duration::from_millis(half_open_after_ms),
                half_open_max_calls: 2,
            },
            "test",
        )
    }

    // ----- state helpers -----

    async fn ok_call(cb: &CircuitBreaker) -> Result<(), BackendError> {
        cb.call(async { Ok::<(), BackendError>(()) }).await
    }

    async fn fail_call(cb: &CircuitBreaker) -> Result<(), BackendError> {
        cb.call(async { Err::<(), BackendError>(BackendError::Kafka("boom".to_string())) })
            .await
    }

    // ----- unit tests -----

    #[tokio::test]
    async fn test_cb_initial_state_is_closed() {
        let cb = cb(3, 2, 100);
        assert_eq!(cb.state(), CbState::Closed);
    }

    #[tokio::test]
    async fn test_cb_closed_passes_calls_through() {
        let cb = cb(3, 2, 100);
        assert!(ok_call(&cb).await.is_ok());
    }

    #[rstest]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[tokio::test]
    async fn test_cb_stays_closed_below_threshold(#[case] threshold: u32) {
        let cb = cb(threshold + 1, 2, 100);
        for _ in 0..threshold {
            let _ = fail_call(&cb).await;
        }
        assert_eq!(cb.state(), CbState::Closed);
    }

    #[tokio::test]
    async fn test_cb_opens_after_failure_threshold() {
        let cb = cb(3, 2, 100);
        for _ in 0..3 {
            let _ = fail_call(&cb).await;
        }
        assert_eq!(cb.state(), CbState::Open);
    }

    #[tokio::test]
    async fn test_cb_open_rejects_calls() {
        let cb = cb(1, 2, 10_000); // very long half_open_after so it stays open
        let _ = fail_call(&cb).await;
        assert_eq!(cb.state(), CbState::Open);

        let result = ok_call(&cb).await;
        assert!(matches!(
            result,
            Err(BackendError::CircuitBreakerOpen { .. })
        ));
    }

    #[tokio::test]
    async fn test_cb_transitions_to_half_open_after_timeout() {
        let cb = cb(1, 2, 1); // 1ms timeout
        let _ = fail_call(&cb).await;
        assert_eq!(cb.state(), CbState::Open);

        tokio::time::sleep(Duration::from_millis(5)).await;
        // First call after timeout should trigger HalfOpen transition
        let _ = ok_call(&cb).await;
        // After one success in HalfOpen (success_threshold=2), still HalfOpen
        // unless we've hit the threshold
    }

    #[tokio::test]
    async fn test_cb_half_open_closes_after_success_threshold() {
        let cb = cb(1, 2, 1);
        let _ = fail_call(&cb).await;
        assert_eq!(cb.state(), CbState::Open);

        tokio::time::sleep(Duration::from_millis(5)).await;
        let _ = ok_call(&cb).await; // triggers HalfOpen, first success
        let _ = ok_call(&cb).await; // second success → Closed
        assert_eq!(cb.state(), CbState::Closed);
    }

    #[tokio::test]
    async fn test_cb_half_open_reopens_on_failure() {
        let cb = cb(1, 5, 1); // high success threshold so one failure re-opens
        let _ = fail_call(&cb).await;
        assert_eq!(cb.state(), CbState::Open);

        tokio::time::sleep(Duration::from_millis(5)).await;
        let _ = fail_call(&cb).await; // triggers HalfOpen then immediately re-opens
        assert_eq!(cb.state(), CbState::Open);
    }

    #[tokio::test]
    async fn test_cb_subscribe_receives_state_changes() {
        let cb = cb(1, 1, 1);
        let mut rx = cb.subscribe();

        let _ = fail_call(&cb).await;
        rx.changed().await.expect("watch should signal");
        assert_eq!(*rx.borrow(), CbState::Open);
    }

    #[tokio::test]
    async fn test_cb_clone_shares_state() {
        let cb1 = cb(1, 2, 10_000);
        let cb2 = cb1.clone();

        let _ = fail_call(&cb1).await; // opens cb1
        assert_eq!(cb2.state(), CbState::Open); // cb2 sees it
    }

    #[tokio::test]
    async fn test_cb_success_resets_failure_count_in_closed() {
        let cb = cb(3, 2, 100);
        // Two failures, then a success — should reset count
        let _ = fail_call(&cb).await;
        let _ = fail_call(&cb).await;
        let _ = ok_call(&cb).await;
        // Two more failures should not open (count was reset)
        let _ = fail_call(&cb).await;
        let _ = fail_call(&cb).await;
        assert_eq!(cb.state(), CbState::Closed);
    }

    #[tokio::test]
    async fn test_cb_from_core_config() {
        let core_cfg = triad_core::config::CircuitBreakerConfig {
            failure_threshold: 5,
            success_threshold: 3,
            timeout_ms: 1000,
            half_open_max_calls: 4,
        };
        let cb_cfg = CbConfig::from(&core_cfg);
        assert_eq!(cb_cfg.failure_threshold, 5);
        assert_eq!(cb_cfg.success_threshold, 3);
        assert_eq!(cb_cfg.half_open_after, Duration::from_millis(1000));
        assert_eq!(cb_cfg.half_open_max_calls, 4);
    }

    #[tokio::test]
    async fn test_cb_half_open_call_limit_enforced() {
        // max 1 concurrent probe call; second is rejected
        let config = CbConfig {
            failure_threshold: 1,
            success_threshold: 5,
            half_open_after: Duration::from_millis(1),
            half_open_max_calls: 1,
        };
        let cb = CircuitBreaker::<()>::new(config.clone(), "test");
        let _ = fail_call(&cb).await;
        assert_eq!(cb.state(), CbState::Open);

        tokio::time::sleep(Duration::from_millis(5)).await;

        // Manually set to HalfOpen
        cb.transition(CbState::HalfOpen);

        // Saturate the counter
        cb.half_open_calls.store(1, Ordering::SeqCst);

        let result = ok_call(&cb).await;
        assert!(
            matches!(result, Err(BackendError::CircuitBreakerOpen { .. })),
            "expected rejection when half_open_calls limit reached"
        );
    }
}
