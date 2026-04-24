use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use backoff::backoff::Backoff;
use metrics::counter;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use triad_core::{
    error::PatternError,
    metrics::names,
    traits::{PatternModule, RunContext},
    types::{PatternName, PipelineName},
};

/// Shared backpressure signal.
///
/// When `active` is `true`, pattern modules should pause their Kafka consumers.
/// The engine sets this flag by monitoring Redis memory usage; modules poll it
/// via `BackpressureSignal::is_active()`.
#[derive(Clone)]
pub struct BackpressureSignal(Arc<AtomicBool>);

impl BackpressureSignal {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn is_active(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    pub fn set(&self, active: bool) {
        self.0.store(active, Ordering::Relaxed);
    }
}

impl Default for BackpressureSignal {
    fn default() -> Self {
        Self::new()
    }
}

/// Monitors Redis memory and signals backpressure when usage exceeds threshold.
pub struct BackpressureController {
    signal: BackpressureSignal,
    /// Fraction (0.0–1.0) of maxmemory at which backpressure activates.
    threshold: f64,
}

impl BackpressureController {
    pub fn new(threshold: f64) -> Self {
        Self {
            signal: BackpressureSignal::new(),
            threshold,
        }
    }

    pub fn signal(&self) -> BackpressureSignal {
        self.signal.clone()
    }

    /// Called with the current Redis memory ratio (used/max).
    /// Activates or deactivates backpressure and emits a metric.
    pub fn update(&self, ratio: f64) {
        let active = ratio >= self.threshold;
        if active != self.signal.is_active() {
            self.signal.set(active);
            if active {
                warn!(threshold = self.threshold, ratio, "backpressure activated");
            } else {
                info!("backpressure deactivated");
            }
            metrics::gauge!(names::BACKPRESSURE_ACTIVE).set(if active { 1.0 } else { 0.0 });
        }
    }
}

/// Supervises all pattern module tasks.
///
/// - Each module runs in its own `tokio::task` inside a `JoinSet`.
/// - A module that returns `Err(_)` (other than `Cancelled`) is restarted
///   with exponential back-off. Cancellation (`Err(Cancelled)` or `Ok(())`)
///   terminates the task cleanly.
/// - The root `CancellationToken` is shared with the `Runner`; each module
///   receives an independent child token so modules can be cancelled individually.
pub struct PatternEngine {
    /// Modules waiting to be spawned. Drained into JoinSet on `start()`.
    pending: Vec<Box<dyn PatternModule>>,
    join_set: JoinSet<(String, Result<(), PatternError>)>,
    /// Root token — cancelling it propagates to all child tokens.
    cancel: CancellationToken,
    pub backpressure: BackpressureController,
}

impl PatternEngine {
    pub fn new(cancel: CancellationToken) -> Self {
        Self {
            pending: Vec::new(),
            join_set: JoinSet::new(),
            cancel,
            backpressure: BackpressureController::new(0.80),
        }
    }

    /// Add a module to the engine. Must be called before `start()`.
    pub fn add_module(&mut self, module: Box<dyn PatternModule>) {
        self.pending.push(module);
    }

    /// Spawn all registered modules. Each runs in a supervised restart loop.
    pub async fn start(&mut self) {
        let modules = std::mem::take(&mut self.pending);
        for module in modules {
            self.spawn_supervised(module);
        }
    }

    /// Spawn a single module with supervisor restart logic.
    fn spawn_supervised(&mut self, mut module: Box<dyn PatternModule>) {
        let child_cancel = self.cancel.child_token();
        let name = module.name().to_string();
        let pattern = PatternName(name.clone());
        let pipeline = PipelineName(name.clone());

        self.join_set.spawn(async move {
            let mut backoff = backoff::ExponentialBackoff {
                initial_interval: std::time::Duration::from_millis(50),
                max_interval: std::time::Duration::from_secs(30),
                max_elapsed_time: None, // retry indefinitely until cancel
                ..Default::default()
            };

            loop {
                let ctx = RunContext {
                    cancel: child_cancel.clone(),
                    pattern: pattern.clone(),
                    pipeline: pipeline.clone(),
                };

                match module.run(ctx).await {
                    Ok(()) => {
                        info!(pattern = %name, "module completed cleanly");
                        break;
                    }
                    Err(PatternError::Cancelled) => {
                        info!(pattern = %name, "module cancelled");
                        break;
                    }
                    Err(e) => {
                        error!(pattern = %name, error = %e, "module crashed — scheduling restart");
                        counter!(names::ERROR_TOTAL, "pattern_name" => name.clone()).increment(1);

                        match backoff.next_backoff() {
                            Some(delay) => {
                                // Wait for the back-off delay but bail early on cancellation.
                                tokio::select! {
                                    _ = tokio::time::sleep(delay) => {}
                                    _ = child_cancel.cancelled() => break,
                                }
                            }
                            None => {
                                error!(pattern = %name, "back-off exhausted — giving up");
                                break;
                            }
                        }
                    }
                }
            }
            (name, Ok(()))
        });
    }

    /// Cancel all module tokens and wait for the JoinSet to drain within
    /// `timeout`. Tasks that do not finish within the timeout are abandoned.
    pub async fn drain(&mut self, timeout: Duration) {
        self.cancel.cancel();

        let drain_fut = async { while self.join_set.join_next().await.is_some() {} };

        if tokio::time::timeout(timeout, drain_fut).await.is_err() {
            warn!(
                timeout_secs = timeout.as_secs(),
                "engine drain timed out — some tasks may still be running"
            );
        }
    }

    /// Number of modules still running in the JoinSet.
    pub fn running_count(&self) -> usize {
        self.join_set.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicU32, Ordering as AOrdering};
    use triad_core::types::ModuleHealth;

    // ── Test double ────────────────────────────────────────────────────────────

    struct TestModule {
        name: String,
        run_count: Arc<AtomicU32>,
        /// Number of times to return Err before returning Ok.
        fail_times: u32,
        /// If true, return Cancelled immediately.
        respond_to_cancel: bool,
    }

    impl TestModule {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                run_count: Arc::new(AtomicU32::new(0)),
                fail_times: 0,
                respond_to_cancel: false,
            }
        }

        fn with_fail_times(mut self, n: u32) -> Self {
            self.fail_times = n;
            self
        }

        fn cancellable(mut self) -> Self {
            self.respond_to_cancel = true;
            self
        }

        fn run_count(&self) -> Arc<AtomicU32> {
            Arc::clone(&self.run_count)
        }
    }

    #[async_trait]
    impl PatternModule for TestModule {
        fn name(&self) -> &str {
            &self.name
        }
        fn pattern_type(&self) -> &str {
            "test"
        }
        async fn run(&mut self, ctx: RunContext) -> Result<(), PatternError> {
            let n = self.run_count.fetch_add(1, AOrdering::SeqCst);
            if self.respond_to_cancel {
                ctx.cancel.cancelled().await;
                return Err(PatternError::Cancelled);
            }
            if n < self.fail_times {
                return Err(PatternError::Saga("simulated crash".into()));
            }
            Ok(())
        }
        async fn drain(&mut self, _timeout: Duration) -> Result<(), PatternError> {
            Ok(())
        }
        fn health(&self) -> ModuleHealth {
            ModuleHealth {
                state: triad_core::types::ModuleState::Running,
                lag: None,
                in_flight: None,
                last_error: None,
            }
        }
    }

    // ── Tests ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_engine_starts_and_drains_clean_module() {
        let cancel = CancellationToken::new();
        let mut engine = PatternEngine::new(cancel);
        engine.add_module(Box::new(TestModule::new("m1")));
        engine.start().await;
        assert_eq!(engine.running_count(), 1);
        engine.drain(Duration::from_secs(1)).await;
        assert_eq!(engine.running_count(), 0);
    }

    #[tokio::test]
    async fn test_engine_restarts_crashing_module() {
        // Module fails twice then succeeds. We verify it was restarted.
        // Strategy: poll run_count until it reaches 3 without calling drain()
        // (which cancels tokens and would interrupt backoff sleeps).
        let cancel = CancellationToken::new();
        let mut engine = PatternEngine::new(cancel.clone());
        let m = TestModule::new("crasher").with_fail_times(2);
        let run_count = m.run_count();
        engine.add_module(Box::new(m));
        engine.start().await;

        // Poll until run_count >= 3 or timeout.
        let rc = run_count.clone();
        tokio::time::timeout(Duration::from_secs(5), async move {
            loop {
                if rc.load(AOrdering::SeqCst) >= 3 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("module should restart and succeed within 5s");

        // The task completed successfully (module returned Ok); cancel for cleanup.
        engine.drain(Duration::from_secs(1)).await;
        assert!(run_count.load(AOrdering::SeqCst) >= 3);
    }

    #[tokio::test]
    async fn test_engine_cancel_stops_modules() {
        let cancel = CancellationToken::new();
        let mut engine = PatternEngine::new(cancel.clone());
        engine.add_module(Box::new(TestModule::new("looper").cancellable()));
        engine.start().await;
        assert_eq!(engine.running_count(), 1);
        cancel.cancel();
        // drain immediately — modules should stop on cancel
        engine.drain(Duration::from_secs(1)).await;
        assert_eq!(engine.running_count(), 0);
    }

    #[tokio::test]
    async fn test_engine_multiple_modules() {
        let cancel = CancellationToken::new();
        let mut engine = PatternEngine::new(cancel.clone());
        for i in 0..3 {
            engine.add_module(Box::new(TestModule::new(&format!("mod-{i}"))));
        }
        engine.start().await;
        assert_eq!(engine.running_count(), 3);
        engine.drain(Duration::from_secs(1)).await;
        assert_eq!(engine.running_count(), 0);
    }

    #[test]
    fn test_backpressure_signal_default_inactive() {
        let sig = BackpressureSignal::new();
        assert!(!sig.is_active());
    }

    #[test]
    fn test_backpressure_signal_set() {
        let sig = BackpressureSignal::new();
        sig.set(true);
        assert!(sig.is_active());
        sig.set(false);
        assert!(!sig.is_active());
    }

    #[test]
    fn test_backpressure_controller_activates_above_threshold() {
        let ctrl = BackpressureController::new(0.80);
        ctrl.update(0.79);
        assert!(!ctrl.signal().is_active());
        ctrl.update(0.80);
        assert!(ctrl.signal().is_active());
        ctrl.update(0.50);
        assert!(!ctrl.signal().is_active());
    }
}
