use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Holds the root cancellation token and installs the SIGTERM signal handler.
///
/// CancellationToken flows top-down: Runner owns the root; PatternEngine
/// receives a child token; each module task receives a grandchild token.
pub struct ShutdownCoordinator {
    token: CancellationToken,
    drain_timeout: Duration,
}

impl ShutdownCoordinator {
    pub fn new(drain_timeout_seconds: u64) -> Self {
        Self {
            token: CancellationToken::new(),
            drain_timeout: Duration::from_secs(drain_timeout_seconds),
        }
    }

    /// Return a clone of the root cancellation token.
    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    /// Return the configured drain timeout.
    pub fn drain_timeout(&self) -> Duration {
        self.drain_timeout
    }

    /// Trigger shutdown immediately (useful in tests or programmatic shutdown).
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// Spawn a background task that cancels the token on SIGTERM, then
    /// force-exits after the drain timeout if the process has not yet stopped.
    pub fn install_signal_handler(&self) {
        let token = self.token.clone();
        let drain_timeout = self.drain_timeout;

        tokio::spawn(async move {
            wait_for_sigterm().await;
            info!("SIGTERM received — initiating graceful shutdown");
            token.cancel();

            tokio::time::sleep(drain_timeout).await;
            warn!(
                drain_timeout_secs = drain_timeout.as_secs(),
                "drain timeout exceeded — forcing exit"
            );
            std::process::exit(1);
        });
    }

    /// Await until the root token is cancelled (i.e. shutdown has been
    /// triggered, either by SIGTERM or by calling `cancel()`).
    pub async fn wait(&self) {
        self.token.cancelled().await;
    }
}

/// Platform-specific SIGTERM waiter.
#[cfg(unix)]
async fn wait_for_sigterm() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut stream = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
    stream.recv().await;
}

/// On non-Unix platforms (e.g. Windows CI) fall back to Ctrl-C.
#[cfg(not(unix))]
async fn wait_for_sigterm() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl-C handler");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_shutdown_coordinator_creates_uncancelled_token() {
        let coord = ShutdownCoordinator::new(30);
        assert!(!coord.token().is_cancelled());
    }

    #[test]
    fn test_shutdown_coordinator_drain_timeout() {
        let coord = ShutdownCoordinator::new(45);
        assert_eq!(coord.drain_timeout(), Duration::from_secs(45));
    }

    #[tokio::test]
    async fn test_shutdown_coordinator_cancel_cancels_token() {
        let coord = ShutdownCoordinator::new(30);
        let token = coord.token();
        assert!(!token.is_cancelled());
        coord.cancel();
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_shutdown_coordinator_wait_returns_after_cancel() {
        let coord = ShutdownCoordinator::new(30);
        let coord2 = ShutdownCoordinator {
            token: coord.token(),
            drain_timeout: coord.drain_timeout,
        };
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            coord2.cancel();
        });
        tokio::time::timeout(Duration::from_secs(1), coord.wait())
            .await
            .expect("wait() should return after cancel()");
    }

    #[test]
    fn test_child_token_cancelled_when_parent_cancelled() {
        let coord = ShutdownCoordinator::new(30);
        let child = coord.token().child_token();
        assert!(!child.is_cancelled());
        coord.cancel();
        assert!(child.is_cancelled());
    }
}
