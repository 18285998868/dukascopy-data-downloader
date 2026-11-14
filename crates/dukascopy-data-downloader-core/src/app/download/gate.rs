//! Shared download gate.

use std::num::NonZeroUsize;
use std::sync::Arc;

use tokio::sync::{AcquireError, Semaphore, SemaphorePermit};

/// Bounded-concurrency gate shared by every pair of a run.
///
/// The gate owns both the ceiling and the permits, so a caller cannot pass a
/// concurrency value that disagrees with the semaphore. Cloning is cheap: the
/// permits live behind one [`Arc`], which means every pair of a run competes
/// for the same global budget instead of each getting its own copy.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroUsize;
///
/// let gate = ddd::DownloadGate::new(NonZeroUsize::new(4).unwrap());
/// assert_eq!(gate.max_in_flight().get(), 4);
/// ```
#[derive(Debug, Clone)]
pub struct DownloadGate {
    max_in_flight: NonZeroUsize,
    permits: Arc<Semaphore>,
}

impl DownloadGate {
    /// Create a gate admitting at most `max_in_flight` downloads at once.
    #[must_use]
    pub fn new(max_in_flight: NonZeroUsize) -> Self {
        Self {
            max_in_flight,
            permits: Arc::new(Semaphore::new(max_in_flight.get())),
        }
    }

    /// Maximum number of downloads that may be in flight at once.
    ///
    /// Also used as the width of the per-batch `buffer_unordered` stream, so
    /// the shared gate is never the narrower of the two by accident.
    #[must_use]
    pub const fn max_in_flight(&self) -> NonZeroUsize {
        self.max_in_flight
    }

    /// Close the gate: every current and future acquire fails.
    ///
    /// This is a deliberate stop signal, not an error path in the download
    /// itself; waiters surface it as [`crate::AppError::GateClosed`].
    pub fn close(&self) {
        self.permits.close();
    }

    /// Wait for one permit from the shared pool.
    pub(crate) async fn acquire(&self) -> Result<SemaphorePermit<'_>, AcquireError> {
        self.permits.acquire().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn gate(n: usize) -> DownloadGate {
        DownloadGate::new(NonZeroUsize::new(n).expect("test gate is non-zero"))
    }

    #[test]
    fn the_ceiling_is_reported() {
        assert_eq!(gate(7).max_in_flight().get(), 7);
    }

    #[tokio::test]
    async fn cloned_gates_share_one_permit_pool() {
        let gate = gate(1);
        let clone = gate.clone();
        let _held = gate.acquire().await.expect("first permit is available");

        assert!(
            tokio::time::timeout(Duration::from_millis(50), clone.acquire())
                .await
                .is_err(),
            "a clone must compete for the same permit"
        );
    }

    #[tokio::test]
    async fn a_closed_gate_refuses_new_permits() {
        let gate = gate(1);
        gate.close();
        assert!(gate.acquire().await.is_err());
    }
}
