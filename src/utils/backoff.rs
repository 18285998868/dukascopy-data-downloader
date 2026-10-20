//! Retry policy for transient download failures.
//!
//! The policy is a plain exponential backoff with one crucial twist: the
//! retry budget is an **absolute wall-clock deadline**, not a sum of
//! intervals. Both the time a failed attempt spends on the wire and the time
//! the server asks us to wait (`Retry-After`) are charged against that
//! deadline.
//!
//! The reason is [`backoff::future::retry`]: when a failure carries a
//! `retry_after` hint, that crate uses the hint *instead of* asking the
//! backoff policy for the next interval — and the only place a stop decision
//! can be made is inside that method. A hint therefore disables the budget
//! entirely, and a server that answers every retry with `Retry-After` would
//! keep a single file retrying forever. Here the hint is consumed inside the
//! operation instead, so the policy is consulted on every attempt and the
//! deadline always applies.

use std::sync::Arc;
use std::time::{Duration, Instant};

use backoff::ExponentialBackoff;
use backoff::backoff::Backoff;

/// Elapsed-time budget for retrying a single file.
///
/// Deliberately ~4x the per-request timeout in `app::build_client`: a single
/// stalled attempt must not be able to consume the entire retry window, or the
/// retry policy silently degrades into "try once".
pub(crate) const MAX_ELAPSED_SECS: u64 = 120;

/// The wall-clock budget of one download, shared by the operation and the
/// backoff policy.
///
/// `Arc` because the operation needs the remaining time to clamp a server
/// wait hint, while the policy needs it to decide when to stop.
#[derive(Debug, Clone)]
pub(crate) struct RetryBudget {
    deadline: Instant,
}

impl RetryBudget {
    /// Start a budget that expires `max_elapsed` from now.
    pub(crate) fn new(max_elapsed: Duration) -> Self {
        Self {
            deadline: Instant::now() + max_elapsed,
        }
    }

    /// Time left before the deadline.
    pub(crate) fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// Whether the budget is used up.
    pub(crate) fn is_exhausted(&self) -> bool {
        self.remaining().is_zero()
    }
}

/// Exponential backoff that never outlives its [`RetryBudget`].
///
/// [`Backoff::next_backoff`] is the only place where a stop decision can be
/// made, so the budget is enforced here rather than delegated to
/// [`ExponentialBackoff`]'s own `max_elapsed_time`: that field accounts only
/// for the *sum of intervals*, which ignores how long the attempts themselves
/// took.
#[derive(Debug)]
pub(crate) struct BoundedBackoff {
    base: ExponentialBackoff,
    budget: Arc<RetryBudget>,
}

impl BoundedBackoff {
    /// Build a policy that stops when `budget` expires.
    pub(crate) fn new(budget: Arc<RetryBudget>) -> Self {
        Self {
            base: ExponentialBackoff::default(),
            budget,
        }
    }

    /// Build a policy with an explicit base interval and multiplier.
    ///
    /// Used by the tests to keep the retry wall-clock short.
    #[cfg(test)]
    pub(crate) fn with_base(
        budget: Arc<RetryBudget>,
        initial_interval: Duration,
        multiplier: f64,
    ) -> Self {
        Self {
            base: ExponentialBackoff {
                initial_interval,
                current_interval: initial_interval,
                multiplier,
                ..Default::default()
            },
            budget,
        }
    }

    /// The budget this policy enforces.
    pub(crate) fn budget(&self) -> Arc<RetryBudget> {
        Arc::clone(&self.budget)
    }
}

impl Backoff for BoundedBackoff {
    fn next_backoff(&mut self) -> Option<Duration> {
        if self.budget.is_exhausted() {
            return None;
        }
        let remaining = self.budget.remaining();
        // `ExponentialBackoff::default()` carries no `max_elapsed_time` of its
        // own, so it always yields an interval; clamping that interval to the
        // remaining budget is what keeps the deadline authoritative.
        let interval = self.base.next_backoff()?;
        Some(interval.min(remaining))
    }

    fn reset(&mut self) {
        self.base.reset();
    }
}

/// Default retry policy: exponential backoff under a 120s wall-clock budget.
///
/// Returns a fresh value per download because the policy tracks the current
/// attempt and therefore cannot be shared.
pub(crate) fn default_backoff() -> BoundedBackoff {
    BoundedBackoff::new(Arc::new(RetryBudget::new(Duration::from_secs(
        MAX_ELAPSED_SECS,
    ))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_backoff_uses_the_documented_budget() {
        let b = default_backoff();
        assert!(
            b.budget().remaining() <= Duration::from_secs(MAX_ELAPSED_SECS),
            "the budget must not exceed the documented window"
        );
    }

    #[test]
    fn an_exhausted_budget_yields_no_interval() {
        let budget = Arc::new(RetryBudget::new(Duration::ZERO));
        let mut b = BoundedBackoff::new(budget);
        assert_eq!(b.next_backoff(), None);
    }

    #[test]
    fn the_interval_never_outlives_the_remaining_budget() {
        let budget = Arc::new(RetryBudget::new(Duration::from_millis(10)));
        let mut b = BoundedBackoff::new(budget);
        let interval = b.next_backoff().expect("budget is not exhausted yet");
        assert!(
            interval <= Duration::from_millis(10),
            "interval {interval:?} exceeds the remaining budget"
        );
    }

    /// The base interval grows between attempts.
    ///
    /// The *returned* interval is randomised by ±50%, so two consecutive
    /// delays are not ordered — the growth has to be read from the policy's
    /// own `current_interval` instead.
    #[test]
    fn default_backoff_grows_between_attempts() {
        let mut b = default_backoff();
        let _ = b.next_backoff().expect("first interval");
        let before = b.base.current_interval;
        let _ = b.next_backoff().expect("second interval");
        assert!(
            b.base.current_interval > before,
            "the interval must grow: {before:?} then {:?}",
            b.base.current_interval
        );
    }

    #[test]
    fn default_backoff_is_fresh_per_call() {
        let mut a = default_backoff();
        let fresh = default_backoff();
        let _ = a.next_backoff();
        // `a` has advanced, `fresh` has not: they are independent states.
        assert!(a.base.current_interval > fresh.base.current_interval);
    }

    #[test]
    fn the_budget_counts_down() {
        let budget = RetryBudget::new(Duration::from_millis(50));
        assert!(!budget.is_exhausted());
        std::thread::sleep(Duration::from_millis(60));
        assert!(budget.is_exhausted());
        assert_eq!(budget.remaining(), Duration::ZERO);
    }
}
