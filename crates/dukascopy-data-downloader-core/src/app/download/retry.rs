//! Retry policy for transient download failures.
//!
//! The policy is a backon exponential schedule with one crucial twist: the
//! retry budget is an **absolute wall-clock deadline**, not a sum of
//! intervals. Both the time a failed attempt spends on the wire and the time
//! the server asks us to wait (`Retry-After`) are charged against that
//! deadline.
//!
//! Backon's schedule alone cannot express that: `total_delay` only counts the
//! sleeps it schedules, so a slow attempt (or a server-provided wait) would
//! not shorten the window. `Retry::run` therefore clamps every scheduled
//! delay to the remaining budget and stops once the deadline has passed.
//!
//! A `Retry-After` hint is spent by the *operation*, not handed to the retry
//! loop: the operation sleeps it before returning `RetryError::Transient`,
//! so the hint is charged to the deadline and the exponential schedule stays
//! in charge of the next attempt.

use std::future::Future;
use std::time::{Duration, Instant};

use backon::Retryable as _;

/// Elapsed-time budget for retrying a single file.
///
/// Deliberately ~4x the per-request timeout in `app::client`: a single
/// stalled attempt must not be able to consume the entire retry window, or the
/// retry policy silently degrades into "try once".
pub(crate) const MAX_ELAPSED_SECS: u64 = 120;

/// Backon's builder, re-exported under a local name so call sites do not
/// depend on the backon API directly.
pub(crate) use backon::ExponentialBuilder as RetryPolicy;

/// Default retry policy: 500 ms initial delay, 1.5x growth, 60 s cap, bounded
/// by the 120 s cumulative-sleep budget.
#[must_use]
pub(crate) fn default_policy() -> RetryPolicy {
    RetryPolicy::new()
        .with_min_delay(Duration::from_millis(500))
        .with_max_delay(Duration::from_secs(60))
        .with_factor(1.5)
        .with_total_delay(Some(Duration::from_secs(MAX_ELAPSED_SECS)))
        .without_max_times()
}

/// The wall-clock budget of one download.
#[derive(Debug, Clone, Copy)]
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
}

/// The result of one attempt, with enough metadata for the retry loop.
#[derive(Debug)]
pub(crate) enum RetryError<E> {
    /// The operation may succeed if tried again.
    Transient(E),
    /// The operation must not be retried.
    Permanent(E),
}

impl<E> RetryError<E> {
    /// Mark an error as retryable.
    pub(crate) fn transient(err: E) -> Self {
        Self::Transient(err)
    }

    /// Mark an error as permanent.
    pub(crate) fn permanent(err: E) -> Self {
        Self::Permanent(err)
    }
}

/// A retry schedule together with the absolute deadline it must respect.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Retry {
    policy: RetryPolicy,
    budget: RetryBudget,
}

impl Retry {
    /// Pair `policy` with a budget that expires `max_elapsed` from now.
    pub(crate) fn new(policy: RetryPolicy, max_elapsed: Duration) -> Self {
        Self {
            policy,
            budget: RetryBudget::new(max_elapsed),
        }
    }

    /// The budget the operation must charge a `Retry-After` hint against.
    pub(crate) fn budget(&self) -> RetryBudget {
        self.budget
    }

    /// Run `operation` until it succeeds, fails permanently, or the budget
    /// expires.
    ///
    /// Every delay is capped at the remaining budget; a permanent error stops
    /// immediately. The returned error is the underlying [`crate::AppError`],
    /// so callers do not need to unwrap the retry wrapper.
    ///
    /// # Errors
    ///
    /// Returns the final underlying error when the operation is permanent or
    /// the retry budget is exhausted.
    pub(crate) async fn run<T, F, Fut>(self, operation: F) -> Result<T, crate::AppError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, RetryError<crate::AppError>>>,
    {
        let budget = self.budget;
        let result = operation
            .retry(self.policy)
            .when(|error| matches!(error, RetryError::Transient(_)))
            .adjust(move |_, delay| {
                // `None` means backon's own schedule is exhausted (its
                // `total_delay` or `max_times`): keep that stop decision.
                let delay = delay?;
                let remaining = budget.remaining();
                if remaining.is_zero() {
                    None
                } else {
                    Some(delay.min(remaining))
                }
            })
            .notify(|error, delay| {
                if let RetryError::Transient(err) = error {
                    tracing::warn!(error = %err, ?delay, "download failed; retrying");
                }
            })
            .await;

        match result {
            Ok(value) => Ok(value),
            Err(RetryError::Transient(err) | RetryError::Permanent(err)) => Err(err),
        }
    }
}

/// Default retry: the documented exponential schedule under a 120 s budget.
#[must_use]
pub(crate) fn default_retry() -> Retry {
    Retry::new(default_policy(), Duration::from_secs(MAX_ELAPSED_SECS))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn transient() -> RetryError<crate::AppError> {
        RetryError::transient(crate::AppError::DownloadFailed {
            url: "http://example.invalid/a.bi5".to_string(),
            reason: "HTTP 500".to_string(),
        })
    }

    fn instant_policy() -> RetryPolicy {
        RetryPolicy::new()
            .with_min_delay(Duration::ZERO)
            .with_max_delay(Duration::ZERO)
            .with_total_delay(Some(Duration::from_secs(1)))
            .without_max_times()
    }

    #[test]
    fn default_retry_uses_the_documented_budget() {
        let retry = default_retry();
        assert!(
            retry.budget().remaining() <= Duration::from_secs(MAX_ELAPSED_SECS),
            "the budget must not exceed the documented window"
        );
    }

    #[test]
    fn the_budget_counts_down() {
        let budget = RetryBudget::new(Duration::from_millis(50));
        assert!(!budget.remaining().is_zero());
        std::thread::sleep(Duration::from_millis(60));
        assert!(budget.remaining().is_zero());
    }

    #[tokio::test]
    async fn retries_transient_errors_then_succeeds() {
        let calls = AtomicUsize::new(0);
        let result = Retry::new(instant_policy(), Duration::from_secs(5))
            .run(|| {
                let call = calls.fetch_add(1, Ordering::Relaxed);
                async move { if call < 2 { Err(transient()) } else { Ok(7_u8) } }
            })
            .await;
        assert_eq!(result.unwrap(), 7);
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn does_not_retry_permanent_errors() {
        let calls = AtomicUsize::new(0);
        let result: Result<(), _> = Retry::new(instant_policy(), Duration::from_secs(5))
            .run(|| {
                calls.fetch_add(1, Ordering::Relaxed);
                async {
                    Err(RetryError::permanent(crate::AppError::DownloadFailed {
                        url: "u".to_string(),
                        reason: "HTTP 404".to_string(),
                    }))
                }
            })
            .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn an_exhausted_budget_allows_exactly_one_attempt() {
        let calls = AtomicUsize::new(0);
        let result: Result<(), _> = Retry::new(instant_policy(), Duration::ZERO)
            .run(|| {
                calls.fetch_add(1, Ordering::Relaxed);
                async { Err(transient()) }
            })
            .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    /// The deadline, not backon's `total_delay`, bounds the retry loop.
    #[tokio::test]
    async fn the_deadline_caps_every_delay() {
        let calls = AtomicUsize::new(0);
        let retry = Retry::new(
            RetryPolicy::new()
                .with_min_delay(Duration::from_secs(60))
                .with_max_delay(Duration::from_secs(60))
                .without_max_times(),
            Duration::from_millis(100),
        );
        let started = Instant::now();
        let result: Result<(), _> = retry
            .run(|| {
                calls.fetch_add(1, Ordering::Relaxed);
                async { Err(transient()) }
            })
            .await;
        assert!(result.is_err());
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "a 60s interval must be clamped to the 100ms budget"
        );
        assert!(
            calls.load(Ordering::Relaxed) >= 2,
            "the operation must be retried at least once"
        );
    }
}
