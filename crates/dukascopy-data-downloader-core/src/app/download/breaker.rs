//! The per-pair circuit breaker.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::AppError;

use super::ratio::cmp_ratio;

/// Number of recent attempts the breaker keeps in its sliding window.
///
/// Must stay above `MAX_REPORTED_FAILURES`: the breaker caps how many files
/// a pair may fail on, and a cap below the reason limit would make the limit
/// unreachable.
pub(super) const BREAKER_WINDOW: usize = 32;
/// Failure ratio inside the window that opens the breaker: 1/2 = 50%.
pub(super) const BREAKER_FAIL_NUM: usize = 1;
/// Denominator of [`BREAKER_FAIL_NUM`].
pub(super) const BREAKER_FAIL_DEN: usize = 2;

/// A sliding-window circuit breaker for one pair.
///
/// It records the outcome of the last [`BREAKER_WINDOW`] attempts and opens
/// once the failure ratio inside the window reaches
/// [`BREAKER_FAIL_NUM`]/[`BREAKER_FAIL_DEN`].
///
/// A *consecutive* failure counter would be simpler, but it has a blind spot
/// exactly where it is needed: any single success resets it, so a feed that
/// fails half of its requests never trips it and the run keeps burning the
/// retry budget on every single file. A window has no such blind spot.
#[derive(Debug)]
pub(super) struct Breaker {
    /// Outcome of the recent attempts: `true` means the attempt succeeded.
    window: Mutex<std::collections::VecDeque<bool>>,
    /// Latched "stop attempting" flag.
    open: AtomicBool,
}

impl Breaker {
    /// Create a closed breaker.
    pub(super) fn new() -> Self {
        Self {
            window: Mutex::new(std::collections::VecDeque::with_capacity(BREAKER_WINDOW)),
            open: AtomicBool::new(false),
        }
    }

    /// Record one attempt and report whether the breaker is now open.
    ///
    /// `succeeded` is true for a download that worked *and* for a 404: a file
    /// the server does not have says nothing about the pair's reachability.
    pub(super) fn record(&self, succeeded: bool) -> bool {
        let mut window = self.window.lock().unwrap_or_else(|poisoned| {
            // The window only holds booleans, so the data is still consistent
            // after a panic in a previous holder. Recover it rather than
            // turning a poisoned lock into a second panic.
            tracing::warn!("breaker window mutex was poisoned; recovering its contents");
            poisoned.into_inner()
        });
        if window.len() == BREAKER_WINDOW {
            window.pop_front();
        }
        window.push_back(succeeded);
        // The ratio only carries information once the window is full.
        if window.len() < BREAKER_WINDOW {
            return self.open.load(Ordering::Relaxed);
        }
        let failures = window.iter().filter(|&&ok| !ok).count();
        if cmp_ratio(failures, window.len(), BREAKER_FAIL_NUM, BREAKER_FAIL_DEN)
            != std::cmp::Ordering::Less
        {
            self.open.store(true, Ordering::Relaxed);
        }
        self.open.load(Ordering::Relaxed)
    }

    /// Open the breaker unconditionally.
    pub(super) fn trip(&self) {
        self.open.store(true, Ordering::Relaxed);
    }

    /// Whether the breaker is open, i.e. further attempts must be skipped.
    pub(super) fn is_open(&self) -> bool {
        self.open.load(Ordering::Relaxed)
    }
}

/// Whether the error means the endpoint could not be reached at all.
///
/// A transport failure (DNS, connect, timeout) applies to every file of the
/// pair and is proof that the pair is unreachable. An HTTP status failure is
/// about one answer, may well be transient, and says nothing about the rest
/// of the range.
pub(super) fn is_transport_failure(err: &AppError) -> bool {
    matches!(err, AppError::Http(_))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Half of the window failing is exactly the trip point.
    ///
    /// The threshold is "at least half", so the breaker opens on the
    /// attempt that completes the window with 16 failures out of 32 — not
    /// one attempt earlier.
    #[test]
    fn the_breaker_trips_at_exactly_half_the_window() {
        let breaker = Breaker::new();
        let half = BREAKER_WINDOW / 2;
        for _ in 0..half {
            assert!(
                !breaker.record(true),
                "a success must never trip the breaker"
            );
        }
        for i in 0..half {
            let last = i + 1 == half;
            let open = breaker.record(false);
            if last {
                assert!(
                    open,
                    "the breaker must trip once the window holds exactly half failures"
                );
            } else {
                assert!(
                    !open,
                    "the breaker must stay shut while the window is still filling"
                );
            }
        }
        assert!(breaker.is_open());
    }
}
