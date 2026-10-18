//! Retry policy for transient download failures.

use backoff::ExponentialBackoff;

/// Elapsed-time budget for retrying a single file.
///
/// Deliberately ~4x the per-request timeout in `app::build_client`: a single
/// stalled attempt must not be able to consume the entire retry window, or the
/// retry policy silently degrades into "try once".
const MAX_ELAPSED_SECS: u64 = 120;

/// Default exponential backoff with a 30s elapsed-time cap.
///
/// Returns a fresh value per download because [`backoff::ExponentialBackoff`]
/// tracks the current attempt and therefore cannot be shared.
pub(crate) fn default_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        max_elapsed_time: Some(std::time::Duration::from_secs(MAX_ELAPSED_SECS)),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use backoff::backoff::Backoff as _;

    #[test]
    fn default_backoff_caps_elapsed_time() {
        let b = default_backoff();
        assert_eq!(
            b.max_elapsed_time,
            Some(std::time::Duration::from_secs(MAX_ELAPSED_SECS))
        );
    }

    #[test]
    fn default_backoff_grows_between_attempts() {
        let b = default_backoff();
        assert!(
            b.initial_interval.as_millis() >= 100,
            "{:?}",
            b.initial_interval
        );
        assert!(b.multiplier > 1.0, "{}", b.multiplier);
    }

    #[test]
    fn default_backoff_is_fresh_per_call() {
        let mut a = default_backoff();
        let mut fresh = default_backoff();
        let _ = a.next_backoff();
        let _ = fresh.next_backoff();
        assert_ne!(
            a.next_backoff(),
            fresh.next_backoff(),
            "each download needs its own backoff state"
        );
    }
}
