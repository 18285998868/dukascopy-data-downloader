//! Aggregate batch statistics.

/// Aggregate statistics for a completed batch of downloads.
///
/// The four outcome counters are mutually exclusive and add up to
/// [`DownloadStats::total`]: every task of the batch ended in exactly one
/// of them. The struct is only produced by the download pipeline; its
/// counters are read through accessors.
///
/// # Examples
///
/// ```
/// let stats = ddd::DownloadStats::default();
/// assert_eq!(stats.total(), 0);
/// assert_eq!(stats.ok(), 0);
/// assert_eq!(stats.not_found(), 0);
/// assert_eq!(stats.failed(), 0);
/// assert_eq!(stats.skipped(), 0);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct DownloadStats {
    total: usize,
    ok: usize,
    not_found: usize,
    failed: usize,
    skipped: usize,
}

impl DownloadStats {
    /// Statistics for a batch of `total` tasks that has not run yet.
    pub(super) const fn with_total(total: usize) -> Self {
        Self {
            total,
            ok: 0,
            not_found: 0,
            failed: 0,
            skipped: 0,
        }
    }

    /// Number of tasks in the batch.
    #[must_use]
    pub const fn total(&self) -> usize {
        self.total
    }

    /// Tasks written to disk successfully.
    #[must_use]
    pub const fn ok(&self) -> usize {
        self.ok
    }

    /// Tasks answered with HTTP 404: the feed does not have that file, which
    /// usually means the requested range is outside its coverage.
    #[must_use]
    pub const fn not_found(&self) -> usize {
        self.not_found
    }

    /// Tasks that failed for any other reason.
    #[must_use]
    pub const fn failed(&self) -> usize {
        self.failed
    }

    /// Tasks never attempted because the circuit breaker opened.
    #[must_use]
    pub const fn skipped(&self) -> usize {
        self.skipped
    }

    /// Number of tasks whose outcome has been accounted for.
    pub(super) const fn done(&self) -> usize {
        self.ok
            .saturating_add(self.not_found)
            .saturating_add(self.failed)
            .saturating_add(self.skipped)
    }

    /// Whether the circuit breaker tripped during this batch.
    pub(super) const fn circuit_open(&self) -> bool {
        self.skipped > 0
    }

    /// Record one successful task.
    pub(super) fn record_ok(&mut self) {
        self.ok = self.ok.saturating_add(1);
    }

    /// Record one HTTP 404 task.
    pub(super) fn record_not_found(&mut self) {
        self.not_found = self.not_found.saturating_add(1);
    }

    /// Record one failed task.
    pub(super) fn record_failed(&mut self) {
        self.failed = self.failed.saturating_add(1);
    }

    /// Record one task skipped by the circuit breaker.
    pub(super) fn record_skipped(&mut self) {
        self.skipped = self.skipped.saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The four counters are mutually exclusive and add up to the total.
    #[test]
    fn the_counters_add_up_to_the_total() {
        let mut stats = DownloadStats::with_total(10);
        for _ in 0..4 {
            stats.record_ok();
        }
        for _ in 0..3 {
            stats.record_not_found();
        }
        for _ in 0..2 {
            stats.record_failed();
        }
        stats.record_skipped();
        assert_eq!(stats.done(), stats.total());
    }

    /// The breaker is considered open exactly when files were skipped.
    #[test]
    fn the_circuit_is_open_only_when_files_were_skipped() {
        let mut stats = DownloadStats::with_total(3);
        for _ in 0..3 {
            stats.record_ok();
        }
        assert!(!stats.circuit_open());
        stats.record_skipped();
        assert!(stats.circuit_open());
    }
}
