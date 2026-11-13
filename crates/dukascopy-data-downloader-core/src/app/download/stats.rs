//! Aggregate batch statistics.

/// Aggregate statistics for a completed batch of downloads.
///
/// The four outcome counters are mutually exclusive and add up to
/// [`DownloadStats::total`]: every task of the batch ended in exactly one
/// of them.
///
/// # Examples
///
/// ```
/// use ddd::app::download::DownloadStats;
///
/// let stats = DownloadStats {
///     total: 10,
///     ok: 7,
///     not_found: 2,
///     failed: 1,
///     skipped: 0,
/// };
/// // The counters are mutually exclusive and add up to `total`.
/// assert_eq!(
///     stats.ok + stats.not_found + stats.failed + stats.skipped,
///     stats.total
/// );
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DownloadStats {
    /// Number of tasks in the batch.
    pub total: usize,
    /// Tasks written to disk successfully.
    pub ok: usize,
    /// Tasks answered with HTTP 404: the feed does not have that file, which
    /// usually means the requested range is outside its coverage.
    pub not_found: usize,
    /// Tasks that failed for any other reason.
    pub failed: usize,
    /// Tasks never attempted because the circuit breaker opened.
    pub skipped: usize,
}

impl DownloadStats {
    /// Number of tasks whose outcome has been accounted for.
    pub(super) const fn done(&self) -> usize {
        self.ok + self.not_found + self.failed + self.skipped
    }

    /// Whether the circuit breaker tripped during this batch.
    pub(super) const fn circuit_open(&self) -> bool {
        self.skipped > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The four counters are mutually exclusive and add up to the total.
    #[test]
    fn the_counters_add_up_to_the_total() {
        let stats = DownloadStats {
            total: 10,
            ok: 4,
            not_found: 3,
            failed: 2,
            skipped: 1,
        };
        assert_eq!(stats.done(), stats.total);
    }

    /// The breaker is considered open exactly when files were skipped.
    #[test]
    fn the_circuit_is_open_only_when_files_were_skipped() {
        let mut stats = DownloadStats {
            total: 3,
            ok: 3,
            ..DownloadStats::default()
        };
        assert!(!stats.circuit_open());
        stats.skipped = 1;
        assert!(stats.circuit_open());
    }
}
