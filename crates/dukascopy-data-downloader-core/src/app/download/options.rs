//! Batch-level options.

/// Options that control how a batch reacts to a range the feed does not have.
///
/// The default is the strict behaviour: a requested range that is (largely)
/// absent from the feed fails the run. The opt-out exists for the
/// "fetch whatever is there" workflow, which the strict default cannot
/// serve — the feed lags behind the market by weeks, so *any* range that
/// reaches into the lag zone would otherwise fail.
///
/// # Examples
///
/// ```
/// // The default is strict.
/// assert!(!ddd::DownloadOptions::default().allow_missing());
///
/// // The opt-out is explicit.
/// let opts = ddd::DownloadOptions::new().with_allow_missing(true);
/// assert!(opts.allow_missing());
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct DownloadOptions {
    allow_missing: bool,
}

impl DownloadOptions {
    /// Strict options: a range the feed does not have fails the batch.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            allow_missing: false,
        }
    }

    /// Tolerate a requested range that is missing from the data feed.
    #[must_use]
    pub const fn with_allow_missing(mut self, allow_missing: bool) -> Self {
        self.allow_missing = allow_missing;
        self
    }

    /// Whether the range checks are relaxed.
    #[must_use]
    pub const fn allow_missing(&self) -> bool {
        self.allow_missing
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_is_strict() {
        assert!(!DownloadOptions::default().allow_missing());
        assert!(!DownloadOptions::new().allow_missing());
    }

    #[test]
    fn the_builder_sets_and_clears_the_opt_out() {
        let opts = DownloadOptions::new().with_allow_missing(true);
        assert!(opts.allow_missing());
        assert!(!opts.with_allow_missing(false).allow_missing());
    }
}
