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
/// assert!(!ddd::DownloadOptions::default().allow_missing);
///
/// // The opt-out is explicit.
/// let opts = ddd::DownloadOptions {
///     allow_missing: true,
/// };
/// assert!(opts.allow_missing);
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DownloadOptions {
    /// Tolerate a requested range that is missing from the data feed.
    ///
    /// When `false` (the default) a batch whose range is largely absent
    /// fails with [`crate::AppError::RangeUnavailable`]. When `true` the same
    /// situation is only logged and the run finishes with whatever the feed
    /// did have.
    ///
    /// This covers the *range* checks only — the share of 404s, the longest
    /// consecutive gap, and the case where every requested file is missing.
    /// A file that actually failed to download (a transport error, a 5xx
    /// that outlived the retry budget) still fails the batch: that is a
    /// broken download, not a range the feed lacks.
    pub allow_missing: bool,
}
