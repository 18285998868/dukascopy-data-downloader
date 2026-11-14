//! Concrete download tasks.

use std::path::{Path, PathBuf};

use crate::config::{BucketKey, Symbol, Timeframe};

/// A download task: source URL, destination path and identifying metadata.
///
/// Build one with [`DownloadTask::new`] or let
/// [`crate::app::build_download_tasks`] do it. Fields are private so a task
/// can never pair a URL or destination with the wrong bucket; the tests and
/// the pipeline consume the read-only accessors.
///
/// # Examples
///
/// ```
/// use ddd::{BucketKey, DownloadTask, Symbol, Timeframe};
///
/// # fn main() -> Result<(), ddd::AppError> {
/// let task = DownloadTask::new(
///     "https://datafeed.dukascopy.com/datafeed/EURUSD/2025/BID_candles_day_1.bi5"
///         .to_string(),
///     "/tmp/dukascopy/EURUSD/2025/BID_candles_day_1.bi5".into(),
///     Symbol::new("EURUSD")?,
///     BucketKey::new(
///         Timeframe::D1,
///         chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
///     ),
/// );
/// assert_eq!(task.timeframe(), Timeframe::D1);
/// assert_eq!(task.symbol().as_str(), "EURUSD");
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DownloadTask {
    url: String,
    dest: PathBuf,
    symbol: Symbol,
    bucket: BucketKey,
}

impl DownloadTask {
    /// Build a task from its source URL, destination and identifying metadata.
    #[must_use]
    pub fn new(url: String, dest: PathBuf, symbol: Symbol, bucket: BucketKey) -> Self {
        Self {
            url,
            dest,
            symbol,
            bucket,
        }
    }

    /// Fully-qualified source URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Destination path for the payload.
    #[must_use]
    pub fn dest(&self) -> &Path {
        &self.dest
    }

    /// Trading symbol this task belongs to.
    #[must_use]
    pub const fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    /// Calendar bucket this task's file covers.
    #[must_use]
    pub const fn bucket(&self) -> BucketKey {
        self.bucket
    }

    /// Timeframe this task belongs to, derived from its bucket.
    #[must_use]
    pub const fn timeframe(&self) -> Timeframe {
        self.bucket.timeframe()
    }
}
