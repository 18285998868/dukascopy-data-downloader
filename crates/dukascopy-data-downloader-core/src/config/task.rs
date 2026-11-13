//! Concrete download tasks.

use std::path::PathBuf;

use crate::config::{BucketKey, Symbol, Timeframe};

/// A download task: source URL, destination path and identifying metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadTask {
    /// Fully-qualified source URL.
    pub url: String,
    /// Absolute or `output_dir`-relative destination path for the payload.
    pub dest: PathBuf,
    /// Trading symbol this task belongs to.
    pub symbol: Symbol,
    /// Calendar bucket this task's file covers.
    pub bucket: BucketKey,
}

impl DownloadTask {
    /// Timeframe this task belongs to, derived from its bucket.
    #[must_use]
    pub const fn timeframe(&self) -> Timeframe {
        self.bucket.timeframe()
    }
}
