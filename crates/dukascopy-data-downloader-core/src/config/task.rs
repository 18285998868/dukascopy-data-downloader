//! Concrete download tasks.

use std::path::PathBuf;

use crate::config::{Symbol, Timeframe};

/// A download task: source URL, destination path and identifying metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadTask {
    /// Fully-qualified source URL.
    pub url: String,
    /// Absolute or `output_dir`-relative destination path for the payload.
    pub dest: PathBuf,
    /// Trading symbol this task belongs to.
    pub symbol: Symbol,
    /// Timeframe this task belongs to.
    pub timeframe: Timeframe,
}
