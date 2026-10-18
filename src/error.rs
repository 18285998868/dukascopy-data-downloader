//! Error types shared by the downloader.
//!
//! [`AppError`] is the single error type crossing every layer boundary; it is
//! built with `thiserror` so `?` conversions are automatic and the `source()`
//! chain is preserved for diagnostics.

use thiserror::Error;

/// Application-level error.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AppError {
    /// HTTP transport error.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    /// I/O error.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Configuration error.
    #[error("config error: {0}")]
    Config(String),

    /// Data processing error.
    #[error("data error: {0}")]
    Data(String),

    /// A single download failed.
    #[error("download failed for {url}: {reason}")]
    DownloadFailed {
        /// URL that failed.
        url: String,
        /// Human-readable cause.
        reason: String,
    },

    /// Requested resource was not found (HTTP 404).
    #[error("not found: {url}")]
    NotFound {
        /// URL that returned 404.
        url: String,
    },

    /// One or more files inside a bounded-concurrency batch failed.
    ///
    /// `reasons` carries a bounded sample of the per-file causes so the
    /// aggregate message stays readable; the counts are always exact.
    #[error(
        "batch download failed: {failed}/{total} file(s) failed, {skipped} skipped: {}",
        .reasons.join("; ")
    )]
    BatchFailed {
        /// Number of files that failed.
        failed: usize,
        /// Number of files in the batch.
        total: usize,
        /// Files never attempted because the circuit breaker opened.
        skipped: usize,
        /// Bounded sample of per-file failure reasons.
        reasons: Vec<String>,
    },

    /// Tokio task join error.
    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

impl AppError {
    /// Render this error together with its full `source()` chain on one line.
    ///
    /// `Display` only shows the outermost message, which hides the underlying
    /// transport/IO cause; logs and batch summaries should use this instead.
    ///
    /// # Examples
    ///
    /// ```
    /// use ddd::AppError;
    ///
    /// let err = AppError::NotFound {
    ///     url: "https://example.invalid/x.bi5".to_string(),
    /// };
    /// assert_eq!(err.chain(), "not found: https://example.invalid/x.bi5");
    /// ```
    #[must_use]
    pub fn chain(&self) -> String {
        let mut out = self.to_string();
        let mut src = std::error::Error::source(self);
        while let Some(cause) = src {
            out.push_str(": ");
            out.push_str(&cause.to_string());
            src = cause.source();
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_without_source_is_plain_message() {
        let err = AppError::Config("boom".into());
        assert_eq!(err.chain(), "config error: boom");
    }

    #[test]
    fn chain_includes_source_cause() {
        let io = std::io::Error::new(std::io::ErrorKind::NotFound, "no such file");
        let err = AppError::Io(io);
        let rendered = err.chain();
        assert!(rendered.starts_with("i/o error: "), "{rendered}");
        assert!(rendered.contains("no such file"), "{rendered}");
    }

    #[test]
    fn chain_walks_nested_causes() {
        let inner = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset by peer");
        let io = std::io::Error::other(format!("outer: {inner}"));
        let err = AppError::Io(io);
        let rendered = err.chain();
        assert!(rendered.contains("outer"), "{rendered}");
        assert!(rendered.contains("reset by peer"), "{rendered}");
    }

    #[test]
    fn batch_failed_lists_reasons() {
        let err = AppError::BatchFailed {
            failed: 2,
            total: 5,
            skipped: 0,
            reasons: vec!["a: HTTP 500".into(), "b: HTTP 503".into()],
        };
        let msg = err.to_string();
        assert!(msg.contains("2/5"), "{msg}");
        assert!(msg.contains("a: HTTP 500"), "{msg}");
        assert!(msg.contains("b: HTTP 503"), "{msg}");
    }

    #[test]
    fn batch_failed_with_empty_reasons_still_renders() {
        let err = AppError::BatchFailed {
            failed: 1,
            total: 1,
            skipped: 0,
            reasons: Vec::new(),
        };
        assert!(err.to_string().contains("1/1"), "{err}");
    }

    #[test]
    fn batch_failed_reports_skipped_count() {
        let err = AppError::BatchFailed {
            failed: 5,
            total: 20,
            skipped: 15,
            reasons: vec!["boom".into()],
        };
        let msg = err.to_string();
        assert!(msg.contains("5/20"), "{msg}");
        assert!(msg.contains("15 skipped"), "{msg}");
    }

    #[test]
    fn batch_failed_always_states_the_skipped_count() {
        let err = AppError::BatchFailed {
            failed: 1,
            total: 1,
            skipped: 0,
            reasons: vec![],
        };
        assert!(err.to_string().contains("0 skipped"), "{err}");
    }
}
