//! Error types shared by the downloader.
//!
//! [`AppError`] is the single error type crossing every layer boundary; it is
//! built with `thiserror` so `?` conversions are automatic and the `source()`
//! chain is preserved for diagnostics.

use thiserror::Error;

/// Render a reason list as a parenthesised suffix.
///
/// An empty list yields an empty string, so a summary never ends in a
/// dangling separator.
fn reasons_suffix(reasons: &[String]) -> String {
    if reasons.is_empty() {
        String::new()
    } else {
        format!(" ({})", reasons.join("; "))
    }
}

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

    /// I/O error while operating on a specific path.
    ///
    /// Carries the path because a batch failure collects up to 20 such
    /// reasons: a bare [`std::io::Error`] renders as "i/o error: No such
    /// file or directory (os error 2)", which cannot be told apart from any
    /// other entry in the list.
    #[error("i/o error at {path}: {source}")]
    FileIo {
        /// Path that was being read or written.
        path: std::path::PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Configuration error.
    #[error("config error: {0}")]
    Config(String),

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
        "batch download failed: {failed}/{total} file(s) failed, {skipped} skipped{}",
        reasons_suffix(.reasons)
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

    /// One or more symbol/timeframe pairs failed during a run.
    ///
    /// Distinct from [`AppError::BatchFailed`], which reports failures of
    /// *files* inside a single pair. This variant reports failures of *pairs*
    /// inside a whole run, so the two levels never share a variant and the
    /// counts cannot be confused.
    ///
    /// `reasons` carries a bounded sample of the per-pair causes so the
    /// aggregate message stays readable; the count is always exact.
    #[error("{failed} pair(s) failed{}", reasons_suffix(.reasons))]
    PairsFailed {
        /// Number of pairs that failed.
        failed: usize,
        /// Bounded sample of per-pair failure reasons.
        reasons: Vec<String>,
    },

    /// Most of the requested range is missing from the data feed.
    ///
    /// No file *failed* — the server simply does not have them. Dukascopy
    /// publishes a file for every calendar day (weekends and holidays
    /// included), so a missing file means the requested range lies outside
    /// the feed's coverage rather than "that day had no trading".
    ///
    /// Reported as an error rather than silently counted as "not found",
    /// because otherwise a run over a range the feed has not published yet
    /// exits successfully having downloaded nothing.
    #[error(
        "{not_found}/{total} requested file(s) are missing from the data feed; \
         the range is probably outside the feed's coverage \
         (longest consecutive gap: {longest_run} bucket(s))"
    )]
    RangeUnavailable {
        /// Number of files the server does not have.
        not_found: usize,
        /// Number of files requested.
        total: usize,
        /// Longest run of consecutively missing buckets.
        longest_run: usize,
    },

    /// The run was interrupted by a termination signal (Ctrl+C / SIGTERM).
    ///
    /// Not a failure of the download itself: the batch is abandoned, the
    /// files already written stay on disk, and the run lock is released so
    /// the next run can start without a manual cleanup.
    ///
    /// The run still exits non-zero — an interrupted run did not deliver
    /// the requested range, and a caller that only looks at the exit code
    /// must not mistake it for a complete download.
    #[error("interrupted by {signal}; the run was cancelled and the run lock was released")]
    Interrupted {
        /// Name of the signal that cancelled the run.
        signal: &'static str,
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

    #[test]
    fn pairs_failed_reports_count_and_reasons() {
        let err = AppError::PairsFailed {
            failed: 2,
            reasons: vec!["EURUSD/D1: boom".into(), "GBPUSD/H1: bang".into()],
        };
        let msg = err.to_string();
        assert!(msg.contains("2 pair(s) failed"), "{msg}");
        assert!(msg.contains("EURUSD/D1: boom"), "{msg}");
        assert!(msg.contains("GBPUSD/H1: bang"), "{msg}");
    }

    #[test]
    fn pairs_failed_with_empty_reasons_still_renders_the_count() {
        let err = AppError::PairsFailed {
            failed: 3,
            reasons: Vec::new(),
        };
        assert!(err.to_string().contains("3 pair(s) failed"), "{err}");
    }

    /// The pair level and the batch level must stay distinguishable: the same
    /// word "failed" must not hide which unit the counts refer to.
    #[test]
    fn pairs_failed_is_distinct_from_batch_failed() {
        let pairs = AppError::PairsFailed {
            failed: 1,
            reasons: vec![],
        };
        let batch = AppError::BatchFailed {
            failed: 1,
            total: 2,
            skipped: 0,
            reasons: vec![],
        };
        assert_ne!(pairs.to_string(), batch.to_string());
        assert!(!pairs.to_string().contains("file(s) failed"));
    }

    /// An empty reason list must not leave a dangling separator behind.
    #[test]
    fn summaries_never_end_in_a_dangling_separator() {
        let batch = AppError::BatchFailed {
            failed: 1,
            total: 2,
            skipped: 0,
            reasons: Vec::new(),
        };
        let pairs = AppError::PairsFailed {
            failed: 1,
            reasons: Vec::new(),
        };
        for msg in [batch.to_string(), pairs.to_string()] {
            assert!(!msg.ends_with(": "), "{msg}");
            assert!(!msg.ends_with("; "), "{msg}");
            assert!(!msg.ends_with(' '), "{msg}");
        }
    }

    /// A file-scoped I/O failure must name the path it happened on.
    #[test]
    fn file_io_error_names_the_path_and_keeps_its_source() {
        let err = AppError::FileIo {
            path: std::path::PathBuf::from("/tmp/x/BID_candles_day_1.bi5.tmp"),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "no such file"),
        };
        let msg = err.to_string();
        assert!(msg.contains("BID_candles_day_1.bi5.tmp"), "{msg}");
        assert!(msg.contains("no such file"), "{msg}");
        assert!(
            err.chain().contains("no such file"),
            "the source chain must survive: {}",
            err.chain()
        );
    }

    /// The unavailable-range error must state all three numbers: how many
    /// files were missing, how many were requested, and how long the
    /// longest consecutive gap was.
    #[test]
    fn range_unavailable_reports_the_counts() {
        let err = AppError::RangeUnavailable {
            not_found: 50,
            total: 100,
            longest_run: 42,
        };
        let msg = err.to_string();
        assert!(msg.contains("50/100"), "{msg}");
        assert!(msg.contains("42"), "{msg}");
        assert!(msg.contains("coverage"), "{msg}");
    }

    /// The chain of an error without a source is just its own message.
    #[test]
    fn the_chain_of_a_flat_error_is_not_padded() {
        let err = AppError::NotFound {
            url: "https://example.invalid/x.bi5".into(),
        };
        assert_eq!(err.chain(), "not found: https://example.invalid/x.bi5");
    }

    /// An interruption has to say which signal caused it and that the lock
    /// was released — the user has to be able to start the next run right
    /// away.
    #[test]
    fn an_interruption_names_the_signal_and_the_lock_release() {
        let err = AppError::Interrupted { signal: "SIGINT" };
        let msg = err.to_string();
        assert!(msg.contains("SIGINT"), "{msg}");
        assert!(msg.contains("run lock was released"), "{msg}");
    }
}
