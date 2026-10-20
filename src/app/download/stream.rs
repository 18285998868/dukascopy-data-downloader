//! Streaming download of a single `.bi5` payload.
//!
//! The response body is written straight to a `.bi5.tmp` sibling, flushed to
//! stable storage, and only then atomically renamed into place. The payload is
//! therefore never visible under its final name while incomplete, a retry never
//! appends to stale bytes, and a crash cannot leave a truncated file that the
//! cache would mistake for complete.

use std::time::Duration;

use backoff::Error as BackoffError;
use backoff::future::retry;
use tokio::io::AsyncWriteExt;

use crate::config::DownloadTask;
use crate::utils::backoff::{BoundedBackoff, default_backoff};

/// Download a bi5 file with retry, streaming directly to disk.
///
/// Writes to a `.bi5.tmp` file, fsyncs it, then atomically renames to `.bi5`.
///
/// # Errors
///
/// Returns [`crate::AppError::NotFound`] on HTTP 404 (not retried), and any
/// other [`crate::AppError`] once the backoff policy is exhausted.
///
/// # Examples
///
/// ```no_run
/// # use std::path::PathBuf;
/// # use ddd::config::{DownloadTask, Symbol, Timeframe};
/// # async fn example() -> Result<(), ddd::AppError> {
/// let client = reqwest::Client::new();
/// let task = DownloadTask {
///     url: "https://datafeed.dukascopy.com/datafeed/EURUSD/2025/BID_candles_day_1.bi5"
///         .to_string(),
///     dest: PathBuf::from("/tmp/dukascopy/EURUSD/2025/BID_candles_day_1.bi5"),
///     symbol: Symbol::new("EURUSD")?,
///     timeframe: Timeframe::D1,
/// };
///
/// ddd::app::download::stream::download_to_file(&client, &task).await
/// # }
/// ```
pub async fn download_to_file(
    client: &reqwest::Client,
    task: &DownloadTask,
) -> Result<(), crate::AppError> {
    download_to_file_with_backoff(client, task, default_backoff()).await
}

/// The verdict on one HTTP response.
struct StatusVerdict {
    /// The underlying error.
    error: crate::AppError,
    /// Whether repeating the attempt can plausibly succeed.
    transient: bool,
    /// How long the server asked us to wait before trying again.
    wait_hint: Option<Duration>,
}

/// Classify an HTTP response: `None` means success.
///
/// `retry_after` is the server-supplied wait hint (from `Retry-After`).
/// It is reported separately from the error because it must be spent by the
/// *operation*, not handed to the retry driver: see
/// [`download_to_file_with_backoff`] for why.
fn classify_status(
    status: reqwest::StatusCode,
    retry_after: Option<Duration>,
    url: &str,
) -> Option<StatusVerdict> {
    if status.is_success() {
        return None;
    }
    let reason = format!("HTTP {status}");
    if status == reqwest::StatusCode::NOT_FOUND {
        Some(StatusVerdict {
            error: crate::AppError::NotFound {
                url: url.to_string(),
            },
            transient: false,
            wait_hint: None,
        })
    } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        Some(StatusVerdict {
            error: crate::AppError::DownloadFailed {
                url: url.to_string(),
                reason,
            },
            transient: true,
            wait_hint: retry_after,
        })
    } else {
        Some(StatusVerdict {
            error: crate::AppError::DownloadFailed {
                url: url.to_string(),
                reason,
            },
            transient: false,
            wait_hint: None,
        })
    }
}

/// Parse a `Retry-After` header expressed in delta-seconds.
///
/// The HTTP-date form is intentionally ignored: resolving it needs a trusted
/// clock and an HTTP-date parser, and the exponential fallback already bounds
/// the wait. Absent or malformed values yield `None`.
fn parse_retry_after(value: &str) -> Option<Duration> {
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}

/// Path of the scratch file a task writes before its atomic rename.
fn tmp_path_for(dest: &std::path::Path) -> std::path::PathBuf {
    dest.with_extension("bi5.tmp")
}

/// Attach the path an I/O error happened on.
fn io_error(path: &std::path::Path, source: std::io::Error) -> crate::AppError {
    crate::AppError::FileIo {
        path: path.to_path_buf(),
        source,
    }
}

async fn download_to_file_with_backoff(
    client: &reqwest::Client,
    task: &DownloadTask,
    backoff: BoundedBackoff,
) -> Result<(), crate::AppError> {
    // The budget is shared: the operation clamps a server wait hint against
    // it, the policy stops when it is gone.
    let budget = backoff.budget();
    let op = || {
        let c = client.clone();
        let u = task.url.clone();
        let dest = task.dest.clone();
        let budget = budget.clone();
        async move {
            let mut resp = match c.get(&u).send().await {
                Ok(r) => r,
                Err(e) => {
                    return Err(BackoffError::Transient {
                        err: crate::AppError::from(e),
                        retry_after: None,
                    });
                }
            };
            if let Some(verdict) = classify_status(
                resp.status(),
                resp.headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(parse_retry_after),
                &u,
            ) {
                if !verdict.transient {
                    return Err(BackoffError::Permanent(verdict.error));
                }
                // The wait hint is spent here and *not* handed to
                // `backoff::future::retry`: that crate prefers the hint over
                // the policy's own interval, which means the policy — and
                // with it the only budget check — is never consulted. A
                // server answering every retry with `Retry-After` would
                // then keep this file retrying forever. Waiting here charges
                // the hint to the budget and keeps the policy in charge.
                if let Some(hint) = verdict.wait_hint {
                    let wait = hint.min(budget.remaining());
                    if !wait.is_zero() {
                        tokio::time::sleep(wait).await;
                    }
                }
                return Err(BackoffError::Transient {
                    err: verdict.error,
                    retry_after: None,
                });
            }

            if let Some(p) = dest.parent() {
                tokio::fs::create_dir_all(p)
                    .await
                    .map_err(|e| BackoffError::Permanent(io_error(p, e)))?;
            }

            // Drop any scratch file left by an interrupted run before writing,
            // so `File::create` cannot append to stale bytes.
            let tmp_path = tmp_path_for(&dest);
            match tokio::fs::remove_file(&tmp_path).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(BackoffError::Permanent(io_error(&tmp_path, e))),
            }
            let mut file = tokio::fs::File::create(&tmp_path)
                .await
                .map_err(|e| BackoffError::Permanent(io_error(&tmp_path, e)))?;

            while let Some(chunk) = resp.chunk().await.map_err(|e| BackoffError::Transient {
                err: crate::AppError::from(e),
                retry_after: None,
            })? {
                file.write_all(&chunk)
                    .await
                    .map_err(|e| BackoffError::Permanent(io_error(&tmp_path, e)))?;
            }

            // `flush` only hands the bytes to the kernel. Without `sync_all` a
            // power loss could leave a renamed-but-empty `.bi5`, which the
            // cache scan treats as complete forever.
            file.flush()
                .await
                .map_err(|e| BackoffError::Permanent(io_error(&tmp_path, e)))?;
            file.sync_all()
                .await
                .map_err(|e| BackoffError::Permanent(io_error(&tmp_path, e)))?;
            drop(file);

            tokio::fs::rename(&tmp_path, &dest)
                .await
                .map_err(|e| BackoffError::Permanent(io_error(&dest, e)))?;

            // `sync_all` on the file made its *contents* durable, but the
            // directory entry created by the rename is still in the page
            // cache. Without this fsync a power loss can undo the rename
            // itself, leaving the payload behind under its scratch name.
            if let Some(parent) = dest.parent() {
                if let Ok(dir) = tokio::fs::File::open(parent).await {
                    let _ = dir.sync_all().await;
                }
            }

            Ok(())
        }
    };

    retry(backoff, op).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppError;
    use crate::config::{Symbol, Timeframe};
    use crate::utils::backoff::RetryBudget;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use wiremock::Request;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Backoff whose budget is already exhausted: it never retries.
    fn no_retry() -> BoundedBackoff {
        BoundedBackoff::new(Arc::new(RetryBudget::new(Duration::ZERO)))
    }

    /// Backoff that retries for a bounded window with tiny intervals.
    fn fast_retry(max_elapsed: Duration) -> BoundedBackoff {
        BoundedBackoff::with_base(
            Arc::new(RetryBudget::new(max_elapsed)),
            Duration::from_millis(1),
            1.0,
        )
    }

    const URL_PATH: &str = "/datafeed/EURUSD/2025/BID_candles_day_1.bi5";

    fn task(server: &MockServer, dest: &std::path::Path) -> DownloadTask {
        DownloadTask {
            url: format!("{}{URL_PATH}", server.uri()),
            dest: dest.to_path_buf(),
            symbol: Symbol::new("EURUSD").unwrap(),
            timeframe: Timeframe::D1,
        }
    }

    async fn server_returning(resp: ResponseTemplate) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(resp)
            .mount(&server)
            .await;
        server
    }

    fn client() -> reqwest::Client {
        reqwest::Client::new()
    }

    // — status classification —

    #[test]
    fn classify_2xx_is_success() {
        for code in [200u16, 201, 204, 206] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            assert!(classify_status(status, None, "u").is_none(), "{code}");
        }
    }

    #[test]
    fn classify_404_is_permanent_not_found() {
        let verdict =
            classify_status(reqwest::StatusCode::NOT_FOUND, None, "http://x/a.bi5").unwrap();
        assert!(!verdict.transient);
        match verdict.error {
            crate::AppError::NotFound { url } => assert_eq!(url, "http://x/a.bi5"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn classify_5xx_and_429_are_transient() {
        for code in [429u16, 500, 502, 503, 504] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            let verdict = classify_status(status, None, "u").expect("should be an error");
            assert!(verdict.transient, "{code} should be transient");
            assert!(matches!(
                verdict.error,
                crate::AppError::DownloadFailed { .. }
            ));
        }
    }

    /// A 429 must carry the server's wait hint through to the retry policy.
    #[test]
    fn classify_429_forwards_retry_after() {
        let verdict = classify_status(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            Some(Duration::from_secs(7)),
            "u",
        )
        .unwrap();
        assert!(verdict.transient);
        assert_eq!(verdict.wait_hint, Some(Duration::from_secs(7)));
    }

    #[test]
    fn classify_other_4xx_is_permanent() {
        for code in [400u16, 401, 403, 410, 451] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            let verdict = classify_status(status, None, "u").expect("should be an error");
            assert!(!verdict.transient, "{code} should be permanent");
            match verdict.error {
                crate::AppError::DownloadFailed { reason, .. } => {
                    assert!(reason.starts_with("HTTP "), "{reason}");
                }
                other => panic!("{code} should be a download failure, got {other:?}"),
            }
        }
    }

    // — Retry-After parsing —

    #[test]
    fn parse_retry_after_accepts_delta_seconds() {
        assert_eq!(parse_retry_after("5"), Some(Duration::from_secs(5)));
        assert_eq!(parse_retry_after("  120  "), Some(Duration::from_secs(120)));
        assert_eq!(parse_retry_after("0"), Some(Duration::ZERO));
    }

    #[test]
    fn parse_retry_after_rejects_non_numeric() {
        // HTTP-date form and garbage both fall back to exponential backoff.
        assert_eq!(parse_retry_after("Wed, 21 Oct 2026 07:28:00 GMT"), None);
        assert_eq!(parse_retry_after(""), None);
        assert_eq!(parse_retry_after("-1"), None);
        assert_eq!(parse_retry_after("1.5"), None);
    }

    // — scratch path —

    #[test]
    fn tmp_path_replaces_the_bi5_extension() {
        assert_eq!(
            tmp_path_for(std::path::Path::new("/d/BID_candles_day_1.bi5")),
            PathBuf::from("/d/BID_candles_day_1.bi5.tmp")
        );
    }

    #[test]
    fn tmp_path_survives_dots_in_the_stem() {
        assert_eq!(
            tmp_path_for(std::path::Path::new("/d/EUR.USD/x.bi5")),
            PathBuf::from("/d/EUR.USD/x.bi5.tmp")
        );
    }

    // — end-to-end against a mock server —

    #[tokio::test]
    async fn success_writes_payload_to_dest() {
        let server = server_returning(ResponseTemplate::new(200).set_body_bytes(b"bi5data")).await;
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("nested/EURUSD/BID_candles_day_1.bi5");

        download_to_file_with_backoff(&client(), &task(&server, &dest), no_retry())
            .await
            .unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"bi5data");
        assert!(
            !tmp_path_for(&dest).exists(),
            "the scratch file must be renamed away"
        );
    }

    #[tokio::test]
    async fn overwrites_a_stale_scratch_file() {
        let server = server_returning(ResponseTemplate::new(200).set_body_bytes(b"fresh")).await;
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("BID_candles_day_1.bi5");
        std::fs::write(tmp_path_for(&dest), b"stale-stale-stale").unwrap();

        download_to_file_with_backoff(&client(), &task(&server, &dest), no_retry())
            .await
            .unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"fresh");
    }

    #[tokio::test]
    async fn empty_body_produces_an_empty_file() {
        let server = server_returning(ResponseTemplate::new(200)).await;
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("BID_candles_day_1.bi5");

        download_to_file_with_backoff(&client(), &task(&server, &dest), no_retry())
            .await
            .unwrap();

        assert_eq!(std::fs::read(&dest).unwrap(), b"");
    }

    #[tokio::test]
    async fn large_body_is_written_intact() {
        let payload = vec![0x5du8; 512 * 1024];
        let server =
            server_returning(ResponseTemplate::new(200).set_body_bytes(payload.clone())).await;
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("BID_candles_day_1.bi5");

        download_to_file_with_backoff(&client(), &task(&server, &dest), no_retry())
            .await
            .unwrap();

        assert_eq!(std::fs::read(&dest).unwrap().len(), payload.len());
    }

    #[tokio::test]
    async fn not_found_is_reported_and_writes_nothing() {
        let server = server_returning(ResponseTemplate::new(404)).await;
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("BID_candles_day_1.bi5");

        let err = download_to_file_with_backoff(&client(), &task(&server, &dest), no_retry())
            .await
            .unwrap_err();

        assert!(matches!(err, AppError::NotFound { .. }), "{err:?}");
        assert!(!dest.exists(), "404 must not create the destination file");
    }

    #[tokio::test]
    async fn server_error_fails_after_retries() {
        let server = server_returning(ResponseTemplate::new(500)).await;
        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("BID_candles_day_1.bi5");

        let err = download_to_file_with_backoff(
            &client(),
            &task(&server, &dest),
            fast_retry(Duration::from_millis(50)),
        )
        .await
        .unwrap_err();

        match err {
            AppError::DownloadFailed { reason, .. } => assert!(reason.contains("500"), "{reason}"),
            other => panic!("unexpected: {other:?}"),
        }
        assert!(!dest.exists());
    }

    #[tokio::test]
    async fn retries_a_transient_5xx_then_succeeds() {
        let server = MockServer::start().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        Mock::given(method("GET"))
            .and(path(URL_PATH))
            .respond_with(move |_: &Request| {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(503).set_body_bytes(b"boom")
                } else {
                    ResponseTemplate::new(200).set_body_bytes(b"ok")
                }
            })
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("BID_candles_day_1.bi5");

        download_to_file_with_backoff(
            &client(),
            &task(&server, &dest),
            fast_retry(Duration::from_secs(5)),
        )
        .await
        .expect("retry should recover from a transient 503");

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(std::fs::read(&dest).unwrap(), b"ok");
    }

    #[tokio::test]
    async fn does_not_retry_a_404() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        Mock::given(method("GET"))
            .and(path(URL_PATH))
            .respond_with(move |_: &Request| {
                counter.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(404)
            })
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("BID_candles_day_1.bi5");

        let err = download_to_file_with_backoff(
            &client(),
            &task(&server, &dest),
            fast_retry(Duration::from_secs(5)),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, AppError::NotFound { .. }), "{err:?}");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "404 must be attempted exactly once"
        );
    }

    // — retry budget —

    /// A server that answers every retry with `Retry-After` must not be able
    /// to keep a file retrying forever.
    ///
    /// `backoff::future::retry` prefers the hint over the policy's own
    /// interval, which means the policy — and with it the only budget check —
    /// would never be consulted. The hint is therefore spent by the operation
    /// itself and charged to the budget.
    #[tokio::test]
    async fn a_retry_after_hint_cannot_outlive_the_budget() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        Mock::given(method("GET"))
            .and(path(URL_PATH))
            .respond_with(move |_: &Request| {
                counter.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(429).insert_header("Retry-After", "60")
            })
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("BID_candles_day_1.bi5");

        let started = std::time::Instant::now();
        let err = download_to_file_with_backoff(
            &client(),
            &task(&server, &dest),
            BoundedBackoff::new(Arc::new(RetryBudget::new(Duration::from_millis(200)))),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, AppError::DownloadFailed { .. }), "{err:?}");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "a 60s hint must be clamped to the remaining budget"
        );
        assert!(
            hits.load(Ordering::SeqCst) <= 2,
            "the budget must stop the retry loop, got {} attempts",
            hits.load(Ordering::SeqCst)
        );
        assert!(!dest.exists());
    }

    /// The hint must still be honoured: a rate limiter that says "wait one
    /// second" is not hammered.
    #[tokio::test]
    async fn a_retry_after_hint_is_actually_waited_out() {
        let server = MockServer::start().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        Mock::given(method("GET"))
            .and(path(URL_PATH))
            .respond_with(move |_: &Request| {
                if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(429).insert_header("Retry-After", "1")
                } else {
                    ResponseTemplate::new(200).set_body_bytes(b"ok")
                }
            })
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("BID_candles_day_1.bi5");

        let started = std::time::Instant::now();
        download_to_file_with_backoff(
            &client(),
            &task(&server, &dest),
            fast_retry(Duration::from_secs(10)),
        )
        .await
        .expect("retry should recover from the 429");

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            started.elapsed() >= Duration::from_secs(1),
            "the one-second hint was not honoured"
        );
        assert_eq!(std::fs::read(&dest).unwrap(), b"ok");
    }

    /// The budget bounds the whole attempt sequence, not just the intervals.
    #[tokio::test]
    async fn the_budget_bounds_a_failing_file() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        Mock::given(method("GET"))
            .and(path(URL_PATH))
            .respond_with(move |_: &Request| {
                counter.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(500)
            })
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let dest = dir.path().join("BID_candles_day_1.bi5");

        let started = std::time::Instant::now();
        let err = download_to_file_with_backoff(
            &client(),
            &task(&server, &dest),
            BoundedBackoff::with_base(
                Arc::new(RetryBudget::new(Duration::from_millis(200))),
                Duration::from_millis(10),
                1.0,
            ),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, AppError::DownloadFailed { .. }), "{err:?}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the budget must stop the retry loop"
        );
        assert!(hits.load(Ordering::SeqCst) >= 1);
        assert!(!dest.exists());
    }

    /// An I/O failure must name the path it happened on: a batch summary
    /// collects up to 20 of them and bare I/O errors are indistinguishable.
    #[tokio::test]
    async fn an_io_failure_names_the_path() {
        let server = server_returning(ResponseTemplate::new(200).set_body_bytes(b"x")).await;
        let dir = TempDir::new().unwrap();
        // A plain file where a directory is expected: `create_dir_all` cannot
        // succeed below it.
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();
        let dest = blocker.join("nested/BID_candles_day_1.bi5");

        let err = download_to_file_with_backoff(&client(), &task(&server, &dest), no_retry())
            .await
            .unwrap_err();

        match err {
            AppError::FileIo { path, .. } => {
                assert!(
                    path.to_string_lossy().contains("nested"),
                    "the failing path must be reported: {path:?}"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
