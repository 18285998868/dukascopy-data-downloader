//! Streaming download of a single `.bi5` payload.
//!
//! The response body is written straight to a `.bi5.tmp` sibling, flushed to
//! stable storage, and only then atomically renamed into place. The payload is
//! therefore never visible under its final name while incomplete, a retry never
//! appends to stale bytes, and a crash cannot leave a truncated file that the
//! cache would mistake for complete.

use std::time::Duration;

use backoff::Error as BackoffError;
use backoff::backoff::Backoff;
use backoff::future::retry;
use tokio::io::AsyncWriteExt;

use crate::config::DownloadTask;
use crate::utils::backoff::default_backoff;

/// Download a bi5 file with retry, streaming directly to disk.
///
/// Writes to a `.bi5.tmp` file, fsyncs it, then atomically renames to `.bi5`.
///
/// # Errors
///
/// Returns [`crate::AppError::NotFound`] on HTTP 404 (not retried), and any
/// other [`crate::AppError`] once the backoff policy is exhausted.
pub async fn download_to_file(
    client: &reqwest::Client,
    task: &DownloadTask,
) -> Result<(), crate::AppError> {
    download_to_file_with_backoff(client, task, default_backoff()).await
}

/// Classify an HTTP response: `None` means success, otherwise a retry decision.
///
/// `retry_after` is the server-supplied wait hint (from `Retry-After`), honoured
/// on transient responses so a rate limiter is not hammered.
fn classify_status(
    status: reqwest::StatusCode,
    retry_after: Option<Duration>,
    url: &str,
) -> Option<BackoffError<crate::AppError>> {
    if status.is_success() {
        return None;
    }
    let reason = format!("HTTP {status}");
    if status == reqwest::StatusCode::NOT_FOUND {
        Some(BackoffError::Permanent(crate::AppError::NotFound {
            url: url.to_string(),
        }))
    } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        Some(BackoffError::Transient {
            err: crate::AppError::DownloadFailed {
                url: url.to_string(),
                reason,
            },
            retry_after,
        })
    } else {
        Some(BackoffError::Permanent(crate::AppError::DownloadFailed {
            url: url.to_string(),
            reason,
        }))
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

async fn download_to_file_with_backoff(
    client: &reqwest::Client,
    task: &DownloadTask,
    backoff: impl Backoff + 'static,
) -> Result<(), crate::AppError> {
    let op = || {
        let c = client.clone();
        let u = task.url.clone();
        let dest = task.dest.clone();
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
            if let Some(err) = classify_status(
                resp.status(),
                resp.headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(parse_retry_after),
                &u,
            ) {
                return Err(err);
            }

            if let Some(p) = dest.parent() {
                tokio::fs::create_dir_all(p)
                    .await
                    .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;
            }

            // Drop any scratch file left by an interrupted run before writing,
            // so `File::create` cannot append to stale bytes.
            let tmp_path = tmp_path_for(&dest);
            match tokio::fs::remove_file(&tmp_path).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(BackoffError::Permanent(crate::AppError::Io(e))),
            }
            let mut file = tokio::fs::File::create(&tmp_path)
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;

            while let Some(chunk) = resp.chunk().await.map_err(|e| BackoffError::Transient {
                err: crate::AppError::from(e),
                retry_after: None,
            })? {
                file.write_all(&chunk)
                    .await
                    .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;
            }

            // `flush` only hands the bytes to the kernel. Without `sync_all` a
            // power loss could leave a renamed-but-empty `.bi5`, which the
            // cache scan treats as complete forever.
            file.flush()
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;
            file.sync_all()
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;
            drop(file);

            tokio::fs::rename(&tmp_path, &dest)
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;

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
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use wiremock::Request;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Backoff that never retries (max_elapsed = 0).
    fn no_retry() -> backoff::ExponentialBackoff {
        backoff::ExponentialBackoff {
            current_interval: Duration::ZERO,
            initial_interval: Duration::ZERO,
            max_elapsed_time: Some(Duration::ZERO),
            ..Default::default()
        }
    }

    /// Backoff that retries for a bounded window with tiny intervals.
    fn fast_retry(max_elapsed: Duration) -> backoff::ExponentialBackoff {
        backoff::ExponentialBackoff {
            current_interval: Duration::ZERO,
            initial_interval: Duration::from_millis(1),
            multiplier: 1.0,
            max_elapsed_time: Some(max_elapsed),
            ..Default::default()
        }
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
        let err = classify_status(reqwest::StatusCode::NOT_FOUND, None, "http://x/a.bi5").unwrap();
        match err {
            BackoffError::Permanent(AppError::NotFound { url }) => {
                assert_eq!(url, "http://x/a.bi5")
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn classify_5xx_and_429_are_transient() {
        for code in [429u16, 500, 502, 503, 504] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            match classify_status(status, None, "u").expect("should be an error") {
                BackoffError::Transient { err, .. } => {
                    assert!(matches!(err, AppError::DownloadFailed { .. }));
                }
                BackoffError::Permanent(err) => panic!("{code} should be transient, got {err:?}"),
            }
        }
    }

    /// A 429 must carry the server's wait hint through to the backoff policy.
    #[test]
    fn classify_429_forwards_retry_after() {
        let err = classify_status(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            Some(Duration::from_secs(7)),
            "u",
        )
        .unwrap();
        match err {
            BackoffError::Transient { retry_after, .. } => {
                assert_eq!(retry_after, Some(Duration::from_secs(7)));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn classify_other_4xx_is_permanent() {
        for code in [400u16, 401, 403, 410, 451] {
            let status = reqwest::StatusCode::from_u16(code).unwrap();
            match classify_status(status, None, "u").expect("should be an error") {
                BackoffError::Permanent(AppError::DownloadFailed { reason, .. }) => {
                    assert!(reason.starts_with("HTTP "), "{reason}");
                }
                other => panic!("{code} should be permanent, got {other:?}"),
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
}
