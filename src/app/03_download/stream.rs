use backoff::Error as BackoffError;
use backoff::backoff::Backoff;
use backoff::future::retry;
use tokio::io::AsyncWriteExt;

use crate::config::DownloadTask;
use crate::utils::backoff::default_backoff;

/// Download a bi5 file with retry, streaming directly to disk.
///
/// Writes to a `.bi5.tmp` file, then atomically renames to `.bi5`.
pub async fn download_to_file(
    client: &reqwest::Client,
    task: &DownloadTask,
) -> Result<(), crate::AppError> {
    download_to_file_with_backoff(client, task, default_backoff()).await
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
            let status = resp.status();
            if !status.is_success() {
                return if status == reqwest::StatusCode::NOT_FOUND {
                    Err(BackoffError::Permanent(crate::AppError::NotFound {
                        url: u,
                    }))
                } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    || status.is_server_error()
                {
                    Err(BackoffError::Transient {
                        err: crate::AppError::DownloadFailed {
                            url: u,
                            reason: format!("HTTP {status}"),
                        },
                        retry_after: None,
                    })
                } else {
                    Err(BackoffError::Permanent(crate::AppError::DownloadFailed {
                        url: u,
                        reason: format!("HTTP {status}"),
                    }))
                };
            }

            if let Some(p) = dest.parent() {
                tokio::fs::create_dir_all(p)
                    .await
                    .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;
            }

            let tmp_path = dest.with_extension("bi5.tmp");
            match tokio::fs::remove_file(&tmp_path).await {
                Ok(_) => {}
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

            file.flush()
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
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// Backoff that never retries (max_elapsed = 0).
    fn no_retry() -> backoff::ExponentialBackoff {
        backoff::ExponentialBackoff {
            current_interval: std::time::Duration::ZERO,
            initial_interval: std::time::Duration::ZERO,
            max_elapsed_time: Some(std::time::Duration::ZERO),
            ..Default::default()
        }
    }

    fn make_task(server: &MockServer) -> DownloadTask {
        DownloadTask {
            url: format!(
                "{}/datafeed/EURUSD/2025/BID_candles_day_1.bi5",
                server.uri()
            ),
            dest: std::path::PathBuf::from("/tmp/test-download.bi5"),
            symbol: "EURUSD".into(),
            timeframe: "D1".into(),
        }
    }

    #[tokio::test]
    async fn download_success() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/datafeed/EURUSD/2025/BID_candles_day_1.bi5"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"bi5data"))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let task = make_task(&server);
        let result = download_to_file_with_backoff(&client, &task, no_retry()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn download_404_returns_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let task = make_task(&server);
        let result = download_to_file_with_backoff(&client, &task, no_retry()).await;
        assert!(matches!(result, Err(AppError::NotFound { .. })));
    }

    #[tokio::test]
    async fn download_500_returns_download_failed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let task = make_task(&server);
        let result = download_to_file_with_backoff(&client, &task, no_retry()).await;
        assert!(matches!(result, Err(AppError::DownloadFailed { .. })));
    }
}
