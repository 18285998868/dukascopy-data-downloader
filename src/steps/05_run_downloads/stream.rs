use backoff::Error as BackoffError;
use backoff::backoff::Backoff;
use backoff::future::retry;
use tokio::io::AsyncWriteExt;

use crate::DownloadTask;
use crate::backoff::default_backoff;

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
            let resp = match c.get(&u).send().await {
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
                } else {
                    Err(BackoffError::Transient {
                        err: crate::AppError::DownloadFailed {
                            url: u,
                            reason: format!("HTTP {status}"),
                        },
                        retry_after: None,
                    })
                };
            }

            if let Some(p) = dest.parent() {
                tokio::fs::create_dir_all(p)
                    .await
                    .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;
            }

            let tmp_path = dest.with_extension("bi5.tmp");
            let mut file = tokio::fs::File::create(&tmp_path)
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;

            let bytes = resp.bytes().await.map_err(|e| BackoffError::Transient {
                err: crate::AppError::from(e),
                retry_after: None,
            })?;

            tokio::io::AsyncWriteExt::write_all(&mut file, &bytes)
                .await
                .map_err(|e| BackoffError::Permanent(crate::AppError::Io(e)))?;

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
    use crate::PeriodKind;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn no_retry() -> backoff::ExponentialBackoff {
        backoff::ExponentialBackoff {
            current_interval: std::time::Duration::ZERO,
            initial_interval: std::time::Duration::ZERO,
            max_elapsed_time: Some(std::time::Duration::ZERO),
            ..Default::default()
        }
    }

    fn make_task(
        server: &MockServer,
        period: PeriodKind,
        dest: std::path::PathBuf,
    ) -> DownloadTask {
        let path = match period {
            PeriodKind::Year => "/datafeed/EURUSD/2025/BID_candles_day_1.bi5",
            PeriodKind::Month => "/datafeed/EURUSD/2025/01/BID_candles_hour_1.bi5",
            PeriodKind::Day => "/datafeed/EURUSD/2025/01/02/BID_candles_min_1.bi5",
        };
        let url = format!("{}{}", server.uri(), path);
        DownloadTask {
            url,
            dest,
            symbol: "EURUSD".into(),
            timeframe: "D1".into(),
            date: chrono::NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            period,
        }
    }

    #[tokio::test]
    async fn download_success() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/datafeed/EURUSD/2025/BID_candles_day_1.bi5"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"b5data"))
            .mount(&server)
            .await;

        let client = reqwest::Client::new();
        let dir = tempfile::tempdir().unwrap();
        let task = make_task(
            &server,
            PeriodKind::Year,
            dir.path().join("test-download.bi5"),
        );
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
        let dir = tempfile::tempdir().unwrap();
        let task = make_task(
            &server,
            PeriodKind::Year,
            dir.path().join("test-download.bi5"),
        );
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
        let dir = tempfile::tempdir().unwrap();
        let task = make_task(
            &server,
            PeriodKind::Year,
            dir.path().join("test-download.bi5"),
        );
        let result = download_to_file_with_backoff(&client, &task, no_retry()).await;
        assert!(matches!(result, Err(AppError::DownloadFailed { .. })));
    }
}
