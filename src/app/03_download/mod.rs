pub mod stream;

use std::future::Future;

use crate::AppError;
use crate::config::DownloadTask;
use futures::stream::{StreamExt, iter as stream_iter};
use tracing::Instrument;

/// How often a line of progress is logged, in completed files.
const PROGRESS_EVERY: usize = 100;
/// Maximum number of individual failure details logged per batch.
const MAX_LOGGED_FAILURES: usize = 20;

/// Aggregate statistics for a completed batch of downloads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DownloadStats {
    pub total: usize,
    pub ok: usize,
    pub not_found: usize,
    pub failed: usize,
}

impl DownloadStats {
    const fn done(&self) -> usize {
        self.ok + self.not_found + self.failed
    }
}

/// Download all tasks with bounded concurrency.
///
/// # Errors
///
/// Returns [`AppError::DownloadFailed`] if any download fails or all tasks are not found.
pub async fn run_downloads(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
    concurrency: usize,
) -> Result<DownloadStats, AppError> {
    let download_fn = move |c: reqwest::Client, t: DownloadTask| async move {
        stream::download_to_file(&c, &t).await
    };
    run_downloads_inner(client, tasks, concurrency, download_fn).await
}

/// Core download orchestration, parameterized by the download function for testing.
///
/// Streams tasks through `buffer_unordered(concurrency)`, so at most
/// `concurrency` downloads run at once and results are counted incrementally
/// instead of materializing all futures upfront.
async fn run_downloads_inner<F, Fut>(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
    concurrency: usize,
    download_fn: F,
) -> Result<DownloadStats, AppError>
where
    F: Fn(reqwest::Client, DownloadTask) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), AppError>> + Send,
{
    let total = tasks.len();
    let concurrency = concurrency.max(1);
    if total == 0 {
        return Ok(DownloadStats::default());
    }
    tracing::debug!(total, concurrency, "downloading files");

    let mut stats = DownloadStats {
        total,
        ..DownloadStats::default()
    };
    let mut logged_failures = 0;

    let mut stream = stream_iter(tasks)
        .map(|t| {
            let c = client.clone();
            let span = tracing::info_span!("download", symbol = %t.symbol, tf = %t.timeframe);
            let fut = download_fn(c, t);
            fut.instrument(span)
        })
        .buffer_unordered(concurrency);

    while let Some(result) = stream.next().await {
        match result {
            Ok(()) => stats.ok += 1,
            Err(AppError::NotFound { .. }) => stats.not_found += 1,
            Err(e) => {
                stats.failed += 1;
                if logged_failures < MAX_LOGGED_FAILURES {
                    logged_failures += 1;
                    tracing::warn!(error = %e, "download failed");
                }
            }
        }

        if stats.done() % PROGRESS_EVERY == 0 {
            tracing::info!(
                done = stats.done(),
                total = stats.total,
                ok = stats.ok,
                not_found = stats.not_found,
                failed = stats.failed,
                "download progress",
            );
        }
    }

    if stats.not_found > 0 {
        tracing::debug!(
            not_found = stats.not_found,
            total = stats.total,
            "files not found on server (404, expected for edge dates)",
        );
    }
    if stats.failed > logged_failures {
        tracing::warn!(
            hidden = stats.failed - logged_failures,
            "additional failure details not logged",
        );
    }

    if stats.failed > 0 {
        return Err(AppError::DownloadFailed {
            url: "batch".into(),
            reason: format!("{}/{} failed", stats.failed, stats.total),
        });
    }
    if stats.total > 0 && stats.ok == 0 && stats.not_found == stats.total {
        return Err(AppError::DownloadFailed {
            url: "batch".into(),
            reason: format!("all {}/{} not found", stats.not_found, stats.total),
        });
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AppError;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    // — run_downloads_inner —

    #[tokio::test]
    async fn run_downloads_empty_tasks_returns_ok() {
        let client = reqwest::Client::new();
        let tasks = vec![];
        let download_fn = |_: reqwest::Client, _: DownloadTask| async { unreachable!() };
        let stats = run_downloads_inner(&client, tasks, 16, download_fn)
            .await
            .unwrap();
        assert_eq!(stats.total, 0);
        assert_eq!(stats.ok, 0);
    }

    #[tokio::test]
    async fn run_downloads_all_success() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let call_count = Arc::new(AtomicUsize::new(0));
        let cnt = call_count.clone();
        let download_fn = move |_: reqwest::Client, _: DownloadTask| {
            let cnt = cnt.clone();
            async move {
                cnt.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };
        let stats = run_downloads_inner(&client, tasks, 16, download_fn)
            .await
            .unwrap();
        assert_eq!(call_count.load(Ordering::SeqCst), 2);
        assert_eq!(stats.ok, 2);
        assert_eq!(stats.not_found, 0);
        assert_eq!(stats.failed, 0);
    }

    #[tokio::test]
    async fn run_downloads_not_found_tolerated() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let download_fn = |_: reqwest::Client, t: DownloadTask| async move {
            if t.symbol == "a" {
                Err(AppError::NotFound {
                    url: "http://x".into(),
                })
            } else {
                Ok(())
            }
        };
        let stats = run_downloads_inner(&client, tasks, 16, download_fn)
            .await
            .unwrap();
        assert_eq!(stats.ok, 1);
        assert_eq!(stats.not_found, 1);
        assert_eq!(stats.failed, 0);
    }

    #[tokio::test]
    async fn run_downloads_all_not_found_returns_err() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let download_fn = |_: reqwest::Client, _: DownloadTask| async move {
            Err(AppError::NotFound {
                url: "http://x".into(),
            })
        };
        let err = run_downloads_inner(&client, tasks, 16, download_fn)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("all 2/2 not found"));
    }

    #[tokio::test]
    async fn run_downloads_not_found_not_counted_as_failure() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b"), dummy_task("c")];
        let download_fn = |_: reqwest::Client, t: DownloadTask| async move {
            match t.symbol.as_str() {
                "a" => Err(AppError::NotFound {
                    url: "http://x".into(),
                }),
                "b" => Err(AppError::DownloadFailed {
                    url: "x".into(),
                    reason: "bad".into(),
                }),
                _ => Ok(()),
            }
        };
        let err = run_downloads_inner(&client, tasks, 16, download_fn)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("1/3 failed"));
    }

    #[tokio::test]
    async fn run_downloads_one_fail_returns_err() {
        let client = reqwest::Client::new();
        let tasks = vec![dummy_task("a"), dummy_task("b")];
        let download_fn = |_: reqwest::Client, t: DownloadTask| async move {
            if t.symbol == "b" {
                Err(AppError::DownloadFailed {
                    url: "x".into(),
                    reason: "bad".into(),
                })
            } else {
                Ok(())
            }
        };
        let err = run_downloads_inner(&client, tasks, 16, download_fn)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("1/2 failed"));
    }

    #[tokio::test]
    async fn concurrency_one_runs_sequentially() {
        let client = reqwest::Client::new();
        let tasks: Vec<_> = (0..5).map(|i| dummy_task(&format!("s{i}"))).collect();
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let stats = {
            let active = active.clone();
            let max_active = max_active.clone();
            let download_fn = move |_: reqwest::Client, _: DownloadTask| {
                let active = active.clone();
                let max_active = max_active.clone();
                async move {
                    let cur = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(cur, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }
            };
            run_downloads_inner(&client, tasks, 1, download_fn)
                .await
                .unwrap()
        };
        assert_eq!(stats.ok, 5);
        assert_eq!(max_active.load(Ordering::SeqCst), 1);
    }

    // — helpers —

    fn dummy_task(symbol: &str) -> DownloadTask {
        DownloadTask {
            url: format!("http://example.com/{symbol}"),
            dest: std::path::PathBuf::from("/tmp"),
            symbol: symbol.into(),
            timeframe: crate::config::Timeframe::D1,
        }
    }
}
