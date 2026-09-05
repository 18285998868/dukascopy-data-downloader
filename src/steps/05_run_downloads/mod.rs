pub mod stream;

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::future::join_all;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::AppError;
use crate::DownloadTask;
use crate::build_tasks::period_label;
use crate::cache;

fn check_cached(task: &DownloadTask, ttl_secs: Option<u64>) -> bool {
    let key = format!(
        "{}/{}/{}",
        task.symbol,
        task.timeframe,
        period_label(task.date, task.period)
    );
    cache::read("downloaded", &key, ttl_secs).is_some()
}

fn mark_cached(task: &DownloadTask) {
    let key = format!(
        "{}/{}/{}",
        task.symbol,
        task.timeframe,
        period_label(task.date, task.period)
    );
    cache::write("downloaded", &key, b"1");
}

pub async fn run_downloads(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
    force: bool,
) -> Result<(), AppError> {
    let (to_download, skipped) = if force {
        (tasks, 0usize)
    } else {
        let mut dl = Vec::new();
        let mut sk = 0usize;
        for t in tasks {
            if t.dest.exists() || check_cached(&t, None) {
                sk += 1;
            } else {
                dl.push(t);
            }
        }
        (dl, sk)
    };

    let total = to_download.len() + skipped;
    if to_download.is_empty() {
        tracing::debug!("nothing to download (skipped {skipped})");
        return Ok(());
    }

    tracing::debug!(total = to_download.len(), skipped, "downloading files");

    let counter = Arc::new(AtomicUsize::new(0));
    let total_new = to_download.len();
    let semaphore = Arc::new(Semaphore::new(30));

    let handles = to_download.into_iter().map(|task| {
        let c = client.clone();
        let sem = Arc::clone(&semaphore);
        let cnt = Arc::clone(&counter);
        async move {
            let _permit = sem.acquire().await.unwrap();
            let idx = cnt.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::debug!(
                pair = %task.symbol,
                tf = %task.timeframe,
                date = %task.date,
                "[{idx}/{total_new}] downloading"
            );
            let result = stream::download_to_file(&c, &task).await;
            match &result {
                Ok(()) => {
                    mark_cached(&task);
                    tracing::debug!(
                        pair = %task.symbol,
                        tf = %task.timeframe,
                        date = %task.date,
                        "[{idx}/{total_new}] done"
                    );
                }
                Err(AppError::NotFound { .. }) => {
                    mark_cached(&task);
                    tracing::debug!(
                        pair = %task.symbol,
                        tf = %task.timeframe,
                        date = %task.date,
                        "[{idx}/{total_new}] 404 (no data)"
                    );
                }
                Err(e) => {
                    tracing::error!(
                        pair = %task.symbol,
                        tf = %task.timeframe,
                        date = %task.date,
                        "[{idx}/{total_new}] failed: {e}"
                    );
                }
            }
            result
        }
    });

    let results = join_all(handles).await;
    aggregate_results(results, total)
}

fn aggregate_results(results: Vec<Result<(), AppError>>, total: usize) -> Result<(), AppError> {
    let (_ok, nf, fail) = results.iter().fold((0, 0, 0), |(ok, nf, fail), r| match r {
        Ok(_) => (ok + 1, nf, fail),
        Err(AppError::NotFound { .. }) => (ok, nf + 1, fail),
        _ => (ok, nf, fail + 1),
    });

    if nf > 0 {
        tracing::debug!(
            not_found = nf,
            total,
            "files not found (expected for edge dates)"
        );
    }
    if fail > 0 {
        tracing::error!(
            failed = fail,
            not_found = nf,
            total,
            "some downloads failed"
        );
        return Err(AppError::Data(format!(
            "{fail} download(s) failed ({nf} not found)"
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aggregate_all_ok() {
        let results = vec![Ok(()), Ok(()), Ok(())];
        assert!(aggregate_results(results, 3).is_ok());
    }

    #[test]
    fn aggregate_not_found_only_is_ok() {
        let results = vec![
            Err(AppError::NotFound { url: "a".into() }),
            Err(AppError::NotFound { url: "b".into() }),
        ];
        assert!(aggregate_results(results, 2).is_ok());
    }

    #[test]
    fn aggregate_one_fail_returns_err() {
        let results = vec![
            Ok(()),
            Err(AppError::DownloadFailed {
                url: "a".into(),
                reason: "timeout".into(),
            }),
        ];
        assert!(aggregate_results(results, 2).is_err());
    }

    #[test]
    fn aggregate_empty_vec_ok() {
        assert!(aggregate_results(vec![], 0).is_ok());
    }
}
