pub mod stream;

use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use chrono::Datelike;
use chrono::NaiveDate;
use futures::future::join_all;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::cache;
use crate::error::AppError;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PeriodKind {
    Year,
    Month,
    Day,
}

#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub url: String,
    pub dest: PathBuf,
    pub symbol: String,
    pub timeframe: String,
    pub date: NaiveDate,
    pub period: PeriodKind,
}

fn period_label(date: NaiveDate, period: PeriodKind) -> String {
    match period {
        PeriodKind::Year => format!("{}", date.year()),
        PeriodKind::Month => format!("{}-{:02}", date.year(), date.month()),
        PeriodKind::Day => format!("{}", date.format("%Y-%m-%d")),
    }
}

fn build_date_range(start: NaiveDate, end: NaiveDate, period: PeriodKind) -> Vec<NaiveDate> {
    if start > end {
        return Vec::new();
    }
    match period {
        PeriodKind::Year => {
            let y0 = start.year();
            let y1 = end.year();
            (y0..=y1)
                .map(|y| NaiveDate::from_ymd_opt(y, 1, 1).unwrap())
                .collect()
        }
        PeriodKind::Month => {
            let mut dates = Vec::new();
            let mut y = start.year();
            let mut m = start.month();
            let end_y = end.year();
            let end_m = end.month();
            loop {
                dates.push(NaiveDate::from_ymd_opt(y, m, 1).unwrap());
                if y == end_y && m >= end_m {
                    break;
                }
                m += 1;
                if m > 12 {
                    m = 1;
                    y += 1;
                }
            }
            dates
        }
        PeriodKind::Day => {
            let mut dates = Vec::new();
            let mut d = start;
            while d <= end {
                dates.push(d);
                d = d.succ_opt().unwrap_or(d);
            }
            dates
        }
    }
}

fn dest_filename(symbol: &str, tf: &str, date: NaiveDate, period: PeriodKind) -> String {
    format!("{symbol}-{tf}-{}.bi5", period_label(date, period))
}

pub fn build_tasks(
    symbols: &[String],
    timeframes: &[String],
    price_type: &str,
    start: NaiveDate,
    end: NaiveDate,
    data_dir: &str,
) -> Vec<DownloadTask> {
    let base = PathBuf::from(data_dir).join("dukascopy");
    let mut tasks = Vec::new();

    for symbol in symbols {
        for tf in timeframes {
            let period = stream::tf_period(tf);
            for date in build_date_range(start, end, period) {
                let url = stream::build_url(symbol, price_type, tf, date, period);
                let dest = base
                    .join(symbol)
                    .join(tf)
                    .join(dest_filename(symbol, tf, date, period));
                tasks.push(DownloadTask {
                    url,
                    dest,
                    symbol: symbol.clone(),
                    timeframe: tf.clone(),
                    date,
                    period,
                });
            }
        }
    }

    tasks
}

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
    fn build_tasks_empty_range() {
        let tasks = build_tasks(
            &["EURUSD".into()],
            &["D1".into()],
            "BID",
            NaiveDate::from_ymd_opt(2025, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 3).unwrap(),
            "/tmp/data",
        );
        assert!(tasks.is_empty());
    }

    #[test]
    fn build_tasks_d1_single_day() {
        let tasks = build_tasks(
            &["EURUSD".into()],
            &["D1".into()],
            "BID",
            NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            "/tmp/data",
        );
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].url.contains("EURUSD/2025/BID_candles_day_1.bi5"));
        assert!(
            tasks[0]
                .dest
                .to_string_lossy()
                .ends_with("EURUSD-D1-2025.bi5")
        );
        assert_eq!(tasks[0].period, PeriodKind::Year);
    }

    #[test]
    fn build_tasks_h1_single_day() {
        let tasks = build_tasks(
            &["EURUSD".into()],
            &["H1".into()],
            "ASK",
            NaiveDate::from_ymd_opt(2025, 6, 15).unwrap(),
            NaiveDate::from_ymd_opt(2025, 6, 15).unwrap(),
            "/tmp/data",
        );
        assert_eq!(tasks.len(), 1);
        assert!(
            tasks[0]
                .url
                .contains("EURUSD/2025/06/ASK_candles_hour_1.bi5")
        );
        assert!(
            tasks[0]
                .dest
                .to_string_lossy()
                .ends_with("EURUSD-H1-2025-06.bi5")
        );
        assert_eq!(tasks[0].period, PeriodKind::Month);
    }

    #[test]
    fn build_tasks_m1_single_day() {
        let tasks = build_tasks(
            &["EURUSD".into()],
            &["M1".into()],
            "BID",
            NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            "/tmp/data",
        );
        assert_eq!(tasks.len(), 1);
        assert!(
            tasks[0]
                .url
                .contains("EURUSD/2025/01/02/BID_candles_min_1.bi5")
        );
        assert!(
            tasks[0]
                .dest
                .to_string_lossy()
                .ends_with("EURUSD-M1-2025-01-02.bi5")
        );
        assert_eq!(tasks[0].period, PeriodKind::Day);
    }

    #[test]
    fn build_tasks_multi_symbol_multi_tf() {
        let tasks = build_tasks(
            &["EURUSD".into(), "GBPUSD".into()],
            &["D1".into(), "H1".into()],
            "BID",
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 3).unwrap(),
            "/tmp/data",
        );
        // 2 symbols × (D1: 1 year + H1: 1 month) = 4
        assert_eq!(tasks.len(), 4);
    }

    #[test]
    fn build_tasks_m1_spans_multiple_days() {
        let tasks = build_tasks(
            &["EURUSD".into()],
            &["M1".into()],
            "BID",
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 3).unwrap(),
            "/tmp/data",
        );
        assert_eq!(tasks.len(), 3);
    }

    #[test]
    fn period_label_year() {
        let d = NaiveDate::from_ymd_opt(2025, 6, 15).unwrap();
        assert_eq!(period_label(d, PeriodKind::Year), "2025");
    }

    #[test]
    fn period_label_month() {
        let d = NaiveDate::from_ymd_opt(2025, 6, 15).unwrap();
        assert_eq!(period_label(d, PeriodKind::Month), "2025-06");
    }

    #[test]
    fn period_label_day() {
        let d = NaiveDate::from_ymd_opt(2025, 6, 15).unwrap();
        assert_eq!(period_label(d, PeriodKind::Day), "2025-06-15");
    }

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

    #[test]
    fn build_url_year() {
        let url = stream::build_url(
            "BTCUSD",
            "ASK",
            "D1",
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            PeriodKind::Year,
        );
        assert!(url.contains("BTCUSD/2024/ASK_candles_day_1.bi5"));
    }

    #[test]
    fn build_url_month() {
        let url = stream::build_url(
            "BTCUSD",
            "ASK",
            "H1",
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            PeriodKind::Month,
        );
        assert!(url.contains("BTCUSD/2024/06/ASK_candles_hour_1.bi5"));
    }

    #[test]
    fn build_url_day() {
        let url = stream::build_url(
            "BTCUSD",
            "BID",
            "M1",
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            PeriodKind::Day,
        );
        assert!(url.contains("BTCUSD/2024/06/15/BID_candles_min_1.bi5"));
    }
}
