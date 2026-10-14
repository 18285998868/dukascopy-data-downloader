use std::collections::HashSet;
use std::path::Path;

use super::build_urls::build_urls;
use crate::config::DownloadTask;

/// Build a list of DownloadTasks for a date range, skipping locally-present files.
pub fn build_download_tasks(
    symbol: &str,
    timeframe: &str,
    price_type: &str,
    start: chrono::NaiveDate,
    end: chrono::NaiveDate,
    local: &HashSet<String>,
    data_dir: &Path,
) -> Vec<DownloadTask> {
    build_urls(symbol, price_type, timeframe, start, end)
        .into_iter()
        .filter(|dl| !local.contains(&dl.rel_path))
        .map(|dl| DownloadTask {
            url: dl.url,
            dest: data_dir.join(&dl.rel_path),
            symbol: dl.symbol,
            timeframe: dl.timeframe,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn build_tasks_all_missing() {
        let tasks = build_download_tasks(
            "EURUSD",
            "D1",
            "BID",
            date(2025, 1, 1),
            date(2025, 1, 2),
            &HashSet::new(),
            Path::new("/tmp"),
        );
        // D1 maps to Year: same year collapses to 1 file
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].url.contains("EURUSD"));
        assert_eq!(tasks[0].symbol, "EURUSD");
    }

    #[test]
    fn build_tasks_skip_existing() {
        let mut local = HashSet::new();
        local.insert("dukascopy/EURUSD/2025/BID_candles_day_1.bi5".into());
        let tasks = build_download_tasks(
            "EURUSD",
            "D1",
            "BID",
            date(2025, 1, 1),
            date(2025, 1, 2),
            &local,
            Path::new("/tmp"),
        );
        assert!(tasks.is_empty());
    }

    #[test]
    fn build_tasks_m1_skip_one_day() {
        let mut local = HashSet::new();
        local.insert("dukascopy/EURUSD/2025/01/01/BID_candles_min_1.bi5".into());
        let tasks = build_download_tasks(
            "EURUSD",
            "M1",
            "BID",
            date(2025, 1, 1),
            date(2025, 1, 2),
            &local,
            Path::new("/tmp"),
        );
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].url.contains("2025/01/02"));
    }

    #[test]
    fn build_tasks_empty_range() {
        let tasks = build_download_tasks(
            "EURUSD",
            "M1",
            "BID",
            date(2025, 6, 1),
            date(2025, 5, 31),
            &HashSet::new(),
            Path::new("/tmp"),
        );
        assert!(tasks.is_empty());
    }
}
