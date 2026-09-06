use std::path::Path;

use chrono::Datelike;
use chrono::NaiveDate;

use crate::config::DownloadTask;
use crate::config::PeriodKind;

const FEED_BASE: &str = "https://www.dukascopy.com/datafeed";

pub fn map_timeframe(tf: &str) -> &'static str {
    match tf {
        "1m" | "M1" => "candles_min_1",
        "1h" | "H1" => "candles_hour_1",
        "1d" | "D1" => "candles_day_1",
        _ => "candles_day_1",
    }
}

pub fn tf_period(tf: &str) -> PeriodKind {
    match map_timeframe(tf) {
        "candles_day_1" => PeriodKind::Year,
        "candles_hour_1" => PeriodKind::Month,
        _ => PeriodKind::Day,
    }
}

pub fn build_url(
    symbol: &str,
    price_type: &str,
    tf: &str,
    date: NaiveDate,
    period: PeriodKind,
) -> String {
    let (y, m, d) = (date.year(), date.month(), date.day());
    let tfpart = map_timeframe(tf);
    match period {
        PeriodKind::Year => {
            format!("{FEED_BASE}/{symbol}/{y:04}/{price_type}_{tfpart}.bi5")
        }
        PeriodKind::Month => {
            format!("{FEED_BASE}/{symbol}/{y:04}/{m:02}/{price_type}_{tfpart}.bi5")
        }
        PeriodKind::Day => {
            format!("{FEED_BASE}/{symbol}/{y:04}/{m:02}/{d:02}/{price_type}_{tfpart}.bi5")
        }
    }
}

pub fn period_label(date: NaiveDate, period: PeriodKind) -> String {
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
    data_dir: &Path,
) -> Vec<DownloadTask> {
    let base = data_dir.join("dukascopy");
    let mut tasks = Vec::new();

    for symbol in symbols {
        for tf in timeframes {
            let period = tf_period(tf);
            for date in build_date_range(start, end, period) {
                let url = build_url(symbol, price_type, tf, date, period);
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
            Path::new("/tmp/data"),
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
            Path::new("/tmp/data"),
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
            Path::new("/tmp/data"),
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
            Path::new("/tmp/data"),
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
            Path::new("/tmp/data"),
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
            Path::new("/tmp/data"),
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
    fn build_url_year() {
        let url = build_url(
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
        let url = build_url(
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
        let url = build_url(
            "BTCUSD",
            "BID",
            "M1",
            NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
            PeriodKind::Day,
        );
        assert!(url.contains("BTCUSD/2024/06/15/BID_candles_min_1.bi5"));
    }
}
