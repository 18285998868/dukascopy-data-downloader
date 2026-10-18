use chrono::Datelike;
use chrono::NaiveDate;

use crate::config::{PriceType, Timeframe};

/// A download URL with its relative storage path and metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadUrl {
    pub url: String,
    pub rel_path: String,
    pub symbol: String,
    pub timeframe: Timeframe,
}

/// Server-side base on Dukascopy datafeed (fixed by the exchange).
const FEED_BASE: &str = "https://www.dukascopy.com/datafeed";
/// Local storage root below output_dir (our choice, mirrors server path).
const LOCAL_PREFIX: &str = "dukascopy";

/// Which calendar bucket a timeframe maps to: one file spans a year/month/day.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PeriodKind {
    Year,
    Month,
    Day,
}

/// Map a timeframe to its server-side file stem.
pub(crate) const fn map_timeframe(tf: Timeframe) -> &'static str {
    match tf {
        Timeframe::M1 => "candles_min_1",
        Timeframe::H1 => "candles_hour_1",
        Timeframe::D1 => "candles_day_1",
    }
}

/// Map a timeframe to its calendar bucket granularity.
pub(crate) const fn tf_period(tf: Timeframe) -> PeriodKind {
    match tf {
        Timeframe::D1 => PeriodKind::Year,
        Timeframe::H1 => PeriodKind::Month,
        Timeframe::M1 => PeriodKind::Day,
    }
}

/// Build a single download URL for the given date and period.
pub(crate) fn build_url(
    symbol: &str,
    price_type: PriceType,
    tf: Timeframe,
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

fn build_date_range(start: NaiveDate, end: NaiveDate, period: PeriodKind) -> Vec<NaiveDate> {
    if start > end {
        return Vec::new();
    }
    match period {
        PeriodKind::Year => {
            let y0 = start.year();
            let y1 = end.year();
            (y0..=y1)
                .map(|y| NaiveDate::from_ymd_opt(y, 1, 1).expect("valid year boundary"))
                .collect()
        }
        PeriodKind::Month => {
            let mut dates = Vec::new();
            let mut y = start.year();
            let mut m = start.month();
            let end_y = end.year();
            let end_m = end.month();
            loop {
                dates.push(NaiveDate::from_ymd_opt(y, m, 1).expect("valid month boundary"));
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
                match d.succ_opt() {
                    Some(next) => d = next,
                    None => break,
                }
            }
            dates
        }
    }
}

/// Generate download URLs for a single symbol/timeframe pair.
/// Local path strictly mirrors the server path below `dukascopy/`.
pub fn build_urls(
    symbol: &str,
    price_type: PriceType,
    tf: Timeframe,
    date_start: NaiveDate,
    date_end: NaiveDate,
) -> Vec<DownloadUrl> {
    if date_start > date_end {
        return Vec::new();
    }
    let period = tf_period(tf);
    build_date_range(date_start, date_end, period)
        .into_iter()
        .map(|date| {
            let url = build_url(symbol, price_type, tf, date, period);
            let suffix = url.strip_prefix(&format!("{FEED_BASE}/")).unwrap_or(&url);
            let rel_path = format!("{LOCAL_PREFIX}/{suffix}");
            DownloadUrl {
                url,
                rel_path,
                symbol: symbol.to_string(),
                timeframe: tf,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn build_urls_d1_single_day() {
        let urls = build_urls(
            "EURUSD",
            PriceType::Bid,
            Timeframe::D1,
            date(2025, 1, 2),
            date(2025, 1, 2),
        );
        assert_eq!(urls.len(), 1);
        assert!(urls[0].url.contains("EURUSD/2025/BID_candles_day_1.bi5"));
        assert_eq!(
            urls[0].rel_path,
            "dukascopy/EURUSD/2025/BID_candles_day_1.bi5"
        );
    }

    #[test]
    fn build_urls_h1_single_day() {
        let urls = build_urls(
            "EURUSD",
            PriceType::Ask,
            Timeframe::H1,
            date(2025, 6, 15),
            date(2025, 6, 15),
        );
        assert_eq!(urls.len(), 1);
        assert!(
            urls[0]
                .url
                .contains("EURUSD/2025/06/ASK_candles_hour_1.bi5")
        );
        assert_eq!(
            urls[0].rel_path,
            "dukascopy/EURUSD/2025/06/ASK_candles_hour_1.bi5"
        );
    }

    #[test]
    fn build_urls_m1_single_day() {
        let urls = build_urls(
            "EURUSD",
            PriceType::Bid,
            Timeframe::M1,
            date(2025, 1, 2),
            date(2025, 1, 2),
        );
        assert_eq!(urls.len(), 1);
        assert!(
            urls[0]
                .url
                .contains("EURUSD/2025/01/02/BID_candles_min_1.bi5")
        );
        assert_eq!(
            urls[0].rel_path,
            "dukascopy/EURUSD/2025/01/02/BID_candles_min_1.bi5"
        );
    }

    #[test]
    fn build_urls_m1_spans_multiple_days() {
        let urls = build_urls(
            "EURUSD",
            PriceType::Bid,
            Timeframe::M1,
            date(2025, 1, 1),
            date(2025, 1, 3),
        );
        assert_eq!(urls.len(), 3);
    }

    #[test]
    fn build_urls_multi_year() {
        let urls = build_urls(
            "EURUSD",
            PriceType::Bid,
            Timeframe::D1,
            date(2025, 1, 1),
            date(2025, 1, 3),
        );
        // D1 maps to Year: same year collapses to 1 file
        assert_eq!(urls.len(), 1);
    }

    #[test]
    fn build_urls_empty_range() {
        assert!(
            build_urls(
                "EURUSD",
                PriceType::Bid,
                Timeframe::D1,
                date(2025, 1, 5),
                date(2025, 1, 3)
            )
            .is_empty()
        );
    }

    #[test]
    fn build_url_year() {
        let url = build_url(
            "BTCUSD",
            PriceType::Ask,
            Timeframe::D1,
            date(2024, 6, 15),
            PeriodKind::Year,
        );
        assert!(url.contains("BTCUSD/2024/ASK_candles_day_1.bi5"));
    }

    #[test]
    fn build_url_month() {
        let url = build_url(
            "BTCUSD",
            PriceType::Ask,
            Timeframe::H1,
            date(2024, 6, 15),
            PeriodKind::Month,
        );
        assert!(url.contains("BTCUSD/2024/06/ASK_candles_hour_1.bi5"));
    }

    #[test]
    fn build_url_day() {
        let url = build_url(
            "BTCUSD",
            PriceType::Bid,
            Timeframe::M1,
            date(2024, 6, 15),
            PeriodKind::Day,
        );
        assert!(url.contains("BTCUSD/2024/06/15/BID_candles_min_1.bi5"));
    }
}
