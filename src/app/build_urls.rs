//! Stage 1: expand one symbol/timeframe pair into server URLs.
//!
//! Dukascopy packs candles into one file per calendar bucket: `D1` is one file
//! per year, `H1` one file per month and `M1` one file per day. This module maps
//! a timeframe onto that bucket granularity and renders both the remote URL
//! and the mirrored local relative path.
//!
//! The expansion is **lazy**: [`build_urls`] returns an iterator and allocates
//! nothing until it is consumed.

use chrono::Datelike;
use chrono::NaiveDate;

use crate::config::{PriceType, Symbol, Timeframe};

/// Server-side base on the Dukascopy datafeed.
///
/// We address the **terminal** host directly rather than the public entry
/// point. Measured 2026-10-18, same file, same network:
///
/// ```text
/// https://www.dukascopy.com/datafeed/...        2 redirects, 3.51s
/// https://datafeed.dukascopy.com/datafeed/...   0 redirects, 2.03s
/// ```
///
/// The entry host bounces through a plaintext hop before coming back to TLS:
///
/// ```text
/// https://www.dukascopy.com/datafeed/...        301 -> http://datafeed.dukascopy.com/...
/// http://datafeed.dukascopy.com/datafeed/...    301 -> https://datafeed.dukascopy.com/...
/// https://datafeed.dukascopy.com/datafeed/...   200 | 429   <- payload always over TLS
/// ```
///
/// So the candle payload is *always* delivered over TLS regardless of which
/// host is requested, and skipping the entry host costs nothing but saves two
/// round-trips on every single file — which is 2 x N for a full range.
///
/// Risk: `datafeed.dukascopy.com` is the terminal hop rather than a documented
/// public entry point, so Dukascopy could rename it. If every download starts
/// failing with HTTP 404, reverting this constant to the `www` entry host is
/// the fix.
pub(crate) const FEED_BASE: &str = "https://datafeed.dukascopy.com/datafeed";
/// Local storage root below output_dir (our choice, mirrors server path).
const LOCAL_PREFIX: &str = "dukascopy";

/// A download URL paired with the local path that mirrors it.
///
/// Symbol and timeframe are intentionally *not* stored here: every entry
/// produced by a single [`build_urls`] call shares them, so the caller supplies
/// them once when constructing a
/// [`DownloadTask`](crate::config::DownloadTask).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadUrl {
    /// Fully-qualified server URL.
    pub url: String,
    /// `output_dir`-relative path mirroring the server path.
    pub rel_path: String,
}

/// Which calendar bucket a timeframe maps to: one file spans a year/month/day.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeriodKind {
    /// One file per calendar year.
    Year,
    /// One file per calendar month.
    Month,
    /// One file per calendar day.
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

/// Render the server path of one bucket file, relative to [`FEED_BASE`].
///
/// Both the remote URL and the local mirror are derived from this single
/// string so the two can never drift apart.
pub(crate) fn server_path(
    symbol: &Symbol,
    price_type: PriceType,
    tf: Timeframe,
    date: NaiveDate,
    period: PeriodKind,
) -> String {
    let (y, m, d) = (date.year(), date.month(), date.day());
    let tfpart = map_timeframe(tf);
    match period {
        PeriodKind::Year => format!("{symbol}/{y:04}/{price_type}_{tfpart}.bi5"),
        PeriodKind::Month => format!("{symbol}/{y:04}/{m:02}/{price_type}_{tfpart}.bi5"),
        PeriodKind::Day => format!("{symbol}/{y:04}/{m:02}/{d:02}/{price_type}_{tfpart}.bi5"),
    }
}

/// Advance from one bucket start to the next, stopping after `last`.
///
/// One step function serves all three granularities, which is what lets
/// [`bucket_dates`] return a single concrete iterator type.
fn next_bucket(d: NaiveDate, period: PeriodKind, last: NaiveDate) -> Option<NaiveDate> {
    if d >= last {
        return None;
    }
    Some(match period {
        // Both bounds are years taken from real NaiveDates, so +1 stays in range.
        PeriodKind::Year => {
            NaiveDate::from_ymd_opt(d.year() + 1, 1, 1).expect("year step stays in range")
        }
        PeriodKind::Month => {
            let (y, m) = if d.month() == 12 {
                (d.year() + 1, 1)
            } else {
                (d.year(), d.month() + 1)
            };
            NaiveDate::from_ymd_opt(y, m, 1).expect("month step stays in range")
        }
        PeriodKind::Day => d.succ_opt()?,
    })
}

/// Truncate `date` down to the start of its bucket and normalise `end` the same way.
fn bucket_bounds(start: NaiveDate, end: NaiveDate, period: PeriodKind) -> (NaiveDate, NaiveDate) {
    let floor = |d: NaiveDate| match period {
        PeriodKind::Year => NaiveDate::from_ymd_opt(d.year(), 1, 1).expect("year is in range"),
        PeriodKind::Month => {
            NaiveDate::from_ymd_opt(d.year(), d.month(), 1).expect("month is in range")
        }
        PeriodKind::Day => d,
    };
    (floor(start), floor(end))
}

/// Lazily yield the inclusive list of bucket-start dates in `[start, end]`.
fn bucket_dates(
    start: NaiveDate,
    end: NaiveDate,
    period: PeriodKind,
) -> impl Iterator<Item = NaiveDate> {
    let (first, last) = bucket_bounds(start, end, period);
    // `successors` yields its seed unconditionally, so an inverted range must
    // seed with `None`. Comparing the *raw* dates matters: flooring both to
    // bucket starts would make `2025-01-05 .. 2025-01-03` look non-empty for a
    // yearly bucket, where both floors are `2025-01-01`.
    let seed = (start <= end).then_some(first);
    std::iter::successors(seed, move |d| next_bucket(*d, period, last))
}

/// Generate download URLs for a single symbol/timeframe pair, lazily.
///
/// Local path strictly mirrors the server path below `dukascopy/`. Yields
/// nothing when `date_start > date_end`.
///
/// # Examples
///
/// ```
/// use ddd::app::build_urls::build_urls;
/// use ddd::config::{PriceType, Symbol, Timeframe};
///
/// let date = chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap();
/// let urls: Vec<_> = build_urls(
///     &Symbol::new("EURUSD").unwrap(),
///     PriceType::Bid,
///     Timeframe::H1,
///     date,
///     date,
/// )
/// .collect();
///
/// assert_eq!(urls.len(), 1);
/// assert_eq!(
///     urls[0].rel_path,
///     "dukascopy/EURUSD/2025/06/BID_candles_hour_1.bi5"
/// );
/// ```
pub fn build_urls<'a>(
    symbol: &'a Symbol,
    price_type: PriceType,
    tf: Timeframe,
    date_start: NaiveDate,
    date_end: NaiveDate,
) -> impl Iterator<Item = DownloadUrl> + 'a {
    let period = tf_period(tf);
    bucket_dates(date_start, date_end, period).map(move |date| {
        let path = server_path(symbol, price_type, tf, date, period);
        DownloadUrl {
            url: format!("{FEED_BASE}/{path}"),
            rel_path: format!("{LOCAL_PREFIX}/{path}"),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn sym(s: &str) -> Symbol {
        Symbol::new(s).unwrap()
    }

    fn collect(
        symbol: &str,
        pt: PriceType,
        tf: Timeframe,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Vec<DownloadUrl> {
        build_urls(&sym(symbol), pt, tf, start, end).collect()
    }

    fn paths(urls: &[DownloadUrl]) -> Vec<&str> {
        urls.iter().map(|u| u.rel_path.as_str()).collect()
    }

    #[test]
    fn d1_single_day() {
        let urls = collect(
            "EURUSD",
            PriceType::Bid,
            Timeframe::D1,
            date(2025, 1, 2),
            date(2025, 1, 2),
        );
        assert_eq!(
            urls,
            vec![DownloadUrl {
                url: format!("{FEED_BASE}/EURUSD/2025/BID_candles_day_1.bi5"),
                rel_path: "dukascopy/EURUSD/2025/BID_candles_day_1.bi5".to_string(),
            }]
        );
    }

    #[test]
    fn h1_single_day() {
        let urls = collect(
            "EURUSD",
            PriceType::Ask,
            Timeframe::H1,
            date(2025, 6, 15),
            date(2025, 6, 15),
        );
        assert_eq!(
            urls[0].url,
            format!("{FEED_BASE}/EURUSD/2025/06/ASK_candles_hour_1.bi5")
        );
        assert_eq!(
            urls[0].rel_path,
            "dukascopy/EURUSD/2025/06/ASK_candles_hour_1.bi5"
        );
    }

    #[test]
    fn m1_single_day() {
        let urls = collect(
            "EURUSD",
            PriceType::Bid,
            Timeframe::M1,
            date(2025, 1, 2),
            date(2025, 1, 2),
        );
        assert_eq!(
            urls[0].rel_path,
            "dukascopy/EURUSD/2025/01/02/BID_candles_min_1.bi5"
        );
    }

    /// Guards the host choice itself: we deliberately address the terminal hop
    /// to skip two redirects. Revert to `www.dukascopy.com/datafeed` if this
    /// ever starts 404ing in bulk.
    #[test]
    fn feed_base_targets_the_terminal_host() {
        assert_eq!(FEED_BASE, "https://datafeed.dukascopy.com/datafeed");
    }

    /// The local mirror must be exactly the URL path after the feed base.
    #[test]
    fn rel_path_mirrors_url_path_suffix() {
        for tf in [Timeframe::M1, Timeframe::H1, Timeframe::D1] {
            for pt in [PriceType::Bid, PriceType::Ask] {
                let urls = collect("XAUUSD", pt, tf, date(2021, 2, 3), date(2024, 11, 29));
                assert!(!urls.is_empty());
                for u in &urls {
                    assert_eq!(
                        u.url,
                        format!("{FEED_BASE}/{}", &u.rel_path["dukascopy/".len()..])
                    );
                }
            }
        }
    }

    #[test]
    fn m1_spans_multiple_days() {
        let urls = collect(
            "EURUSD",
            PriceType::Bid,
            Timeframe::M1,
            date(2025, 1, 1),
            date(2025, 1, 3),
        );
        assert_eq!(urls.len(), 3);
    }

    #[test]
    fn m1_crosses_month_and_year_boundaries() {
        let urls = collect(
            "EURUSD",
            PriceType::Bid,
            Timeframe::M1,
            date(2024, 12, 30),
            date(2025, 1, 2),
        );
        assert_eq!(
            paths(&urls),
            vec![
                "dukascopy/EURUSD/2024/12/30/BID_candles_min_1.bi5",
                "dukascopy/EURUSD/2024/12/31/BID_candles_min_1.bi5",
                "dukascopy/EURUSD/2025/01/01/BID_candles_min_1.bi5",
                "dukascopy/EURUSD/2025/01/02/BID_candles_min_1.bi5",
            ]
        );
    }

    #[test]
    fn h1_collapses_to_month_buckets() {
        let urls = collect(
            "EURUSD",
            PriceType::Bid,
            Timeframe::H1,
            date(2025, 1, 1),
            date(2025, 3, 31),
        );
        assert_eq!(
            paths(&urls),
            vec![
                "dukascopy/EURUSD/2025/01/BID_candles_hour_1.bi5",
                "dukascopy/EURUSD/2025/02/BID_candles_hour_1.bi5",
                "dukascopy/EURUSD/2025/03/BID_candles_hour_1.bi5",
            ]
        );
    }

    /// D1 maps to Year: same year collapses to 1 file.
    #[test]
    fn d1_collapses_to_year_buckets() {
        let urls = collect(
            "EURUSD",
            PriceType::Bid,
            Timeframe::D1,
            date(2025, 1, 1),
            date(2025, 1, 3),
        );
        assert_eq!(urls.len(), 1);
    }

    #[test]
    fn d1_spans_multiple_years() {
        let urls = collect(
            "EURUSD",
            PriceType::Bid,
            Timeframe::D1,
            date(2023, 6, 1),
            date(2025, 6, 1),
        );
        assert_eq!(
            paths(&urls),
            vec![
                "dukascopy/EURUSD/2023/BID_candles_day_1.bi5",
                "dukascopy/EURUSD/2024/BID_candles_day_1.bi5",
                "dukascopy/EURUSD/2025/BID_candles_day_1.bi5",
            ]
        );
    }

    /// Month buckets must roll over December -> January correctly.
    #[test]
    fn h1_rolls_over_december_into_next_year() {
        let urls = collect(
            "EURUSD",
            PriceType::Bid,
            Timeframe::H1,
            date(2024, 11, 15),
            date(2025, 2, 3),
        );
        assert_eq!(
            paths(&urls),
            vec![
                "dukascopy/EURUSD/2024/11/BID_candles_hour_1.bi5",
                "dukascopy/EURUSD/2024/12/BID_candles_hour_1.bi5",
                "dukascopy/EURUSD/2025/01/BID_candles_hour_1.bi5",
                "dukascopy/EURUSD/2025/02/BID_candles_hour_1.bi5",
            ]
        );
    }

    #[test]
    fn empty_range_yields_nothing() {
        assert!(
            collect(
                "EURUSD",
                PriceType::Bid,
                Timeframe::D1,
                date(2025, 1, 5),
                date(2025, 1, 3)
            )
            .is_empty()
        );
    }

    /// The iterator is lazy: taking a prefix must not build the whole range.
    #[test]
    fn expansion_is_lazy() {
        let first: Vec<_> = build_urls(
            &sym("EURUSD"),
            PriceType::Bid,
            Timeframe::M1,
            date(2020, 1, 1),
            date(2024, 12, 31),
        )
        .take(3)
        .collect();
        assert_eq!(first.len(), 3);
        assert_eq!(
            paths(&first),
            vec![
                "dukascopy/EURUSD/2020/01/01/BID_candles_min_1.bi5",
                "dukascopy/EURUSD/2020/01/02/BID_candles_min_1.bi5",
                "dukascopy/EURUSD/2020/01/03/BID_candles_min_1.bi5",
            ]
        );
    }

    #[test]
    fn count_matches_bucket_count_for_a_full_year_of_m1() {
        let n = build_urls(
            &sym("EURUSD"),
            PriceType::Bid,
            Timeframe::M1,
            date(2023, 1, 1),
            date(2023, 12, 31),
        )
        .count();
        assert_eq!(n, 365, "2023 is not a leap year");
    }

    // — timeframe mapping —

    #[test]
    fn map_timeframe_covers_all_variants() {
        assert_eq!(map_timeframe(Timeframe::M1), "candles_min_1");
        assert_eq!(map_timeframe(Timeframe::H1), "candles_hour_1");
        assert_eq!(map_timeframe(Timeframe::D1), "candles_day_1");
    }

    #[test]
    fn tf_period_covers_all_variants() {
        assert_eq!(tf_period(Timeframe::M1), PeriodKind::Day);
        assert_eq!(tf_period(Timeframe::H1), PeriodKind::Month);
        assert_eq!(tf_period(Timeframe::D1), PeriodKind::Year);
    }

    // — server_path —

    #[test]
    fn server_path_year() {
        assert_eq!(
            server_path(
                &sym("BTCUSD"),
                PriceType::Ask,
                Timeframe::D1,
                date(2024, 6, 15),
                PeriodKind::Year
            ),
            "BTCUSD/2024/ASK_candles_day_1.bi5"
        );
    }

    #[test]
    fn server_path_month() {
        assert_eq!(
            server_path(
                &sym("BTCUSD"),
                PriceType::Ask,
                Timeframe::H1,
                date(2024, 6, 15),
                PeriodKind::Month
            ),
            "BTCUSD/2024/06/ASK_candles_hour_1.bi5"
        );
    }

    #[test]
    fn server_path_day() {
        assert_eq!(
            server_path(
                &sym("BTCUSD"),
                PriceType::Bid,
                Timeframe::M1,
                date(2024, 6, 15),
                PeriodKind::Day
            ),
            "BTCUSD/2024/06/15/BID_candles_min_1.bi5"
        );
    }

    #[test]
    fn server_path_zero_pads_every_component() {
        assert_eq!(
            server_path(
                &sym("EURUSD"),
                PriceType::Bid,
                Timeframe::M1,
                date(2007, 1, 2),
                PeriodKind::Day
            ),
            "EURUSD/2007/01/02/BID_candles_min_1.bi5"
        );
    }
}
