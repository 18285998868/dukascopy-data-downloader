//! Stage 2: turn stage-1 URLs into concrete download tasks.
//!
//! The only value added over [`build_urls`] is dropping files already present
//! on disk and binding each entry to a destination path. Both stages are lazy,
//! so exactly one `Vec` is allocated per pair.

use std::collections::HashSet;
use std::path::Path;

use super::build_urls::build_urls;
use crate::config::{DownloadTask, PriceType, Symbol, Timeframe};

/// Build a list of `DownloadTask`s for a date range, skipping locally-present files.
///
/// `local` holds `output_dir`-relative paths (e.g.
/// `dukascopy/EURUSD/2025/BID_candles_day_1.bi5`) as produced by
/// [`crate::utils::scan_local_files::scan_local_files`].
///
/// The cache set is generic over its hasher so a caller is free to use a
/// cheaper one than the default `RandomState`.
///
/// # Examples
///
/// ```
/// use std::collections::HashSet;
/// use std::path::Path;
/// use ddd::app::build_download_tasks::build_download_tasks;
/// use ddd::config::{PriceType, Symbol, Timeframe};
///
/// let symbol = Symbol::new("EURUSD").unwrap();
/// let date = chrono::NaiveDate::from_ymd_opt(2025, 1, 2).unwrap();
/// let tasks = build_download_tasks(
///     &symbol, Timeframe::D1, PriceType::Bid, date, date,
///     &HashSet::new(), Path::new("/tmp/data"),
/// );
///
/// assert_eq!(tasks.len(), 1);
/// assert_eq!(
///     tasks[0].dest,
///     Path::new("/tmp/data/dukascopy/EURUSD/2025/BID_candles_day_1.bi5")
/// );
/// assert_eq!(tasks[0].symbol, symbol);
/// ```
#[must_use]
pub fn build_download_tasks<S: std::hash::BuildHasher>(
    symbol: &Symbol,
    timeframe: Timeframe,
    price_type: PriceType,
    start: chrono::NaiveDate,
    end: chrono::NaiveDate,
    local: &HashSet<String, S>,
    data_dir: &Path,
) -> Vec<DownloadTask> {
    build_urls(symbol, price_type, timeframe, start, end)
        .filter(|dl| !local.contains(&dl.rel_path))
        .map(|dl| DownloadTask {
            url: dl.url,
            dest: data_dir.join(&dl.rel_path),
            symbol: symbol.clone(),
            timeframe,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn sym(s: &str) -> Symbol {
        Symbol::new(s).unwrap()
    }

    fn local_with(paths: &[&str]) -> HashSet<String> {
        paths.iter().map(|s| (*s).to_string()).collect()
    }

    fn dests(tasks: &[DownloadTask]) -> Vec<String> {
        tasks
            .iter()
            .map(|t| t.dest.to_string_lossy().into_owned())
            .collect()
    }

    fn build(
        symbol: &str,
        tf: Timeframe,
        pt: PriceType,
        start: chrono::NaiveDate,
        end: chrono::NaiveDate,
        local: &HashSet<String>,
    ) -> Vec<DownloadTask> {
        build_download_tasks(&sym(symbol), tf, pt, start, end, local, Path::new("/tmp"))
    }

    #[test]
    fn all_missing_produces_tasks() {
        // D1 maps to Year: same year collapses to 1 file.
        let tasks = build(
            "EURUSD",
            Timeframe::D1,
            PriceType::Bid,
            date(2025, 1, 1),
            date(2025, 1, 2),
            &HashSet::new(),
        );
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].symbol, sym("EURUSD"));
        assert_eq!(tasks[0].timeframe, Timeframe::D1);
        assert_eq!(
            tasks[0].dest,
            Path::new("/tmp/dukascopy/EURUSD/2025/BID_candles_day_1.bi5")
        );
    }

    #[test]
    fn skips_fully_cached_pair() {
        let local = local_with(&["dukascopy/EURUSD/2025/BID_candles_day_1.bi5"]);
        let tasks = build(
            "EURUSD",
            Timeframe::D1,
            PriceType::Bid,
            date(2025, 1, 1),
            date(2025, 1, 2),
            &local,
        );
        assert_eq!(tasks.len(), 0);
    }

    #[test]
    fn skips_only_the_cached_day() {
        let local = local_with(&["dukascopy/EURUSD/2025/01/01/BID_candles_min_1.bi5"]);
        let tasks = build(
            "EURUSD",
            Timeframe::M1,
            PriceType::Bid,
            date(2025, 1, 1),
            date(2025, 1, 3),
            &local,
        );
        assert_eq!(
            dests(&tasks),
            vec![
                "/tmp/dukascopy/EURUSD/2025/01/02/BID_candles_min_1.bi5",
                "/tmp/dukascopy/EURUSD/2025/01/03/BID_candles_min_1.bi5",
            ]
        );
    }

    #[test]
    fn empty_range_produces_nothing() {
        let tasks = build(
            "EURUSD",
            Timeframe::M1,
            PriceType::Bid,
            date(2025, 6, 1),
            date(2025, 5, 31),
            &HashSet::new(),
        );
        assert_eq!(tasks.len(), 0);
    }

    /// A cache hit for the BID file must not suppress the ASK file.
    #[test]
    fn price_type_is_part_of_the_cache_key() {
        let local = local_with(&["dukascopy/EURUSD/2025/BID_candles_day_1.bi5"]);
        let tasks = build(
            "EURUSD",
            Timeframe::D1,
            PriceType::Ask,
            date(2025, 1, 1),
            date(2025, 1, 1),
            &local,
        );
        assert_eq!(tasks.len(), 1);
        assert!(
            tasks[0]
                .dest
                .to_string_lossy()
                .contains("ASK_candles_day_1.bi5")
        );
    }

    #[test]
    fn a_different_symbols_cache_entry_does_not_suppress_this_one() {
        let local = local_with(&["dukascopy/GBPUSD/2025/BID_candles_day_1.bi5"]);
        let tasks = build(
            "EURUSD",
            Timeframe::D1,
            PriceType::Bid,
            date(2025, 1, 1),
            date(2025, 1, 1),
            &local,
        );
        assert_eq!(tasks.len(), 1);
    }

    #[test]
    fn task_urls_match_their_destinations() {
        let tasks = build(
            "EURUSD",
            Timeframe::H1,
            PriceType::Bid,
            date(2025, 1, 1),
            date(2025, 3, 31),
            &HashSet::new(),
        );
        assert_eq!(tasks.len(), 3);
        let feed_prefix = format!("{}/", crate::app::build_urls::FEED_BASE);
        for t in &tasks {
            let suffix = t
                .url
                .strip_prefix(&feed_prefix)
                .expect("url uses the documented feed base");
            assert_eq!(t.dest, Path::new("/tmp/dukascopy").join(suffix));
        }
    }
}
