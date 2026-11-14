//! Date-range defaults and resolution.

use crate::AppError;
use crate::config::AppConfig;

/// Upper bound for the *effective* start..=end range (fail fast on typos).
///
/// 366 × 15 days — fifteen years of the widest bucket (one file per day).
/// The limit is part of the public contract: a configuration that exceeds
/// it is rejected instead of expanding into tens of thousands of requests.
pub const MAX_RANGE_DAYS: i64 = 366 * 15;
/// Preferred start date when `start` is not configured.
///
/// Once this date falls outside the [`MAX_RANGE_DAYS`] window,
/// [`AppConfig::effective_range`] moves the default start forward to the
/// earliest date that still fits and logs a warning. The default therefore
/// keeps working after 2035 without silently expanding the range.
pub const DEFAULT_START: chrono::NaiveDate = match chrono::NaiveDate::from_ymd_opt(2020, 1, 1) {
    Some(date) => date,
    // INVARIANT: 2020-01-01 is a valid calendar date. `NaiveDate::MIN`
    // keeps the constant total without a panicking path the compiler
    // cannot prove unreachable.
    None => chrono::NaiveDate::MIN,
};

impl AppConfig {
    /// Resolve the download end date: `min(config.end, yesterday)`.
    ///
    /// Computed once by the caller; downstream only consumes the value.
    ///
    /// # Examples
    ///
    /// ```
    /// # use ddd::AppConfig;
    /// let cfg = AppConfig::from_toml_str("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]").unwrap();
    /// let today = chrono::NaiveDate::from_ymd_opt(2025, 3, 2).unwrap();
    ///
    /// // Without an explicit `end`, the range stops at yesterday.
    /// assert_eq!(
    ///     cfg.resolved_end(today),
    ///     chrono::NaiveDate::from_ymd_opt(2025, 3, 1).unwrap()
    /// );
    /// ```
    #[must_use]
    pub fn resolved_end(&self, today: chrono::NaiveDate) -> chrono::NaiveDate {
        let yesterday = today.pred_opt().unwrap_or(today);
        self.end().map_or(yesterday, |e| e.min(yesterday))
    }

    /// Resolve the *effective* `(start, end)` range and enforce the day cap.
    ///
    /// This is the only place that knows both defaults, so the cap applies
    /// whether `end` was configured explicitly or defaulted to yesterday —
    /// checking only explicit pairs would let `start` alone open an unbounded
    /// range.
    ///
    /// When `start` is omitted and [`DEFAULT_START`] has fallen out of the
    /// 15-year window, the default is moved forward to the earliest date that
    /// still fits (with a warning). Without that adjustment the built-in
    /// default would start failing in 2035 even though the user configured
    /// nothing wrong.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Config`] if an *explicit* range spans more than
    /// [`MAX_RANGE_DAYS`] days. A reversed range is *not* an error here; the
    /// per-pair pipeline skips it with a warning.
    ///
    /// # Examples
    ///
    /// ```
    /// # use ddd::AppConfig;
    /// let cfg = AppConfig::from_toml_str("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]").unwrap();
    /// let today = chrono::NaiveDate::from_ymd_opt(2025, 3, 2).unwrap();
    /// let (start, end) = cfg.effective_range(today).unwrap();
    /// assert_eq!(start, chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
    /// assert_eq!(end, chrono::NaiveDate::from_ymd_opt(2025, 3, 1).unwrap());
    /// ```
    pub fn effective_range(
        &self,
        today: chrono::NaiveDate,
    ) -> Result<(chrono::NaiveDate, chrono::NaiveDate), AppError> {
        let end = self.resolved_end(today);
        let start = self.start().unwrap_or_else(|| {
            // Keep the default inside the cap once 2020-01-01 is too old.
            // `checked_sub_signed` avoids panicking for extreme `today`
            // values supplied through the public API.
            let earliest = end.checked_sub_signed(chrono::Duration::days(MAX_RANGE_DAYS - 1));
            match earliest {
                Some(earliest) if earliest > DEFAULT_START => {
                    tracing::warn!(
                        default_start = %DEFAULT_START,
                        effective_start = %earliest,
                        "the default start is outside the 15-year window; \
                         using the earliest date inside the window",
                    );
                    earliest
                }
                _ => DEFAULT_START,
            }
        });
        // `start..=end` is inclusive, so the range covers one more calendar
        // day than the distance between the two dates.
        let days = (end - start).num_days() + 1;
        if days > MAX_RANGE_DAYS {
            return Err(AppError::Config(format!(
                "date range {start}..={end} spans {days} days, exceeding maximum {MAX_RANGE_DAYS}"
            )));
        }
        Ok((start, end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn load(contents: &str) -> Result<AppConfig, AppError> {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "{contents}").unwrap();
        drop(f);
        AppConfig::load(&path)
    }

    fn cfg(contents: &str) -> AppConfig {
        load(contents).expect("config should be valid")
    }

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    // — resolved_end —

    #[test]
    fn resolved_end_clamps_future_to_yesterday() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nend = \"2030-01-10\"");
        assert_eq!(cfg.resolved_end(date(2030, 1, 10)), date(2030, 1, 9));
    }

    #[test]
    fn resolved_end_defaults_to_yesterday() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]");
        assert_eq!(cfg.resolved_end(date(2024, 6, 15)), date(2024, 6, 14));
    }

    #[test]
    fn resolved_end_keeps_past_end_untouched() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nend = \"2020-05-06\"");
        assert_eq!(cfg.resolved_end(date(2024, 6, 15)), date(2020, 5, 6));
    }

    // — effective_range —

    #[test]
    fn effective_range_defaults_to_2020_and_yesterday() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]");
        assert_eq!(
            cfg.effective_range(date(2024, 6, 15)).unwrap(),
            (date(2020, 1, 1), date(2024, 6, 14))
        );
    }

    #[test]
    fn default_start_is_2020_01_01() {
        assert_eq!(DEFAULT_START, date(2020, 1, 1));
    }

    /// The day before the default becomes too old, it is still used as-is.
    #[test]
    fn default_start_is_kept_until_the_cap_is_reached() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]");
        let (start, end) = cfg.effective_range(date(2035, 1, 12)).unwrap();
        assert_eq!(start, DEFAULT_START);
        assert_eq!(end, date(2035, 1, 11));
        assert_eq!((end - start).num_days() + 1, MAX_RANGE_DAYS);
    }

    /// From 2035-01-13 on, the fixed 2020-01-01 default would exceed the
    /// 15-year cap. The default is moved forward instead of failing a
    /// configuration the user never wrote.
    #[test]
    fn default_start_moves_into_the_cap_after_2035() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]");
        let (start, end) = cfg.effective_range(date(2035, 1, 13)).unwrap();
        assert_eq!(start, date(2020, 1, 2));
        assert_eq!(end, date(2035, 1, 12));
        assert_eq!((end - start).num_days() + 1, MAX_RANGE_DAYS);
    }

    /// The automatic adjustment applies to the *default* only. A user who
    /// explicitly asks for an oversized range still gets an error.
    #[test]
    fn explicit_start_after_2035_still_errors() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2020-01-01\"");
        let err = cfg.effective_range(date(2035, 1, 13)).unwrap_err();
        assert!(err.to_string().contains("exceeding maximum"), "{err}");
    }

    #[test]
    fn effective_range_rejects_excessive_explicit_range() {
        let cfg = cfg(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2010-01-01\"\nend = \"2030-01-01\"",
        );
        let err = cfg.effective_range(date(2026, 1, 1)).unwrap_err();
        assert!(err.to_string().contains("exceeding maximum"), "{err}");
    }

    /// A far-future `end` is clamped to yesterday *before* the day cap is
    /// measured, so a `9999-12-31` typo cannot open an oversized range.
    #[test]
    fn effective_range_clamps_far_future_end_before_applying_cap() {
        let cfg = cfg(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2020-01-01\"\nend = \"9999-12-31\"",
        );
        let (start, end) = cfg.effective_range(date(2026, 1, 1)).unwrap();
        assert_eq!(start, date(2020, 1, 1));
        assert_eq!(end, date(2025, 12, 31));
    }

    #[test]
    fn effective_range_accepts_range_well_inside_the_cap() {
        let cfg = cfg(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2025-01-01\"\nend = \"2025-12-31\"",
        );
        assert!(cfg.effective_range(date(2026, 1, 1)).is_ok());
    }

    /// Regression: the day cap used to be checked only when `end` was set
    /// explicitly, so `start` alone could open a ~126 year range.
    #[test]
    fn effective_range_rejects_excessive_range_when_end_is_omitted() {
        let text = "symbol = [\"EURUSD\"]\ntimeframe = [\"M1\"]\nstart = \"1900-01-01\"";
        let cfg = load(text).expect("load itself succeeds");
        let err = cfg.effective_range(date(2026, 10, 18)).unwrap_err();
        assert!(err.to_string().contains("exceeding maximum"), "{err}");
    }

    #[test]
    fn effective_range_allows_reversed_range_without_error() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2024-01-01\"");
        let (start, end) = cfg.effective_range(date(2023, 1, 1)).unwrap();
        assert!(
            start > end,
            "reversed range must survive for run_one to skip"
        );
    }

    /// The cap counts the *calendar days in the inclusive range*: an
    /// off-by-one must not let one extra day through.
    #[test]
    fn effective_range_counts_the_inclusive_range() {
        let start = date(2020, 1, 1);
        let end = start + chrono::Duration::days(MAX_RANGE_DAYS);
        let cfg = load(&format!(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"{start}\"\nend = \"{end}\""
        ))
        .expect("load itself succeeds");

        // `start..=end` covers MAX_RANGE_DAYS + 1 calendar days.
        let err = cfg
            .effective_range(end + chrono::Duration::days(1))
            .unwrap_err();
        assert!(err.to_string().contains("exceeding maximum"), "{err}");
    }

    #[test]
    fn effective_range_accepts_exactly_the_cap() {
        let start = date(2020, 1, 1);
        let end = start + chrono::Duration::days(MAX_RANGE_DAYS - 1);
        let cfg = load(&format!(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"{start}\"\nend = \"{end}\""
        ))
        .expect("load itself succeeds");

        // `start..=end` covers exactly MAX_RANGE_DAYS calendar days.
        let (s, e) = cfg
            .effective_range(end + chrono::Duration::days(1))
            .unwrap();
        assert_eq!(s, start);
        assert_eq!(e, end);
    }

    /// The cap is part of the public contract, so it is exported rather
    /// than kept private to the module.
    #[test]
    fn the_range_cap_is_a_public_constant() {
        assert_eq!(MAX_RANGE_DAYS, 366 * 15);
        assert_eq!(crate::MAX_RANGE_DAYS, MAX_RANGE_DAYS);
    }
}
