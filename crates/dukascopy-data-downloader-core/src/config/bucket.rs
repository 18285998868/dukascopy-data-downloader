//! Typed calendar buckets.
//!
//! This module is the single source of truth for the calendar layout of a
//! Dukascopy file: which days a bucket covers, how buckets advance, and where
//! the last *closed* bucket ends. URL rendering and cache paths derive their
//! date components from [`BucketKey`] instead of re-implementing the calendar.

use std::fmt;

use chrono::Datelike;
use chrono::NaiveDate;

use crate::config::Timeframe;

/// The calendar bucket a `.bi5` file covers.
///
/// The bucket granularity follows the timeframe: one year for `D1`, one month
/// for `H1` and one day for `M1`. The key is normalised to the bucket's first
/// calendar day, so two files of the same bucket always compare equal no
/// matter which day inside the bucket produced them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BucketKey {
    timeframe: Timeframe,
    start: NaiveDate,
}

impl BucketKey {
    /// Build the bucket containing `date` for `timeframe`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ddd::config::{BucketKey, Timeframe};
    ///
    /// let date = chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap();
    /// let key = BucketKey::new(Timeframe::H1, date);
    /// assert_eq!(key.start(), chrono::NaiveDate::from_ymd_opt(2025, 6, 1).unwrap());
    /// assert_eq!(key.to_string(), "2025/06");
    /// ```
    #[must_use]
    pub fn new(timeframe: Timeframe, date: NaiveDate) -> Self {
        let start = match timeframe {
            Timeframe::D1 => NaiveDate::from_ymd_opt(date.year(), 1, 1),
            Timeframe::H1 => NaiveDate::from_ymd_opt(date.year(), date.month(), 1),
            Timeframe::M1 => Some(date),
        }
        .unwrap_or(date);
        Self { timeframe, start }
    }

    /// Timeframe whose calendar this bucket belongs to.
    #[must_use]
    pub const fn timeframe(self) -> Timeframe {
        self.timeframe
    }

    /// First calendar day of the bucket.
    #[must_use]
    pub const fn start(self) -> NaiveDate {
        self.start
    }

    /// Last calendar day covered by the bucket.
    ///
    /// For `M1` this is [`BucketKey::start`]; for `H1` the last day of the
    /// month and for `D1` the last day of the year.
    ///
    /// # Examples
    ///
    /// ```
    /// use ddd::config::{BucketKey, Timeframe};
    ///
    /// let date = chrono::NaiveDate::from_ymd_opt(2024, 2, 15).unwrap();
    /// let month = BucketKey::new(Timeframe::H1, date);
    /// assert_eq!(month.end(), chrono::NaiveDate::from_ymd_opt(2024, 2, 29).unwrap());
    /// ```
    #[must_use]
    pub fn end(self) -> NaiveDate {
        match self.timeframe {
            Timeframe::D1 => {
                NaiveDate::from_ymd_opt(self.start.year(), 12, 31).unwrap_or(self.start)
            }
            Timeframe::H1 => {
                end_of_month(self.start.year(), self.start.month()).unwrap_or(self.start)
            }
            Timeframe::M1 => self.start,
        }
    }

    /// Next bucket on the same calendar, if the calendar can represent it.
    ///
    /// # Examples
    ///
    /// ```
    /// use ddd::config::{BucketKey, Timeframe};
    ///
    /// let key = BucketKey::new(
    ///     Timeframe::H1,
    ///     chrono::NaiveDate::from_ymd_opt(2025, 12, 15).unwrap(),
    /// );
    /// assert_eq!(
    ///     key.next().unwrap().start(),
    ///     chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
    /// );
    /// ```
    #[must_use]
    pub fn next(self) -> Option<Self> {
        let start = match self.timeframe {
            Timeframe::D1 => {
                let year = self.start.year().checked_add(1)?;
                NaiveDate::from_ymd_opt(year, 1, 1)
            }
            Timeframe::H1 => {
                let (year, month) = if self.start.month() == 12 {
                    (self.start.year().checked_add(1)?, 1)
                } else {
                    (self.start.year(), self.start.month() + 1)
                };
                NaiveDate::from_ymd_opt(year, month, 1)
            }
            Timeframe::M1 => self.start.succ_opt(),
        }?;
        Some(Self {
            timeframe: self.timeframe,
            start,
        })
    }

    /// Previous bucket on the same calendar, if the calendar can represent it.
    fn prev(self) -> Option<Self> {
        let start = match self.timeframe {
            Timeframe::D1 => {
                let year = self.start.year().checked_sub(1)?;
                NaiveDate::from_ymd_opt(year, 1, 1)
            }
            Timeframe::H1 => {
                let (year, month) = if self.start.month() == 1 {
                    (self.start.year().checked_sub(1)?, 12)
                } else {
                    (self.start.year(), self.start.month() - 1)
                };
                NaiveDate::from_ymd_opt(year, month, 1)
            }
            Timeframe::M1 => self.start.pred_opt(),
        }?;
        Some(Self {
            timeframe: self.timeframe,
            start,
        })
    }
}

impl fmt::Display for BucketKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.timeframe {
            Timeframe::D1 => write!(f, "{:04}", self.start.year()),
            Timeframe::H1 => write!(f, "{:04}/{:02}", self.start.year(), self.start.month()),
            Timeframe::M1 => write!(
                f,
                "{:04}/{:02}/{:02}",
                self.start.year(),
                self.start.month(),
                self.start.day()
            ),
        }
    }
}

/// Last day of a calendar month, or `None` if the month is not representable.
fn end_of_month(year: i32, month: u32) -> Option<NaiveDate> {
    let (next_year, next_month) = if month == 12 {
        (year.checked_add(1)?, 1)
    } else {
        (year, month + 1)
    };
    NaiveDate::from_ymd_opt(next_year, next_month, 1)?.pred_opt()
}

/// Lazily yield every bucket of `timeframe` overlapping `[start, end]`.
///
/// The bucket containing `start` is yielded first and the last yielded bucket
/// starts no later than `end`. An inverted range yields nothing.
pub(crate) fn buckets_between(
    timeframe: Timeframe,
    start: NaiveDate,
    end: NaiveDate,
) -> impl Iterator<Item = BucketKey> {
    // `successors` yields its seed unconditionally, so an inverted range must
    // seed with `None`. Comparing the *raw* dates matters: flooring both to
    // bucket starts would make `2025-01-05 .. 2025-01-03` look non-empty for a
    // yearly bucket, where both floors are `2025-01-01`.
    let seed = (start <= end).then(|| BucketKey::new(timeframe, start));
    std::iter::successors(seed, move |bucket| {
        bucket.next().filter(|next| next.start() <= end)
    })
}

/// The last day that a *closed* bucket of `timeframe` can cover.
///
/// A bucket is one file: one day for `M1`, one month for `H1`, one year for
/// `D1`. The server keeps appending to a bucket until the last calendar day
/// it covers has ended, so a bucket is only final afterwards. Downloading an
/// unfinished bucket caches a file that will never be updated again — the
/// missing tail becomes a permanent hole in the data.
///
/// `end` is the requested upper bound (already clamped to yesterday by
/// [`crate::config::AppConfig::resolved_end`]); `today` is the current UTC
/// date. `M1` returns `end` unchanged: `end` is already in the past by
/// construction and a day bucket closes at midnight.
///
/// The result is never greater than `end`, and it is the *end of the last
/// closed bucket* — so a range whose buckets are all still open yields a
/// value below `start`, which callers treat as "nothing to download".
pub(crate) fn last_closed_day(timeframe: Timeframe, end: NaiveDate, today: NaiveDate) -> NaiveDate {
    match timeframe {
        Timeframe::M1 => end,
        Timeframe::H1 | Timeframe::D1 => {
            let bucket = BucketKey::new(timeframe, end);
            if bucket.end() < today {
                end
            } else {
                // The bucket is still running: fall back to the end of the
                // previous bucket on the same calendar.
                bucket.prev().map_or(end, BucketKey::end)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn the_key_is_normalised_to_the_bucket_start() {
        assert_eq!(
            BucketKey::new(Timeframe::D1, date(2025, 6, 15)).start(),
            date(2025, 1, 1)
        );
        assert_eq!(
            BucketKey::new(Timeframe::H1, date(2025, 6, 15)).start(),
            date(2025, 6, 1)
        );
        assert_eq!(
            BucketKey::new(Timeframe::M1, date(2025, 6, 15)).start(),
            date(2025, 6, 15)
        );
    }

    #[test]
    fn the_key_remembers_its_timeframe() {
        let key = BucketKey::new(Timeframe::H1, date(2025, 6, 15));
        assert_eq!(key.timeframe(), Timeframe::H1);
    }

    #[test]
    fn display_uses_the_server_layout() {
        assert_eq!(
            BucketKey::new(Timeframe::D1, date(2025, 6, 15)).to_string(),
            "2025"
        );
        assert_eq!(
            BucketKey::new(Timeframe::H1, date(2025, 6, 15)).to_string(),
            "2025/06"
        );
        assert_eq!(
            BucketKey::new(Timeframe::M1, date(2007, 1, 2)).to_string(),
            "2007/01/02"
        );
    }

    #[test]
    fn next_moves_by_the_timeframe_granularity() {
        let d1 = BucketKey::new(Timeframe::D1, date(2025, 6, 15));
        assert_eq!(d1.next().unwrap().start(), date(2026, 1, 1));

        let h1 = BucketKey::new(Timeframe::H1, date(2025, 12, 15));
        assert_eq!(h1.next().unwrap().start(), date(2026, 1, 1));

        let m1 = BucketKey::new(Timeframe::M1, date(2024, 2, 29));
        assert_eq!(m1.next().unwrap().start(), date(2024, 3, 1));
    }

    #[test]
    fn prev_moves_back_by_the_timeframe_granularity() {
        let d1 = BucketKey::new(Timeframe::D1, date(2025, 6, 15));
        assert_eq!(d1.prev().unwrap().start(), date(2024, 1, 1));

        let h1 = BucketKey::new(Timeframe::H1, date(2026, 1, 15));
        assert_eq!(h1.prev().unwrap().start(), date(2025, 12, 1));

        let m1 = BucketKey::new(Timeframe::M1, date(2024, 3, 1));
        assert_eq!(m1.prev().unwrap().start(), date(2024, 2, 29));
    }

    #[test]
    fn keys_of_the_same_bucket_are_equal() {
        assert_eq!(
            BucketKey::new(Timeframe::H1, date(2025, 6, 1)),
            BucketKey::new(Timeframe::H1, date(2025, 6, 30))
        );
    }

    // — bucket bounds —

    #[test]
    fn end_is_the_last_day_of_the_bucket() {
        assert_eq!(
            BucketKey::new(Timeframe::M1, date(2025, 6, 15)).end(),
            date(2025, 6, 15)
        );
        assert_eq!(
            BucketKey::new(Timeframe::H1, date(2026, 12, 5)).end(),
            date(2026, 12, 31)
        );
        assert_eq!(
            BucketKey::new(Timeframe::H1, date(2024, 2, 5)).end(),
            date(2024, 2, 29)
        );
        assert_eq!(
            BucketKey::new(Timeframe::D1, date(2024, 2, 5)).end(),
            date(2024, 12, 31)
        );
    }

    // — bucket iteration —

    #[test]
    fn buckets_between_collapses_by_timeframe() {
        let d1: Vec<_> = buckets_between(Timeframe::D1, date(2025, 1, 1), date(2025, 3, 31))
            .map(|b| b.to_string())
            .collect();
        assert_eq!(d1, vec!["2025"]);

        let h1: Vec<_> = buckets_between(Timeframe::H1, date(2025, 1, 1), date(2025, 3, 31))
            .map(|b| b.to_string())
            .collect();
        assert_eq!(h1, vec!["2025/01", "2025/02", "2025/03"]);

        let m1: Vec<_> = buckets_between(Timeframe::M1, date(2025, 1, 1), date(2025, 1, 3))
            .map(|b| b.to_string())
            .collect();
        assert_eq!(m1, vec!["2025/01/01", "2025/01/02", "2025/01/03"]);
    }

    #[test]
    fn buckets_between_rolls_over_month_and_year_boundaries() {
        let collected: Vec<_> =
            buckets_between(Timeframe::H1, date(2024, 11, 15), date(2025, 2, 3))
                .map(|b| b.to_string())
                .collect();
        assert_eq!(collected, vec!["2024/11", "2024/12", "2025/01", "2025/02"]);

        let collected: Vec<_> =
            buckets_between(Timeframe::M1, date(2024, 12, 30), date(2025, 1, 2))
                .map(|b| b.to_string())
                .collect();
        assert_eq!(
            collected,
            vec!["2024/12/30", "2024/12/31", "2025/01/01", "2025/01/02"]
        );
    }

    #[test]
    fn an_inverted_range_yields_no_bucket() {
        assert_eq!(
            buckets_between(Timeframe::D1, date(2025, 1, 5), date(2025, 1, 3)).count(),
            0
        );
        assert_eq!(
            buckets_between(Timeframe::M1, date(2025, 1, 5), date(2025, 1, 3)).count(),
            0
        );
    }

    #[test]
    fn buckets_between_is_lazy() {
        let first: Vec<_> = buckets_between(Timeframe::M1, date(2020, 1, 1), date(2024, 12, 31))
            .take(3)
            .map(|b| b.to_string())
            .collect();
        assert_eq!(first, vec!["2020/01/01", "2020/01/02", "2020/01/03"]);
    }

    // — closed buckets —

    #[test]
    fn a_day_bucket_is_closed_as_soon_as_the_day_is_over() {
        let end = date(2026, 10, 19);
        assert_eq!(last_closed_day(Timeframe::M1, end, date(2026, 10, 20)), end);
    }

    /// The requested range ends inside October, but October's file is still
    /// being written — the last closed month is September.
    #[test]
    fn a_month_bucket_is_not_closed_while_the_month_runs() {
        assert_eq!(
            last_closed_day(Timeframe::H1, date(2026, 10, 19), date(2026, 10, 20)),
            date(2026, 9, 30)
        );
    }

    #[test]
    fn a_month_bucket_is_closed_once_the_month_is_over() {
        let end = date(2026, 9, 30);
        assert_eq!(last_closed_day(Timeframe::H1, end, date(2026, 10, 20)), end);
    }

    #[test]
    fn a_year_bucket_is_not_closed_while_the_year_runs() {
        assert_eq!(
            last_closed_day(Timeframe::D1, date(2026, 10, 19), date(2026, 10, 20)),
            date(2025, 12, 31)
        );
    }

    #[test]
    fn a_year_bucket_is_closed_once_the_year_is_over() {
        let end = date(2025, 6, 30);
        assert_eq!(last_closed_day(Timeframe::D1, end, date(2026, 10, 20)), end);
    }

    /// The closed bound must never exceed the requested one, whatever the
    /// timeframe and the date.
    #[test]
    fn last_closed_day_never_exceeds_the_requested_end() {
        for tf in Timeframe::ALL {
            for end in [date(2026, 1, 1), date(2026, 10, 19), date(2025, 12, 31)] {
                let closed = last_closed_day(tf, end, date(2026, 10, 20));
                assert!(closed <= end, "{tf:?}: {closed} is after {end}");
            }
        }
    }

    /// New Year's Day is the one boundary where both the yearly and the
    /// monthly bucket have just closed: the previous year and the previous
    /// month end on the same calendar day.
    #[test]
    fn new_year_closes_the_previous_year_and_month() {
        let today = date(2026, 1, 1);
        for tf in [Timeframe::D1, Timeframe::H1] {
            assert_eq!(
                last_closed_day(tf, date(2025, 12, 31), today),
                date(2025, 12, 31),
                "{tf:?}: the bucket that ended yesterday is closed"
            );
            assert_eq!(
                last_closed_day(tf, date(2026, 1, 1), today),
                date(2025, 12, 31),
                "{tf:?}: the bucket that started today is still open"
            );
        }
    }
}
