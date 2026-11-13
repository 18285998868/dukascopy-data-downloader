//! Typed calendar buckets.

use std::fmt;

use chrono::Datelike;

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
    start: chrono::NaiveDate,
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
    pub fn new(timeframe: Timeframe, date: chrono::NaiveDate) -> Self {
        let start = match timeframe {
            Timeframe::D1 => chrono::NaiveDate::from_ymd_opt(date.year(), 1, 1),
            Timeframe::H1 => chrono::NaiveDate::from_ymd_opt(date.year(), date.month(), 1),
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
    pub const fn start(self) -> chrono::NaiveDate {
        self.start
    }

    /// Next bucket on the same calendar, if the calendar can represent it.
    pub(crate) fn next(self) -> Option<Self> {
        let start = match self.timeframe {
            Timeframe::D1 => {
                let year = self.start.year().checked_add(1)?;
                chrono::NaiveDate::from_ymd_opt(year, 1, 1)
            }
            Timeframe::H1 => {
                let (year, month) = if self.start.month() == 12 {
                    (self.start.year().checked_add(1)?, 1)
                } else {
                    (self.start.year(), self.start.month() + 1)
                };
                chrono::NaiveDate::from_ymd_opt(year, month, 1)
            }
            Timeframe::M1 => self.start.succ_opt(),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
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
    fn keys_of_the_same_bucket_are_equal() {
        assert_eq!(
            BucketKey::new(Timeframe::H1, date(2025, 6, 1)),
            BucketKey::new(Timeframe::H1, date(2025, 6, 30))
        );
    }
}
