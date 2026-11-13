//! Missing-bucket bookkeeping and calendar-gap analysis.

use crate::config::Timeframe;

/// A calendar slot a file was expected in, but the feed does not have it.
///
/// The timeframe is part of the value because it defines the slot's
/// granularity: a `D1` bucket is a whole year, an `H1` bucket a month and
/// an `M1` bucket a day. Two buckets are only comparable when they share
/// that granularity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct MissingBucket {
    /// Timeframe whose calendar the bucket belongs to.
    pub(super) timeframe: Timeframe,
    /// Bucket key within that calendar, e.g. `2025/06/15`.
    pub(super) key: String,
}

/// The bucket key of a destination path.
///
/// The trailing *directory* components identify the bucket: the year for
/// `D1`, year and month for `H1`, year, month and day for `M1`. They are
/// fixed-width numbers, so the keys order chronologically as strings.
pub(super) fn bucket_key(dest: &std::path::Path, timeframe: Timeframe) -> MissingBucket {
    let depth = match timeframe {
        Timeframe::D1 => 1,
        Timeframe::H1 => 2,
        Timeframe::M1 => 3,
    };
    let parts: Vec<String> = dest
        .parent()
        .map(|dir| dir.components().rev().take(depth).collect::<Vec<_>>())
        .unwrap_or_default()
        .into_iter()
        .rev()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    MissingBucket {
        timeframe,
        key: parts.join("/"),
    }
}

/// The calendar position of a bucket key, as `(year, month, day)`.
///
/// Components the timeframe does not have default to 1, so a year key
/// becomes `(year, 1, 1)`.
///
/// A key the calendar cannot represent is rejected here rather than carried
/// into [`is_adjacent_bucket`]: that function does `+ 1` arithmetic on the
/// components, and an out-of-range year or month would overflow it (an
/// `M1`/`H1` destination is user-controllable through the public
/// [`super::run::run_downloads`] API).
pub(super) fn bucket_position(key: &str, timeframe: Timeframe) -> Option<(i32, u32, u32)> {
    let mut parts = key.split('/');
    let year: i32 = parts.next()?.parse().ok()?;
    let (month, day) = match timeframe {
        Timeframe::D1 => (1, 1),
        Timeframe::H1 => (parts.next()?.parse().ok()?, 1),
        Timeframe::M1 => (parts.next()?.parse().ok()?, parts.next()?.parse().ok()?),
    };
    chrono::NaiveDate::from_ymd_opt(year, month, day)?;
    Some((year, month, day))
}

/// Whether two buckets are adjacent on the calendar.
fn is_adjacent_bucket(a: (i32, u32, u32), b: (i32, u32, u32), timeframe: Timeframe) -> bool {
    match timeframe {
        Timeframe::D1 => b == (a.0 + 1, 1, 1),
        Timeframe::H1 => {
            // December rolls over into the next year.
            let next = if a.1 == 12 {
                (a.0 + 1, 1)
            } else {
                (a.0, a.1 + 1)
            };
            b == (next.0, next.1, 1)
        }
        Timeframe::M1 => match (
            chrono::NaiveDate::from_ymd_opt(a.0, a.1, a.2),
            chrono::NaiveDate::from_ymd_opt(b.0, b.1, b.2),
        ) {
            (Some(da), Some(db)) => da.succ_opt() == Some(db),
            _ => false,
        },
    }
}

/// The longest run of consecutively missing buckets.
///
/// The buckets are grouped by timeframe *first*: a `D1` bucket is keyed by
/// its year while an `M1` bucket is keyed by a full date, so mixing the two
/// in one sorted list would compare incomparable keys — and a `D1` run of
/// three years would be read as three consecutive *days* of a different
/// granularity. Within a group the keys are sorted chronologically and
/// every neighbouring pair is checked for calendar adjacency: the run only
/// continues when no calendar slot in between was actually served.
pub(super) fn longest_missing_run(buckets: &[MissingBucket]) -> usize {
    let mut longest = 0;
    for timeframe in [Timeframe::M1, Timeframe::H1, Timeframe::D1] {
        // Parse straight into positions: cloning every key into a
        // `Vec<String>` first only to parse it would allocate once per
        // missing file.
        let mut positions: Vec<(i32, u32, u32)> = buckets
            .iter()
            .filter(|b| b.timeframe == timeframe)
            .filter_map(|b| bucket_position(&b.key, timeframe))
            .collect();
        longest = longest.max(longest_run_in(&mut positions, timeframe));
    }
    longest
}

/// The longest run of consecutive missing buckets within one timeframe.
///
/// Only buckets that can be *placed on the calendar* take part. A key that
/// does not parse belongs to a destination that does not mirror the server
/// layout; such a bucket can neither extend a run nor break one, so it is
/// left out instead of silently truncating a real gap.
fn longest_run_in(positions: &mut Vec<(i32, u32, u32)>, timeframe: Timeframe) -> usize {
    if positions.is_empty() {
        return 0;
    }
    // Sorting the parsed positions puts the buckets in calendar order and
    // makes the duplicate removal exact.
    positions.sort_unstable();
    positions.dedup();
    let mut longest = 1;
    let mut current = 1;
    for pair in positions.windows(2) {
        if is_adjacent_bucket(pair[0], pair[1], timeframe) {
            current += 1;
            longest = longest.max(current);
        } else {
            current = 1;
        }
    }
    longest
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// Build a missing-bucket list for one timeframe.
    fn missing(timeframe: Timeframe, keys: &[&str]) -> Vec<MissingBucket> {
        keys.iter()
            .map(|k| MissingBucket {
                timeframe,
                key: (*k).to_string(),
            })
            .collect()
    }

    /// The bucket key names the calendar slot the file belongs to.
    #[test]
    fn the_bucket_key_uses_the_calendar_components() {
        let d1 = Path::new("/out/dukascopy/EURUSD/2025/BID_candles_day_1.bi5");
        assert_eq!(bucket_key(d1, Timeframe::D1).key, "2025");

        let h1 = Path::new("/out/dukascopy/EURUSD/2025/06/BID_candles_hour_1.bi5");
        assert_eq!(bucket_key(h1, Timeframe::H1).key, "2025/06");

        let m1 = Path::new("/out/dukascopy/EURUSD/2025/06/15/BID_candles_min_1.bi5");
        assert_eq!(bucket_key(m1, Timeframe::M1).key, "2025/06/15");
    }

    /// The key carries the timeframe it was derived for: the same
    /// destination yields a different key depth per granularity.
    #[test]
    fn the_bucket_key_records_its_timeframe() {
        let h1 = Path::new("/out/dukascopy/EURUSD/2025/06/BID_candles_hour_1.bi5");
        assert_eq!(bucket_key(h1, Timeframe::H1).timeframe, Timeframe::H1);
    }

    /// A destination without any directory component must not panic: the
    /// key degenerates to an empty string, which simply never matches a
    /// calendar slot.
    #[test]
    fn a_destination_without_directories_yields_an_empty_key() {
        let dest = Path::new("BID_candles_day_1.bi5");
        assert_eq!(bucket_key(dest, Timeframe::D1).key, "");
        assert_eq!(bucket_position("", Timeframe::D1), None);
    }

    /// A key that is not a date cannot be placed on the calendar, so it
    /// never counts as adjacent to anything.
    #[test]
    fn a_non_numeric_key_has_no_calendar_position() {
        assert_eq!(bucket_position("not-a-year", Timeframe::D1), None);
        assert_eq!(bucket_position("2025/not-a-month", Timeframe::H1), None);
        assert_eq!(bucket_position("2025/06/not-a-day", Timeframe::M1), None);
    }

    /// A key the calendar cannot represent is not a position at all.
    ///
    /// This is what keeps `is_adjacent_bucket`'s `+ 1` arithmetic in range:
    /// an impossible day, a 13th month or an out-of-range year is rejected
    /// here, before it can reach the comparison.
    #[test]
    fn an_out_of_range_calendar_key_is_unplaceable() {
        assert_eq!(bucket_position("2025/02/30", Timeframe::M1), None);
        assert_eq!(bucket_position("2025/13", Timeframe::H1), None);
        assert_eq!(bucket_position("2025/00", Timeframe::H1), None);
        assert_eq!(bucket_position("2025/06/00", Timeframe::M1), None);
        assert_eq!(
            bucket_position("2147483647", Timeframe::D1),
            None,
            "an i32 year outside the calendar must not reach the +1 arithmetic"
        );
        assert_eq!(
            bucket_position("2025/4294967295", Timeframe::H1),
            None,
            "a u32::MAX month must not reach the +1 arithmetic"
        );
    }

    #[test]
    fn consecutive_missing_days_form_one_run() {
        let buckets = missing(Timeframe::M1, &["2025/06/17", "2025/06/15", "2025/06/16"]);
        assert_eq!(longest_missing_run(&buckets), 3);
    }

    /// A served day in between breaks the run.
    #[test]
    fn a_gap_breaks_the_run() {
        let buckets = missing(Timeframe::M1, &["2025/06/15", "2025/06/17"]);
        assert_eq!(longest_missing_run(&buckets), 1);
    }

    #[test]
    fn a_month_end_and_the_next_month_start_are_adjacent() {
        let buckets = missing(Timeframe::M1, &["2025/06/30", "2025/07/01"]);
        assert_eq!(longest_missing_run(&buckets), 2);
    }

    #[test]
    fn month_buckets_run_across_the_year() {
        let buckets = missing(Timeframe::H1, &["2025/12", "2026/01"]);
        assert_eq!(longest_missing_run(&buckets), 2);
    }

    #[test]
    fn year_buckets_run_across_the_year() {
        let buckets = missing(Timeframe::D1, &["2025", "2026"]);
        assert_eq!(longest_missing_run(&buckets), 2);
    }

    /// The run is measured *within* each timeframe's own calendar.
    ///
    /// A `D1` run of three years and an `M1` run of three days are both
    /// three buckets long, but they are not comparable: mixing the two key
    /// spaces would sort `"2025"` next to `"2025/06/15"` and call the pair
    /// non-adjacent, splitting both runs in two.
    #[test]
    fn the_run_is_measured_per_timeframe() {
        let mut buckets = missing(Timeframe::D1, &["2023", "2024", "2025"]);
        buckets.extend(missing(
            Timeframe::M1,
            &["2025/06/15", "2025/06/16", "2025/06/17"],
        ));
        assert_eq!(longest_missing_run(&buckets), 3);
    }

    /// A batch that mixes timeframes must not report a run that is longer
    /// than the one actually present in either calendar.
    #[test]
    fn mixing_timeframes_does_not_inflate_the_run() {
        // Two D1 years and one unrelated M1 day: the D1 run is 2, the M1
        // run is 1 — the answer must be 2, not 3.
        let mut buckets = missing(Timeframe::D1, &["2024", "2025"]);
        buckets.extend(missing(Timeframe::M1, &["2025/06/15"]));
        assert_eq!(longest_missing_run(&buckets), 2);
    }

    /// No missing bucket at all means no run.
    #[test]
    fn an_empty_missing_list_has_no_run() {
        assert_eq!(longest_missing_run(&[]), 0);
    }

    /// A key that cannot be placed on the calendar must not truncate a
    /// real gap.
    ///
    /// Such a key comes from a destination that does not mirror the server
    /// layout. It can neither extend a run nor break one, so the two
    /// consecutive years around it still count as one run of two.
    #[test]
    fn an_unplaceable_key_does_not_break_a_run() {
        let buckets = missing(Timeframe::D1, &["2024", "not-a-year", "2025"]);
        assert_eq!(
            longest_missing_run(&buckets),
            2,
            "the garbage key must not cut the run in two"
        );
    }

    /// A batch whose keys are *all* unplaceable has no measurable gap.
    #[test]
    fn a_batch_of_only_unplaceable_keys_has_no_run() {
        let buckets = missing(Timeframe::D1, &["nope", "also-nope"]);
        assert_eq!(longest_missing_run(&buckets), 0);
    }
}
