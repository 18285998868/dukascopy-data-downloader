//! Calendar-gap analysis over typed missing buckets.

use crate::config::{BucketKey, Timeframe};

/// The longest run of consecutively missing buckets.
///
/// The buckets are grouped by timeframe *first*: a `D1` bucket is keyed by
/// its year while an `M1` bucket is keyed by a full date, so mixing the two
/// in one sorted list would compare incomparable keys — and a `D1` run of
/// three years would be read as three consecutive *days* of a different
/// granularity. Within a group the keys are sorted chronologically and
/// every neighbouring pair is checked for calendar adjacency: the run only
/// continues when no calendar slot in between was actually served.
pub(super) fn longest_missing_run(buckets: &[BucketKey]) -> usize {
    let mut longest = 0;
    for timeframe in [Timeframe::M1, Timeframe::H1, Timeframe::D1] {
        let mut starts: Vec<chrono::NaiveDate> = buckets
            .iter()
            .filter(|b| b.timeframe() == timeframe)
            .map(|b| b.start())
            .collect();
        longest = longest.max(longest_run_in(&mut starts, timeframe));
    }
    longest
}

/// The longest run of consecutive missing buckets within one timeframe.
fn longest_run_in(starts: &mut Vec<chrono::NaiveDate>, timeframe: Timeframe) -> usize {
    if starts.is_empty() {
        return 0;
    }
    // Sorting the starts puts the buckets in calendar order and makes the
    // duplicate removal exact.
    starts.sort_unstable();
    starts.dedup();
    let mut longest = 1;
    let mut current = 1;
    for pair in starts.windows(2) {
        let adjacent = BucketKey::new(timeframe, pair[0])
            .next()
            .is_some_and(|next| next.start() == pair[1]);
        if adjacent {
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

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    /// Build a missing-bucket list for one timeframe.
    fn missing(timeframe: Timeframe, dates: &[(i32, u32, u32)]) -> Vec<BucketKey> {
        dates
            .iter()
            .map(|&(y, m, d)| BucketKey::new(timeframe, date(y, m, d)))
            .collect()
    }

    #[test]
    fn consecutive_missing_days_form_one_run() {
        let buckets = missing(
            Timeframe::M1,
            &[(2025, 6, 17), (2025, 6, 15), (2025, 6, 16)],
        );
        assert_eq!(longest_missing_run(&buckets), 3);
    }

    /// A served day in between breaks the run.
    #[test]
    fn a_gap_breaks_the_run() {
        let buckets = missing(Timeframe::M1, &[(2025, 6, 15), (2025, 6, 17)]);
        assert_eq!(longest_missing_run(&buckets), 1);
    }

    #[test]
    fn a_month_end_and_the_next_month_start_are_adjacent() {
        let buckets = missing(Timeframe::M1, &[(2025, 6, 30), (2025, 7, 1)]);
        assert_eq!(longest_missing_run(&buckets), 2);
    }

    #[test]
    fn month_buckets_run_across_the_year() {
        let buckets = missing(Timeframe::H1, &[(2025, 12, 15), (2026, 1, 2)]);
        assert_eq!(longest_missing_run(&buckets), 2);
    }

    #[test]
    fn year_buckets_run_across_the_year() {
        let buckets = missing(Timeframe::D1, &[(2025, 6, 15), (2026, 1, 2)]);
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
        let mut buckets = missing(Timeframe::D1, &[(2023, 1, 1), (2024, 1, 1), (2025, 1, 1)]);
        buckets.extend(missing(
            Timeframe::M1,
            &[(2025, 6, 15), (2025, 6, 16), (2025, 6, 17)],
        ));
        assert_eq!(longest_missing_run(&buckets), 3);
    }

    /// A batch that mixes timeframes must not report a run that is longer
    /// than the one actually present in either calendar.
    #[test]
    fn mixing_timeframes_does_not_inflate_the_run() {
        // Two D1 years and one unrelated M1 day: the D1 run is 2, the M1
        // run is 1 — the answer must be 2, not 3.
        let mut buckets = missing(Timeframe::D1, &[(2024, 6, 15), (2025, 6, 15)]);
        buckets.extend(missing(Timeframe::M1, &[(2025, 6, 15)]));
        assert_eq!(longest_missing_run(&buckets), 2);
    }

    /// No missing bucket at all means no run.
    #[test]
    fn an_empty_missing_list_has_no_run() {
        assert_eq!(longest_missing_run(&[]), 0);
    }

    /// The same bucket reported twice still counts as one calendar slot.
    #[test]
    fn a_repeated_bucket_counts_once() {
        let buckets = missing(Timeframe::D1, &[(2025, 1, 1), (2025, 6, 15)]);
        assert_eq!(longest_missing_run(&buckets), 1);
    }
}
