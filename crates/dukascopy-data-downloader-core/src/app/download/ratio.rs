//! The "requested range is not in the feed" verdict.

use crate::AppError;

use super::missing::{MissingBucket, longest_missing_run};
use super::options::DownloadOptions;
use super::stats::DownloadStats;

/// Numerator of the "not found" ratio above which the batch is warned about.
///
/// Dukascopy publishes a file for every calendar day — weekends and
/// holidays included — so a missing file is not a normal holiday. A
/// noticeable share of 404s means the requested range reaches beyond the
/// feed's coverage.
const NOT_FOUND_WARN_NUM: usize = 1;
/// Denominator of [`NOT_FOUND_WARN_NUM`]: 1/20 = 5%.
const NOT_FOUND_WARN_DEN: usize = 20;
/// Numerator of the "not found" ratio above which the batch fails.
///
/// Past this point the run almost certainly downloaded nothing useful, and
/// reporting success would be actively misleading.
const NOT_FOUND_FAIL_NUM: usize = 1;
/// Denominator of [`NOT_FOUND_FAIL_NUM`]: 1/2 = 50%.
const NOT_FOUND_FAIL_DEN: usize = 2;
/// Smallest batch for which the 404 ratio is meaningful.
///
/// With only a handful of files the ratio is dominated by a single missing
/// day, so the *ratio* check never fails such a batch. The unambiguous
/// all-404 case is handled separately, without a minimum sample size.
const MIN_SAMPLES_FOR_NOT_FOUND_RATIO: usize = 8;
/// Number of consecutively missing buckets that means the requested range
/// is outside the feed's coverage.
///
/// A weekend plus a public holiday is three days in a row, so the threshold
/// sits above that. The rule is independent of the overall ratio: a long
/// gap points at the feed's coverage even when the batch as a whole looks
/// mostly healthy.
const MISSING_RUN_FAIL_LEN: usize = 5;

/// Compare the fraction `part`/`whole` against the exact fraction `num`/`den`.
///
/// Returns [`Ordering::Greater`] when `part`/`whole` is strictly greater
/// than `num`/`den`.
///
/// The comparison is integer arithmetic on purpose: both sides are counts
/// and the thresholds are exact fractions, so a float division would only
/// add rounding to a comparison that does not need it — and would make the
/// boundary cases ("exactly 5%") depend on the binary representation of
/// the threshold.
pub(super) fn cmp_ratio(part: usize, whole: usize, num: usize, den: usize) -> std::cmp::Ordering {
    (part * den).cmp(&(whole * num))
}

/// The "the requested range is not in the feed" verdict.
///
/// Three independent checks share one outcome and one input set:
///
/// - the share of 404s, meaningful only once the batch has enough samples;
/// - the longest calendar run of consecutive missing buckets, which catches
///   a whole missing stretch even when the overall ratio looks modest;
/// - the unambiguous case where nothing was downloaded at all, which is not
///   subject to any minimum sample size.
pub(super) fn range_verdict(
    stats: DownloadStats,
    missing_buckets: &[MissingBucket],
    opts: DownloadOptions,
) -> Result<(), AppError> {
    // A 404 is not a holiday: the feed publishes a file for every calendar
    // day, weekends and holidays included. A noticeable share of them means
    // the requested range reaches beyond what the feed has published — and
    // reporting success would be actively misleading, because the run
    // downloaded nothing while exiting with a success status.
    let longest_run = longest_missing_run(missing_buckets);
    if stats.total >= MIN_SAMPLES_FOR_NOT_FOUND_RATIO {
        let ratio = cmp_ratio(
            stats.not_found,
            stats.total,
            NOT_FOUND_FAIL_NUM,
            NOT_FOUND_FAIL_DEN,
        );
        if ratio == std::cmp::Ordering::Greater {
            if opts.allow_missing {
                tracing::warn!(
                    not_found = stats.not_found,
                    total = stats.total,
                    longest_run,
                    "a large part of the requested range is missing from the data feed; \
                     continuing because missing data was allowed",
                );
            } else {
                return Err(AppError::RangeUnavailable {
                    not_found: stats.not_found,
                    total: stats.total,
                    longest_run,
                });
            }
        }
        let ratio = cmp_ratio(
            stats.not_found,
            stats.total,
            NOT_FOUND_WARN_NUM,
            NOT_FOUND_WARN_DEN,
        );
        if ratio == std::cmp::Ordering::Greater {
            tracing::warn!(
                not_found = stats.not_found,
                total = stats.total,
                ratio = %format!("{}/{}", stats.not_found, stats.total),
                "a noticeable share of the requested range is missing from the data feed",
            );
        }
    }
    // A long stretch of consecutive missing buckets points at the same
    // problem even when the overall ratio is modest: the feed does not have
    // that stretch of the range at all.
    if longest_run >= MISSING_RUN_FAIL_LEN {
        if opts.allow_missing {
            tracing::warn!(
                not_found = stats.not_found,
                total = stats.total,
                longest_run,
                "a long stretch of the requested range is missing from the data feed; \
                 continuing because missing data was allowed",
            );
        } else {
            return Err(AppError::RangeUnavailable {
                not_found: stats.not_found,
                total: stats.total,
                longest_run,
            });
        }
    }
    // A batch that downloaded nothing is never a success: every requested
    // file is missing from the feed, so the run did not deliver the range.
    // The ratio check above needs a minimum sample size before it is
    // meaningful, which is why a batch of one or two files used to slip
    // through and exit successfully having downloaded nothing — exactly the
    // outcome `RangeUnavailable` exists to prevent. This check does not care
    // about the sample size: "nothing at all" is unambiguous at any size.
    if stats.ok == 0 && stats.not_found == stats.total {
        if opts.allow_missing {
            tracing::warn!(
                not_found = stats.not_found,
                total = stats.total,
                longest_run,
                "every requested file is missing from the data feed; \
                 continuing because missing data was allowed",
            );
        } else {
            tracing::error!(
                not_found = stats.not_found,
                total = stats.total,
                longest_run,
                "every requested file is missing from the data feed",
            );
            return Err(AppError::RangeUnavailable {
                not_found: stats.not_found,
                total: stats.total,
                longest_run,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ratio comparison is exact: "exactly 5%" must not warn.
    #[test]
    fn the_warn_ratio_boundary_is_exact() {
        assert_eq!(
            cmp_ratio(1, 20, NOT_FOUND_WARN_NUM, NOT_FOUND_WARN_DEN),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            cmp_ratio(21, 400, NOT_FOUND_WARN_NUM, NOT_FOUND_WARN_DEN),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            cmp_ratio(19, 400, NOT_FOUND_WARN_NUM, NOT_FOUND_WARN_DEN),
            std::cmp::Ordering::Less
        );
    }

    /// "Exactly 50%" must not fail the batch.
    #[test]
    fn the_fail_ratio_boundary_is_exact() {
        assert_eq!(
            cmp_ratio(1, 2, NOT_FOUND_FAIL_NUM, NOT_FOUND_FAIL_DEN),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            cmp_ratio(3, 5, NOT_FOUND_FAIL_NUM, NOT_FOUND_FAIL_DEN),
            std::cmp::Ordering::Greater
        );
    }
}
