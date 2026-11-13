//! Bounded-concurrency download orchestration.
//!
//! The first file is downloaded **on its own** before the rest are released.
//! An unreachable pair would otherwise spend a full wave of `concurrency`
//! requests — each burning the whole retry budget — to reach the verdict a
//! single request already gives. The remaining files then stream through
//! `buffer_unordered(concurrency)`, each additionally holding a permit from
//! the shared [`Semaphore`] gate.

use std::future::Future;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::Semaphore;
use tracing::Instrument;

use crate::AppError;
use crate::config::DownloadTask;

use super::breaker::{
    BREAKER_FAIL_DEN, BREAKER_FAIL_NUM, BREAKER_WINDOW, Breaker, is_transport_failure,
};
use super::missing::{MissingBucket, bucket_key, bucket_position};
use super::options::DownloadOptions;
use super::ratio::range_verdict;
use super::stats::DownloadStats;
use super::stream;

/// How often a line of progress is logged, in completed files.
const PROGRESS_EVERY: usize = 100;
/// Maximum number of individual failure details retained in the batch summary.
const MAX_REPORTED_FAILURES: usize = 20;

/// Outcome of one task inside a batch.
#[derive(Debug)]
enum TaskOutcome {
    /// Downloaded successfully.
    Ok,
    /// The server does not have this file (HTTP 404), with the bucket it was
    /// missing for.
    NotFound {
        /// Calendar slot the file was missing from.
        bucket: MissingBucket,
    },
    /// Failed for any other reason.
    Failed(AppError),
    /// Never attempted: the circuit breaker was already open.
    Skipped,
}

/// Download all tasks, bounded by the shared `gate`.
///
/// The batch is the unit of every per-batch decision: the probe, the
/// circuit breaker and the "the range is not in the feed" analysis all
/// look at the whole batch at once.
///
/// Tasks may mix timeframes. The missing-range analysis then measures the
/// longest run of consecutive missing buckets **within each timeframe's
/// own calendar** — a `D1` gap is counted in years, an `M1` gap in days —
/// and reports the longest of them. Mixing the two key spaces would make
/// a three-year gap look like three unrelated days.
///
/// `gate` must have at least one permit. The function only waits for
/// permits and never adds any, so a zero-permit semaphore makes the first
/// task wait forever. A gate that is momentarily exhausted by another
/// batch is fine: those permits are released when that batch finishes.
///
/// # Errors
///
/// Returns [`AppError::BatchFailed`] carrying a bounded sample of per-file
/// causes when any task fails; a task answered with 404 is counted in
/// [`DownloadStats::not_found`] and does **not by itself** fail the batch.
/// Returns [`AppError::RangeUnavailable`] when the requested range is
/// (largely) absent from the feed, including the case where every requested
/// file is a 404.
///
/// # Examples
///
/// ```no_run
/// # use std::sync::Arc;
/// # use tokio::sync::Semaphore;
/// # async fn example() -> Result<(), ddd::AppError> {
/// let client = reqwest::Client::new();
/// let gate = Arc::new(Semaphore::new(4));
///
/// // An empty task list is a no-op that still yields usable statistics.
/// let stats = ddd::app::download::run_downloads(&client, Vec::new(), 4, &gate).await?;
/// assert_eq!(stats.total, 0);
/// assert_eq!(stats.ok, 0);
/// # Ok(())
/// # }
/// ```
pub async fn run_downloads(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
    concurrency: usize,
    gate: &Arc<Semaphore>,
) -> Result<DownloadStats, AppError> {
    let download_fn = move |c: reqwest::Client, t: DownloadTask| async move {
        stream::download_to_file(&c, &t).await
    };
    run_downloads_inner(client, tasks, concurrency, gate, download_fn).await
}

/// Download all tasks, bounded by the shared `gate`, with explicit options.
///
/// This is [`run_downloads`] plus the options that decide what happens to
/// a range the feed does not have; see [`DownloadOptions::allow_missing`].
///
/// The `gate` precondition is the same as [`run_downloads`]: it must have
/// at least one permit, because this function never adds permits itself.
///
/// # Errors
///
/// Returns [`AppError::BatchFailed`] when any task fails, and
/// [`AppError::RangeUnavailable`] when the requested range is (largely)
/// absent from the feed and [`DownloadOptions::allow_missing`] is not set.
///
/// # Examples
///
/// ```no_run
/// # use std::sync::Arc;
/// # use tokio::sync::Semaphore;
/// # async fn example() -> Result<(), ddd::AppError> {
/// let client = reqwest::Client::new();
/// let gate = Arc::new(Semaphore::new(4));
/// let opts = ddd::DownloadOptions {
///     allow_missing: true,
/// };
///
/// let stats =
///     ddd::app::download::run_downloads_with(&client, Vec::new(), 4, &gate, &opts).await?;
/// assert_eq!(stats.total, 0);
/// # Ok(())
/// # }
/// ```
pub async fn run_downloads_with(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
    concurrency: usize,
    gate: &Arc<Semaphore>,
    opts: &DownloadOptions,
) -> Result<DownloadStats, AppError> {
    let download_fn = move |c: reqwest::Client, t: DownloadTask| async move {
        stream::download_to_file(&c, &t).await
    };
    run_downloads_inner_with(client, tasks, concurrency, gate, *opts, download_fn).await
}

/// Core download orchestration with the default (strict) options.
///
/// Parameterized by the download function for testing.
async fn run_downloads_inner<F, Fut>(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
    concurrency: usize,
    gate: &Arc<Semaphore>,
    download_fn: F,
) -> Result<DownloadStats, AppError>
where
    F: Fn(reqwest::Client, DownloadTask) -> Fut,
    Fut: Future<Output = Result<(), AppError>>,
{
    run_downloads_inner_with(
        client,
        tasks,
        concurrency,
        gate,
        DownloadOptions::default(),
        download_fn,
    )
    .await
}

/// Core download orchestration, parameterized by the download function for testing.
///
/// The first file is downloaded **on its own** before the rest are released.
/// An unreachable pair would otherwise spend a full wave of `concurrency`
/// requests — each burning the whole retry budget — to reach the verdict a
/// single request already gives. The remaining files then stream through
/// `buffer_unordered(concurrency)`, each additionally holding a permit from
/// the shared `gate`.
///
/// The breaker closes the batch when the recent attempts look unhealthy: its
/// window opens on a failure ratio, and a transport failure on the probe
/// trips it outright.
async fn run_downloads_inner_with<F, Fut>(
    client: &reqwest::Client,
    tasks: Vec<DownloadTask>,
    concurrency: usize,
    gate: &Arc<Semaphore>,
    opts: DownloadOptions,
    download_fn: F,
) -> Result<DownloadStats, AppError>
where
    F: Fn(reqwest::Client, DownloadTask) -> Fut,
    Fut: Future<Output = Result<(), AppError>>,
{
    let total = tasks.len();
    let concurrency = concurrency.max(1);
    if total == 0 {
        return Ok(DownloadStats::default());
    }
    tracing::debug!(total, concurrency, "downloading files");

    let mut stats = DownloadStats {
        total,
        ..DownloadStats::default()
    };
    let mut reasons: Vec<String> = Vec::with_capacity(MAX_REPORTED_FAILURES);
    let mut missing_buckets: Vec<MissingBucket> = Vec::new();
    let breaker = Breaker::new();

    let mut tasks = tasks.into_iter();

    // — probe —
    // `total > 0` is checked above, so there is a first task.
    let probe = tasks.next().expect("total > 0");
    let outcome = run_one_task(client, gate, &breaker, &download_fn, probe).await;
    let endpoint_unreachable =
        matches!(&outcome, TaskOutcome::Failed(err) if is_transport_failure(err));
    record_outcome(outcome, &mut stats, &mut reasons, &mut missing_buckets);
    if endpoint_unreachable {
        // The endpoint could not be reached at all. Every remaining
        // file of this pair would spend the whole retry budget to
        // reach the same verdict, so stop here.
        breaker.trip();
        tracing::warn!(
            failed = stats.failed,
            total = stats.total,
            "probe could not reach the endpoint; remaining files were not attempted",
        );
    }

    // — release the rest —
    // The per-task async block is constructed once per element, so
    // `download_fn` is captured by reference; capturing it by value would
    // move it out of the `FnMut` `map` closure on the first element.
    let download_fn = &download_fn;
    let mut stream = futures::stream::iter(tasks)
        .map(|t| {
            let c = client.clone();
            let gate = gate.clone();
            let breaker = &breaker;
            async move { run_one_task(&c, &gate, breaker, download_fn, t).await }
        })
        .buffer_unordered(concurrency);
    while let Some(outcome) = stream.next().await {
        record_outcome(outcome, &mut stats, &mut reasons, &mut missing_buckets);

        if stats.done() % PROGRESS_EVERY == 0 {
            tracing::info!(
                done = stats.done(),
                total = stats.total,
                ok = stats.ok,
                not_found = stats.not_found,
                failed = stats.failed,
                skipped = stats.skipped,
                "download progress",
            );
        }
    }

    finalize_batch(stats, reasons, &missing_buckets, opts)
}

/// Fold one task outcome into the batch accumulators.
///
/// Keeping this in one place is what makes the probe and the streamed
/// files count identically: the two used to duplicate the same four-arm
/// match, and a new counter only ever reached one of them.
fn record_outcome(
    outcome: TaskOutcome,
    stats: &mut DownloadStats,
    reasons: &mut Vec<String>,
    missing_buckets: &mut Vec<MissingBucket>,
) {
    match outcome {
        TaskOutcome::Ok => stats.ok += 1,
        TaskOutcome::NotFound { bucket } => {
            stats.not_found += 1;
            missing_buckets.push(bucket);
        }
        TaskOutcome::Failed(err) => {
            stats.failed += 1;
            if reasons.len() < MAX_REPORTED_FAILURES {
                reasons.push(err.chain());
            } else {
                tracing::debug!(error = %err.chain(), "download failed (detail not retained)");
            }
        }
        TaskOutcome::Skipped => stats.skipped += 1,
    }
}

/// Turn the accumulated batch state into the final verdict.
///
/// The two "the requested range is not in the feed" checks live in
/// [`super::ratio::range_verdict`]; they need the complete batch, not a
/// single file, and they are the only reason a batch that downloaded
/// *something* can still fail.
fn finalize_batch(
    stats: DownloadStats,
    reasons: Vec<String>,
    missing_buckets: &[MissingBucket],
    opts: DownloadOptions,
) -> Result<DownloadStats, AppError> {
    if stats.not_found > 0 {
        tracing::debug!(
            not_found = stats.not_found,
            total = stats.total,
            "files not found on server (404)",
        );
    }
    if stats.circuit_open() {
        tracing::warn!(
            skipped = stats.skipped,
            failed = stats.failed,
            window = BREAKER_WINDOW,
            fail_ratio = format!("{BREAKER_FAIL_NUM}/{}", BREAKER_FAIL_DEN),
            "circuit breaker opened; remaining files for this pair were not attempted",
        );
    }
    if stats.failed > reasons.len() {
        tracing::warn!(
            hidden = stats.failed - reasons.len(),
            "additional failure details not retained",
        );
    }

    if stats.failed > 0 {
        return Err(AppError::BatchFailed {
            failed: stats.failed,
            total: stats.total,
            skipped: stats.skipped,
            reasons,
        });
    }

    range_verdict(stats, missing_buckets, opts)?;
    Ok(stats)
}

/// Run one download task, update the breaker, and classify the outcome.
///
/// A task that the breaker has already given up on is reported as
/// [`TaskOutcome::Skipped`] rather than as an error, so skipped files never
/// inflate the failure count.
async fn run_one_task<F, Fut>(
    client: &reqwest::Client,
    gate: &Arc<Semaphore>,
    breaker: &Breaker,
    download_fn: &F,
    task: DownloadTask,
) -> TaskOutcome
where
    F: Fn(reqwest::Client, DownloadTask) -> Fut,
    Fut: Future<Output = Result<(), AppError>>,
{
    let span = tracing::info_span!("download", symbol = %task.symbol, tf = %task.timeframe);
    async {
        if breaker.is_open() {
            return TaskOutcome::Skipped;
        }
        // `download_fn` consumes the task, but a 404 needs the bucket it
        // belonged to. Cloning just the destination is one allocation;
        // building the full key for every task — including the ones that
        // succeed — costs six.
        let dest = task.dest.clone();
        let timeframe = task.timeframe;
        // The gate is never closed, so acquiring can only fail on a bug.
        let _permit = gate
            .acquire()
            .await
            .expect("BUG: download gate is never closed");
        match download_fn(client.clone(), task).await {
            Ok(()) => {
                breaker.record(true);
                TaskOutcome::Ok
            }
            Err(AppError::NotFound { .. }) => {
                // A file the server does not have says nothing about whether
                // the pair is reachable, so it must not count as a failure.
                breaker.record(true);
                let bucket = bucket_key(&dest, timeframe);
                if bucket_position(&bucket.key, bucket.timeframe).is_none() {
                    // The destination does not mirror the server layout, so
                    // the missing-range analysis cannot place this file on
                    // the calendar. Say so instead of quietly mis-measuring
                    // the gap.
                    tracing::warn!(
                        bucket = %bucket.key,
                        timeframe = ?bucket.timeframe,
                        "the missing file's destination does not mirror the server layout; \
                         the missing-range analysis cannot place it on the calendar",
                    );
                }
                TaskOutcome::NotFound { bucket }
            }
            Err(err) => {
                breaker.record(false);
                TaskOutcome::Failed(err)
            }
        }
    }
    .instrument(span)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Symbol, Timeframe};
    use std::sync::atomic::AtomicUsize as Counter;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    fn task_named(symbol: &str, tag: &str) -> DownloadTask {
        DownloadTask {
            url: format!("http://example.com/{symbol}/{tag}"),
            dest: std::path::PathBuf::from("/tmp"),
            symbol: Symbol::new(symbol).unwrap(),
            timeframe: Timeframe::D1,
        }
    }

    /// A task with an explicit timeframe and destination path.
    fn task_for(timeframe: Timeframe, dest: &str, tag: &str) -> DownloadTask {
        DownloadTask {
            url: format!("http://example.com/EURUSD/{tag}"),
            dest: std::path::PathBuf::from(dest),
            symbol: Symbol::new("EURUSD").unwrap(),
            timeframe,
        }
    }

    /// `n` M1 tasks on consecutive days, starting 2025-01-01.
    ///
    /// Real calendar days matter here: the gap detection compares buckets by
    /// date, so an impossible day number would break the chain.
    fn daily_tasks(n: usize) -> Vec<DownloadTask> {
        use chrono::Datelike;
        let start = chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
        (0..n)
            .map(|i| {
                let day = start + chrono::Duration::days(i64::try_from(i).expect("n fits in i64"));
                task_for(
                    Timeframe::M1,
                    &format!(
                        "/tmp/dukascopy/EURUSD/{}/{:02}/{:02}/BID_candles_min_1.bi5",
                        day.year(),
                        day.month(),
                        day.day()
                    ),
                    &i.to_string(),
                )
            })
            .collect()
    }

    fn tasks(n: usize) -> Vec<DownloadTask> {
        (0..n)
            .map(|i| task_named("EURUSD", &i.to_string()))
            .collect()
    }

    fn client() -> reqwest::Client {
        reqwest::Client::new()
    }

    /// A genuine transport error.
    ///
    /// `reqwest::Error` cannot be constructed by hand, so the error is
    /// produced by actually connecting to a closed port — the refusal comes
    /// back immediately.
    async fn transport_error() -> AppError {
        reqwest::Client::new()
            .get("http://127.0.0.1:1/never")
            .send()
            .await
            .expect_err("connecting to a closed port must fail")
            .into()
    }

    fn gate(n: usize) -> Arc<Semaphore> {
        Arc::new(Semaphore::new(n))
    }

    fn boom() -> AppError {
        AppError::DownloadFailed {
            url: "u".into(),
            reason: "boom".into(),
        }
    }

    fn batch_failed(err: AppError) -> AppError {
        match err {
            AppError::BatchFailed { .. } => err,
            other => panic!("expected BatchFailed, got {other:?}"),
        }
    }

    /// Runs `f`, asserting it behaved like `always_ok`.
    async fn ok_with<F, Fut>(f: F) -> DownloadStats
    where
        F: Fn(reqwest::Client, DownloadTask) -> Fut,
        Fut: Future<Output = Result<(), AppError>>,
    {
        let tasks = tasks(1);
        run_downloads_inner(&client(), tasks, 16, &gate(16), f)
            .await
            .unwrap()
    }

    // — counting —

    #[tokio::test]
    async fn empty_tasks_return_zeroed_stats() {
        let stats = run_downloads_inner(
            &client(),
            vec![],
            16,
            &gate(16),
            |_: reqwest::Client, _: DownloadTask| async { unreachable!("no task runs") },
        )
        .await
        .unwrap();
        assert_eq!(stats, DownloadStats::default());
    }

    #[tokio::test]
    async fn all_success_counts_every_task() {
        let calls = Arc::new(Counter::new(0));
        let counter = calls.clone();

        let stats = run_downloads_inner(
            &client(),
            tasks(2),
            16,
            &gate(16),
            move |_: reqwest::Client, _: DownloadTask| {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            stats,
            DownloadStats {
                total: 2,
                ok: 2,
                not_found: 0,
                failed: 0,
                skipped: 0
            }
        );
    }

    #[tokio::test]
    async fn not_found_is_counted_separately_from_failures() {
        let stats = run_downloads_inner(
            &client(),
            tasks(2),
            16,
            &gate(16),
            |_: reqwest::Client, t: DownloadTask| async move {
                if t.url.ends_with('0') {
                    Err(AppError::NotFound { url: t.url })
                } else {
                    Ok(())
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(
            stats,
            DownloadStats {
                total: 2,
                ok: 1,
                not_found: 1,
                failed: 0,
                skipped: 0
            }
        );
    }

    /// A batch where every file is missing is an unavailable range, even when
    /// the batch is too small for the ratio check to be meaningful.
    #[tokio::test]
    async fn all_not_found_is_an_unavailable_range() {
        let err = run_downloads_inner(
            &client(),
            daily_tasks(2),
            16,
            &gate(16),
            |_: reqwest::Client, _: DownloadTask| async {
                Err(AppError::NotFound {
                    url: "http://x".into(),
                })
            },
        )
        .await
        .unwrap_err();

        match err {
            AppError::RangeUnavailable {
                not_found,
                total,
                longest_run,
            } => {
                assert_eq!(not_found, 2);
                assert_eq!(total, 2);
                assert_eq!(longest_run, 2, "the two missing days are consecutive");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // — failure reporting —

    /// A transport failure must keep its cause.
    ///
    /// `Display` alone says "http error: …" and never *why*; the cause
    /// only exists in the `source()` chain, so a summary built from
    /// `Display` would be useless for diagnosis.
    #[tokio::test]
    async fn a_transport_failure_keeps_its_cause() {
        let err = transport_error().await;
        let plain = err.to_string();
        let chain = err.chain();
        assert!(chain.starts_with("http error: "), "{chain}");
        assert!(
            chain.len() > plain.len(),
            "the cause must be appended: {chain} vs {plain}"
        );
    }

    #[tokio::test]
    async fn single_failure_reports_ratio_and_reason() {
        let err = batch_failed(
            run_downloads_inner(
                &client(),
                tasks(2),
                16,
                &gate(16),
                |_: reqwest::Client, t: DownloadTask| async move {
                    if t.url.ends_with('1') {
                        Err(AppError::DownloadFailed {
                            url: t.url,
                            reason: "HTTP 500".into(),
                        })
                    } else {
                        Ok(())
                    }
                },
            )
            .await
            .unwrap_err(),
        );

        let msg = err.to_string();
        assert!(msg.contains("1/2"), "{msg}");
        assert!(msg.contains("HTTP 500"), "{msg}");
    }

    #[tokio::test]
    async fn failure_count_excludes_not_found_and_successes() {
        let err = batch_failed(
            run_downloads_inner(
                &client(),
                tasks(3),
                16,
                &gate(16),
                |_: reqwest::Client, t: DownloadTask| async move {
                    match t.url.as_str() {
                        u if u.ends_with("/0") => Err(AppError::NotFound { url: t.url }),
                        u if u.ends_with("/1") => Err(AppError::DownloadFailed {
                            url: t.url,
                            reason: "bad".into(),
                        }),
                        _ => Ok(()),
                    }
                },
            )
            .await
            .unwrap_err(),
        );
        // 1 of 3 failed: the 404 is tolerated and the success is not a failure.
        assert!(err.to_string().contains("1/3"), "{err}");
    }

    #[tokio::test]
    async fn retained_reasons_are_capped_but_counts_stay_exact() {
        let n = MAX_REPORTED_FAILURES * 3;
        // All-fail keeps the trip point predictable: the window fills with
        // failures, so the retained sample is bounded while the counts stay
        // exact.
        let err = batch_failed(
            run_downloads_inner(
                &client(),
                tasks(n),
                1,
                &gate(1),
                |_: reqwest::Client, _: DownloadTask| async { Err(boom()) },
            )
            .await
            .unwrap_err(),
        );

        match err {
            AppError::BatchFailed {
                failed,
                total,
                skipped,
                reasons,
            } => {
                assert_eq!(total, n);
                assert_eq!(failed, BREAKER_WINDOW);
                assert_eq!(skipped, n - BREAKER_WINDOW);
                assert_eq!(
                    reasons.len(),
                    MAX_REPORTED_FAILURES,
                    "reasons must be bounded"
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    // — circuit breaker —

    /// A transport failure on the probe proves the pair is unreachable: the
    /// remaining files must not be attempted at all.
    ///
    /// This is what the probe is for — without it, a full wave of
    /// `concurrency` requests would each burn the whole retry budget to
    /// reach the verdict a single request already gives.
    #[tokio::test]
    async fn a_transport_failure_on_the_probe_skips_the_rest() {
        let attempts = Arc::new(Counter::new(0));
        let counter = attempts.clone();
        let err = batch_failed(
            run_downloads_inner(
                &client(),
                tasks(100),
                30,
                &gate(30),
                move |_: reqwest::Client, _: DownloadTask| {
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Err(transport_error().await)
                    }
                },
            )
            .await
            .unwrap_err(),
        );

        match err {
            AppError::BatchFailed {
                failed,
                total,
                skipped,
                ..
            } => {
                assert_eq!(failed, 1, "only the probe was attempted");
                assert_eq!(total, 100);
                assert_eq!(skipped, 99);
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "no request may be sent once the probe has proven the pair unreachable"
        );
    }

    /// An HTTP status failure is not proof of unreachability: the server may
    /// simply be busy. The probe must not give up on the pair in that case.
    #[tokio::test]
    async fn a_status_failure_on_the_probe_does_not_abandon_the_pair() {
        let attempts = Arc::new(Counter::new(0));
        let counter = attempts.clone();
        let err = batch_failed(
            run_downloads_inner(
                &client(),
                tasks(100),
                1,
                &gate(1),
                move |_: reqwest::Client, _: DownloadTask| {
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Err(boom())
                    }
                },
            )
            .await
            .unwrap_err(),
        );

        match err {
            AppError::BatchFailed { failed, .. } => {
                assert_eq!(failed, BREAKER_WINDOW, "the window fills before it trips");
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert!(
            attempts.load(Ordering::SeqCst) > 1,
            "the probe must not abandon the pair on a status failure"
        );
    }

    /// The breaker opens once the recent attempts look unhealthy: a full
    /// window of failures trips it and the remaining files are skipped.
    #[tokio::test]
    async fn the_breaker_opens_when_the_window_is_all_failures() {
        let err = batch_failed(
            run_downloads_inner(
                &client(),
                tasks(200),
                1,
                &gate(1),
                |_: reqwest::Client, _: DownloadTask| async { Err(boom()) },
            )
            .await
            .unwrap_err(),
        );

        match err {
            AppError::BatchFailed {
                failed,
                total,
                skipped,
                ..
            } => {
                assert_eq!(total, 200);
                assert_eq!(failed, BREAKER_WINDOW, "the window fills before it trips");
                assert_eq!(skipped, 200 - BREAKER_WINDOW);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// A 50% failure rate trips the breaker. A consecutive-failure counter
    /// never could: every success reset it.
    #[tokio::test]
    async fn interleaved_failures_trip_the_breaker() {
        let attempts = Arc::new(Counter::new(0));
        let counter = attempts.clone();
        let err = batch_failed(
            run_downloads_inner(
                &client(),
                tasks(300),
                1,
                &gate(1),
                move |_: reqwest::Client, t: DownloadTask| {
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        let i: usize = t.url.rsplit('/').next().unwrap().parse().unwrap();
                        if i % 2 == 0 { Err(boom()) } else { Ok(()) }
                    }
                },
            )
            .await
            .unwrap_err(),
        );

        match err {
            AppError::BatchFailed {
                failed,
                total,
                skipped,
                ..
            } => {
                assert_eq!(total, 300);
                assert!(skipped > 0, "a 50% failure rate must trip the breaker");
                assert!(failed > 0);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// A low failure rate keeps the breaker closed: an occasional hiccup must
    /// not abandon a whole pair.
    #[tokio::test]
    async fn a_low_failure_rate_keeps_the_breaker_closed() {
        let err = batch_failed(
            run_downloads_inner(
                &client(),
                tasks(100),
                1,
                &gate(1),
                |_: reqwest::Client, t: DownloadTask| async move {
                    let i: usize = t.url.rsplit('/').next().unwrap().parse().unwrap();
                    if i % 10 == 0 { Err(boom()) } else { Ok(()) }
                },
            )
            .await
            .unwrap_err(),
        );

        match err {
            AppError::BatchFailed {
                failed,
                total,
                skipped,
                ..
            } => {
                assert_eq!(total, 100);
                assert_eq!(failed, 10);
                assert_eq!(skipped, 0, "a 10% failure rate must not trip the breaker");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn skipped_files_are_never_counted_as_failures() {
        // The window fills with failures; the remaining files must land in
        // `skipped`, never in `failed`.
        let err = batch_failed(
            run_downloads_inner(
                &client(),
                tasks(200),
                1,
                &gate(1),
                |_: reqwest::Client, _: DownloadTask| async { Err(boom()) },
            )
            .await
            .unwrap_err(),
        );

        match err {
            AppError::BatchFailed {
                failed,
                total,
                skipped,
                ..
            } => {
                assert_eq!(failed, BREAKER_WINDOW);
                assert_eq!(skipped, total - BREAKER_WINDOW);
                assert_ne!(failed, total, "skipped must not inflate the failure count");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn not_found_does_not_trip_the_breaker() {
        let stats = run_downloads_inner(
            &client(),
            tasks(40),
            1,
            &gate(1),
            |_: reqwest::Client, t: DownloadTask| async move {
                let i: usize = t.url.rsplit('/').next().unwrap().parse().unwrap();
                if i % 20 == 0 {
                    Err(AppError::NotFound { url: t.url })
                } else {
                    Ok(())
                }
            },
        )
        .await
        .unwrap();

        // 404s are expected answers, not failures: the breaker stays shut and
        // every file is attempted.
        assert_eq!(stats.not_found, 2);
        assert_eq!(stats.ok, 38);
        assert_eq!(stats.skipped, 0);
    }

    // — missing-range verdicts —

    /// A long stretch of consecutive missing buckets points outside the
    /// feed's coverage even when the overall ratio is not extreme.
    #[tokio::test]
    async fn a_long_consecutive_gap_fails_even_at_a_modest_ratio() {
        let err = run_downloads_inner(
            &client(),
            daily_tasks(20),
            16,
            &gate(16),
            |_: reqwest::Client, t: DownloadTask| async move {
                let i: usize = t.url.rsplit('/').next().unwrap().parse().unwrap();
                if i < 10 {
                    Err(AppError::NotFound { url: t.url })
                } else {
                    Ok(())
                }
            },
        )
        .await
        .unwrap_err();

        match err {
            AppError::RangeUnavailable {
                not_found,
                total,
                longest_run,
            } => {
                assert_eq!(not_found, 10);
                assert_eq!(total, 20);
                assert_eq!(longest_run, 10, "the ten missing days are consecutive");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// The feed publishes a file for every calendar day, so a large share of
    /// 404s means the range lies outside its coverage. Reporting that as a
    /// successful run would be actively misleading.
    #[tokio::test]
    async fn a_mostly_missing_range_is_reported_as_an_error() {
        let err = run_downloads_inner(
            &client(),
            daily_tasks(20),
            16,
            &gate(16),
            |_: reqwest::Client, _: DownloadTask| async {
                Err(AppError::NotFound { url: "u".into() })
            },
        )
        .await
        .unwrap_err();

        match err {
            AppError::RangeUnavailable {
                not_found,
                total,
                longest_run,
            } => {
                assert_eq!(not_found, 20);
                assert_eq!(total, 20);
                assert_eq!(longest_run, 20);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    /// A small share of 404s is tolerated.
    #[tokio::test]
    async fn a_small_share_of_missing_files_is_tolerated() {
        let stats = run_downloads_inner(
            &client(),
            daily_tasks(100),
            16,
            &gate(16),
            |_: reqwest::Client, t: DownloadTask| async move {
                let i: usize = t.url.rsplit('/').next().unwrap().parse().unwrap();
                if i % 20 == 0 {
                    Err(AppError::NotFound { url: t.url })
                } else {
                    Ok(())
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(stats.not_found, 5);
        assert_eq!(stats.ok, 95);
        assert_eq!(stats.failed, 0);
    }

    /// A tiny batch that downloaded nothing is still an unavailable range:
    /// the minimum sample size only gates the *ratio* check, not the
    /// unambiguous "nothing was downloaded" verdict.
    #[tokio::test]
    async fn a_tiny_batch_of_missing_files_is_still_an_error() {
        let err = run_downloads_inner(
            &client(),
            daily_tasks(3),
            16,
            &gate(16),
            |_: reqwest::Client, _: DownloadTask| async {
                Err(AppError::NotFound {
                    url: "http://x".into(),
                })
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(err, AppError::RangeUnavailable { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn all_ok_shortcut_helper_reports_success() {
        let stats = ok_with(|_: reqwest::Client, _: DownloadTask| async { Ok(()) }).await;
        assert_eq!(stats.ok, 1);
        assert_eq!(stats.total, 1);
    }

    // — the "allow missing" opt-out —

    /// A range the feed does not have fails the batch by default.
    #[tokio::test]
    async fn an_unavailable_range_fails_by_default() {
        let err = run_downloads_inner_with(
            &client(),
            daily_tasks(20),
            16,
            &gate(16),
            DownloadOptions::default(),
            |_: reqwest::Client, t: DownloadTask| async move {
                let i: usize = t.url.rsplit('/').next().unwrap().parse().unwrap();
                if i < 10 {
                    Err(AppError::NotFound { url: t.url })
                } else {
                    Ok(())
                }
            },
        )
        .await
        .unwrap_err();

        assert!(matches!(err, AppError::RangeUnavailable { .. }), "{err:?}");
    }

    /// With `allow_missing` the same batch succeeds and still reports the
    /// 404s — the opt-out relaxes the *verdict*, not the bookkeeping.
    #[tokio::test]
    async fn allow_missing_turns_the_unavailable_range_into_a_warning() {
        let stats = run_downloads_inner_with(
            &client(),
            daily_tasks(20),
            16,
            &gate(16),
            DownloadOptions {
                allow_missing: true,
            },
            |_: reqwest::Client, t: DownloadTask| async move {
                let i: usize = t.url.rsplit('/').next().unwrap().parse().unwrap();
                if i < 10 {
                    Err(AppError::NotFound { url: t.url })
                } else {
                    Ok(())
                }
            },
        )
        .await
        .expect("the range check is relaxed, the batch must succeed");

        assert_eq!(stats.not_found, 10);
        assert_eq!(stats.ok, 10);
        assert_eq!(stats.failed, 0);
    }

    /// Even an all-missing batch succeeds when the opt-out is set: the
    /// verdict is relaxed, the bookkeeping stays exact.
    #[tokio::test]
    async fn allow_missing_turns_an_all_missing_batch_into_a_warning() {
        let stats = run_downloads_inner_with(
            &client(),
            daily_tasks(3),
            16,
            &gate(16),
            DownloadOptions {
                allow_missing: true,
            },
            |_: reqwest::Client, _: DownloadTask| async {
                Err(AppError::NotFound {
                    url: "http://x".into(),
                })
            },
        )
        .await
        .expect("the range check is relaxed, the batch must succeed");

        assert_eq!(stats.total, 3);
        assert_eq!(stats.not_found, 3);
        assert_eq!(stats.ok, 0);
    }

    /// The opt-out covers the *range* checks only. A file that actually
    /// failed to download still fails the batch.
    #[tokio::test]
    async fn allow_missing_does_not_hide_a_real_download_failure() {
        let err = run_downloads_inner_with(
            &client(),
            daily_tasks(20),
            16,
            &gate(16),
            DownloadOptions {
                allow_missing: true,
            },
            |_: reqwest::Client, _: DownloadTask| async { Err(boom()) },
        )
        .await
        .unwrap_err();

        assert!(matches!(err, AppError::BatchFailed { .. }), "{err:?}");
    }

    // — global gate —

    #[tokio::test]
    async fn gate_bounds_global_in_flight_across_pairs() {
        let gate = gate(3);
        let active = Arc::new(Counter::new(0));
        let peak = Arc::new(Counter::new(0));

        let mut pairs = Vec::new();
        for _ in 0..4 {
            let gate = gate.clone();
            let active = active.clone();
            let peak = peak.clone();
            pairs.push(tokio::spawn(async move {
                run_downloads_inner(
                    &reqwest::Client::new(),
                    tasks(10),
                    16, // per-pair bound is far above the gate
                    &gate,
                    move |_: reqwest::Client, _: DownloadTask| {
                        let active = active.clone();
                        let peak = peak.clone();
                        async move {
                            let cur = active.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(cur, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(5)).await;
                            active.fetch_sub(1, Ordering::SeqCst);
                            Ok(())
                        }
                    },
                )
                .await
            }));
        }
        for p in pairs {
            p.await.unwrap().unwrap();
        }

        assert_eq!(
            peak.load(Ordering::SeqCst),
            3,
            "4 concurrent pairs must still never exceed the shared gate"
        );
    }

    #[tokio::test]
    async fn concurrency_one_runs_sequentially() {
        let active = Arc::new(Counter::new(0));
        let peak = Arc::new(Counter::new(0));
        let in_flight = active.clone();
        let p = peak.clone();

        let stats = run_downloads_inner(
            &client(),
            tasks(5),
            1,
            &gate(16),
            move |_: reqwest::Client, _: DownloadTask| {
                let in_flight = in_flight.clone();
                let p = p.clone();
                async move {
                    let cur = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    p.fetch_max(cur, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(stats.ok, 5);
        assert_eq!(peak.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn zero_concurrency_still_makes_progress() {
        let stats = run_downloads_inner(
            &client(),
            tasks(1),
            0,
            &gate(1),
            |_: reqwest::Client, _: DownloadTask| async { Ok(()) },
        )
        .await
        .unwrap();
        assert_eq!(stats.ok, 1);
    }

    // — breaker semantics under concurrency —

    /// Under concurrency the breaker still works: it is the *window* that
    /// decides, not the order in which the failures happen.
    #[tokio::test]
    async fn the_breaker_opens_under_concurrency_too() {
        let err = batch_failed(
            run_downloads_inner(
                &client(),
                tasks(200),
                30,
                &gate(30),
                |_: reqwest::Client, _: DownloadTask| async {
                    // A real download awaits the network; without an await
                    // point the future is Ready on first poll and the whole
                    // batch degenerates into a serial loop.
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    Err(boom())
                },
            )
            .await
            .unwrap_err(),
        );

        match err {
            AppError::BatchFailed {
                failed,
                total,
                skipped,
                ..
            } => {
                assert_eq!(total, 200);
                assert!(
                    failed >= BREAKER_WINDOW,
                    "the window must fill before it trips: {failed}"
                );
                assert!(skipped > 0, "the breaker must stop the remaining files");
                assert!(failed < total, "not every file may be attempted");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }
}
