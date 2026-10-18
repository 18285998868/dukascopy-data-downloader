//! Stage 3: bounded-concurrency download orchestration.
//!
//! Every pair in the run shares one [`Semaphore`] gate, so `concurrency`
//! bounds downloads **across the whole run** rather than per pair: with 15
//! pairs downloading simultaneously, at most `concurrency` files are ever in
//! flight. Tasks are streamed through [`StreamExt::buffer_unordered`] and
//! folded into [`DownloadStats`] incrementally.

pub mod stream;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Semaphore;
use tracing::Instrument;

use crate::AppError;
use crate::config::DownloadTask;
use futures::StreamExt;

/// How often a line of progress is logged, in completed files.
const PROGRESS_EVERY: usize = 100;
/// Maximum number of individual failure details retained in the batch summary.
const MAX_REPORTED_FAILURES: usize = 20;
/// Consecutive failures after which a pair is abandoned for this run.
///
/// An unreachable symbol would otherwise burn the full retry budget on every
/// single file. Tripping this stops the waste and lets the remaining pairs
/// finish. A success — or a 404, which is normal on market holidays — resets
/// the streak.
pub const MAX_CONSECUTIVE_FAILURES: usize = 5;

/// Outcome of one task inside a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskOutcome {
    /// Attempted and accounted for.
    Done,
    /// Never attempted: the circuit breaker was already open.
    Skipped,
}

/// Aggregate statistics for a completed batch of downloads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DownloadStats {
    /// Number of tasks in the batch.
    pub total: usize,
    /// Tasks written to disk successfully.
    pub ok: usize,
    /// Tasks answered with HTTP 404 (expected on market-holiday edge dates).
    pub not_found: usize,
    /// Tasks that failed for any other reason.
    pub failed: usize,
    /// Tasks never attempted because the circuit breaker opened.
    pub skipped: usize,
}

impl DownloadStats {
    /// Number of tasks whose outcome has been accounted for.
    const fn done(&self) -> usize {
        self.ok + self.not_found + self.failed + self.skipped
    }

    /// Whether the circuit breaker tripped during this batch.
    const fn circuit_open(&self) -> bool {
        self.skipped > 0
    }
}

/// Download all tasks, bounded by the shared `gate`.
///
/// # Errors
///
/// Returns [`AppError::BatchFailed`] carrying a bounded sample of per-file
/// causes when any task fails; a task answered with 404 is counted in
/// [`DownloadStats::not_found`] and does **not** fail the batch.
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

/// Core download orchestration, parameterized by the download function for testing.
///
/// Streams tasks through `buffer_unordered(concurrency)`; each task additionally
/// holds a permit from the shared `gate`, so the effective ceiling is
/// `min(concurrency, gate permits)` no matter how many pairs run at once.
///
/// The closure reports [`TaskOutcome::Skipped`] instead of an error when the
/// circuit breaker is open, so skipped files never inflate `failed`.
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
    let mut reasons: Vec<String> = Vec::new();
    let streak = Arc::new(AtomicUsize::new(0));

    // The per-task async block is constructed once per element, so `download_fn`
    // is captured by reference; capturing it by value would move it out of the
    // `FnMut` `map` closure on the first element.
    let download_fn = &download_fn;
    let mut stream = futures::stream::iter(tasks)
        .map(|t| {
            let c = client.clone();
            let gate = gate.clone();
            let streak = streak.clone();
            let span = tracing::info_span!("download", symbol = %t.symbol, tf = %t.timeframe);
            async move {
                if streak.load(Ordering::Relaxed) >= MAX_CONSECUTIVE_FAILURES {
                    return Ok(TaskOutcome::Skipped);
                }
                // The gate is never closed, so acquiring can only fail on a bug.
                let _permit = gate.acquire().await.expect("download gate is never closed");
                let result = download_fn(c, t).await;
                match &result {
                    // A success or an expected 404 clears the failure streak.
                    Ok(()) | Err(AppError::NotFound { .. }) => {
                        streak.store(0, Ordering::Relaxed);
                    }
                    Err(_) => {
                        streak.fetch_add(1, Ordering::Relaxed);
                    }
                }
                result.map(|()| TaskOutcome::Done)
            }
            .instrument(span)
        })
        .buffer_unordered(concurrency);

    while let Some(result) = stream.next().await {
        match result {
            Ok(TaskOutcome::Done) => stats.ok += 1,
            Ok(TaskOutcome::Skipped) => stats.skipped += 1,
            Err(AppError::NotFound { .. }) => stats.not_found += 1,
            Err(e) => {
                stats.failed += 1;
                if reasons.len() < MAX_REPORTED_FAILURES {
                    reasons.push(e.chain());
                } else {
                    tracing::debug!(error = %e, "download failed (detail not retained)");
                }
            }
        }

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

    if stats.not_found > 0 {
        tracing::debug!(
            not_found = stats.not_found,
            total = stats.total,
            "files not found on server (404, expected for edge dates)",
        );
    }
    if stats.circuit_open() {
        tracing::warn!(
            skipped = stats.skipped,
            failed = stats.failed,
            max_consecutive = MAX_CONSECUTIVE_FAILURES,
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
    if stats.ok == 0 && stats.not_found == stats.total {
        tracing::warn!(
            not_found = stats.not_found,
            total = stats.total,
            "all requested files not found; continuing",
        );
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Symbol, Timeframe};
    use std::sync::atomic::AtomicUsize as Counter;
    use std::time::Duration;

    fn task_named(symbol: &str, tag: &str) -> DownloadTask {
        DownloadTask {
            url: format!("http://example.com/{symbol}/{tag}"),
            dest: std::path::PathBuf::from("/tmp"),
            symbol: Symbol::new(symbol).unwrap(),
            timeframe: Timeframe::D1,
        }
    }

    fn tasks(n: usize) -> Vec<DownloadTask> {
        (0..n)
            .map(|i| task_named("EURUSD", &i.to_string()))
            .collect()
    }

    fn client() -> reqwest::Client {
        reqwest::Client::new()
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

    #[tokio::test]
    async fn all_not_found_is_not_an_error() {
        let stats = run_downloads_inner(
            &client(),
            tasks(2),
            16,
            &gate(16),
            |_: reqwest::Client, _: DownloadTask| async {
                Err(AppError::NotFound {
                    url: "http://x".into(),
                })
            },
        )
        .await
        .unwrap();

        assert_eq!(
            stats,
            DownloadStats {
                total: 2,
                ok: 0,
                not_found: 2,
                failed: 0,
                skipped: 0
            }
        );
    }

    // — failure reporting —

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
        // Alternating fail/succeed keeps the circuit breaker shut, so this
        // isolates the reason cap from the breaker.
        let err = batch_failed(
            run_downloads_inner(
                &client(),
                tasks(n),
                1,
                &gate(1),
                |_: reqwest::Client, t: DownloadTask| async move {
                    let i: usize = t.url.rsplit('/').next().unwrap().parse().unwrap();
                    if i % 2 == 0 { Err(boom()) } else { Ok(()) }
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
                reasons,
            } => {
                assert_eq!(total, n);
                assert_eq!(failed, n / 2);
                assert_eq!(skipped, 0, "the breaker must stay shut here");
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

    #[tokio::test]
    async fn breaker_opens_after_consecutive_failures_and_skips_the_rest() {
        let attempts = Arc::new(Counter::new(0));
        let counter = attempts.clone();

        let err = batch_failed(
            run_downloads_inner(
                &client(),
                tasks(20),
                1, // serial, so failures really are consecutive
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

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            MAX_CONSECUTIVE_FAILURES,
            "must stop attempting once the streak limit is reached"
        );
        match err {
            AppError::BatchFailed {
                failed,
                total,
                skipped,
                ..
            } => {
                assert_eq!(failed, MAX_CONSECUTIVE_FAILURES);
                assert_eq!(total, 20);
                assert_eq!(skipped, 20 - MAX_CONSECUTIVE_FAILURES);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn skipped_files_are_never_counted_as_failures() {
        // 5 failures trip the breaker; the remaining 15 must land in `skipped`.
        let stats = run_downloads_inner(
            &client(),
            tasks(20),
            1,
            &gate(1),
            |_: reqwest::Client, _: DownloadTask| async { Err(boom()) },
        )
        .await;
        let stats = stats.unwrap_err();
        match stats {
            AppError::BatchFailed {
                failed, skipped, ..
            } => {
                assert_eq!(failed, MAX_CONSECUTIVE_FAILURES);
                assert_eq!(skipped, 15);
                assert_ne!(failed, 20, "skipped must not inflate the failure count");
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_success_resets_the_failure_streak() {
        // Fail 4, succeed 1, repeat: the streak never reaches 5, so nothing is
        // skipped even though 24 of 30 tasks fail.
        let stats = run_downloads_inner(
            &client(),
            tasks(30),
            1,
            &gate(1),
            |_: reqwest::Client, t: DownloadTask| async move {
                let n: usize = t.url.rsplit('/').next().unwrap().parse().unwrap();
                if n % 5 == 4 { Ok(()) } else { Err(boom()) }
            },
        )
        .await;

        match batch_failed(stats.unwrap_err()) {
            AppError::BatchFailed {
                failed, skipped, ..
            } => {
                assert_eq!(failed, 24);
                assert_eq!(
                    skipped, 0,
                    "a reset streak must prevent the breaker opening"
                );
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
            |_: reqwest::Client, _: DownloadTask| async {
                Err(AppError::NotFound { url: "u".into() })
            },
        )
        .await
        .unwrap();

        assert_eq!(stats.not_found, 40);
        assert_eq!(stats.skipped, 0, "404s are expected on holidays");
        assert!(!stats.circuit_open());
    }

    #[tokio::test]
    async fn all_ok_shortcut_helper_reports_success() {
        let stats = ok_with(|_: reqwest::Client, _: DownloadTask| async { Ok(()) }).await;
        assert_eq!(stats.ok, 1);
        assert_eq!(stats.total, 1);
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
}
