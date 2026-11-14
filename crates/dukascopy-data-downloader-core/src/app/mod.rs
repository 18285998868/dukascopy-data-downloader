//! Application orchestration for the download pipeline.
//!
//! The pipeline runs in numbered stages that mirror the data flow:
//!
//! 1. [`build_urls`] — expand one symbol/timeframe pair into server URLs.
//! 2. [`build_download_tasks`] — drop locally cached files and bind destinations.
//! 3. [`download`] — stream the remaining tasks with bounded concurrency.
//!
//! All symbol x timeframe pairs run through a [`tokio::task::JoinSet`], at
//! most `MAX_CONCURRENT_PAIRS` at a time, and they share a single download
//! gate, so `concurrency` bounds in-flight requests across the whole run. A
//! failing pair is recorded and does not abort the others.

pub mod build_download_tasks;
pub mod build_urls;
pub mod download;

mod cache;
mod client;
mod lock;
mod pair;
mod signal;

pub use cache::scan_local_files;

use std::path::Path;
use std::sync::Arc;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::AppError;
use crate::config::AppConfig;
use client::build_client;
use download::{DownloadGate, DownloadOptions};
use pair::{SharedRun, run_pair};
use signal::termination_signal;

/// Maximum number of pair-level error details retained in the batch summary.
const MAX_BATCH_ERRORS: usize = 20;
/// Maximum number of symbol x timeframe pairs in flight at once.
///
/// The shared download gate already bounds in-flight requests; this bound
/// keeps a large symbol x timeframe matrix from allocating one task and one
/// task list per pair up front. Completed pairs release their slot while the
/// remaining pairs are still being scheduled.
const MAX_CONCURRENT_PAIRS: usize = 32;

/// Run the full batch download pipeline.
///
/// 1. Load config
/// 2. Scan existing local files (once)
/// 3. Run every symbol x timeframe pair concurrently through the shared gate
///
/// The run is interruptible: a `SIGINT` (Ctrl+C) or a `SIGTERM` cancels it,
/// releases the run lock and makes the program exit with
/// [`AppError::Interrupted`].
///
/// # Errors
///
/// Returns [`AppError`] if configuration loading, client construction, scanning,
/// or the resolved date range fails, if any pair failed, or if the run was
/// interrupted.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> Result<(), ddd::AppError> {
/// ddd::app::run(std::path::Path::new("config.toml")).await
/// # }
/// ```
pub async fn run(config_path: &Path) -> Result<(), AppError> {
    run_with(config_path, &DownloadOptions::default()).await
}

/// Run the full batch download pipeline with explicit options.
///
/// This is [`run`] plus the options that decide what happens to a range
/// the feed does not have; see [`DownloadOptions::allow_missing`].
///
/// # Errors
///
/// Returns [`AppError`] if configuration loading, client construction, scanning,
/// or the resolved date range fails, if any pair failed, or if the run was
/// interrupted.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> Result<(), ddd::AppError> {
/// // Strict: a range the feed does not have fails the run.
/// ddd::app::run_with(
///     std::path::Path::new("config.toml"),
///     &ddd::DownloadOptions::default(),
/// )
/// .await?;
///
/// // Best effort: keep whatever the feed did have.
/// ddd::app::run_with(
///     std::path::Path::new("config.toml"),
///     &ddd::DownloadOptions::new().with_allow_missing(true),
/// )
/// .await
/// # }
/// ```
pub async fn run_with(config_path: &Path, opts: &DownloadOptions) -> Result<(), AppError> {
    run_with_today(config_path, opts, chrono::Utc::now().date_naive()).await
}

/// [`run_with`] with an explicit current date.
///
/// `today` is injected so tests can pin the boundary between finished and
/// unfinished buckets; production passes the current UTC date.
async fn run_with_today(
    config_path: &Path,
    opts: &DownloadOptions,
    today: chrono::NaiveDate,
) -> Result<(), AppError> {
    // Install the OS handlers for the termination signals *first*: the
    // handler has to be in place before the run lock is taken. Otherwise a
    // Ctrl+C during start-up would kill the process with the default
    // disposition — no unwinding, no `Drop` — and leave the lock behind.
    let termination = termination_signal();

    // Reading the file is blocking I/O; `spawn_blocking` keeps it off the
    // async worker threads. The file is tiny, so the scheduling cost is in
    // the same order as the read itself — but the worker never stalls.
    let config_path = config_path.to_path_buf();
    let cfg = tokio::task::spawn_blocking(move || AppConfig::load(&config_path))
        .await
        .map_err(AppError::from)??;

    // Take the exclusive lock on the output directory. Two instances sharing
    // one `output_dir` would delete each other's scratch files.
    let output_dir = cfg.output_dir().to_path_buf();
    let lock = tokio::task::spawn_blocking(move || lock::acquire_run_lock(&output_dir))
        .await
        .map_err(AppError::from)??;

    // The lock guard is owned by the *pipeline future*, so dropping that
    // future — which is exactly what an interruption does — releases the
    // lock. The `spawn_blocking` task that creates it cannot be cancelled: if
    // the future is dropped first, the task still finishes and its `RunLock`
    // is dropped immediately, so no lock is leaked either way.
    tokio::select! {
        biased;
        result = run_pipeline(cfg, opts, lock, today) => result,
        signal = termination => Err(AppError::Interrupted { signal }),
    }
}

/// The download pipeline proper.
///
/// Owns the run lock for its whole lifetime: dropping this future — which
/// is what an interruption does — releases the lock, so the next run can
/// start without a manual cleanup.
async fn run_pipeline(
    cfg: AppConfig,
    opts: &DownloadOptions,
    _lock: lock::RunLock,
    today: chrono::NaiveDate,
) -> Result<(), AppError> {
    // Resolve the range *before* any I/O: it is pure arithmetic, so a
    // misconfigured range costs nothing instead of a full disk scan.
    let (start, end) = cfg.effective_range(today)?;

    // A reversed range means every pair would be skipped anyway, so bail out
    // before the disk is even scanned.
    if start > end {
        tracing::warn!(
            start = %start,
            end = %end,
            "start date is after end date, nothing to download",
        );
        return Ok(());
    }

    // Building the client may touch the platform trust store, which is
    // blocking file I/O — keep it off the worker threads too.
    let client = tokio::task::spawn_blocking(build_client)
        .await
        .map_err(AppError::from)??;

    let local = cache::scan_local_files(cfg.output_dir()).await?;
    let concurrency = cfg.concurrency();

    tracing::info!(
        symbols = cfg.symbols().len(),
        timeframes = cfg.timeframes().len(),
        pairs = cfg.symbols().len() * cfg.timeframes().len(),
        concurrency = concurrency.get(),
        start = %start,
        end = %end,
        "starting dukascopy batch download",
    );

    let cfg = Arc::new(cfg);
    let shared = SharedRun {
        client,
        cfg,
        // One gate for the whole run: `concurrency` is a global in-flight ceiling.
        gate: DownloadGate::new(concurrency),
        local: Arc::new(local),
        start,
        end,
        today,
        opts: *opts,
    };

    let mut pairs = JoinSet::new();
    let pair_limit = Arc::new(Semaphore::new(MAX_CONCURRENT_PAIRS));
    // Borrow the config through its `Arc` instead of cloning the vectors out
    // of it: `Vec<Symbol>` is a `Vec<String>`, so cloning it would allocate
    // one `String` per symbol for no reason.
    let cfg = Arc::clone(&shared.cfg);
    'schedule: for symbol in cfg.symbols() {
        for timeframe in cfg.timeframes() {
            let symbol = symbol.clone();
            let timeframe = *timeframe;
            let shared = shared.clone();
            let Ok(permit) = Arc::clone(&pair_limit).acquire_owned().await else {
                // The semaphore is never closed; treat an unexpected closure
                // as "stop scheduling" instead of panicking.
                break 'schedule;
            };
            pairs.spawn(async move {
                let _permit = permit;
                run_pair(shared, symbol, timeframe).await
            });
        }
    }

    let mut failed_pairs = 0;
    let mut errors: Vec<String> = Vec::with_capacity(MAX_BATCH_ERRORS);

    while let Some(joined) = pairs.join_next().await {
        let (symbol, timeframe, result) = match joined {
            Ok(v) => v,
            Err(e) => {
                // Covers panics and cancellation alike; either way this pair is lost.
                failed_pairs += 1;
                let detail = describe_join_error(e);
                tracing::error!(is_panic = detail.is_panic, error = %detail.message, "pair task did not complete");
                if errors.len() < MAX_BATCH_ERRORS {
                    errors.push(format!("task error: {}", detail.message));
                }
                continue;
            }
        };

        if let Err(e) = result {
            failed_pairs += 1;
            tracing::error!(
                symbol = %symbol,
                timeframe = %timeframe,
                error = e.chain(),
                "pair failed",
            );
            if errors.len() < MAX_BATCH_ERRORS {
                errors.push(format!("{symbol}/{timeframe}: {}", e.chain()));
            }
        }
    }

    if failed_pairs == 0 {
        tracing::info!("batch download complete");
        return Ok(());
    }

    tracing::error!(count = failed_pairs, "batch download completed with errors");
    let mut reasons = errors;
    if reasons.len() < failed_pairs {
        reasons.push(format!(
            "{} additional failure(s) not listed",
            failed_pairs - reasons.len()
        ));
    }
    Err(AppError::PairsFailed {
        failed: failed_pairs,
        reasons,
    })
}

/// A [`tokio::task::JoinError`] plus the panic payload it may carry.
struct JoinFailure {
    /// Whether the task died from a panic.
    is_panic: bool,
    /// Human-readable cause, including the panic message when there is one.
    message: String,
}

/// Render a [`tokio::task::JoinError`], keeping the panic payload when there is one.
///
/// [`tokio::task::JoinError`]'s `Display` only says *that* a task panicked, never *why*:
/// the message lives in the panic payload, which `Display` drops. Recovering
/// it here is the difference between "task 7 panicked" and a usable bug
/// report — the panic payload is the only place the original message exists
/// once the task frame is gone.
fn describe_join_error(err: tokio::task::JoinError) -> JoinFailure {
    if !err.is_panic() {
        return JoinFailure {
            is_panic: false,
            message: err.to_string(),
        };
    }
    let summary = err.to_string();
    let payload = err.into_panic();
    let msg = payload
        .downcast_ref::<&'static str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string());
    JoinFailure {
        is_panic: true,
        message: format!("{summary} (panic: {msg})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// A fixed "current day" so bucket-boundary tests never depend on the
    /// wall clock.
    fn today() -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(2025, 6, 15).expect("valid fixed date")
    }

    /// Write a config whose `output_dir` is the temp dir, and pre-create the
    /// cache file `rel` inside it.
    fn config_with_cache(extra: &str, rel: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.toml");
        std::fs::write(
            &path,
            format!("{extra}\noutput_dir = {:?}\n", dir.path().to_string_lossy()),
        )
        .unwrap();
        let cached = dir.path().join(rel);
        std::fs::create_dir_all(cached.parent().unwrap()).unwrap();
        std::fs::write(&cached, b"bi5").unwrap();
        (dir, path)
    }

    /// Write a config into a fresh temp directory and point `output_dir` at
    /// it.
    ///
    /// Every test needs its own output directory: the run lock is taken per
    /// directory, so tests sharing one would refuse to start.
    fn config(body: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.toml");
        std::fs::write(
            &path,
            format!("{body}\noutput_dir = {:?}\n", dir.path().to_string_lossy()),
        )
        .unwrap();
        (dir, path)
    }

    /// A second run on the same output directory must be refused before it
    /// can unlink the first run's scratch files.
    ///
    /// The lock lives in the *pipeline*, so this also pins down that the
    /// refusal surfaces through the public [`run`] entry point instead of
    /// being swallowed by the signal select.
    #[tokio::test]
    async fn run_refuses_a_second_instance_on_the_same_output_dir() {
        let (dir, path) = config(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2025-01-01\"\nend = \"2025-01-01\"\n",
        );
        let lock = lock::acquire_run_lock(dir.path()).unwrap();

        let err = run(&path).await.unwrap_err();
        assert!(err.to_string().contains("another instance"), "{err}");

        drop(lock);
    }

    #[tokio::test]
    async fn run_rejects_a_missing_config_file() {
        let err = run(Path::new("/nonexistent/ddd-config.toml"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("ddd-config.toml"), "{err}");
    }

    #[tokio::test]
    async fn run_rejects_an_oversized_effective_range_before_any_request() {
        let (_dir, path) =
            config("symbol = [\"EURUSD\"]\ntimeframe = [\"M1\"]\nstart = \"1900-01-01\"\n");
        let err = run(&path).await.unwrap_err();
        assert!(err.to_string().contains("exceeding maximum"), "{err}");
    }

    #[tokio::test]
    async fn run_rejects_an_invalid_symbol_before_any_request() {
        let (_dir, path) = config("symbol = [\"../etc\"]\ntimeframe = [\"D1\"]\n");
        let err = run(&path).await.unwrap_err();
        assert!(err.to_string().contains("symbol"), "{err}");
    }

    /// A fully cached pair must issue no requests and still succeed.
    #[tokio::test]
    async fn run_succeeds_when_everything_is_cached() {
        let (_dir, path) = config_with_cache(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2025-01-01\"\nend = \"2025-01-01\"",
            "dukascopy/EURUSD/2025/BID_candles_day_1.bi5",
        );
        run_with_today(&path, &DownloadOptions::default(), today())
            .await
            .unwrap();
    }

    /// A start date beyond `today` yields a reversed effective range; the pair
    /// must be skipped locally rather than requested.
    #[tokio::test]
    async fn run_skips_a_reversed_range_without_touching_the_network() {
        let (_dir, path) = config(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2999-01-01\"\nend = \"2999-12-31\"\n",
        );
        run_with_today(&path, &DownloadOptions::default(), today())
            .await
            .unwrap();
    }

    /// A D1 range inside the current year has no finished bucket yet: the
    /// year file is still being written, so nothing is requested.
    #[tokio::test]
    async fn run_skips_a_year_bucket_that_is_still_open() {
        let (_dir, path) = config(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2025-01-01\"\nend = \"2025-06-30\"\n",
        );
        run_with_today(&path, &DownloadOptions::default(), today())
            .await
            .unwrap();
    }

    /// The same for the H1 file of the month that is still running.
    #[tokio::test]
    async fn run_skips_a_month_bucket_that_is_still_open() {
        let (_dir, path) =
            config("symbol = [\"EURUSD\"]\ntimeframe = [\"H1\"]\nstart = \"2025-06-01\"\n");
        run_with_today(&path, &DownloadOptions::default(), today())
            .await
            .unwrap();
    }

    // — join-error reporting —

    /// A panicked task must report *why* it panicked. `JoinError`'s `Display`
    /// drops the payload, so the message has to be recovered explicitly.
    ///
    /// The default panic hook prints the panic to stderr; that noise is
    /// expected here and does not indicate a failure.
    #[tokio::test]
    async fn describe_join_error_recovers_the_panic_message() {
        let handle = tokio::spawn(async { panic!("kaboom") });
        let err = handle.await.unwrap_err();

        let failure = describe_join_error(err);
        assert!(failure.is_panic);
        assert!(
            failure.message.contains("kaboom"),
            "panic payload lost: {}",
            failure.message
        );
    }

    /// A cancelled task carries no payload and must not be mislabelled as a
    /// panic.
    #[tokio::test]
    async fn describe_join_error_does_not_claim_a_panic_for_cancellation() {
        let handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        });
        handle.abort();
        let err = handle.await.unwrap_err();

        let failure = describe_join_error(err);
        assert!(!failure.is_panic);
        assert_ne!(failure.message, "");
    }
}
