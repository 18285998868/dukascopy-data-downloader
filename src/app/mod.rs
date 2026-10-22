//! Application orchestration for the download pipeline.
//!
//! The pipeline runs in numbered stages that mirror the data flow:
//!
//! 1. [`build_urls`] — expand one symbol/timeframe pair into server URLs.
//! 2. [`build_download_tasks`] — drop locally cached files and bind destinations.
//! 3. [`download`] — stream the remaining tasks with bounded concurrency.
//!
//! All symbol x timeframe pairs run **concurrently** ([`tokio::task::JoinSet`]),
//! but they share a single download gate, so `concurrency` bounds in-flight
//! requests across the whole run. A failing pair is recorded and does not abort
//! the others.

pub mod build_download_tasks;
pub mod build_urls;
pub mod download;

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::AppError;
use crate::config::{AppConfig, Symbol, Timeframe};
use download::DownloadOptions;

/// Maximum number of pair-level error details retained in the batch summary.
const MAX_BATCH_ERRORS: usize = 20;
/// TCP/TLS connect budget for a single request.
///
/// 30s rather than something tighter: the handshake includes the TLS
/// session, and a slow link to the data feed can spend a long time on it.
/// A connect timeout that is too tight does not make the run faster, it just
/// turns a slow download into a failed one.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Total budget for a single request, including body download.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Idle-connection lifetime in the pool.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// State shared by every pair task. Cloning is cheap: the client is a refcount
/// bump and the rest are `Arc`s.
#[derive(Clone)]
struct SharedRun {
    client: reqwest::Client,
    cfg: Arc<AppConfig>,
    local: Arc<HashSet<String>>,
    gate: Arc<Semaphore>,
    start: chrono::NaiveDate,
    end: chrono::NaiveDate,
    /// Current UTC date, used to tell final buckets from unfinished ones.
    today: chrono::NaiveDate,
    /// How a range the feed does not have is treated.
    opts: DownloadOptions,
}

/// Everything one pair needs, borrowed from a [`SharedRun`].
struct PairCtx<'a> {
    shared: &'a SharedRun,
    symbol: &'a Symbol,
    timeframe: Timeframe,
}

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
///     &ddd::DownloadOptions {
///         allow_missing: true,
///     },
/// )
/// .await
/// # }
/// ```
pub async fn run_with(config_path: &Path, opts: &DownloadOptions) -> Result<(), AppError> {
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
    let output_dir = cfg.output_dir.clone();
    let lock =
        tokio::task::spawn_blocking(move || crate::utils::run_lock::acquire_run_lock(&output_dir))
            .await
            .map_err(AppError::from)??;

    // The lock guard is owned by the *pipeline future*, so dropping that
    // future — which is exactly what an interruption does — releases the
    // lock. The `spawn_blocking` task that creates it cannot be cancelled: if
    // the future is dropped first, the task still finishes and its `RunLock`
    // is dropped immediately, so no lock is leaked either way.
    tokio::select! {
        biased;
        result = run_pipeline(cfg, opts, lock) => result,
        signal = termination => Err(AppError::Interrupted { signal }),
    }
}

/// Wait for a signal that asks the process to stop.
///
/// Returns the name of the signal that arrived.
///
/// The OS handlers are installed when this function is *called*, not when
/// the returned future is first polled: `tokio::signal::unix::signal`
/// registers the handler at call time, while `tokio::signal::ctrl_c` would
/// only do so on the first poll. The distinction matters — the handler has
/// to be in place before the run lock is taken.
fn termination_signal() -> impl Future<Output = &'static str> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let interrupt = signal(SignalKind::interrupt());
        let terminate = signal(SignalKind::terminate());
        async move {
            match (interrupt, terminate) {
                (Ok(mut interrupt), Ok(mut terminate)) => {
                    tokio::select! {
                        _ = interrupt.recv() => "SIGINT",
                        _ = terminate.recv() => "SIGTERM",
                    }
                }
                (Ok(mut interrupt), Err(e)) => {
                    tracing::warn!(
                        error = %e,
                        "could not install the SIGTERM handler; Ctrl+C still works"
                    );
                    let _ = interrupt.recv().await;
                    "SIGINT"
                }
                (Err(e), _) => {
                    tracing::warn!(
                        error = %e,
                        "could not install the SIGINT handler; the run cannot be interrupted cleanly"
                    );
                    // Nothing to wait for: park forever so this branch of
                    // the outer select never wins.
                    std::future::pending::<&'static str>().await
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        let ctrl_c = tokio::signal::ctrl_c();
        async move {
            let _ = ctrl_c.await;
            "SIGINT"
        }
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
    _lock: crate::utils::run_lock::RunLock,
) -> Result<(), AppError> {
    // Resolve the range *before* any I/O: it is pure arithmetic, so a
    // misconfigured range costs nothing instead of a full disk scan.
    let today = chrono::Utc::now().date_naive();
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

    let local = crate::utils::scan_local_files::scan_local_files(&cfg.output_dir).await?;
    let concurrency = cfg.concurrency;

    tracing::info!(
        symbols = cfg.symbol.len(),
        timeframes = cfg.timeframe.len(),
        pairs = cfg.symbol.len() * cfg.timeframe.len(),
        concurrency,
        start = %start,
        end = %end,
        "starting dukascopy batch download",
    );

    let cfg = Arc::new(cfg);
    let shared = SharedRun {
        client,
        cfg,
        // One gate for the whole run: `concurrency` is a global in-flight ceiling.
        gate: Arc::new(Semaphore::new(concurrency)),
        local: Arc::new(local),
        start,
        end,
        today,
        opts: *opts,
    };

    let mut pairs = JoinSet::new();
    // Borrow the config through its `Arc` instead of cloning the vectors out
    // of it: `Vec<Symbol>` is a `Vec<String>`, so cloning it would allocate
    // one `String` per symbol for no reason.
    let cfg = Arc::clone(&shared.cfg);
    for symbol in &cfg.symbol {
        for timeframe in &cfg.timeframe {
            pairs.spawn(run_pair(shared.clone(), symbol.clone(), *timeframe));
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

/// Download one symbol/timeframe pair and report which pair it was.
///
/// The label is returned alongside the result because [`JoinSet`] yields
/// completions out of order.
#[tracing::instrument(skip(shared), fields(symbol = %symbol, timeframe = %timeframe))]
async fn run_pair(
    shared: SharedRun,
    symbol: Symbol,
    timeframe: Timeframe,
) -> (String, Timeframe, Result<(), AppError>) {
    let ctx = PairCtx {
        shared: &shared,
        symbol: &symbol,
        timeframe,
    };
    let result = run_one(ctx).await;
    (symbol.to_string(), timeframe, result)
}

/// Download one symbol/timeframe pair.
async fn run_one(ctx: PairCtx<'_>) -> Result<(), AppError> {
    let PairCtx {
        shared,
        symbol,
        timeframe,
    } = ctx;
    let SharedRun {
        client,
        cfg,
        local,
        gate,
        start,
        end,
        today,
        opts,
    } = shared;
    let (start, end, today) = (*start, *end, *today);

    if start > end {
        tracing::warn!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %start,
            end = %end,
            "start date after end date, skipping",
        );
        return Ok(());
    }

    // Restrict this pair to buckets that are already final. The server keeps
    // appending to an unfinished bucket, and the cache never re-downloads a
    // file it already has — so an unfinished bucket would be frozen at
    // whatever it held at download time, leaving a permanent hole.
    let closed_end = build_urls::last_closed_day(timeframe, end, today);
    if start > closed_end {
        tracing::debug!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %start,
            end = %end,
            "no finished bucket in the requested range, skipping",
        );
        return Ok(());
    }

    tracing::info!(
        symbol = %symbol,
        timeframe = %timeframe,
        start = %start,
        end = %closed_end,
        "processing symbol-timeframe pair",
    );

    let tasks = build_download_tasks::build_download_tasks(
        symbol,
        timeframe,
        cfg.price_type,
        start,
        closed_end,
        local,
        &cfg.output_dir,
    );

    if tasks.is_empty() {
        tracing::debug!(
            symbol = %symbol,
            timeframe = %timeframe,
            "no files to download, all cached or out of range",
        );
        return Ok(());
    }

    tracing::debug!(symbol = %symbol, files = tasks.len(), "downloading");
    let stats = download::run_downloads_with(client, tasks, cfg.concurrency, gate, opts).await?;
    tracing::info!(
        symbol = %symbol,
        timeframe = %timeframe,
        ok = stats.ok,
        not_found = stats.not_found,
        failed = stats.failed,
        skipped = stats.skipped,
        total = stats.total,
        "pair download complete",
    );

    Ok(())
}

/// Build a direct-connection client (no proxy fallback is attempted).
///
/// `REQUEST_TIMEOUT` bounds one attempt while the backoff policy in
/// [`crate::utils::backoff`] governs the retry window. The two are deliberately
/// on different scales: at equal values a single stalled attempt could consume
/// the whole retry budget and leave no room for a second try.
fn build_client() -> Result<reqwest::Client, AppError> {
    Ok(reqwest::Client::builder()
        .user_agent(concat!(
            "dukascopy-data-downloader/",
            env!("CARGO_PKG_VERSION")
        ))
        .pool_max_idle_per_host(256)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .build()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;
    use std::path::PathBuf;

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

    #[test]
    fn build_client_succeeds() {
        assert!(build_client().is_ok());
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
        let lock = crate::utils::run_lock::acquire_run_lock(dir.path()).unwrap();

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
        run(&path).await.unwrap();
    }

    /// A start date beyond `today` yields a reversed effective range; the pair
    /// must be skipped locally rather than requested.
    #[tokio::test]
    async fn run_skips_a_reversed_range_without_touching_the_network() {
        let (_dir, path) = config(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2999-01-01\"\nend = \"2999-12-31\"\n",
        );
        run(&path).await.unwrap();
    }

    /// A D1 range inside the current year has no finished bucket yet: the
    /// year file is still being written, so nothing is requested.
    #[tokio::test]
    async fn run_skips_a_year_bucket_that_is_still_open() {
        let today = chrono::Utc::now().date_naive();
        let year = today.year();
        let (_dir, path) = config(&format!(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"{year}-01-01\"\nend = \"{year}-06-30\"\n"
        ));
        run(&path).await.unwrap();
    }

    /// The same for the H1 file of the month that is still running.
    #[tokio::test]
    async fn run_skips_a_month_bucket_that_is_still_open() {
        let today = chrono::Utc::now().date_naive();
        let first_of_month = today.with_day(1).unwrap();
        let (_dir, path) = config(&format!(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"H1\"]\nstart = \"{first_of_month}\"\n"
        ));
        run(&path).await.unwrap();
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
        assert!(!failure.message.is_empty());
    }
}
