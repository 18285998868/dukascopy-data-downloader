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

/// Maximum number of pair-level error details retained in the batch summary.
const MAX_BATCH_ERRORS: usize = 20;
/// TCP/TLS connect budget for a single request.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
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
/// # Errors
///
/// Returns [`AppError`] if configuration loading, client construction, scanning,
/// or the resolved date range fails, or if any pair failed.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> Result<(), ddd::AppError> {
/// ddd::app::run(std::path::Path::new("config.toml")).await
/// # }
/// ```
pub async fn run(config_path: &Path) -> Result<(), AppError> {
    let cfg = AppConfig::load(config_path)?;

    let client = build_client()?;
    let local = crate::utils::scan_local_files::scan_local_files(&cfg.output_dir).await?;
    let (start, end) = cfg.effective_range(chrono::Utc::now().date_naive())?;
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
    };

    let mut pairs = JoinSet::new();
    let symbol_list = shared.cfg.symbol.clone();
    let timeframe_list = shared.cfg.timeframe.clone();
    for symbol in &symbol_list {
        for timeframe in &timeframe_list {
            pairs.spawn(run_pair(shared.clone(), symbol.clone(), *timeframe));
        }
    }

    let mut failed_pairs = 0;
    let mut errors: Vec<String> = Vec::new();

    while let Some(joined) = pairs.join_next().await {
        let (symbol, timeframe, result) = match joined {
            Ok(v) => v,
            Err(e) => {
                // Covers panics and cancellation alike; either way this pair is lost.
                failed_pairs += 1;
                tracing::error!(is_panic = e.is_panic(), error = %e, "pair task did not complete");
                if errors.len() < MAX_BATCH_ERRORS {
                    errors.push(format!("task error: {e}"));
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
    let truncated = if errors.len() < failed_pairs {
        " (additional failures not listed)"
    } else {
        ""
    };
    Err(AppError::Data(format!(
        "{} pair(s) failed: {}{truncated}",
        failed_pairs,
        errors.join("; ")
    )))
}

/// Download one symbol/timeframe pair and report which pair it was.
///
/// The label is returned alongside the result because [`JoinSet`] yields
/// completions out of order.
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
    } = shared;
    let (start, end) = (*start, *end);

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

    tracing::info!(
        symbol = %symbol,
        timeframe = %timeframe,
        start = %start,
        end = %end,
        "processing symbol-timeframe pair",
    );

    let tasks = build_download_tasks::build_download_tasks(
        symbol,
        timeframe,
        cfg.price_type,
        start,
        end,
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
    let stats = download::run_downloads(client, tasks, cfg.concurrency, gate).await?;
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

    fn config(body: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.toml");
        std::fs::write(&path, body).unwrap();
        (dir, path)
    }

    #[test]
    fn build_client_succeeds() {
        assert!(build_client().is_ok());
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
        let (_dir, path) = config(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"M1\"]\nstart = \"1900-01-01\"\noutput_dir = \"/tmp/ddd-never-used\"\n",
        );
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
}
