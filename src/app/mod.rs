#[path = "02_build_download_tasks.rs"]
pub mod build_download_tasks;
#[path = "01_build_urls.rs"]
pub mod build_urls;
#[path = "03_download/mod.rs"]
pub mod download;

use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use crate::AppError;
use crate::config::{AppConfig, Timeframe};

/// Run the full batch download pipeline.
///
/// 1. Load config
/// 2. Scan existing local files (once)
/// 3. Iterate symbol (serial) → timeframe (serial), downloading missing files
///
/// # Errors
///
/// Returns [`AppError`] if configuration loading, client construction, scanning,
/// or any download pair fails.
pub async fn run(config_path: &Path) -> Result<(), AppError> {
    let cfg = AppConfig::load(config_path)?;

    let client = build_client()?;

    let local = crate::utils::scan_local_files::scan_local_files(&cfg.output_dir).await?;

    let concurrency = cfg.concurrency;
    let end = cfg.resolved_end(chrono::Utc::now().date_naive());
    let start_default = chrono::NaiveDate::from_ymd_opt(2020, 1, 1).expect("valid default date");
    let start_cfg = cfg.start.unwrap_or(start_default);

    tracing::info!(
        symbols = cfg.symbol.len(),
        timeframes = cfg.timeframe.len(),
        concurrency,
        "starting dukascopy batch download",
    );

    let mut failed_pairs = 0;
    let mut errors = Vec::new();

    for symbol in &cfg.symbol {
        for timeframe in &cfg.timeframe {
            tracing::info!(symbol = %symbol, timeframe = %timeframe, "running pair");
            let ctx = PairCtx {
                start: start_cfg,
                end,
                cfg: &cfg,
                local: &local,
            };
            if let Err(e) = run_one(&client, symbol, *timeframe, &ctx).await {
                failed_pairs += 1;
                tracing::error!(symbol = %symbol, timeframe = %timeframe, error = %e, "pair failed");
                if errors.len() < MAX_BATCH_ERRORS {
                    errors.push(format!("{symbol}/{timeframe}: {e}"));
                }
            }
        }
    }

    if failed_pairs == 0 {
        tracing::info!("batch download complete");
        Ok(())
    } else {
        tracing::error!(count = failed_pairs, "batch download completed with errors");
        let truncated = if errors.len() < failed_pairs {
            "\n(additional failures not listed)"
        } else {
            ""
        };
        Err(AppError::Data(format!(
            "{} pair(s) failed: {}{}",
            failed_pairs,
            errors.join("; "),
            truncated,
        )))
    }
}

/// Maximum number of pair-level error details retained in the batch summary.
const MAX_BATCH_ERRORS: usize = 20;

/// Per-pair context: resolved dates plus shared borrows.
struct PairCtx<'a> {
    start: chrono::NaiveDate,
    end: chrono::NaiveDate,
    cfg: &'a AppConfig,
    local: &'a HashSet<String>,
}

/// Download one symbol/timeframe pair.
async fn run_one(
    client: &reqwest::Client,
    symbol: &str,
    timeframe: Timeframe,
    ctx: &PairCtx<'_>,
) -> Result<(), AppError> {
    if ctx.start > ctx.end {
        tracing::warn!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %ctx.start,
            end = %ctx.end,
            "start date after end date, skipping",
        );
        return Ok(());
    }

    tracing::info!(
        symbol = %symbol,
        timeframe = %timeframe,
        start = %ctx.start,
        end = %ctx.end,
        "processing symbol-timeframe pair",
    );

    let tasks = build_download_tasks::build_download_tasks(
        symbol,
        timeframe,
        ctx.cfg.price_type,
        ctx.start,
        ctx.end,
        ctx.local,
        &ctx.cfg.output_dir,
    );

    if !tasks.is_empty() {
        tracing::debug!(
            files = tasks.len(),
            start = %ctx.start,
            end = %ctx.end,
            "downloading all",
        );
        let stats = download::run_downloads(client, tasks, ctx.cfg.concurrency).await?;
        tracing::info!(
            symbol = %symbol,
            timeframe = %timeframe,
            ok = stats.ok,
            not_found = stats.not_found,
            failed = stats.failed,
            total = stats.total,
            "pair download complete",
        );
    } else {
        tracing::debug!(
            symbol = %symbol,
            timeframe = %timeframe,
            start = %ctx.start,
            end = %ctx.end,
            "no files to download, all cached or out of range",
        );
    }

    Ok(())
}

/// Build a direct-connection client (no proxy).
fn build_client() -> Result<reqwest::Client, AppError> {
    Ok(reqwest::Client::builder()
        .user_agent("dukascopy-data-downloader/0.1")
        .pool_max_idle_per_host(256)
        .pool_idle_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(30))
        .build()?)
}
