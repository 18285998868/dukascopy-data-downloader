//! One symbol/timeframe pair of the batch.

use std::collections::HashSet;
use std::sync::Arc;

use super::build_download_tasks;
use super::download;
use super::download::{DownloadGate, DownloadOptions};
use crate::AppError;
use crate::config::{AppConfig, Symbol, Timeframe, last_closed_day};

/// State shared by every pair task. Cloning is cheap: the client is a refcount
/// bump and the rest are `Arc`s.
#[derive(Clone)]
pub(super) struct SharedRun {
    pub(super) client: reqwest::Client,
    pub(super) cfg: Arc<AppConfig>,
    pub(super) local: Arc<HashSet<String>>,
    pub(super) gate: DownloadGate,
    pub(super) start: chrono::NaiveDate,
    pub(super) end: chrono::NaiveDate,
    /// Current UTC date, used to tell final buckets from unfinished ones.
    pub(super) today: chrono::NaiveDate,
    /// How a range the feed does not have is treated.
    pub(super) opts: DownloadOptions,
}

/// Everything one pair needs, borrowed from a [`SharedRun`].
struct PairCtx<'a> {
    shared: &'a SharedRun,
    symbol: &'a Symbol,
    timeframe: Timeframe,
}

/// Download one symbol/timeframe pair and report which pair it was.
///
/// The label is returned alongside the result because [`tokio::task::JoinSet`]
/// yields completions out of order.
#[tracing::instrument(skip(shared), fields(symbol = %symbol, timeframe = %timeframe))]
pub(super) async fn run_pair(
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
    let closed_end = last_closed_day(timeframe, end, today);
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
        cfg.price_type(),
        start,
        closed_end,
        local,
        cfg.output_dir(),
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
    let stats = download::run_downloads_with(client, tasks, gate, opts).await?;
    tracing::info!(
        symbol = %symbol,
        timeframe = %timeframe,
        ok = stats.ok(),
        not_found = stats.not_found(),
        failed = stats.failed(),
        skipped = stats.skipped(),
        total = stats.total(),
        "pair download complete",
    );

    Ok(())
}
