use std::path::Path;
use std::time::Duration;

use crate::config::Config;

/// Run the full batch download pipeline.
///
/// 1. Load config (validated: symbols, timeframes, price types, date order)
/// 2. Build every download task (symbol × timeframe × period)
/// 3. Probe network (direct → SOCKS5 proxy fallback)
/// 4. Download all tasks with bounded concurrency
pub async fn run(config_path: &Path) -> anyhow::Result<()> {
    let cfg = Config::load(config_path)?;

    let tasks = crate::build_tasks::build_tasks(
        &cfg.symbol,
        &cfg.timeframe,
        &cfg.price_type,
        cfg.start,
        cfg.end,
        &cfg.dir,
    );

    let total = tasks.len();
    tracing::info!(
        symbols = cfg.symbol.len(),
        timeframes = cfg.timeframe.len(),
        total,
        "starting dukascopy batch download",
    );

    let client = build_client().await?;

    crate::run_downloads::run_downloads(&client, tasks, false).await?;

    tracing::info!("batch download complete");
    Ok(())
}

/// Build a client that tries direct first, then falls back to SOCKS5 proxy.
async fn build_client() -> anyhow::Result<reqwest::Client> {
    let base_builder = || reqwest::Client::builder().user_agent("dukascopy-data-downloader/0.1");

    let direct = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect("www.dukascopy.com:443"),
    )
    .await
    .ok()
    .and_then(|r| r.ok());

    if direct.is_some() {
        return Ok(base_builder().timeout(Duration::from_secs(30)).build()?);
    }

    let proxy = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::TcpStream::connect("127.0.0.1:7890"),
    )
    .await
    .ok()
    .and_then(|r| r.ok());

    if proxy.is_some() {
        return Ok(base_builder()
            .timeout(Duration::from_secs(60))
            .proxy(reqwest::Proxy::all("socks5h://127.0.0.1:7890")?)
            .build()?);
    }

    anyhow::bail!("no network (tried direct and SOCKS5 proxy at 127.0.0.1:7890)")
}
