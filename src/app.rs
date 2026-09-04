use std::path::Path;

use chrono::NaiveDate;

use crate::config::Config;
use crate::download;

/// Build a client that tries direct first, then falls back to SOCKS5 proxy.
async fn build_client() -> anyhow::Result<reqwest::Client> {
    use std::time::Duration;
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

pub async fn run(config_path: Option<&Path>) -> anyhow::Result<()> {
    let cfg = Config::load(config_path)?;

    let start = parse_date(&cfg.start, "start")?;
    let end = parse_date(&cfg.end, "end")?;

    if start > end {
        anyhow::bail!("start date {start} is after end date {end}");
    }

    let tasks = download::build_tasks(
        &cfg.symbol,
        &cfg.timeframe,
        &cfg.price_type,
        start,
        end,
        &cfg.dir,
    );

    let total = tasks.len();
    tracing::info!(
        symbols = cfg.symbol.len(),
        timeframes = cfg.timeframe.len(),
        total,
        "starting dukascopy batch download"
    );

    let client = build_client().await?;

    download::run_downloads(&client, tasks, false).await?;

    tracing::info!("batch download complete");
    Ok(())
}

fn parse_date(s: &str, label: &str) -> Result<NaiveDate, anyhow::Error> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| anyhow::anyhow!("invalid {label} date '{s}': {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_date_valid() {
        let d = parse_date("2025-01-01", "start").unwrap();
        assert_eq!(d, NaiveDate::from_ymd_opt(2025, 1, 1).unwrap());
    }

    #[test]
    fn parse_date_invalid() {
        assert!(parse_date("2025-13-01", "start").is_err());
        assert!(parse_date("abc", "start").is_err());
    }
}
