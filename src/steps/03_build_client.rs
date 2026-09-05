use std::time::Duration;

/// Build a client that tries direct first, then falls back to SOCKS5 proxy.
pub async fn build_client() -> anyhow::Result<reqwest::Client> {
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
