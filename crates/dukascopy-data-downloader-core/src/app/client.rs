//! HTTP client construction shared by the whole run.

use std::time::Duration;

use crate::AppError;

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

/// Build a direct-connection client (no proxy fallback is attempted).
///
/// `REQUEST_TIMEOUT` bounds one attempt while the backoff policy in
/// [`crate::utils::backoff`] governs the retry window. The two are deliberately
/// on different scales: at equal values a single stalled attempt could consume
/// the whole retry budget and leave no room for a second try.
pub(super) fn build_client() -> Result<reqwest::Client, AppError> {
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

    #[test]
    fn build_client_succeeds() {
        assert!(build_client().is_ok());
    }
}
