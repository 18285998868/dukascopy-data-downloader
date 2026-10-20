//! End-to-end download flow against a mock server, through the public API.
//!
//! Scope: only the paths that terminate promptly. A 5xx answer is *not*
//! covered here — the public `download_to_file` retries under the production
//! backoff budget (120s), which would stall the suite. The 5xx path is
//! covered by the unit tests in `src/app/download/`, where the backoff
//! policy can be injected.

use std::path::PathBuf;
use std::sync::Arc;

use ddd::config::{DownloadTask, Symbol, Timeframe};
use tokio::sync::Semaphore;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const URL_PATH: &str = "/datafeed/EURUSD/2025/06/BID_candles_hour_1.bi5";

fn task(server: &MockServer, dest: PathBuf) -> DownloadTask {
    DownloadTask {
        url: format!("{}{URL_PATH}", server.uri()),
        dest,
        symbol: Symbol::new("EURUSD").unwrap(),
        timeframe: Timeframe::H1,
    }
}

/// The scratch file a download writes before its atomic rename.
fn tmp_path(dest: &std::path::Path) -> PathBuf {
    dest.with_extension("bi5.tmp")
}

async fn server_returning(resp: ResponseTemplate) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(resp)
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn a_successful_download_lands_at_its_destination() {
    let server = server_returning(ResponseTemplate::new(200).set_body_bytes(b"payload")).await;
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("EURUSD/2025/06/BID_candles_hour_1.bi5");

    ddd::app::download::stream::download_to_file(
        &reqwest::Client::new(),
        &task(&server, dest.clone()),
    )
    .await
    .unwrap();

    assert_eq!(std::fs::read(&dest).unwrap(), b"payload");
    assert!(
        !tmp_path(&dest).exists(),
        "the scratch file must be renamed away"
    );
}

#[tokio::test]
async fn a_missing_file_is_reported_and_leaves_no_trace() {
    let server = server_returning(ResponseTemplate::new(404)).await;
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("EURUSD/2025/06/BID_candles_hour_1.bi5");

    let err = ddd::app::download::stream::download_to_file(
        &reqwest::Client::new(),
        &task(&server, dest.clone()),
    )
    .await
    .unwrap_err();

    assert!(matches!(err, ddd::AppError::NotFound { .. }), "{err:?}");
    assert!(!dest.exists(), "a 404 must not create the destination file");
    assert!(
        !tmp_path(&dest).exists(),
        "a 404 must not leave a scratch file behind"
    );
}

#[tokio::test]
async fn a_batch_counts_a_404_without_failing() {
    let server = server_returning(ResponseTemplate::new(404)).await;
    let dir = tempfile::tempdir().unwrap();
    let tasks = vec![task(&server, dir.path().join("a/BID_candles_hour_1.bi5"))];

    let gate = Arc::new(Semaphore::new(4));
    let stats = ddd::app::download::run_downloads(&reqwest::Client::new(), tasks, 4, &gate)
        .await
        .expect("a 404 is an expected answer, not a batch failure");

    assert_eq!(stats.total, 1);
    assert_eq!(stats.not_found, 1);
    assert_eq!(stats.ok, 0);
    assert_eq!(stats.failed, 0);
}

#[tokio::test]
async fn a_batch_mixes_success_and_not_found() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/ok.bi5"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ok"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/missing.bi5"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let make = |name: &str| DownloadTask {
        url: format!("{}/{name}", server.uri()),
        dest: dir.path().join(name),
        symbol: Symbol::new("EURUSD").unwrap(),
        timeframe: Timeframe::H1,
    };
    let tasks = vec![make("ok.bi5"), make("missing.bi5")];

    let gate = Arc::new(Semaphore::new(4));
    let stats = ddd::app::download::run_downloads(&reqwest::Client::new(), tasks, 4, &gate)
        .await
        .unwrap();

    assert_eq!(stats.ok, 1);
    assert_eq!(stats.not_found, 1);
    assert_eq!(stats.failed, 0);
    assert_eq!(std::fs::read(dir.path().join("ok.bi5")).unwrap(), b"ok");
}
