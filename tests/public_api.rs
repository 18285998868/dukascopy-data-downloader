//! Integration tests for the public API contract.
//!
//! The unit tests inside `src/` may reach private helpers; these tests may
//! not, so they pin down the contract an external consumer actually depends
//! on: configuration loading, URL/task expansion, cache de-duplication and
//! error reporting — all without touching the network.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ddd::config::{AppConfig, PriceType, Symbol, Timeframe};
use ddd::utils::scan_local_files::scan_local_files;

/// Write `body` into a fresh temp directory and return the config path.
fn write_config(body: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.toml");
    std::fs::write(&path, body).unwrap();
    (dir, path)
}

/// 2025-06-15, the single day used by the expansion tests.
fn day() -> chrono::NaiveDate {
    chrono::NaiveDate::from_ymd_opt(2025, 6, 15).unwrap()
}

#[test]
fn config_round_trips_through_the_public_loader() {
    let (_dir, path) = write_config(
        "symbol = [\"EURUSD\", \"EURUSD\"]\ntimeframe = [\"1d\"]\nprice_type = \"ASK\"\nconcurrency = 8\n",
    );
    let cfg = AppConfig::load(&path).unwrap();

    assert_eq!(cfg.symbol.len(), 1, "duplicates must be collapsed");
    assert_eq!(cfg.symbol[0].as_str(), "EURUSD");
    assert_eq!(cfg.timeframe, vec![Timeframe::D1], "aliases normalise");
    assert_eq!(cfg.price_type, PriceType::Ask);
    assert_eq!(cfg.concurrency, 8);
}

#[test]
fn unknown_config_keys_are_rejected() {
    let (_dir, path) = write_config("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nnope = 1\n");
    let err = AppConfig::load(&path).unwrap_err();
    assert!(err.to_string().contains("nope"), "{err}");
}

#[test]
fn a_symbol_that_escapes_its_directory_is_rejected() {
    let (_dir, path) = write_config("symbol = [\"../../etc/passwd\"]\ntimeframe = [\"D1\"]\n");
    let err = AppConfig::load(&path).unwrap_err();
    assert!(err.to_string().contains("symbol"), "{err}");
}

#[test]
fn urls_destinations_and_task_metadata_stay_in_sync() {
    let symbol = Symbol::new("EURUSD").unwrap();
    let local: HashSet<String> = HashSet::new();

    let tasks = ddd::app::build_download_tasks::build_download_tasks(
        &symbol,
        Timeframe::H1,
        PriceType::Bid,
        day(),
        day(),
        &local,
        Path::new("/out"),
    );

    assert_eq!(tasks.len(), 1);
    let task = &tasks[0];
    assert!(
        task.url.ends_with("/EURUSD/2025/06/BID_candles_hour_1.bi5"),
        "{}",
        task.url
    );
    assert_eq!(
        task.dest,
        Path::new("/out/dukascopy/EURUSD/2025/06/BID_candles_hour_1.bi5")
    );
    assert_eq!(task.symbol.as_str(), "EURUSD");
    assert_eq!(task.timeframe, Timeframe::H1);
}

/// The cache is a *file*-level contract: the scanner and the task builder
/// must agree on the exact relative-path spelling.
#[tokio::test]
async fn a_cached_file_is_never_downloaded_twice() {
    let dir = tempfile::tempdir().unwrap();
    let cached = dir
        .path()
        .join("dukascopy/EURUSD/2025/06/BID_candles_hour_1.bi5");
    std::fs::create_dir_all(cached.parent().unwrap()).unwrap();
    std::fs::write(&cached, b"payload").unwrap();

    let local = scan_local_files(dir.path()).await.unwrap();
    assert!(
        local.contains("dukascopy/EURUSD/2025/06/BID_candles_hour_1.bi5"),
        "{local:?}"
    );

    let tasks = ddd::app::build_download_tasks::build_download_tasks(
        &Symbol::new("EURUSD").unwrap(),
        Timeframe::H1,
        PriceType::Bid,
        day(),
        day(),
        &local,
        dir.path(),
    );
    assert!(tasks.is_empty(), "a cached file must not be re-downloaded");
}

/// The reverse direction: a cached *BID* file must not suppress the *ASK*
/// file for the same bucket.
#[tokio::test]
async fn the_price_type_is_part_of_the_cache_key() {
    let dir = tempfile::tempdir().unwrap();
    let cached = dir
        .path()
        .join("dukascopy/EURUSD/2025/06/BID_candles_hour_1.bi5");
    std::fs::create_dir_all(cached.parent().unwrap()).unwrap();
    std::fs::write(&cached, b"payload").unwrap();

    let local = scan_local_files(dir.path()).await.unwrap();
    let tasks = ddd::app::build_download_tasks::build_download_tasks(
        &Symbol::new("EURUSD").unwrap(),
        Timeframe::H1,
        PriceType::Ask,
        day(),
        day(),
        &local,
        dir.path(),
    );

    assert_eq!(tasks.len(), 1);
    assert!(tasks[0].dest.ends_with("ASK_candles_hour_1.bi5"));
}

#[test]
fn the_error_chain_exposes_the_root_cause() {
    let err = ddd::AppError::Io(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "root cause",
    ));
    let rendered = err.chain();
    assert!(rendered.starts_with("i/o error: "), "{rendered}");
    assert!(rendered.contains("root cause"), "{rendered}");
}

#[tokio::test]
async fn an_empty_batch_succeeds_with_zeroed_statistics() {
    let client = reqwest::Client::new();
    let gate = Arc::new(tokio::sync::Semaphore::new(2));

    let stats = ddd::app::download::run_downloads(&client, Vec::new(), 2, &gate)
        .await
        .unwrap();

    assert_eq!(stats.total, 0);
    assert_eq!(stats.ok, 0);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.skipped, 0);
}
