//! Dukascopy historical kline downloader (.bi5 raw files).
//!
//! Module layout mirrors the download pipeline in `app/`; on-disk files
//! carry a `NN_` step prefix and the Rust module names match the readable
//! step name (`cli`, `config`, `build_client`, `build_tasks`,
//! `run_downloads`, `app`), wired via `#[path]`.

pub mod app;
#[path = "app/03_build_client.rs"]
pub mod build_client;
#[path = "app/04_build_tasks.rs"]
pub mod build_tasks;
#[path = "app/01_cli.rs"]
pub mod cli;
#[path = "app/02_config.rs"]
pub mod config;
#[path = "app/05_run_downloads/mod.rs"]
pub mod run_downloads;

pub mod backoff;
pub mod cache;
pub mod error;

use std::path::PathBuf;

pub use error::AppError;

use chrono::NaiveDate;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PeriodKind {
    Year,
    Month,
    Day,
}

#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub url: String,
    pub dest: PathBuf,
    pub symbol: String,
    pub timeframe: String,
    pub date: NaiveDate,
    pub period: PeriodKind,
}
