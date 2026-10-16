//! Dukascopy historical kline downloader (.bi5 raw files).
//!
//! Module layout: `app/` holds the naive pipeline (`01_build_urls`,
//! `02_build_download_tasks`, `03_download/`); patches live in `utils/`
//! (`scan_local_files`, `backoff`).

pub mod app;

pub mod cli;
pub mod config;
pub mod error;
pub mod utils;

pub use app::run;
pub use error::AppError;
