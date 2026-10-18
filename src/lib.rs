//! Dukascopy historical kline downloader (.bi5 raw files).
//!
//! Downloads raw LZMA-compressed `.bi5` candle files from the
//! [Dukascopy Datafeed](https://www.dukascopy.com/datafeed) without decoding
//! them, mirroring the server directory layout under `output_dir/dukascopy/`.
//!
//! Module layout: [`app`] holds the pipeline ([`app::build_urls`],
//! [`app::build_download_tasks`], [`app::download`]); [`utils`] holds
//! [`utils::scan_local_files`] and the backoff policy.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), ddd::AppError> {
//! ddd::app::run(std::path::Path::new("config.toml")).await
//! # }
//! ```

#![deny(clippy::correctness)]
#![warn(
    clippy::suspicious,
    clippy::style,
    clippy::complexity,
    clippy::perf,
    missing_docs,
    missing_debug_implementations
)]

pub mod app;

pub mod cli;
pub mod config;
pub mod error;
pub mod utils;

pub use app::run;
pub use config::{AppConfig, DownloadTask, PriceType, Symbol, Timeframe};
pub use error::AppError;
