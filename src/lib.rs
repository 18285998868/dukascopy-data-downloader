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
//!
//! The lint policy (`missing_docs`, `clippy::correctness`, …) is declared in
//! `Cargo.toml` under `[workspace.lints]` so that it applies to the library,
//! the binary and the integration tests alike.

pub mod app;

pub mod cli;
pub mod config;
pub mod error;
pub mod utils;

pub use app::download::DownloadOptions;
pub use app::run;
pub use config::{
    AppConfig, DEFAULT_START, DownloadTask, MAX_RANGE_DAYS, PriceType, Symbol, Timeframe,
};
pub use error::AppError;
