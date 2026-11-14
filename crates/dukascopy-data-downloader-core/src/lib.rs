//! Dukascopy historical kline downloader (.bi5 raw files).
//!
//! Downloads raw LZMA-compressed `.bi5` candle files from the
//! [Dukascopy Datafeed](https://www.dukascopy.com/datafeed) without decoding
//! them, mirroring the server directory layout under `output_dir/dukascopy/`.
//!
//! Module layout: [`app`] holds the pipeline ([`app::build_urls`],
//! [`app::build_download_tasks`], [`app::download`]); [`config`] holds the
//! validated configuration and the domain types. The pipeline owns its
//! cache scan, run lock and retry policy, so nothing outside [`app`] has to
//! know about them.
//!
//! The crate root re-exports the types an embedder needs; the module paths
//! stay available for code that wants to be explicit.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), ddd::AppError> {
//! ddd::run(std::path::Path::new("config.toml")).await
//! # }
//! ```
//!
//! The lint policy (`missing_docs`, `clippy::correctness`, …) is declared in
//! `Cargo.toml` under `[workspace.lints]` so that it applies to the library,
//! the binary and the integration tests alike.

pub mod app;
pub mod config;
pub mod error;

pub use app::download::{DownloadGate, DownloadOptions, DownloadStats, DownloadTask};
pub use app::scan_local_files;
pub use app::{run, run_with};
pub use config::{
    AppConfig, BucketKey, DEFAULT_START, MAX_RANGE_DAYS, PriceType, Symbol, Timeframe,
};
pub use error::AppError;
