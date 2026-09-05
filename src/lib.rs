//! Dukascopy historical kline downloader (.bi5 raw files).
//!
//! Module layout mirrors the download pipeline in `app/`; on-disk files
//! carry a `NN_` step prefix and the Rust module names match the readable
//! step name (`build_tasks`, `run_downloads`, `app`), wired via `#[path]`.

pub mod app;

#[path = "app/01_build_tasks.rs"]
pub mod build_tasks;
#[path = "app/02_run_downloads/mod.rs"]
pub mod run_downloads;

pub mod backoff;
pub mod cache;
pub mod cli;
pub mod config;
pub mod error;

pub use app::run;
pub use error::AppError;
