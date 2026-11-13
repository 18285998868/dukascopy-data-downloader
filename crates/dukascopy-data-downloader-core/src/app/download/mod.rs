//! Stage 3: bounded-concurrency download orchestration.
//!
//! Every pair in the run shares one [`Semaphore`](tokio::sync::Semaphore)
//! gate, so `concurrency` bounds downloads **across the whole run** rather
//! than per pair: with 15 pairs downloading simultaneously, at most
//! `concurrency` files are ever in flight. Tasks are streamed through
//! [`buffer_unordered`](futures::StreamExt::buffer_unordered) and folded into
//! [`DownloadStats`] incrementally.

mod breaker;
mod missing;
mod options;
mod ratio;
mod run;
mod stats;
pub mod stream;

pub use options::DownloadOptions;
pub use run::{run_downloads, run_downloads_with};
pub use stats::DownloadStats;
