//! Binary entry point for `dukascopy-data-downloader`.
//!
//! Initializes tracing, parses command-line arguments, then runs the
//! full download pipeline with the provided TOML configuration.

mod cli;

use anyhow::Context;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing with `RUST_LOG` (falls back to `info`).
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Parse command-line arguments (`--config`/`-c` is required).
    let cli = cli::parse();

    // Run the download pipeline, annotating any error with pipeline context.
    let opts = ddd::app::download::DownloadOptions {
        allow_missing: cli.allow_missing,
    };
    ddd::app::run_with(&cli.config, &opts)
        .await
        .context("dukascopy-data-downloader run failed")?;
    Ok(())
}
