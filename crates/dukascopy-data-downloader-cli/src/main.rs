//! Binary entry point for `dukascopy-data-downloader`.
//!
//! Initializes tracing, parses command-line arguments, then runs the
//! full download pipeline with the provided TOML configuration.

mod cli;

use anyhow::Context;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing with `RUST_LOG` (falls back to `info`). `try_init`
    // keeps a second initialisation (embedded use, tests) from panicking.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init()
        .map_err(|e| anyhow::anyhow!("failed to initialize tracing: {e}"))?;

    // Parse command-line arguments (`--config`/`-c` is required).
    let cli = cli::parse();

    // Run the download pipeline, annotating any error with pipeline context.
    let opts = ddd::DownloadOptions::new().with_allow_missing(cli.allow_missing);
    ddd::run_with(&cli.config, &opts)
        .await
        .context("dukascopy-data-downloader run failed")?;
    Ok(())
}
