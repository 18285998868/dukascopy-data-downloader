//! Command-line interface for the downloader.

use clap::Parser;
use std::path::PathBuf;

/// Parsed command-line arguments.
#[derive(Parser)]
#[command(name = "dukascopy-data-downloader")]
pub struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short = 'c', long = "config")]
    pub config: PathBuf,
}

/// Parse command-line arguments (exits on failure).
pub fn parse() -> Cli {
    Cli::parse()
}
