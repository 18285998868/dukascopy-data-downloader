use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "dukascopy-data-downloader")]
pub struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short = 'c', long = "config")]
    pub config: PathBuf,
}

pub fn parse() -> Cli {
    Cli::parse()
}
