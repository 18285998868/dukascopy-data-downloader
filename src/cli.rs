use std::path::PathBuf;

use clap::Parser;

#[derive(Parser)]
#[command(version, about)]
pub struct Cli {
    #[arg(short = 'c', long = "config", help = "Config file path")]
    pub config: Option<PathBuf>,
}

pub fn parse() -> Cli {
    Cli::parse()
}
