//! Command-line interface for the downloader.

use clap::Parser;
use std::path::PathBuf;

/// Parsed command-line arguments.
#[derive(Debug, Parser)]
#[command(name = "dukascopy-data-downloader")]
pub struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short = 'c', long = "config")]
    pub config: PathBuf,
}

/// Parse command-line arguments (exits on failure).
#[must_use]
pub fn parse() -> Cli {
    Cli::parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_accepts_short_and_long_config_flag() {
        for args in [
            vec!["ddd", "-c", "a.toml"],
            vec!["ddd", "--config", "a.toml"],
        ] {
            let cli = Cli::try_parse_from(args).unwrap();
            assert_eq!(cli.config, PathBuf::from("a.toml"));
        }
    }

    #[test]
    fn cli_requires_config_flag() {
        assert!(Cli::try_parse_from(["ddd"]).is_err());
    }

    #[test]
    fn cli_rejects_unknown_flag() {
        assert!(Cli::try_parse_from(["ddd", "-c", "a.toml", "--nope"]).is_err());
    }
}
