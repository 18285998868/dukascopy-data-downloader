//! Command-line interface for the downloader.

use clap::Parser;
use std::path::PathBuf;

/// Parsed command-line arguments.
#[derive(Debug, Parser)]
#[command(
    name = "dukascopy-data-downloader",
    about = "Download raw Dukascopy historical kline (.bi5) files from the public Datafeed"
)]
pub struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short = 'c', long = "config")]
    pub config: PathBuf,

    /// Tolerate a requested range that the data feed does not have.
    ///
    /// Without this flag the run fails when the feed is missing the
    /// requested range (too many 404s, or a long stretch of consecutive
    /// missing files). With it, the run finishes successfully with whatever
    /// the feed did have. Files that actually failed to download still fail
    /// the run.
    #[arg(long = "allow-missing")]
    pub allow_missing: bool,
}

/// Parse command-line arguments (exits on failure).
#[must_use]
pub fn parse() -> Cli {
    Cli::parse()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

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

    /// The help text has to name the flag, otherwise a user who only sees
    /// `--help` cannot find out how to point the program at a config.
    #[test]
    fn the_help_text_names_the_config_flag() {
        let help = Cli::command().render_long_help().to_string();
        assert!(help.contains("--config"), "{help}");
        assert!(help.contains("-c"), "{help}");
    }

    /// The opt-out has to be discoverable in the help text too.
    #[test]
    fn the_help_text_names_the_allow_missing_flag() {
        let help = Cli::command().render_long_help().to_string();
        assert!(help.contains("--allow-missing"), "{help}");
    }

    /// The opt-out is off unless it is asked for.
    #[test]
    fn allow_missing_is_off_by_default() {
        let cli = Cli::try_parse_from(["ddd", "-c", "a.toml"]).unwrap();
        assert!(!cli.allow_missing);
    }

    #[test]
    fn allow_missing_can_be_turned_on() {
        let cli = Cli::try_parse_from(["ddd", "-c", "a.toml", "--allow-missing"]).unwrap();
        assert!(cli.allow_missing);
    }
}
