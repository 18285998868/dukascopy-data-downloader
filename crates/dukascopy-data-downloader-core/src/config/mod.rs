//! Configuration model: parsing, validation and range resolution.
//!
//! [`AppConfig`] is deserialized straight from TOML. Every field is validated
//! at load time — notably [`Symbol`], which rejects anything that is not a
//! bare Dukascopy symbol code before it can reach a URL or a local path — so
//! the download pipeline can assume well-formed input and never re-check user
//! data downstream.
//!
//! The domain types live next to this module: [`Symbol`], [`Timeframe`],
//! [`PriceType`], [`DownloadTask`] and the date-range defaults.

mod price_type;
mod range;
mod symbol;
mod task;
mod timeframe;

pub use price_type::PriceType;
pub use range::{DEFAULT_START, MAX_RANGE_DAYS};
pub use symbol::Symbol;
pub use task::DownloadTask;
pub use timeframe::Timeframe;

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::AppError;

/// Application configuration loaded from TOML.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct AppConfig {
    /// Trading symbols to download.
    pub symbol: Vec<Symbol>,
    /// Timeframes to download.
    pub timeframe: Vec<Timeframe>,
    /// Quote type to download.
    #[serde(default)]
    pub price_type: PriceType,
    /// Explicit range start; defaults to [`DEFAULT_START`] when absent.
    #[serde(default)]
    pub start: Option<chrono::NaiveDate>,
    /// Explicit range end; clamped to yesterday by [`AppConfig::resolved_end`].
    #[serde(default)]
    pub end: Option<chrono::NaiveDate>,
    /// Data root directory; the `dukascopy/` prefix is appended automatically.
    #[serde(default = "default_output_dir")]
    pub output_dir: PathBuf,
    /// Maximum downloads in flight **across the whole run**, not per pair.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
}

fn default_output_dir() -> PathBuf {
    PathBuf::from("data")
}

fn default_concurrency() -> usize {
    30
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            symbol: Vec::new(),
            timeframe: Vec::new(),
            price_type: PriceType::Bid,
            start: None,
            end: None,
            output_dir: default_output_dir(),
            concurrency: default_concurrency(),
        }
    }
}

/// Upper bound for the shared download gate (also the client pool size).
const MAX_CONCURRENCY: usize = 256;

impl AppConfig {
    /// Load and validate configuration from `path`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Config`] if the file cannot be read, parsed, or validated.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::path::Path;
    ///
    /// # fn main() -> Result<(), ddd::AppError> {
    /// let cfg = ddd::config::AppConfig::load(Path::new("config.toml"))?;
    /// println!("{} symbols", cfg.symbol.len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn load(path: &Path) -> Result<Self, AppError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| AppError::Config(format!("{}: {e}", path.display())))?;
        let cfg: Self = toml::from_str(&raw)
            .map_err(|e| AppError::Config(format!("{}: {e}", path.display())))?;
        cfg.validate()
    }

    fn validate(mut self) -> Result<Self, AppError> {
        if self.symbol.is_empty() {
            return Err(AppError::Config(
                "symbol must contain at least one symbol".into(),
            ));
        }
        if self.timeframe.is_empty() {
            return Err(AppError::Config(
                "timeframe must contain at least one timeframe".into(),
            ));
        }
        if self.concurrency == 0 {
            return Err(AppError::Config(
                "concurrency must be greater than zero".into(),
            ));
        }
        if self.concurrency > MAX_CONCURRENCY {
            return Err(AppError::Config(format!(
                "concurrency {} exceeds maximum {MAX_CONCURRENCY}",
                self.concurrency
            )));
        }
        if let (Some(start), Some(end)) = (self.start, self.end) {
            if start > end {
                return Err(AppError::Config(format!(
                    "start {start} is after end {end}"
                )));
            }
        }
        self.symbol.sort();
        self.symbol.dedup();
        self.timeframe.sort();
        self.timeframe.dedup();
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn load(contents: &str) -> Result<AppConfig, AppError> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "{contents}").unwrap();
        drop(f);
        AppConfig::load(&path)
    }

    fn cfg(contents: &str) -> AppConfig {
        load(contents).expect("config should be valid")
    }

    fn date(y: i32, m: u32, d: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn symbols(cfg: &AppConfig) -> Vec<&str> {
        cfg.symbol.iter().map(Symbol::as_str).collect()
    }

    #[test]
    fn symbol_sorts_lexicographically() {
        let cfg = cfg("symbol = [\"GBPUSD\", \"EURUSD\"]\ntimeframe = [\"D1\"]");
        assert_eq!(symbols(&cfg), vec!["EURUSD", "GBPUSD"]);
    }

    #[test]
    fn symbol_dedups_after_normalisation() {
        let cfg = cfg("symbol = [\"EURUSD\", \"EURUSD\"]\ntimeframe = [\"D1\"]");
        assert_eq!(cfg.symbol.len(), 1);
    }

    // — loading —

    #[test]
    fn load_full_config() {
        let cfg = cfg("symbol = [\"EURUSD\", \"GBPUSD\"]\n\
             timeframe = [\"D1\", \"H1\"]\n\
             price_type = \"ASK\"\n\
             start = \"2024-01-01\"\n\
             end = \"2025-01-01\"\n\
             output_dir = \"/tmp/data\"\n\
             concurrency = 8");
        assert_eq!(symbols(&cfg), vec!["EURUSD", "GBPUSD"]);
        assert_eq!(cfg.timeframe, vec![Timeframe::H1, Timeframe::D1]);
        assert_eq!(cfg.price_type, PriceType::Ask);
        assert_eq!(cfg.start, Some(date(2024, 1, 1)));
        assert_eq!(cfg.end, Some(date(2025, 1, 1)));
        assert_eq!(cfg.output_dir, PathBuf::from("/tmp/data"));
        assert_eq!(cfg.concurrency, 8);
    }

    #[test]
    fn load_minimal_config() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]");
        assert_eq!(symbols(&cfg), vec!["EURUSD"]);
        assert_eq!(cfg.timeframe, vec![Timeframe::D1]);
        assert_eq!(cfg.price_type, PriceType::Bid);
        assert_eq!(cfg.start, None);
        assert_eq!(cfg.end, None);
        assert_eq!(cfg.output_dir, PathBuf::from("data"));
        assert_eq!(cfg.concurrency, 30);
    }

    #[test]
    fn duplicates_are_sorted_and_deduped() {
        let cfg = cfg(
            "symbol = [\"GBPUSD\", \"EURUSD\", \"GBPUSD\"]\ntimeframe = [\"H1\", \"D1\", \"D1\"]",
        );
        assert_eq!(symbols(&cfg), vec!["EURUSD", "GBPUSD"]);
        assert_eq!(cfg.timeframe, vec![Timeframe::H1, Timeframe::D1]);
    }

    #[test]
    fn nonexistent_path_fails() {
        assert!(AppConfig::load(Path::new("/nonexistent/config.toml")).is_err());
    }

    #[test]
    fn invalid_symbol_error_mentions_the_file() {
        let err = load("symbol = [\"../etc\"]\ntimeframe = [\"D1\"]").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("test.toml"), "{msg}");
        assert!(msg.contains("symbol"), "{msg}");
    }

    #[test]
    fn start_default_key_is_no_longer_accepted() {
        // It was an undocumented, never-documented escape hatch; now it is gone.
        let err =
            load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart_default = \"2021-01-01\"")
                .unwrap_err();
        assert!(err.to_string().contains("start_default"), "{err}");
    }

    // — timeframe / price type aliases —

    #[test]
    fn timeframe_aliases_normalize_and_dedup() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"M1\", \"1m\", \"1h\", \"H1\"]");
        assert_eq!(cfg.timeframe, vec![Timeframe::M1, Timeframe::H1]);
    }

    #[test]
    fn invalid_timeframe_fails() {
        let err = load("symbol = [\"EURUSD\"]\ntimeframe = [\"M5\"]").unwrap_err();
        assert!(err.to_string().contains("M5"), "{err}");
    }

    #[test]
    fn invalid_price_type_fails() {
        let err =
            load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nprice_type = \"MID\"").unwrap_err();
        assert!(err.to_string().contains("MID"), "{err}");
    }

    // — required fields —

    #[test]
    fn missing_symbol_fails() {
        let err = load("timeframe = [\"D1\"]").unwrap_err();
        assert!(err.to_string().contains("symbol"), "{err}");
    }

    #[test]
    fn empty_symbol_fails() {
        let err = load("symbol = []\ntimeframe = [\"D1\"]").unwrap_err();
        assert!(err.to_string().contains("symbol"), "{err}");
    }

    #[test]
    fn missing_timeframe_fails() {
        let err = load("symbol = [\"EURUSD\"]").unwrap_err();
        assert!(err.to_string().contains("timeframe"), "{err}");
    }

    #[test]
    fn empty_timeframe_fails() {
        let err = load("symbol = [\"EURUSD\"]\ntimeframe = []").unwrap_err();
        assert!(err.to_string().contains("timeframe"), "{err}");
    }

    // — concurrency —

    #[test]
    fn zero_concurrency_fails() {
        let err = load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nconcurrency = 0").unwrap_err();
        assert!(err.to_string().contains("concurrency"), "{err}");
    }

    #[test]
    fn excessive_concurrency_fails() {
        let err =
            load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nconcurrency = 10000").unwrap_err();
        assert!(err.to_string().contains("concurrency"), "{err}");
    }

    // — dates —

    #[test]
    fn invalid_date_fails() {
        assert!(
            load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"not-a-date\"").is_err()
        );
    }

    #[test]
    fn unknown_field_fails() {
        assert!(load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\ntomframe = \"D1\"").is_err());
    }

    #[test]
    fn start_after_end_fails() {
        let err = load(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2025-01-01\"\nend = \"2024-01-01\"",
        )
        .unwrap_err();
        assert!(err.to_string().contains("start"), "{err}");
    }
}
