//! Configuration model: parsing, validation and range resolution.
//!
//! `RawConfig` mirrors the TOML file and is only used inside this module.
//! [`AppConfig`] is the validated form: its fields are private and exposed
//! through read-only accessors, so every value the pipeline sees has already
//! passed validation — notably [`Symbol`], which rejects anything that is not
//! a bare Dukascopy symbol code before it can reach a URL or a local path.
//!
//! The domain types live next to this module: [`Symbol`], [`Timeframe`],
//! [`PriceType`], [`BucketKey`] and the date-range defaults. Download tasks
//! are execution objects and live in [`crate::app::download`].

mod bucket;
mod price_type;
mod range;
mod symbol;
mod timeframe;

pub use bucket::BucketKey;
pub(crate) use bucket::{buckets_between, last_closed_day};
pub use price_type::PriceType;
pub use range::{DEFAULT_START, MAX_RANGE_DAYS};
pub use symbol::Symbol;
pub use timeframe::Timeframe;

use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::AppError;

/// TOML-facing configuration.
///
/// Kept private: callers go through [`AppConfig`], which has already
/// validated every field.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    symbol: Vec<Symbol>,
    timeframe: Vec<Timeframe>,
    #[serde(default)]
    price_type: PriceType,
    #[serde(default)]
    start: Option<chrono::NaiveDate>,
    #[serde(default)]
    end: Option<chrono::NaiveDate>,
    #[serde(default = "default_output_dir")]
    output_dir: PathBuf,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
}

fn default_output_dir() -> PathBuf {
    PathBuf::from("data")
}

fn default_concurrency() -> usize {
    30
}

/// Upper bound for the shared download gate (also the client pool size).
const MAX_CONCURRENCY: usize = 256;

/// Validated application configuration.
///
/// Construct it with [`AppConfig::load`] or [`AppConfig::from_toml_str`];
/// fields are read-only so the invariants checked at construction (non-empty
/// symbol/timeframe lists, `1..=256` concurrency, no reversed explicit range)
/// cannot be broken afterwards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppConfig {
    symbol: Vec<Symbol>,
    timeframe: Vec<Timeframe>,
    price_type: PriceType,
    start: Option<chrono::NaiveDate>,
    end: Option<chrono::NaiveDate>,
    output_dir: PathBuf,
    concurrency: NonZeroUsize,
}

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
    /// let cfg = ddd::AppConfig::load(Path::new("config.toml"))?;
    /// println!("{} symbols", cfg.symbols().len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn load(path: &Path) -> Result<Self, AppError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| AppError::Config(format!("{}: {e}", path.display())))?;
        Self::from_toml_str(&raw).map_err(|e| match e {
            AppError::Config(message) => AppError::Config(format!("{}: {message}", path.display())),
            other => other,
        })
    }

    /// Parse and validate configuration from a TOML string.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Config`] if the text is not valid TOML, contains
    /// unknown keys, or violates a validation rule.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), ddd::AppError> {
    /// let cfg = ddd::AppConfig::from_toml_str(
    ///     "symbol = [\"EURUSD\"]\ntimeframe = [\"1d\"]",
    /// )?;
    /// assert_eq!(cfg.symbols().len(), 1);
    /// assert_eq!(cfg.timeframes(), &[ddd::Timeframe::D1]);
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_toml_str(text: &str) -> Result<Self, AppError> {
        let raw: RawConfig = toml::from_str(text).map_err(|e| AppError::Config(e.to_string()))?;
        Self::try_from(raw)
    }

    /// Trading symbols to download, sorted and deduplicated.
    #[must_use]
    pub fn symbols(&self) -> &[Symbol] {
        &self.symbol
    }

    /// Timeframes to download, sorted and deduplicated.
    #[must_use]
    pub fn timeframes(&self) -> &[Timeframe] {
        &self.timeframe
    }

    /// Quote type to download.
    #[must_use]
    pub const fn price_type(&self) -> PriceType {
        self.price_type
    }

    /// Explicit range start, if configured.
    #[must_use]
    pub const fn start(&self) -> Option<chrono::NaiveDate> {
        self.start
    }

    /// Explicit range end, if configured.
    #[must_use]
    pub const fn end(&self) -> Option<chrono::NaiveDate> {
        self.end
    }

    /// Data root directory; the `dukascopy/` prefix is appended automatically.
    #[must_use]
    pub fn output_dir(&self) -> &Path {
        &self.output_dir
    }

    /// Maximum downloads in flight **across the whole run**, not per pair.
    #[must_use]
    pub const fn concurrency(&self) -> NonZeroUsize {
        self.concurrency
    }
}

impl TryFrom<RawConfig> for AppConfig {
    type Error = AppError;

    fn try_from(mut raw: RawConfig) -> Result<Self, Self::Error> {
        if raw.symbol.is_empty() {
            return Err(AppError::Config(
                "symbol must contain at least one symbol".into(),
            ));
        }
        if raw.timeframe.is_empty() {
            return Err(AppError::Config(
                "timeframe must contain at least one timeframe".into(),
            ));
        }
        let concurrency = NonZeroUsize::new(raw.concurrency)
            .ok_or_else(|| AppError::Config("concurrency must be greater than zero".into()))?;
        if concurrency.get() > MAX_CONCURRENCY {
            return Err(AppError::Config(format!(
                "concurrency {} exceeds maximum {MAX_CONCURRENCY}",
                concurrency.get()
            )));
        }
        if let (Some(start), Some(end)) = (raw.start, raw.end) {
            if start > end {
                return Err(AppError::Config(format!(
                    "start {start} is after end {end}"
                )));
            }
        }
        raw.symbol.sort();
        raw.symbol.dedup();
        raw.timeframe.sort();
        raw.timeframe.dedup();
        Ok(Self {
            symbol: raw.symbol,
            timeframe: raw.timeframe,
            price_type: raw.price_type,
            start: raw.start,
            end: raw.end,
            output_dir: raw.output_dir,
            concurrency,
        })
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
        cfg.symbols().iter().map(Symbol::as_str).collect()
    }

    #[test]
    fn symbol_sorts_lexicographically() {
        let cfg = cfg("symbol = [\"GBPUSD\", \"EURUSD\"]\ntimeframe = [\"D1\"]");
        assert_eq!(symbols(&cfg), vec!["EURUSD", "GBPUSD"]);
    }

    #[test]
    fn symbol_dedups_after_normalisation() {
        let cfg = cfg("symbol = [\"EURUSD\", \"EURUSD\"]\ntimeframe = [\"D1\"]");
        assert_eq!(cfg.symbols().len(), 1);
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
        assert_eq!(cfg.timeframes(), &[Timeframe::H1, Timeframe::D1]);
        assert_eq!(cfg.price_type(), PriceType::Ask);
        assert_eq!(cfg.start(), Some(date(2024, 1, 1)));
        assert_eq!(cfg.end(), Some(date(2025, 1, 1)));
        assert_eq!(cfg.output_dir(), Path::new("/tmp/data"));
        assert_eq!(cfg.concurrency().get(), 8);
    }

    #[test]
    fn load_minimal_config() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]");
        assert_eq!(symbols(&cfg), vec!["EURUSD"]);
        assert_eq!(cfg.timeframes(), &[Timeframe::D1]);
        assert_eq!(cfg.price_type(), PriceType::Bid);
        assert_eq!(cfg.start(), None);
        assert_eq!(cfg.end(), None);
        assert_eq!(cfg.output_dir(), Path::new("data"));
        assert_eq!(cfg.concurrency().get(), 30);
    }

    #[test]
    fn from_toml_str_matches_the_file_loader() {
        let from_text =
            AppConfig::from_toml_str("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]").unwrap();
        let from_file = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]");
        assert_eq!(from_text, from_file);
    }

    #[test]
    fn duplicates_are_sorted_and_deduped() {
        let cfg = cfg(
            "symbol = [\"GBPUSD\", \"EURUSD\", \"GBPUSD\"]\ntimeframe = [\"H1\", \"D1\", \"D1\"]",
        );
        assert_eq!(symbols(&cfg), vec!["EURUSD", "GBPUSD"]);
        assert_eq!(cfg.timeframes(), &[Timeframe::H1, Timeframe::D1]);
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
        // It was an undocumented escape hatch; now it is gone.
        let err =
            load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart_default = \"2021-01-01\"")
                .unwrap_err();
        assert!(err.to_string().contains("start_default"), "{err}");
    }

    // — timeframe / price type aliases —

    #[test]
    fn timeframe_aliases_normalize_and_dedup() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"M1\", \"1m\", \"1h\", \"H1\"]");
        assert_eq!(cfg.timeframes(), &[Timeframe::M1, Timeframe::H1]);
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
