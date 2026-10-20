//! Configuration model: parsing, validation and range resolution.
//!
//! [`AppConfig`] is deserialized straight from TOML. Every field is validated
//! at load time — notably [`Symbol`], which rejects anything that is not a
//! bare Dukascopy symbol code before it can reach a URL or a local path — so
//! the download pipeline can assume well-formed input and never re-check user
//! data downstream.

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::Deserialize;

use crate::AppError;

/// Shortest accepted symbol code.
const MIN_SYMBOL_LEN: usize = 3;
/// Longest accepted symbol code.
const MAX_SYMBOL_LEN: usize = 20;

/// A validated Dukascopy symbol code such as `EURUSD`.
///
/// Symbols are interpolated verbatim into both a URL path and a local
/// directory, so the character set is restricted to `[A-Z0-9]`: anything else
/// (whitespace, `?`, `#`, `%`, separators, `..`) would either escape the
/// intended path or inject URL query/fragment delimiters. The type makes an
/// invalid symbol unrepresentable rather than merely unlikely.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Symbol(String);

impl Symbol {
    /// Validate `value` as a symbol code and wrap it.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Config`] if the length is outside
    /// `3..=20` or any character is outside `[A-Z0-9]`.
    ///
    /// # Examples
    ///
    /// ```
    /// use ddd::config::Symbol;
    ///
    /// let s = Symbol::new("EURUSD").unwrap();
    /// assert_eq!(s.as_str(), "EURUSD");
    /// assert!(Symbol::new("../etc").is_err());
    /// ```
    pub fn new(value: impl Into<String>) -> Result<Self, AppError> {
        let value = value.into();
        // Count *characters*, not bytes: the limit is documented as a
        // character count, and a two-character non-ASCII name is three
        // bytes long — measuring bytes would let it past a length check
        // that claims to count characters.
        let len = value.chars().count();
        if !(MIN_SYMBOL_LEN..=MAX_SYMBOL_LEN).contains(&len) {
            return Err(AppError::Config(format!(
                "symbol {value:?} must be {MIN_SYMBOL_LEN}..={MAX_SYMBOL_LEN} characters"
            )));
        }
        if !value
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
        {
            return Err(AppError::Config(format!(
                "symbol {value:?} must contain only uppercase ASCII letters and digits"
            )));
        }
        Ok(Self(value))
    }

    /// Borrow the underlying symbol code.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Symbol {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}

/// Canonical timeframe. Aliases (`1m`/`1h`/`1d`) parse to the same variant,
/// so `["M1", "1m"]` dedups instead of downloading twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Timeframe {
    /// One-minute bars (server file stem `candles_min_1`).
    M1,
    /// One-hour bars (server file stem `candles_hour_1`).
    H1,
    /// One-day bars (server file stem `candles_day_1`).
    D1,
}

impl FromStr for Timeframe {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "M1" | "1m" => Ok(Self::M1),
            "H1" | "1h" => Ok(Self::H1),
            "D1" | "1d" => Ok(Self::D1),
            _ => Err(AppError::Config(format!(
                "unsupported timeframe: {s}, valid values: [\"M1\", \"H1\", \"D1\", \"1m\", \"1h\", \"1d\"]"
            ))),
        }
    }
}

impl fmt::Display for Timeframe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::M1 => write!(f, "M1"),
            Self::H1 => write!(f, "H1"),
            Self::D1 => write!(f, "D1"),
        }
    }
}

impl<'de> Deserialize<'de> for Timeframe {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Canonical price type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum PriceType {
    /// Bid/ASK candles (`BID_*` server file prefix). Default.
    #[default]
    Bid,
    /// Ask candles (`ASK_*` server file prefix).
    Ask,
}

impl FromStr for PriceType {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "BID" => Ok(Self::Bid),
            "ASK" => Ok(Self::Ask),
            _ => Err(AppError::Config(format!(
                "unsupported price type: {s}, valid values: [\"BID\", \"ASK\"]"
            ))),
        }
    }
}

impl fmt::Display for PriceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bid => write!(f, "BID"),
            Self::Ask => write!(f, "ASK"),
        }
    }
}

impl<'de> Deserialize<'de> for PriceType {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

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
/// Upper bound for the *effective* start..=end range (fail fast on typos).
///
/// 366 × 15 days — fifteen years of the widest bucket (one file per day).
/// The limit is part of the public contract: a configuration that exceeds
/// it is rejected instead of expanding into tens of thousands of requests.
pub const MAX_RANGE_DAYS: i64 = 366 * 15;
/// Start date used when `start` is not configured.
pub const DEFAULT_START: chrono::NaiveDate =
    chrono::NaiveDate::from_ymd_opt(2020, 1, 1).expect("2020-01-01 is a valid calendar date");

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

    /// Resolve the download end date: `min(config.end, yesterday)`.
    ///
    /// Computed once by the caller; downstream only consumes the value.
    ///
    /// # Examples
    ///
    /// ```
    /// # use ddd::config::AppConfig;
    /// let cfg = AppConfig::default();
    /// let today = chrono::NaiveDate::from_ymd_opt(2025, 3, 2).unwrap();
    ///
    /// // Without an explicit `end`, the range stops at yesterday.
    /// assert_eq!(
    ///     cfg.resolved_end(today),
    ///     chrono::NaiveDate::from_ymd_opt(2025, 3, 1).unwrap()
    /// );
    /// ```
    #[must_use]
    pub fn resolved_end(&self, today: chrono::NaiveDate) -> chrono::NaiveDate {
        let yesterday = today.pred_opt().unwrap_or(today);
        self.end.map_or(yesterday, |e| e.min(yesterday))
    }

    /// Resolve the *effective* `(start, end)` range and enforce the day cap.
    ///
    /// This is the only place that knows both defaults, so the cap applies
    /// whether `end` was configured explicitly or defaulted to yesterday —
    /// checking only explicit pairs would let `start` alone open an unbounded
    /// range.
    ///
    /// # Errors
    ///
    /// Returns [`AppError::Config`] if the resolved range spans more than
    /// [`MAX_RANGE_DAYS`] days. A reversed range is *not* an error here; the
    /// per-pair pipeline skips it with a warning.
    ///
    /// # Examples
    ///
    /// ```
    /// # use ddd::config::AppConfig;
    /// let cfg = AppConfig::default();
    /// let today = chrono::NaiveDate::from_ymd_opt(2025, 3, 2).unwrap();
    /// let (start, end) = cfg.effective_range(today).unwrap();
    /// assert_eq!(start, chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
    /// assert_eq!(end, chrono::NaiveDate::from_ymd_opt(2025, 3, 1).unwrap());
    /// ```
    pub fn effective_range(
        &self,
        today: chrono::NaiveDate,
    ) -> Result<(chrono::NaiveDate, chrono::NaiveDate), AppError> {
        let start = self.start.unwrap_or(DEFAULT_START);
        let end = self.resolved_end(today);
        // `start..=end` is inclusive, so the range covers one more calendar
        // day than the distance between the two dates.
        let days = (end - start).num_days() + 1;
        if days > MAX_RANGE_DAYS {
            return Err(AppError::Config(format!(
                "date range {start}..={end} spans {days} days, exceeding maximum {MAX_RANGE_DAYS}"
            )));
        }
        Ok((start, end))
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

/// A download task: source URL, destination path and identifying metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadTask {
    /// Fully-qualified source URL.
    pub url: String,
    /// Absolute or `output_dir`-relative destination path for the payload.
    pub dest: PathBuf,
    /// Trading symbol this task belongs to.
    pub symbol: Symbol,
    /// Timeframe this task belongs to.
    pub timeframe: Timeframe,
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

    // — Symbol —

    #[test]
    fn symbol_accepts_uppercase_alnum() {
        for s in ["EURUSD", "XAUUSD", "USDJPY", "ABC", "CRUDEOIL"] {
            assert_eq!(Symbol::new(s).unwrap().as_str(), s);
        }
    }

    #[test]
    fn symbol_rejects_path_traversal() {
        for bad in ["../..", "EUR/USD", "EUR\\USD", "..", "/etc"] {
            let err = Symbol::new(bad).unwrap_err();
            assert!(err.to_string().contains("symbol"), "{bad}: {err}");
        }
    }

    #[test]
    fn symbol_rejects_url_delimiters() {
        for bad in [
            "EURUSD?x=1",
            "EURUSD#f",
            "EUR USD",
            "eurusd",
            "EUR-US",
            "EUR/USD",
        ] {
            let err = Symbol::new(bad).unwrap_err();
            assert!(err.to_string().contains("symbol"), "{bad}: {err}");
        }
    }

    #[test]
    fn symbol_rejects_bad_length() {
        for bad in ["EU", "", "A23456789012345678901234"] {
            assert!(Symbol::new(bad).is_err(), "{bad} should be rejected");
        }
    }

    /// The length limit counts *characters*, not bytes.
    ///
    /// A two-character non-ASCII symbol is three bytes long, so a
    /// byte-based check would let it through a limit that is documented
    /// as 3..=20 characters.
    #[test]
    fn symbol_length_is_measured_in_characters() {
        // "欧元" is two characters but six bytes.
        let err = Symbol::new("欧元").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("3..=20 characters"), "{msg}");
        assert!(msg.contains("欧元"), "{msg}");
    }

    /// The length check runs before the charset check, so a multi-byte
    /// symbol that is *long enough* still fails on its characters.
    #[test]
    fn symbol_rejects_non_ascii_even_when_it_is_long_enough() {
        let err = Symbol::new("欧元欧元欧元").unwrap_err();
        assert!(err.to_string().contains("uppercase ASCII"), "{err}");
    }

    #[test]
    fn symbol_accepts_boundary_lengths() {
        assert!(Symbol::new("ABC").is_ok());
        assert!(Symbol::new("ABCDEFGHIJKLMNOPQRST").is_ok()); // exactly 20
        assert!(Symbol::new("ABCDEFGHIJKLMNOPQRSTU").is_err()); // 21
    }

    #[test]
    fn symbol_display_and_parse_round_trip() {
        let s = Symbol::new("EURUSD").unwrap();
        assert_eq!(s.to_string(), "EURUSD");
        assert_eq!("EURUSD".parse::<Symbol>().unwrap(), s);
        assert!("nope".parse::<Symbol>().is_err());
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
    fn timeframe_display_round_trips() {
        for tf in [Timeframe::M1, Timeframe::H1, Timeframe::D1] {
            assert_eq!(tf.to_string().parse::<Timeframe>().unwrap(), tf);
        }
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

    #[test]
    fn price_type_display_round_trips() {
        for pt in [PriceType::Bid, PriceType::Ask] {
            assert_eq!(pt.to_string().parse::<PriceType>().unwrap(), pt);
        }
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

    // — resolved_end —

    #[test]
    fn resolved_end_clamps_future_to_yesterday() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nend = \"2030-01-10\"");
        assert_eq!(cfg.resolved_end(date(2030, 1, 10)), date(2030, 1, 9));
    }

    #[test]
    fn resolved_end_defaults_to_yesterday() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]");
        assert_eq!(cfg.resolved_end(date(2024, 6, 15)), date(2024, 6, 14));
    }

    #[test]
    fn resolved_end_keeps_past_end_untouched() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nend = \"2020-05-06\"");
        assert_eq!(cfg.resolved_end(date(2024, 6, 15)), date(2020, 5, 6));
    }

    // — effective_range —

    #[test]
    fn effective_range_defaults_to_2020_and_yesterday() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]");
        assert_eq!(
            cfg.effective_range(date(2024, 6, 15)).unwrap(),
            (date(2020, 1, 1), date(2024, 6, 14))
        );
    }

    #[test]
    fn default_start_is_2020_01_01() {
        assert_eq!(DEFAULT_START, date(2020, 1, 1));
    }

    #[test]
    fn effective_range_rejects_excessive_explicit_range() {
        let cfg = cfg(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2010-01-01\"\nend = \"2030-01-01\"",
        );
        let err = cfg.effective_range(date(2026, 1, 1)).unwrap_err();
        assert!(err.to_string().contains("exceeding maximum"), "{err}");
    }

    /// A far-future `end` is clamped to yesterday *before* the day cap is
    /// measured, so a `9999-12-31` typo cannot open an oversized range.
    #[test]
    fn effective_range_clamps_far_future_end_before_applying_cap() {
        let cfg = cfg(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2020-01-01\"\nend = \"9999-12-31\"",
        );
        let (start, end) = cfg.effective_range(date(2026, 1, 1)).unwrap();
        assert_eq!(start, date(2020, 1, 1));
        assert_eq!(end, date(2025, 12, 31));
    }

    #[test]
    fn effective_range_accepts_range_well_inside_the_cap() {
        let cfg = cfg(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2025-01-01\"\nend = \"2025-12-31\"",
        );
        assert!(cfg.effective_range(date(2026, 1, 1)).is_ok());
    }

    /// Regression: the day cap used to be checked only when `end` was set
    /// explicitly, so `start` alone could open a ~126 year range.
    #[test]
    fn effective_range_rejects_excessive_range_when_end_is_omitted() {
        let text = "symbol = [\"EURUSD\"]\ntimeframe = [\"M1\"]\nstart = \"1900-01-01\"";
        let cfg = load(text).expect("load itself succeeds");
        let err = cfg.effective_range(date(2026, 10, 18)).unwrap_err();
        assert!(err.to_string().contains("exceeding maximum"), "{err}");
    }

    #[test]
    fn effective_range_allows_reversed_range_without_error() {
        let cfg = cfg("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2024-01-01\"");
        let (start, end) = cfg.effective_range(date(2023, 1, 1)).unwrap();
        assert!(
            start > end,
            "reversed range must survive for run_one to skip"
        );
    }

    /// The cap counts the *calendar days in the inclusive range*: an
    /// off-by-one must not let one extra day through.
    #[test]
    fn effective_range_counts_the_inclusive_range() {
        let start = date(2020, 1, 1);
        let end = start + chrono::Duration::days(MAX_RANGE_DAYS);
        let cfg = load(&format!(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"{start}\"\nend = \"{end}\""
        ))
        .expect("load itself succeeds");

        // `start..=end` covers MAX_RANGE_DAYS + 1 calendar days.
        let err = cfg
            .effective_range(end + chrono::Duration::days(1))
            .unwrap_err();
        assert!(err.to_string().contains("exceeding maximum"), "{err}");
    }

    #[test]
    fn effective_range_accepts_exactly_the_cap() {
        let start = date(2020, 1, 1);
        let end = start + chrono::Duration::days(MAX_RANGE_DAYS - 1);
        let cfg = load(&format!(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"{start}\"\nend = \"{end}\""
        ))
        .expect("load itself succeeds");

        // `start..=end` covers exactly MAX_RANGE_DAYS calendar days.
        let (s, e) = cfg
            .effective_range(end + chrono::Duration::days(1))
            .unwrap();
        assert_eq!(s, start);
        assert_eq!(e, end);
    }

    /// The cap is part of the public contract, so it is exported rather
    /// than kept private to the module.
    #[test]
    fn the_range_cap_is_a_public_constant() {
        assert_eq!(MAX_RANGE_DAYS, 366 * 15);
        assert_eq!(crate::MAX_RANGE_DAYS, MAX_RANGE_DAYS);
    }
}
