use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::Deserialize;

use crate::AppError;

/// Canonical timeframe. Aliases (`1m`/`1h`/`1d`) parse to the same variant,
/// so `["M1", "1m"]` dedups instead of downloading twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Timeframe {
    M1,
    H1,
    D1,
}

impl FromStr for Timeframe {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "M1" | "1m" => Ok(Self::M1),
            "H1" | "1h" => Ok(Self::H1),
            "D1" | "1d" => Ok(Self::D1),
            _ => Err(format!(
                "unsupported timeframe: {s}, valid values: [\"M1\", \"H1\", \"D1\", \"1m\", \"1h\", \"1d\"]"
            )),
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
pub enum PriceType {
    #[default]
    Bid,
    Ask,
}

impl FromStr for PriceType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "BID" => Ok(Self::Bid),
            "ASK" => Ok(Self::Ask),
            _ => Err(format!(
                "unsupported price type: {s}, valid values: [\"BID\", \"ASK\"]"
            )),
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

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AppConfig {
    pub symbol: Vec<String>,
    pub timeframe: Vec<Timeframe>,
    pub price_type: PriceType,
    pub start: Option<chrono::NaiveDate>,
    pub end: Option<chrono::NaiveDate>,
    pub output_dir: PathBuf,
    pub concurrency: usize,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            symbol: Vec::new(),
            timeframe: Vec::new(),
            price_type: PriceType::Bid,
            start: None,
            end: None,
            output_dir: PathBuf::from("data"),
            concurrency: 30,
        }
    }
}

/// Upper bound for per-pair concurrency (aligned with client pool).
const MAX_CONCURRENCY: usize = 256;
/// Upper bound for an explicit start..=end range (fail fast on typos like year 9999).
const MAX_RANGE_DAYS: i64 = 366 * 15;

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self, AppError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| AppError::Config(format!("{}: {e}", path.display())))?;
        let cfg: Self = toml::from_str(&raw)
            .map_err(|e| AppError::Config(format!("{}: {e}", path.display())))?;
        cfg.validate()
    }

    /// Resolve the download end date: `min(config.end, yesterday)`.
    /// Computed once by the caller; downstream only consumes the value.
    pub fn resolved_end(&self, today: chrono::NaiveDate) -> chrono::NaiveDate {
        let yesterday = today.pred_opt().unwrap_or(today);
        self.end.map(|e| e.min(yesterday)).unwrap_or(yesterday)
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
            if (end - start).num_days() > MAX_RANGE_DAYS {
                return Err(AppError::Config(format!(
                    "date range {start}..={end} exceeds maximum {MAX_RANGE_DAYS} days"
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
#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub url: String,
    pub dest: PathBuf,
    pub symbol: String,
    pub timeframe: Timeframe,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_config(contents: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "{contents}").unwrap();
        drop(f);
        (dir, path)
    }

    fn load(contents: &str) -> Result<AppConfig, AppError> {
        let (_dir, path) = write_config(contents);
        AppConfig::load(&path)
    }

    #[test]
    fn load_full_config() {
        let cfg = load(
            "symbol = [\"EURUSD\", \"GBPUSD\"]\n\
             timeframe = [\"D1\", \"H1\"]\n\
             price_type = \"ASK\"\n\
             start = \"2024-01-01\"\n\
             end = \"2025-01-01\"\n\
             output_dir = \"/tmp/data\"\n\
             concurrency = 8",
        )
        .unwrap();
        assert_eq!(cfg.symbol, vec!["EURUSD", "GBPUSD"]);
        assert_eq!(cfg.timeframe, vec![Timeframe::H1, Timeframe::D1]);
        assert_eq!(cfg.price_type, PriceType::Ask);
        assert_eq!(
            cfg.start,
            Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap())
        );
        assert_eq!(
            cfg.end,
            Some(chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap())
        );
        assert_eq!(cfg.output_dir, PathBuf::from("/tmp/data"));
        assert_eq!(cfg.concurrency, 8);
    }

    #[test]
    fn load_minimal_config() {
        let cfg = load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]").unwrap();
        assert_eq!(cfg.symbol, vec!["EURUSD"]);
        assert_eq!(cfg.timeframe, vec![Timeframe::D1]);
        assert_eq!(cfg.price_type, PriceType::Bid);
        assert_eq!(cfg.start, None);
        assert_eq!(cfg.end, None);
        assert_eq!(cfg.output_dir, PathBuf::from("data"));
        assert_eq!(cfg.concurrency, 30);
    }

    #[test]
    fn timeframe_aliases_normalize_and_dedup() {
        let cfg =
            load("symbol = [\"EURUSD\"]\ntimeframe = [\"M1\", \"1m\", \"1h\", \"H1\"]").unwrap();
        assert_eq!(cfg.timeframe, vec![Timeframe::M1, Timeframe::H1]);
    }

    #[test]
    fn invalid_timeframe_fails() {
        let err = load("symbol = [\"EURUSD\"]\ntimeframe = [\"M5\"]").unwrap_err();
        assert!(err.to_string().contains("M5"));
    }

    #[test]
    fn missing_symbol_fails() {
        let err = load("timeframe = [\"D1\"]").unwrap_err();
        assert!(err.to_string().contains("symbol"));
    }

    #[test]
    fn empty_symbol_fails() {
        let err = load("symbol = []\ntimeframe = [\"D1\"]").unwrap_err();
        assert!(err.to_string().contains("symbol"));
    }

    #[test]
    fn missing_timeframe_fails() {
        let err = load("symbol = [\"EURUSD\"]").unwrap_err();
        assert!(err.to_string().contains("timeframe"));
    }

    #[test]
    fn empty_timeframe_fails() {
        let err = load("symbol = [\"EURUSD\"]\ntimeframe = []").unwrap_err();
        assert!(err.to_string().contains("timeframe"));
    }

    #[test]
    fn invalid_price_type_fails() {
        let err =
            load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nprice_type = \"MID\"").unwrap_err();
        assert!(err.to_string().contains("MID"));
    }

    #[test]
    fn zero_concurrency_fails() {
        let err = load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nconcurrency = 0").unwrap_err();
        assert!(err.to_string().contains("concurrency"));
    }

    #[test]
    fn excessive_concurrency_fails() {
        let err =
            load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nconcurrency = 10000").unwrap_err();
        assert!(err.to_string().contains("concurrency"));
    }

    #[test]
    fn excessive_range_fails() {
        let err = load(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\n\
             start = \"2020-01-01\"\nend = \"9999-12-31\"",
        )
        .unwrap_err();
        assert!(err.to_string().contains("range"));
    }

    #[test]
    fn start_after_end_fails() {
        let err = load(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\n\
             start = \"2025-01-01\"\nend = \"2024-01-01\"",
        )
        .unwrap_err();
        assert!(err.to_string().contains("start"));
    }

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
    fn resolved_end_clamps_future_to_yesterday() {
        let cfg =
            load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nend = \"2030-01-10\"").unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2030, 1, 10).unwrap();
        assert_eq!(
            cfg.resolved_end(today),
            chrono::NaiveDate::from_ymd_opt(2030, 1, 9).unwrap()
        );
    }

    #[test]
    fn resolved_end_defaults_to_yesterday() {
        let cfg = load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]").unwrap();
        let today = chrono::NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        assert_eq!(
            cfg.resolved_end(today),
            chrono::NaiveDate::from_ymd_opt(2024, 6, 14).unwrap()
        );
    }

    #[test]
    fn duplicates_are_sorted_and_deduped() {
        let cfg = load(
            "symbol = [\"GBPUSD\", \"EURUSD\", \"GBPUSD\"]\ntimeframe = [\"H1\", \"D1\", \"D1\"]",
        )
        .unwrap();
        assert_eq!(cfg.symbol, vec!["EURUSD", "GBPUSD"]);
        assert_eq!(cfg.timeframe, vec![Timeframe::H1, Timeframe::D1]);
    }

    #[test]
    fn nonexistent_path_fails() {
        assert!(AppConfig::load(Path::new("/nonexistent/config.toml")).is_err());
    }
}
