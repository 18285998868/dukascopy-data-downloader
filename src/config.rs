use std::path::{Path, PathBuf};

use chrono::NaiveDate;
use serde::Deserialize;

use crate::AppError;

/// Accepted timeframe values, mapped to Dukascopy candles_* URL segments.
const VALID_TIMEFRAMES: [&str; 6] = ["M1", "H1", "D1", "1m", "1h", "1d"];

const VALID_PRICE_TYPES: [&str; 2] = ["BID", "ASK"];

fn default_price_type() -> String {
    "BID".into()
}

fn default_start() -> NaiveDate {
    NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()
}

fn default_end() -> NaiveDate {
    chrono::Local::now().date_naive() - chrono::Duration::days(1)
}

fn default_dir() -> String {
    "data".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub symbol: Vec<String>,
    pub timeframe: Vec<String>,
    #[serde(default = "default_price_type")]
    pub price_type: String,
    #[serde(default = "default_start")]
    pub start: NaiveDate,
    #[serde(default = "default_end")]
    pub end: NaiveDate,
    #[serde(default = "default_dir")]
    pub dir: String,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, AppError> {
        let raw = std::fs::read_to_string(path).map_err(|e| AppError::Config(e.to_string()))?;
        let cfg: Self = toml::from_str(&raw).map_err(|e| AppError::Config(e.to_string()))?;
        cfg.validate()
    }

    fn validate(self) -> Result<Self, AppError> {
        if self.symbol.is_empty() {
            return Err(AppError::Config(
                "symbol must contain at least one symbol".into(),
            ));
        }
        if let Some(bad) = self
            .timeframe
            .iter()
            .find(|tf| !VALID_TIMEFRAMES.contains(&tf.as_str()))
        {
            return Err(AppError::Config(format!(
                "unsupported timeframe: {bad}, valid values: {VALID_TIMEFRAMES:?}"
            )));
        }
        if !VALID_PRICE_TYPES.contains(&self.price_type.as_str()) {
            return Err(AppError::Config(format!(
                "unsupported price type: {}, valid values: {VALID_PRICE_TYPES:?}",
                self.price_type
            )));
        }
        if self.start > self.end {
            return Err(AppError::Config(format!(
                "start date {} is after end date {}",
                self.start, self.end
            )));
        }
        Ok(self)
    }
}

/// Which calendar bucket a timeframe maps to: one file spans a year/month/day.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PeriodKind {
    Year,
    Month,
    Day,
}

/// A download task: source URL, destination path and identifying metadata.
#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub url: String,
    pub dest: PathBuf,
    pub symbol: String,
    pub timeframe: String,
    pub date: NaiveDate,
    pub period: PeriodKind,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::path::PathBuf;

    fn write_config(contents: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "{contents}").unwrap();
        drop(f);
        (dir, path)
    }

    fn load(contents: &str) -> Result<Config, AppError> {
        let (_dir, path) = write_config(contents);
        Config::load(&path)
    }

    #[test]
    fn load_full_config() {
        let cfg = load(
            "symbol = [\"EURUSD\", \"GBPUSD\"]\n\
             timeframe = [\"D1\", \"H1\"]\n\
             price_type = \"ASK\"\n\
             start = \"2024-01-01\"\n\
             end = \"2025-01-01\"\n\
             dir = \"/tmp/data\"",
        )
        .unwrap();
        assert_eq!(cfg.symbol, vec!["EURUSD", "GBPUSD"]);
        assert_eq!(cfg.timeframe, vec!["D1", "H1"]);
        assert_eq!(cfg.price_type, "ASK");
        assert_eq!(cfg.start, NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(cfg.end, NaiveDate::from_ymd_opt(2025, 1, 1).unwrap());
        assert_eq!(cfg.dir, "/tmp/data");
    }

    #[test]
    fn load_minimal_config() {
        let cfg = load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]").unwrap();
        assert_eq!(cfg.symbol, vec!["EURUSD"]);
        assert_eq!(cfg.timeframe, vec!["D1"]);
        assert_eq!(cfg.price_type, "BID");
        assert_eq!(cfg.start, default_start());
        assert_eq!(cfg.end, default_end());
        assert_eq!(cfg.dir, "data");
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
    fn empty_timeframe_ok() {
        let cfg = load("symbol = [\"EURUSD\"]\ntimeframe = []").unwrap();
        assert!(cfg.timeframe.is_empty());
    }

    #[test]
    fn invalid_price_type_fails() {
        let err =
            load("symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nprice_type = \"MID\"").unwrap_err();
        assert!(err.to_string().contains("MID"));
    }

    #[test]
    fn start_after_end_fails() {
        let err = load(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\n\
             start = \"2025-01-01\"\nend = \"2024-01-01\"",
        )
        .unwrap_err();
        assert!(err.to_string().contains("start date"));
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
    fn nonexistent_path_fails() {
        assert!(Config::load(Path::new("/nonexistent/config.toml")).is_err());
    }
}
