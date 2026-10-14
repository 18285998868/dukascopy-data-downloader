use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::AppError;

/// Accepted timeframe values, mapped to Dukascopy candles_* URL segments.
const VALID_TIMEFRAMES: [&str; 6] = ["M1", "H1", "D1", "1m", "1h", "1d"];

const VALID_PRICE_TYPES: [&str; 2] = ["BID", "ASK"];

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AppConfig {
    pub symbol: Vec<String>,
    pub timeframe: Vec<String>,
    pub price_type: String,
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
            price_type: "BID".into(),
            start: None,
            end: None,
            output_dir: PathBuf::from("data"),
            concurrency: 30,
        }
    }
}

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
        let yesterday = today - chrono::Duration::days(1);
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
        if self.concurrency == 0 {
            return Err(AppError::Config(
                "concurrency must be greater than zero".into(),
            ));
        }
        if let (Some(start), Some(end)) = (self.start, self.end)
            && start > end
        {
            return Err(AppError::Config(format!(
                "start {start} is after end {end}"
            )));
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
    pub timeframe: String,
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
        assert_eq!(cfg.timeframe, vec!["D1", "H1"]);
        assert_eq!(cfg.price_type, "ASK");
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
        assert_eq!(cfg.timeframe, vec!["D1"]);
        assert_eq!(cfg.price_type, "BID");
        assert_eq!(cfg.start, None);
        assert_eq!(cfg.end, None);
        assert_eq!(cfg.output_dir, PathBuf::from("data"));
        assert_eq!(cfg.concurrency, 30);
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
        assert_eq!(cfg.timeframe, vec!["D1", "H1"]);
    }

    #[test]
    fn nonexistent_path_fails() {
        assert!(AppConfig::load(Path::new("/nonexistent/config.toml")).is_err());
    }
}
