use std::path::Path;

use serde::Deserialize;

use crate::error::AppError;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub symbol: Vec<String>,
    pub timeframe: Vec<String>,
    pub price_type: String,
    pub start: String,
    pub end: String,
    pub dir: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            symbol: vec!["EURUSD".into()],
            timeframe: vec!["D1".into()],
            price_type: "BID".into(),
            start: "2020-01-01".into(),
            end: "2025-12-31".into(),
            dir: "data".into(),
        }
    }
}

impl Config {
    pub fn load(path: Option<&Path>) -> Result<Self, AppError> {
        let default_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("config")
            .join("config.toml");
        let p = path.unwrap_or(&default_path);

        let mut cfg: Config = config::Config::builder()
            .add_source(config::File::from(p))
            .build()
            .map_err(|e| AppError::Config(e.to_string()))?
            .try_deserialize()
            .map_err(|e| AppError::Config(e.to_string()))?;

        if cfg.symbol.is_empty() {
            cfg.symbol = Config::default().symbol;
        }
        if cfg.timeframe.is_empty() {
            cfg.timeframe = Config::default().timeframe;
        }
        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_config_default() {
        let cfg = Config::default();
        assert_eq!(cfg.symbol, vec!["EURUSD"]);
        assert!(cfg.timeframe.contains(&"D1".to_string()));
        assert_eq!(cfg.price_type, "BID");
    }

    #[test]
    fn load_default_path() {
        let cfg = Config::load(None).unwrap();
        assert!(!cfg.symbol.is_empty());
        assert!(!cfg.timeframe.is_empty());
        assert_eq!(cfg.price_type, "BID");
    }

    #[test]
    fn load_custom_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.toml");
        let content = r#"
symbol = ["BTCUSD"]
timeframe = ["M1", "H1"]
price_type = "ASK"
start = "2024-01-01"
end = "2024-06-01"
dir = "/tmp/test"
"#;
        std::fs::write(&path, content).unwrap();
        let cfg = Config::load(Some(&path)).unwrap();
        assert_eq!(cfg.symbol, vec!["BTCUSD"]);
        assert_eq!(cfg.timeframe, vec!["M1", "H1"]);
        assert_eq!(cfg.price_type, "ASK");
        assert_eq!(cfg.start, "2024-01-01");
        assert_eq!(cfg.end, "2024-06-01");
    }

    #[test]
    fn load_invalid_path() {
        let path = Path::new("/nonexistent/config.toml");
        assert!(Config::load(Some(path)).is_err());
    }
}
