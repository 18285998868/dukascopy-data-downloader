use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("config error: {0}")]
    Config(String),

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("not found: {url}")]
    NotFound { url: String },

    #[error("download failed: {url}: {reason}")]
    DownloadFailed { url: String, reason: String },

    #[error("data error: {0}")]
    Data(String),
}
