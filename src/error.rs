use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AppError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Config error: {0}")]
    Config(String),

    #[error("API error: {0}")]
    Api(String),

    #[error("Data error: {0}")]
    Data(String),

    #[error("Date error: {0}")]
    Date(String),

    #[error("Cache error: {0}")]
    Cache(String),

    #[error("Download failed for {url}: {reason}")]
    DownloadFailed { url: String, reason: String },

    #[error("Not found")]
    NotFound { url: String },

    #[error("Task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}
