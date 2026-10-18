use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AppError {
    /// HTTP transport error.
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    /// I/O error.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON (de)serialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Configuration error.
    #[error("config error: {0}")]
    Config(String),

    /// API-level error.
    #[error("api error: {0}")]
    Api(String),

    /// Data processing error.
    #[error("data error: {0}")]
    Data(String),

    /// Date handling error.
    #[error("date error: {0}")]
    Date(String),

    /// A download failed.
    #[error("download failed for {url}: {reason}")]
    DownloadFailed { url: String, reason: String },

    /// Requested resource was not found (HTTP 404).
    #[error("not found: {url}")]
    NotFound { url: String },

    /// Tokio task join error.
    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}
