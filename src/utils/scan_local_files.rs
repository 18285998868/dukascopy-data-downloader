use std::collections::HashSet;
use std::path::Path;

use crate::AppError;

/// Scan data_dir for existing .bi5 files.
///
/// Also removes stale .tmp files during the same walk.
///
/// # Errors
///
/// Returns [`AppError::Join`] if the blocking task panics.
pub async fn scan_local_files(data_dir: &Path) -> Result<HashSet<String>, AppError> {
    let dir = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        if !dir.exists() {
            return Ok(HashSet::new());
        }
        let mut files = HashSet::new();
        for e in walkdir::WalkDir::new(&dir) {
            let e = match e {
                Ok(e) => e,
                Err(err) => {
                    tracing::warn!(path = %dir.display(), error = %err, "walkdir entry failed");
                    continue;
                }
            };
            if !e.file_type().is_file() {
                continue;
            }
            let Ok(rel) = e.path().strip_prefix(&dir) else {
                tracing::warn!(path = %e.path().display(), "entry outside scan root, skipped");
                continue;
            };
            let s = rel.to_string_lossy();

            if s.ends_with(".tmp") {
                if let Err(err) = std::fs::remove_file(e.path()) {
                    tracing::warn!(
                        path = %e.path().display(),
                        "failed to remove tmp file: {err}",
                    );
                }
                continue;
            }

            if s.ends_with(".bi5") {
                files.insert(s.into_owned());
            }
        }
        Ok::<_, AppError>(files)
    })
    .await
    .map_err(AppError::from)?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scan_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn scan_missing_dir_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        let files = scan_local_files(&missing).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn scan_finds_bi5_files() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("dukascopy/EURUSD/2025");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("BID_candles_day_1.bi5"), b"bi5").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(files.contains("dukascopy/EURUSD/2025/BID_candles_day_1.bi5"));
    }

    #[tokio::test]
    async fn scan_ignores_non_data_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("readme.txt"), b"hello").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn scan_removes_tmp_files() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("dukascopy/EURUSD/2025");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(data.join("BID_candles_day_1.bi5"), b"bi5").unwrap();
        std::fs::write(data.join("BID_candles_day_1.bi5.tmp"), b"tmp").unwrap();
        let files = scan_local_files(dir.path()).await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(files.contains("dukascopy/EURUSD/2025/BID_candles_day_1.bi5"));
        assert!(!data.join("BID_candles_day_1.bi5.tmp").exists());
    }
}
