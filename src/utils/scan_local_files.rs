//! Local file scanning for the download cache.

use std::collections::HashSet;
use std::path::Path;

use crate::AppError;

/// Scratch-file suffix used while a download is in flight.
const TMP_SUFFIX: &[u8] = b".bi5.tmp";
/// Final payload suffix.
const BI5_SUFFIX: &[u8] = b".bi5";

/// Scan `data_dir` for existing `.bi5` files, removing stale `.bi5.tmp` files.
///
/// Returns a set of `data_dir`-relative paths (e.g.
/// `dukascopy/EURUSD/2025/BID_candles_day_1.bi5`), which is exactly the shape
/// produced by [`crate::app::build_urls::build_urls`], so callers can test
/// membership directly. A missing directory yields an empty set.
///
/// Only `*.bi5.tmp` scratch files are deleted — a blanket `*.tmp` sweep would
/// destroy unrelated temporary files if `data_dir` ever points somewhere shared.
///
/// # Errors
///
/// Returns [`AppError::Join`] if the blocking walk task panics.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> Result<(), ddd::AppError> {
/// let cached = ddd::utils::scan_local_files::scan_local_files(std::path::Path::new("data")).await?;
/// println!("{} cached files", cached.len());
/// # Ok(())
/// # }
/// ```
pub async fn scan_local_files(data_dir: &Path) -> Result<HashSet<String>, AppError> {
    let dir = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        if !dir.exists() {
            return Ok(HashSet::new());
        }
        let mut files = HashSet::new();
        for entry in walkdir::WalkDir::new(&dir) {
            let entry = match entry {
                Ok(e) => e,
                Err(err) => {
                    tracing::warn!(path = %dir.display(), error = %err, "walkdir entry failed");
                    continue;
                }
            };
            if !entry.file_type().is_file() {
                continue;
            }
            let Ok(rel) = entry.path().strip_prefix(&dir) else {
                tracing::warn!(path = %entry.path().display(), "entry outside scan root, skipped");
                continue;
            };

            // Check the suffix on raw bytes first: allocating a `String` for
            // every entry in a tree of ~100k files is pure overhead, and only
            // the `.bi5` survivors need one anyway.
            let name = rel.as_os_str().as_encoded_bytes();

            if name.ends_with(TMP_SUFFIX) {
                if let Err(err) = std::fs::remove_file(entry.path()) {
                    tracing::warn!(path = %entry.path().display(), "failed to remove tmp file: {err}");
                }
                continue;
            }

            if name.ends_with(BI5_SUFFIX) {
                files.insert(rel.to_string_lossy().into_owned());
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

    fn write(path: &std::path::Path, body: &[u8]) {
        if let Some(p) = path.parent() {
            std::fs::create_dir_all(p).unwrap();
        }
        std::fs::write(path, body).unwrap();
    }

    #[tokio::test]
    async fn empty_dir_yields_empty_set() {
        let dir = tempfile::tempdir().unwrap();
        assert!(scan_local_files(dir.path()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn missing_dir_yields_empty_set() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        assert!(scan_local_files(&missing).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn finds_nested_bi5_files_as_relative_paths() {
        let dir = tempfile::tempdir().unwrap();
        write(
            &dir.path()
                .join("dukascopy/EURUSD/2025/BID_candles_day_1.bi5"),
            b"bi5",
        );
        write(
            &dir.path()
                .join("dukascopy/EURUSD/2025/06/BID_candles_hour_1.bi5"),
            b"bi5",
        );

        let files = scan_local_files(dir.path()).await.unwrap();
        assert_eq!(files.len(), 2);
        assert!(files.contains("dukascopy/EURUSD/2025/BID_candles_day_1.bi5"));
        assert!(files.contains("dukascopy/EURUSD/2025/06/BID_candles_hour_1.bi5"));
    }

    #[tokio::test]
    async fn ignores_files_that_are_not_bi5() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("readme.txt"), b"hello");
        write(&dir.path().join("notes.bi5.bak"), b"hello");
        write(&dir.path().join("BID_candles_day_1.bi5x"), b"hello");
        assert!(scan_local_files(dir.path()).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn removes_bi5_tmp_but_keeps_the_bi5_sibling() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("dukascopy/EURUSD/2025");
        write(&data.join("BID_candles_day_1.bi5"), b"bi5");
        let tmp = data.join("BID_candles_day_1.bi5.tmp");
        write(&tmp, b"tmp");

        let files = scan_local_files(dir.path()).await.unwrap();

        assert_eq!(files.len(), 1);
        assert!(files.contains("dukascopy/EURUSD/2025/BID_candles_day_1.bi5"));
        assert!(!tmp.exists(), "stale scratch file must be deleted");
    }

    /// A bare `foo.tmp` is not ours and must survive the sweep.
    #[tokio::test]
    async fn leaves_unrelated_tmp_files_alone() {
        let dir = tempfile::tempdir().unwrap();
        let foreign = dir.path().join("somebody-elses.tmp");
        write(&foreign, b"do not touch");
        write(
            &dir.path()
                .join("dukascopy/EURUSD/2025/BID_candles_day_1.bi5.tmp"),
            b"ours",
        );

        let files = scan_local_files(dir.path()).await.unwrap();

        assert!(foreign.exists(), "unrelated .tmp must not be deleted");
        assert_eq!(files.len(), 0, "a .tmp is never reported as cached");
    }

    #[tokio::test]
    async fn empty_bi5_file_is_still_reported_as_cached() {
        // A zero-length file must not silently cause a re-download loop.
        let dir = tempfile::tempdir().unwrap();
        write(
            &dir.path()
                .join("dukascopy/EURUSD/2025/BID_candles_day_1.bi5"),
            b"",
        );
        assert_eq!(scan_local_files(dir.path()).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn deeply_nested_tree_is_scanned() {
        let dir = tempfile::tempdir().unwrap();
        let mut expected = Vec::new();
        for y in 2000..2010 {
            let rel = format!("dukascopy/EURUSD/{y}/BID_candles_day_1.bi5");
            write(&dir.path().join(&rel), b"bi5");
            expected.push(rel);
        }
        let files = scan_local_files(dir.path()).await.unwrap();
        assert_eq!(files.len(), expected.len());
        for rel in expected {
            assert!(files.contains(&rel), "{rel}");
        }
    }
}
