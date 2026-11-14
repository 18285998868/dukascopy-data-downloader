//! Local file scanning for the download cache.

use std::collections::HashSet;
use std::path::Path;

use crate::AppError;

/// Scratch-file suffix used while a download is in flight.
const TMP_SUFFIX: &[u8] = b".bi5.tmp";
/// Final payload suffix.
const BI5_SUFFIX: &[u8] = b".bi5";

/// The cache key of a scanned file.
///
/// The key has to match the relative paths produced by
/// [`crate::app::build_urls::build_urls`], which always join with `/`. A
/// path read back from the filesystem carries the native separator, so on
/// Windows it would never match and the cache would silently be defeated —
/// every run would download everything again.
fn cache_key(rel: &Path) -> String {
    let raw = rel.to_string_lossy();
    if std::path::MAIN_SEPARATOR == '/' {
        raw.into_owned()
    } else {
        raw.replace(std::path::MAIN_SEPARATOR, "/")
    }
}

/// Scan `data_dir` for existing `.bi5` files, removing stale scratch files.
///
/// Returns a set of `data_dir`-relative paths (e.g.
/// `dukascopy/EURUSD/2025/BID_candles_day_1.bi5`), which is exactly the shape
/// produced by [`crate::app::build_urls::build_urls`], so callers can test
/// membership directly. Paths always use `/` as the separator, on every
/// platform. A missing directory yields an empty set.
///
/// Two kinds of file are deleted rather than reported as cached:
///
/// - `*.bi5.tmp` scratch files left by an interrupted run;
/// - zero-length `.bi5` files, which cannot be a valid LZMA container. A
///   non-empty sweep of `*.tmp` would destroy unrelated temporary files if
///   `data_dir` ever points somewhere shared, so the deletion stays narrow.
///
/// # Errors
///
/// Returns [`AppError::Join`] if the blocking walk task panics.
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> Result<(), ddd::AppError> {
/// let cached = ddd::scan_local_files(std::path::Path::new("data")).await?;
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
                match entry.metadata() {
                    Ok(meta) if meta.len() == 0 => {
                        // A zero-length `.bi5` cannot be a valid LZMA
                        // container. Treating it as cached would freeze the
                        // hole forever; removing it makes the next run fetch
                        // the file again (and fail loudly if the server keeps
                        // answering with an empty body).
                        if let Err(err) = std::fs::remove_file(entry.path()) {
                            tracing::warn!(
                                path = %entry.path().display(),
                                error = %err,
                                "failed to remove empty bi5 file",
                            );
                        }
                        continue;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        // The file exists but cannot be stat'ed. Keep it in
                        // the cache: re-downloading would hit the same
                        // permission problem, and a spurious re-download
                        // loop is worse than trusting the existing entry.
                        tracing::warn!(
                            path = %entry.path().display(),
                            error = %err,
                            "failed to read metadata; treating the file as cached",
                        );
                    }
                }
                files.insert(cache_key(rel));
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

    /// A zero-length `.bi5` is not a valid payload. Leaving it in the cache
    /// would freeze the hole forever, so the scan removes it and lets the
    /// next run redownload it.
    #[tokio::test]
    async fn empty_bi5_file_is_removed_and_not_cached() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir
            .path()
            .join("dukascopy/EURUSD/2025/BID_candles_day_1.bi5");
        write(&empty, b"");
        write(
            &dir.path()
                .join("dukascopy/EURUSD/2026/BID_candles_day_1.bi5"),
            b"bi5",
        );

        let files = scan_local_files(dir.path()).await.unwrap();

        assert_eq!(files.len(), 1, "only the non-empty file is cached");
        assert!(files.contains("dukascopy/EURUSD/2026/BID_candles_day_1.bi5"));
        assert!(!empty.exists(), "the empty payload must be removed");
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

    /// The cache key must use the same separator as the paths the task
    /// builder produces, otherwise the lookup never matches.
    #[test]
    fn the_cache_key_uses_forward_slashes() {
        let key = cache_key(Path::new("dukascopy/EURUSD/2025/BID_candles_day_1.bi5"));
        assert_eq!(key, "dukascopy/EURUSD/2025/BID_candles_day_1.bi5");
        assert!(!key.contains(std::path::MAIN_SEPARATOR) || std::path::MAIN_SEPARATOR == '/');
    }

    /// A nested path keeps every component — the key has to mirror the
    /// server path exactly, not just its last segment.
    #[test]
    fn the_cache_key_keeps_every_component() {
        let key = cache_key(Path::new(
            "dukascopy/EURUSD/2025/06/15/BID_candles_min_1.bi5",
        ));
        assert_eq!(key, "dukascopy/EURUSD/2025/06/15/BID_candles_min_1.bi5");
    }

    /// The run lock lives inside the output directory and must not be
    /// mistaken for cached data.
    #[tokio::test]
    async fn the_run_lock_directory_is_not_reported_as_cache() {
        let dir = tempfile::tempdir().unwrap();
        let lock = crate::app::lock::acquire_run_lock(dir.path()).unwrap();
        write(
            &dir.path()
                .join("dukascopy/EURUSD/2025/BID_candles_day_1.bi5"),
            b"bi5",
        );

        let files = scan_local_files(dir.path()).await.unwrap();
        assert_eq!(files.len(), 1);
        assert!(files.contains("dukascopy/EURUSD/2025/BID_candles_day_1.bi5"));
        assert!(lock.path().is_dir(), "the lock must survive the scan");
    }
}
