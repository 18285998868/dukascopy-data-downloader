//! Exclusive run lock over one output directory.
//!
//! Two instances writing the same `output_dir` are not safe: the startup
//! scan deletes every `*.bi5.tmp` it finds, so the second instance would
//! unlink the scratch file the first one is writing, and that file would then
//! fail with a confusing "No such file or directory".
//!
//! The lock is a directory. Creating a directory is atomic on every POSIX
//! system and on Windows, so it doubles as the lock without pulling in a C
//! dependency or a single `unsafe` block. The downside of not using `flock`
//! is that the lock is *not* released by the kernel when a process dies: a
//! crashed run leaves the directory behind and the next run refuses to
//! start until it is removed. The error message names the path so the
//! remedy is obvious.

use std::path::{Path, PathBuf};

use crate::AppError;

/// Name of the lock directory created inside the output directory.
const LOCK_DIR_NAME: &str = ".ddd.lock";

/// A held run lock.
///
/// The lock is released when the guard is dropped, i.e. when the run ends —
/// normally or through an error.
#[derive(Debug)]
pub struct RunLock {
    path: PathBuf,
}

impl RunLock {
    /// Path of the lock directory.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for RunLock {
    fn drop(&mut self) {
        // Nothing useful can be done about a failure here, and the run is
        // over either way.
        let _ = std::fs::remove_dir(&self.path);
    }
}

/// Take the exclusive lock on `output_dir`.
///
/// # Errors
///
/// Returns [`AppError::Config`] if another instance already holds the lock,
/// or [`AppError::Io`] if the lock directory cannot be created.
pub fn acquire_run_lock(output_dir: &Path) -> Result<RunLock, AppError> {
    let path = output_dir.join(LOCK_DIR_NAME);
    std::fs::create_dir_all(output_dir).map_err(|e| {
        AppError::Io(std::io::Error::new(
            e.kind(),
            format!("{}: {e}", output_dir.display()),
        ))
    })?;
    match std::fs::create_dir(&path) {
        Ok(()) => Ok(RunLock { path }),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(AppError::Config(format!(
            "another instance holds the lock on {} ({}); \
             remove that directory if no download is running",
            output_dir.display(),
            path.display()
        ))),
        Err(e) => Err(AppError::Io(std::io::Error::new(
            e.kind(),
            format!("{}: {e}", path.display()),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_lock_is_taken_and_released() {
        let dir = tempfile::tempdir().unwrap();
        let lock = acquire_run_lock(dir.path()).unwrap();
        let path = lock.path().to_path_buf();
        assert!(path.exists());
        assert_eq!(path, dir.path().join(LOCK_DIR_NAME));
        drop(lock);
        assert!(!path.exists(), "the lock must be released on drop");
    }

    #[test]
    fn a_second_instance_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let _first = acquire_run_lock(dir.path()).unwrap();
        let err = acquire_run_lock(dir.path()).unwrap_err();
        assert!(err.to_string().contains("another instance"), "{err}");
    }

    #[test]
    fn the_lock_can_be_taken_again_after_release() {
        let dir = tempfile::tempdir().unwrap();
        drop(acquire_run_lock(dir.path()).unwrap());
        assert!(acquire_run_lock(dir.path()).is_ok());
    }

    /// The lock directory itself must not be mistaken for cached data.
    #[test]
    fn the_lock_directory_is_created_inside_the_output_directory() {
        let dir = tempfile::tempdir().unwrap();
        let lock = acquire_run_lock(dir.path()).unwrap();
        assert!(lock.path().starts_with(dir.path()));
        assert!(lock.path().is_dir());
    }
}
