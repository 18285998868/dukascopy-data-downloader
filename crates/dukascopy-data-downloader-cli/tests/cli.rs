//! End-to-end checks of the shipped binary's exit codes.
//!
//! The clap unit tests pin argument parsing; these tests run the real
//! executable so the wiring from `main` through `AppError` to the process
//! exit code is covered too.

use std::path::Path;
use std::process::{Command, Output};

/// Invoke the binary built for this integration test.
fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_dukascopy-data-downloader"))
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn help_succeeds_and_names_the_required_flags() {
    let output = bin().arg("--help").output().unwrap();
    assert!(output.status.success(), "{}", stderr(&output));

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--config"), "{stdout}");
    assert!(stdout.contains("--allow-missing"), "{stdout}");
}

#[test]
fn a_missing_config_file_exits_nonzero() {
    let output = bin()
        .args(["-c", "/nonexistent/ddd-config.toml"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("run failed"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn an_invalid_config_exits_nonzero_and_names_the_bad_key() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("bad.toml");
    std::fs::write(
        &config,
        "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nnope = 1\n",
    )
    .unwrap();

    let output = bin()
        .args(["-c", config.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let err = stderr(&output);
    assert!(err.contains("nope"), "{err}");
}

/// A fully cached pair must let the process exit successfully without any
/// network access.
#[test]
fn a_fully_cached_run_exits_successfully() {
    let dir = tempfile::tempdir().unwrap();
    let cached = dir
        .path()
        .join("dukascopy/EURUSD/2025/BID_candles_day_1.bi5");
    std::fs::create_dir_all(cached.parent().unwrap()).unwrap();
    std::fs::write(&cached, b"bi5").unwrap();

    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        format!(
            "symbol = [\"EURUSD\"]\ntimeframe = [\"D1\"]\nstart = \"2025-01-01\"\nend = \"2025-01-01\"\nconcurrency = 1\noutput_dir = {:?}\n",
            dir.path().to_string_lossy()
        ),
    )
    .unwrap();

    let output = bin()
        .args(["-c", config.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(Path::new(&cached).exists());
}
