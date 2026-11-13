#!/usr/bin/env bash
# OS: __CachyOS__
# TIME: 2026-11-13
# DOC: 本地与 CI 共用的 Rust 门禁脚本
# LOGIC:
#   STEP 1: 解析参数 (--ci 使用 --locked)
#   STEP 2: 依次运行 fmt/check/clippy/test/doc/bench
# USAGE:
#   ./scripts/check.sh [--ci]

set -Eeuo pipefail

CI=false
if [[ ${1:-} == "--ci" ]]; then
    CI=true
fi

LOCKED=()
if $CI; then
    LOCKED=(--locked)
fi

echo "==> cargo fmt"
cargo fmt --all -- --check

echo "==> cargo check"
cargo check --workspace --all-targets "${LOCKED[@]}"

echo "==> cargo clippy (all features)"
cargo clippy --workspace --all-targets --all-features "${LOCKED[@]}" -- -D warnings

echo "==> cargo clippy (no default features)"
cargo clippy --workspace --all-targets --no-default-features "${LOCKED[@]}" -- -D warnings

echo "==> cargo test"
cargo nextest run --workspace --all-features "${LOCKED[@]}" 2>/dev/null || cargo test --workspace --all-features "${LOCKED[@]}"

echo "==> cargo test --doc"
cargo test --workspace --doc --all-features "${LOCKED[@]}"

echo "==> cargo doc"
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --all-features "${LOCKED[@]}"

echo "==> cargo bench (compile only)"
cargo bench --workspace --no-run "${LOCKED[@]}"

echo "ALL GATES PASSED"