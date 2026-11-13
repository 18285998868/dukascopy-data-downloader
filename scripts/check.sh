#!/usr/bin/env bash
# OS: __CachyOS__
# TIME: 2026-11-13
# DOC: 本地与 CI 共用的 Rust 门禁脚本
# LOGIC:
#   STEP 1: 定位仓库根目录并解析 --ci
#   STEP 2: 依次运行 fmt/check/clippy/test/doctest/doc/bench
#   STEP 3: 运行 cargo-deny 与 cargo-audit; 未安装时 CI 失败, 本地警告
# USAGE:
#   ./scripts/check.sh [--ci]

set -Eeuo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

ci=false
if [[ ${1:-} == --ci ]]; then
    ci=true
fi

locked=()
if $ci; then
    locked=(--locked)
fi

run() {
    printf '==> %s\n' "$*"
    "$@"
}

run cargo fmt --all -- --check
run cargo check --workspace --all-targets "${locked[@]}"
run cargo clippy --workspace --all-targets --all-features "${locked[@]}" -- -D warnings
run cargo clippy --workspace --all-targets --no-default-features "${locked[@]}" -- -D warnings

if cargo nextest --version >/dev/null 2>&1; then
    run cargo nextest run --workspace --all-features "${locked[@]}"
else
    printf '==> cargo nextest not installed; falling back to cargo test\n'
    run cargo test --workspace --all-features "${locked[@]}"
fi

run cargo test --workspace --doc --all-features "${locked[@]}"

printf '==> cargo doc\n'
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --all-features "${locked[@]}"

run cargo bench --workspace --no-run "${locked[@]}"

missing=0
for tool in cargo-deny cargo-audit; do
    if ! command -v -- "$tool" >/dev/null 2>&1; then
        printf 'missing tool: %s\n' "$tool" >&2
        missing=1
    fi
done
if ((missing)); then
    if $ci; then
        printf 'CI requires cargo-deny and cargo-audit\n' >&2
        exit 1
    fi
    printf 'SKIP: cargo-deny/cargo-audit (install them for the complete gate)\n' >&2
else
    run cargo deny check
    run cargo audit
fi

printf 'ALL GATES PASSED\n'
