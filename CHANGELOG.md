# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 与语义化版本。

## [Unreleased]

### Changed

- 日历统一到 `BucketKey`：`BucketKey::end()` / `next()`、`buckets_between`、`last_closed_day` 成为唯一日历来源；`build_urls` 只负责渲染 URL 与本地路径。
- `AppConfig` 字段私有，新增 `symbols()` / `timeframes()` / `price_type()` / `start()` / `end()` / `output_dir()` / `concurrency()`（`NonZeroUsize`）与 `from_toml_str`；反序列化改由私有 `RawConfig` 承担，校验集中在 `TryFrom`。
- `DownloadTask` 迁到 `app::download`：字段私有，使用 `DownloadTask::new` 与读取器，并标记 `#[non_exhaustive]`。
- `DownloadGate` 取代 `(concurrency, &Arc<Semaphore>)`：`run_downloads` / `run_downloads_with` 只接收 `&DownloadGate`，上限与许可池不可能不一致。
- `DownloadOptions` 字段私有，使用 `new()` / `with_allow_missing()` / `allow_missing()`；结构体标记 `#[non_exhaustive]`。
- `DownloadStats` 计数私有，通过读取器访问，只由下载流水线构造。
- `utils` 模块退场：缓存扫描移到 `app::cache`（crate 根仍可 `ddd::scan_local_files`），运行锁移到 `app::lock`，重试策略移到 `app::download::retry`。
- crate 根部统一 re-export：`DownloadGate`、`DownloadOptions`、`DownloadStats`、`DownloadTask`、`run` / `run_with`、`scan_local_files` 与配置类型。
- CLI 的 tracing 初始化改用 `try_init()`，重复初始化返回错误而不是 panic。

### Removed

- `ddd::utils` 模块与 `ddd::config::DownloadTask` 路径。
- `AppConfig` 的公开字段；`concurrency` 由 `usize` 改为 `NonZeroUsize`。
- `DownloadStats` 的公开字段与结构体字面量构造。
- core crate 的 `anyhow` 依赖。

### Migration

- `cfg.symbol` → `cfg.symbols()`；`cfg.timeframe` → `cfg.timeframes()`；`cfg.price_type` → `cfg.price_type()`；`cfg.output_dir` → `cfg.output_dir()`；`cfg.concurrency` → `cfg.concurrency().get()`。
- `DownloadTask { .. }` → `DownloadTask::new(url, dest, symbol, bucket)`；字段读取改为 `url()` / `dest()` / `symbol()` / `bucket()`。
- `run_downloads(client, tasks, n, &gate)` → `run_downloads(client, tasks, &DownloadGate::new(n))`。
- `DownloadOptions { allow_missing }` → `DownloadOptions::new().with_allow_missing(true)`。
- `stats.ok` → `stats.ok()`，其余计数同理。
- `ddd::utils::scan_local_files::scan_local_files` → `ddd::scan_local_files`。

## [0.1.0] - 2026-11-13

### Added

- `BucketKey` 类型化日历桶；`DownloadTask` 携带桶信息，缺失分析不再反解析路径。
- `AppError::GateClosed`：共享下载闸门被关闭时返回结构化错误。
- CLI 二进制集成测试（帮助、退出码、无效配置、全缓存运行）。
- `deny.toml`、cargo-deny / cargo-audit 门禁、MSRV 1.85 CI job。
- 架构与下载行为文档、CHANGELOG。

### Changed

- 结构拆分：`config.rs` 拆为 `config/*`，下载编排拆为 `download/*`，公开模块路径保持不变。
- 信号处理、pair 调度、`today` 注入与计数器改为可测试、可中止、有上限的实现。
- 缺失段分析按周期分组；缓存、URL 与任务元数据保持同步。
- 重试预算、原子落盘、熔断与 404 分级行为写入文档。

### Fixed

- 熔断器互斥锁中毒后恢复窗口数据，不再二次 panic。
- SIGINT 安装失败时仍可使用 SIGTERM。
- 生产路径移除 `expect` / `unwrap`；公开 gate 关闭不再 panic。
- join 错误保留 panic payload；未闭合桶不会冻结缓存。

### Removed

- `AppConfig::default()`：不再能构造非法空配置；文档示例改用 `toml::from_str`。
