# Changelog

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 与语义化版本。

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
