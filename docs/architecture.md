# 架构与执行流程

## 模块布局

```text
../crates/dukascopy-data-downloader-cli/src/main.rs   入口：tracing、clap、调用 core
../crates/dukascopy-data-downloader-cli/src/cli.rs    -c/--config、--allow-missing
../crates/dukascopy-data-downloader-core/src/lib.rs   公开 API 门面
../crates/dukascopy-data-downloader-core/src/error.rs AppError + 错误链
../crates/dukascopy-data-downloader-core/src/config/
    mod.rs        TOML 解析、校验、区间归一
    bucket.rs     BucketKey（年/月/日桶）
    task.rs       DownloadTask
    symbol.rs     Symbol
    timeframe.rs  Timeframe
    price_type.rs PriceType
    range.rs      DEFAULT_START、MAX_RANGE_DAYS
../crates/dukascopy-data-downloader-core/src/app/
    mod.rs        编排：JoinSet、运行锁、信号
    pair.rs       单个 symbol × timeframe
    signal.rs     SIGINT / SIGTERM
    client.rs     reqwest client
    build_urls.rs ① 生成 URL / 相对路径 / BucketKey
    build_download_tasks.rs ② 剔除缓存并绑定目标路径
    download/     ③ 下载、统计、熔断、缺失分析
../crates/dukascopy-data-downloader-core/src/utils/
    backoff.rs          重试预算
    run_lock.rs         目录运行锁
    scan_local_files.rs 扫描 .bi5 缓存
```

## 执行流程

1. 注册 SIGINT / SIGTERM（先于取锁）。
2. 读取并校验配置，计算 `start..=end`（纯算术，先于 I/O）。
3. 获取 `output_dir` 独占运行锁。
4. 扫描本地缓存，创建全局下载闸门 `Semaphore(concurrency)`。
5. 以最多 32 个在途任务调度符号 × 周期组合。

```text
main
  └─ app::run_with
     ├─ termination_signal        SIGINT / SIGTERM
     ├─ AppConfig::load           spawn_blocking
     ├─ acquire_run_lock          output_dir/.ddd.lock
     └─ select! { run_pipeline | 信号 }
        ├─ effective_range
        ├─ build_client
        ├─ scan_local_files
        ├─ Semaphore(concurrency)
        └─ JoinSet(≤32 pairs)
             └─ run_pair
                 ├─ 跳过未闭合桶
                 ├─ build_download_tasks
                 └─ run_downloads
```

## 并发模型

- `concurrency` 是整个 run 的在途下载上限，不是每个 pair 一份。
- 每个 pair 先单独下载第一个文件作为探针；传输失败则熔断整对，不再发请求。
- 其余文件通过 `buffer_unordered(concurrency)` 流式执行，每个任务额外持有全局闸门许可。
- 熔断器保留最近 32 次尝试，失败率达到 1/2 即跳过该 pair 的剩余文件。

## 运行锁与中断

- 锁是 `output_dir/.ddd.lock/` 目录，创建目录是原子操作。
- 锁由 pipeline future 持有；中断时 future 被丢弃，锁随之释放。
- 被 `kill -9` 强杀时锁目录会残留，错误信息给出待删路径。
- 中断以 `AppError::Interrupted` 非零退出：区间没有交付完成。
