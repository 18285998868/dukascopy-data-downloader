# dukascopy-data-downloader 项目开发

## 需求分析

- 项目目标: 从 [Dukascopy Datafeed](https://www.dukascopy.com/datafeed) 下载历史 K 线原始 `.bi5` 文件(LZMA 压缩,不做解码),按 `symbol × timeframe × 周期(年/月/日)` 一文件一个 `.bi5` 落地
- 布局对齐 `binance-data-downloader`: `app/01_build_urls + 02_build_download_tasks + 03_download`, 支撑件收拢到 `utils/`; 去重用 `scan_local_files` 一次扫描, 不用 redb 下载缓存

## 技术选型

- 语言:rust
- edition = "2024"
- 网络:reqwest(直连,失败自动回退 SOCKS5 `127.0.0.1:7890`)

## 冒烟配置

```toml
symbol = ["EURUSD"] # 必填
timeframe = ["D1"] # 可选 ["M1", "H1", "D1", "1m", "1h", "1d"]
# price_type = "BID" # 可选 ["BID", "ASK"]
start = "2025-01-01" # 可选, 默认无(按 2020-01-01 处理)
end   = "2025-01-31" # 可选, 默认 昨天
output_dir = "/tmp/dukascopy-smoke" # 可选, 数据根目录(不含 dukascopy)
# concurrency = 30 # 可选, 每配对并发数
```

## 逻辑编排

```
启动 -> 初始化 tracing; 解析 --config/-c
v
加载配置 -> toml 反序列化 + validate(symbol/timeframe 非空, timeframe/price_type 白名单, concurrency≠0, start≤end, sort+dedup)
v
扫描本地 -> scan_local_files(output_dir) 一次 walkdir, 收 .bi5 相对路径集 + 清 .tmp
v
准备 -> 建 reqwest Client(TCP 5s 探测直连 www.dukascopy.com:443 → 失败探测 SOCKS5 127.0.0.1:7890; 池 256)
v
双层串行 symbol × timeframe -> run_one: build_download_tasks(起止→URL→剔本地已有) -> 03_download 并发下
v
执行下载 -> buffer_unordered(concurrency); .bi5.tmp 流式落盘 → rename 原子生效; 404 容忍/网络·429·5xx 指数重试≤30s, 其余 4xx 永久失败
v
汇总 -> DownloadStats{total/ok/not_found/failed}; failed>0 返回 Err, 配对级失败不中断后续
```

## 架构设计

```
# 对标 binance-data-downloader: app/ 三段编号, 补丁进 utils/; main → cli → config → 01_build_urls → 02_build_download_tasks → 03_download
main.rs             # 入口: 初始化 tracing(fmt + env-filter, 缺省 info) → ddd::cli::parse() → ddd::app::run(&cli.config)
lib.rs              # 只做导出: pub mod app/cli/config/error/utils; pub use app::run / error::AppError
cli.rs              # 命令行解析 clap 4(derive), 解析 --config/-c(必填)
config.rs           # AppConfig{symbol,timeframe,price_type,start?:date,end?:date,output_dir,concurrency=30} + resolved_end() + DownloadTask{url,dest,symbol,timeframe}
error.rs            # AppError(thiserror, Http/Io/Json/Config/Api/Data/Date/Cache/DownloadFailed/NotFound/Join)
utils/mod.rs        # pub mod backoff/cache/scan_local_files
utils/backoff.rs    # default_backoff() 指数退避, max_elapsed_time = 30s
utils/cache.rs      # redb 通用 KV(带 set_base_dir(output_dir/.cache)), TTL 前缀 8 字节; 当前下载链路不用, 保留占位对齐 bdd
utils/scan_local_files.rs # walkdir 扫 .bi5 相对路径集 + 删 .tmp
app/
app/mod.rs          # run: AppConfig::load → cache::set_base_dir → build_client → scan_local_files → resolved_end → 串行 symbol×timeframe → run_one
app/01_build_urls.rs      # DownloadUrl{url,rel_path,symbol,timeframe} + PeriodKind(Year/Month/Day); map_timeframe/tf_period/build_url/build_urls(单 symbol×tf, 本地 dukascopy/ 严格镜像官方 path)
app/02_build_download_tasks.rs # build_download_tasks(symbol,tf,price,start,end,local,data_dir): build_urls → 剔 local → DownloadTask
app/03_download/    # 有界并发下载
  mod.rs                # DownloadStats + run_downloads(client,tasks,concurrency) → run_downloads_inner(buffer_unordered, progress 每 100, 上限 20 明细)
  stream.rs             # 单文件: GET → 建父目录 → 清旧 .bi5.tmp → chunk 流式写 → rename; 404 Permanent, 429/5xx/网络 Transient, 余 4xx Permanent
```
