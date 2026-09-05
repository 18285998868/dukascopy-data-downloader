# dukascopy-data-downloader 项目开发

## 需求分析

- 项目目标: 从 [Dukascopy Datafeed](https://www.dukascopy.com/datafeed) 下载历史 K 线原始 `.bi5` 文件(LZMA 压缩,不做解码),按 `symbol × timeframe × 周期` 一文件一个 `.bi5` 落到本地。
- 周期随 timeframe 而定:`D1/1d`=年文件、`H1/1h`=月文件、`M1/1m`=日文件。

## 技术选型

- 语言: rust
- edition = "2024"
- 网络: reqwest 0.13(`rustls` + `socks`,直连与 SOCKS5 代理)
- 并发: tokio + futures `join_all` + `Semaphore(30)` 有界并发

## 架构设计

| 文件架构(保持顺序)              | 功能简介       | 库依赖                               | 具体逻辑                                                                                                          |
| ------------------------------- | -------------- | ------------------------------------ | ----------------------------------------------------------------------------------------------------------------- |
| `main.rs`                       | 只做入口调用   | tokio、tracing-subscriber、anyhow    | 初始化 tracing(fmt + env-filter)→ `dukascopy_data_downloader::cli::parse()` → `app::run(config)`                  |
| `lib.rs`                        | 只做模块导入导出 | std(PathBuf)、chrono(NaiveDate)   | `#[path]` 挂载 `app/` 各步骤 + 支撑模块;定义 `DownloadTask`/`PeriodKind`;`pub use error::AppError`              |
| `error.rs`                      | 错误处理       | thiserror 2                          | `AppError` 枚举(Config/Http/Io/NotFound/DownloadFailed/Data),`#[from]` 实现 `?` 自动转换                         |
| `cache.rs`                      | 缓存(redb)     | redb 4                               | `read/write(domain,key,ttl)` 通用 KV 缓存,值带 8 字节时间戳前缀实现 TTL;静态进程级 DB 池                          |
| `backoff.rs`                    | 退避策略       | backoff 0.4(tokio)                   | `default_backoff()`:指数退避,`max_elapsed_time = 30s`                                                             |
| `app/`                          | 步骤目录       | —                                    | 流水线按 `NN_` 编号归档;磁盘名带数字前缀,模块名经 `#[path]` 取可读步骤名                                          |
| `app/mod.rs`                    | 整体流程编配   | chrono、anyhow、reqwest              | load 配置 → `parse_date` + `start>end` 校验 → `build_tasks` → `build_client` → `run_downloads(force=false)`        |
| `app/01_cli.rs`                 | 命令行解析     | clap 4(derive)                       | 解析 `--config/-c`(可选),为 `None` 时回退 crate 内 `config/config.toml`                                           |
| `app/02_config.rs`              | 配置解析       | config 0.15(config-rs, toml)+ serde  | `Config::load` 读 TOML(`#[serde(default)]` 全字段默认值);空 `symbol`/`timeframe` 回落默认                          |
| `app/03_build_client.rs`        | 构建网络客户端 | reqwest、tokio                       | 5s TCP 探测直连 `www.dukascopy.com:443` → 失败再探 SOCKS5 `127.0.0.1:7890`;直连 30s/代理 60s;全挂 bail 无网络      |
| `app/04_build_tasks.rs`         | 构建下载任务   | chrono                               | `map_timeframe`/`tf_period` 映射周期;枚举代表日期;`build_url` + `dest_filename`;三层循环 → `Vec<DownloadTask>`    |
| `app/05_run_downloads/mod.rs`   | 运行下载任务   | tokio(Semaphore)、futures(join_all)、redb | 已存在/缓存过滤 → `Semaphore(30)` + `join_all` 并发 → 成功/404 写缓存 → `aggregate_results`(`failed>0` 才报错) |
| `app/05_run_downloads/stream.rs`| 下载任务流     | reqwest、tokio、backoff              | 单文件 GET → 非 2xx:404 `Permanent(NotFound)`、其余 `Transient`;写 `.bi5.tmp` → flush → rename 原子落盘;重试 ≤30s |

## 参考资料

### 时间粒度与 URL 模式

| 别名 | 后缀 | 周期 | URL 模式 |
|------|------|------|----------|
| M1/1m | `candles_min_1` | 日 | `/{YYYY}/{MM}/{DD}/{price}_candles_min_1.bi5` |
| H1/1h | `candles_hour_1` | 月 | `/{YYYY}/{MM}/{price}_candles_hour_1.bi5` |
| D1/1d | `candles_day_1` | 年 | `/{YYYY}/{price}_candles_day_1.bi5` |

Base URL: `https://www.dukascopy.com/datafeed`

### 输出格式

原始 `.bi5`(LZMA 压缩),文件名按周期:
`{symbol}-D1-{year}.bi5` / `{symbol}-H1-{year}-{month}.bi5` / `{symbol}-M1-{year}-{month}-{day}.bi5`。

### 并发与幂等

- 并发固定 `Semaphore(30)`;配对全量并行,文件级失败不中断同批。
- 成功与 404 都写入 redb 缓存(`domain=downloaded`),重跑自动跳过已下载/已确认无数据的文件。
- 网络错误、HTTP 5xx 为 `Transient` 走 backoff 重试(30s 封顶);404 标记后不再请求。