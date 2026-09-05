# dukascopy-data-downloader 项目开发

## 需求分析

- 项目目标: 从 [Dukascopy Datafeed](https://www.dukascopy.com/datafeed) 下载历史 K 线原始 `.bi5` 文件(LZMA 压缩,不做解码),按 `symbol × timeframe × 周期` 一文件一个 `.bi5` 落到本地。
- 周期随 timeframe 而定:`D1/1d`=年文件、`H1/1h`=月文件、`M1/1m`=日文件。
- 网络策略:优先直连,失败自动回退 SOCKS5 代理(`127.0.0.1:7890`)。

## 冒烟配置

- 根目录 `config.test.toml`: 1 symbol × `D1` × 2025 年,输出 `/tmp/dukascopy-smoke`,用于端到端冒烟。
- 运行: `cargo run --release -p dukascopy-data-downloader -- -c config.test.toml`

## 架构设计

| 文件架构(保持顺序) | 功能简介 | 库依赖 | 具体逻辑 |
| ------------------- | -------- | ------ | -------- |
| `src/main.rs` | 入口 | tokio、tracing-subscriber、anyhow | tracing(fmt + env-filter) → `ddd::cli::parse()` → `ddd::app::run(&config)` 带 context |
| `src/lib.rs` | 只做模块导出 | — | `pub mod` app 步骤(`#[path]` 挂 `app/`) + cli/config/error/cache/backoff;`pub use app::run`、`error::AppError` |
| `src/cli.rs` | 命令行解析 | clap 4(derive) | `--config/-c` **必填** → `PathBuf`;无 version/about |
| `src/config.rs` | 配置解析与校验 | toml 1.1、serde(chrono) | `Config::load(path)`: `deny_unknown_fields`;校验 symbol 非空 / timeframe、price_type 白名单 / start≤end;定义 `DownloadTask`、`PeriodKind`;`#[serde(default=..)]` 兜底默认值 |
| `src/error.rs` | 错误处理 | thiserror 2 | `AppError` 枚举(Config/Http/Io/NotFound/DownloadFailed/Data),`#[from]` 自动转换 |
| `src/cache.rs` | 缓存(redb) | redb 4 | `read/write(domain,key,ttl)` 通用 KV 缓存,值带 8 字节时间戳前缀实现 TTL;静态进程级 DB 池 |
| `src/backoff.rs` | 退避策略 | backoff 0.4(tokio) | `default_backoff()`:指数退避,`max_elapsed_time = 30s` |
| `src/app/` | 步骤目录 | — | 流水线按 `NN_` 编号归档;磁盘名带数字前缀,模块名经 `#[path]` 取可读步骤名 |
| `src/app/mod.rs` | 整体流程编排 | chrono、reqwest、tokio | `Config::load`(含校验) → `build_tasks` → `build_client`(TCP 探测直连→SOCKS5 回退) → `run_downloads(force=false)` |
| `src/app/01_build_tasks.rs` | 构建下载任务 | chrono | `map_timeframe`/`tf_period` 映射周期;`build_url` + `dest_filename`;三层循环 → `Vec<DownloadTask>` |
| `src/app/02_run_downloads/mod.rs` | 运行下载任务 | tokio(Semaphore)、futures(join_all)、redb | 已存在/缓存过滤 → `Semaphore(30)` + `join_all` 并发 → 成功/404 写缓存 → `aggregate_results`(failed>0 才报错) |
| `src/app/02_run_downloads/stream.rs` | 下载任务流 | reqwest、tokio、backoff | 单文件 GET → 404 容忍;429/5xx 走退避重试,其余 4xx 永久失败;`chunk` 流式写 `.bi5.tmp` → flush → rename 原子落盘 |

## 配置说明

根目录 `config.example.toml`(模板),字段:

| 字段 | 必填 | 默认值 | 可选参数 |
| ---- | ---- | ------ | -------- |
| `symbol` | 是 | 无 | 任意品种列表 |
| `timeframe` | 是 | 无 | `["M1","H1","D1","1m","1h","1d"]` |
| `price_type` | 否 | `"BID"` | `["BID","ASK"]` |
| `start` | 否 | `"2020-01-01"` | `YYYY-MM-DD` |
| `end` | 否 | 昨天 | `YYYY-MM-DD` |
| `dir` | 否 | `"data"` | 任意目录 |

`start > end`、非法日期、未知字段、非法 timeframe/price_type、空 symbol 都会在启动时报错退出。

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
`{symbol}-D1-{year}.bi5` / `{symbol}-H1-{year}-{month}.bi5` / `{symbol}-M1-{year}-{month}-{day}.bi5`,落盘 `{dir}/dukascopy/{symbol}/{tf}/...`。

### 并发与幂等

- 并发固定 `Semaphore(30)`;任务全量并行,文件级失败不中断同批。
- 成功与 404 都写入 redb 缓存(`domain=downloaded`),重跑自动跳过已下载/已确认无数据的文件。
- 网络错误、HTTP 429/5xx 为 `Transient` 走 backoff 重试(30s 封顶);404/其余 4xx 永久不重试。