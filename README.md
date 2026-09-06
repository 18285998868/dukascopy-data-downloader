# dukascopy-data-downloader 项目开发

## 需求分析

- 项目目标: 从 [Dukascopy Datafeed](https://www.dukascopy.com/datafeed) 下载历史 K 线原始 `.bi5` 文件(LZMA 压缩,不做解码),按 `symbol × timeframe × 周期(年/月/日)` 一文件一个 `.bi5` 落地

## 技术选型

- 语言:rust
- edition = "2024"
- 网络:reqwest(直连,失败自动回退 SOCKS5 `127.0.0.1:7890`)

## 冒烟配置

```toml
symbol = ["EURUSD"] # 必填
timeframe = ["D1"] # 可选 ["M1", "H1", "D1", "1m", "1h", "1d"]
# price_type = "BID" # 可选 ["BID", "ASK"]
start = "2025-01-01" # 可选, 默认 "2020-01-01"
end   = "2025-01-31" # 可选, 默认 昨天
dir   = "/tmp/dukascopy-smoke" # 可选
```

## 逻辑编排

```
启动 -> 初始化 tracing; 解析 --config/-c
v
加载配置 -> toml 反序列化 + validate(symbol 非空, timeframe/price_type 白名单, start≤end)
v
构建任务 -> symbol × timeframe × 周期(年/月/日, 由 timeframe 映射) 全量 Vec<DownloadTask>
v
准备 -> 建 reqwest Client(TCP 5s 探测直连 www.dukascopy.com:443 → 失败探测 SOCKS5 127.0.0.1:7890)
v
执行下载 -> Semaphore(30) + join_all 有界并发; .bi5.tmp 流式落盘 → rename 原子生效; 404 容忍(写缓存标记)/网络·429·5xx 指数重试≤30s, 其余 4xx 永久失败; 成功·404 写 redb 缓存
v
汇总 -> 聚合 ok/not_found/failed; failed>0 返回 Err 否则 Ok
```

## 架构设计

```
# 支撑件(cli/config/error/cache/backoff)在 crate 根, 不编号; 执行代码在 app/, 按"调用执行流程"编号: main → cli → config → 01_build_tasks → 02_run_downloads
main.rs             # 入口: 初始化 tracing(fmt + env-filter, 缺省 info) → ddd::cli::parse() → ddd::app::run(&cli.config); 错误 anyhow 逐层加 context
lib.rs              # 只做导出: #[path] 挂载 app/NN_* 步骤模块 + 支撑模块; pub use app::run / error::AppError
cli.rs              # 支撑件: 命令行解析 clap 4(derive), 解析 --config/-c(必填), 返回 PathBuf
config.rs           # 支撑件: 配置解析 toml + serde(deny_unknown_fields): symbol[]/timeframe[]/price_type/start/end(dir 默认 "data"); validate 校验(symbol 非空, timeframe/price_type 白名单, start≤end); 定义 DownloadTask{url,dest,symbol,timeframe,date,period} 与 PeriodKind
error.rs            # 支撑件: AppError(thiserror, Config/Http/Io/NotFound/DownloadFailed/Data), #[from] 实现 ? 自动转换
cache.rs            # 支撑件: redb 通用 KV 缓存 read/write(domain,key,ttl), 值带 8 字节时间戳前缀实现 TTL; 进程级静态 DB 池
backoff.rs          # 支撑件: default_backoff() 指数退避, max_elapsed_time = 30s
app/                            # 执行代码
app/mod.rs          # 整体编排 run(config_path) (伪代码):
                    #   1 Config::load + validate (symbol 非空, timeframe/price_type 白名单, start≤end, deny_unknown_fields)
                    #   2 build_tasks: symbol × timeframe × 周期(年/月/日) 全量任务
                    #   3 build_client: TCP 5s 探测直连 www.dukascopy.com:443 → 失败探测 SOCKS5 127.0.0.1:7890; 全挂 bail
                    #   4 run_downloads(client, tasks, false) → 成功/404 写缓存, 失败聚合
app/01_build_tasks.rs   # 构建任务: map_timeframe 映射 candles_min_1/candles_hour_1/candles_day_1 + tf_period(年/月/日); build_url 按周期生成 datafeed URL; 三层循环 → Vec<DownloadTask>
app/02_run_downloads/   # 运行下载
  mod.rs                # 运行下载: dest.exists/缓存过滤 → Semaphore(30) + join_all 有界并发; 成功/404 写下载缓存; 增量 [idx/total]; 聚合 ok/not_found/failed
  stream.rs             # 单文件下载: GET → dest 父目录建链 → .bi5.tmp chunk 流式写 → flush → rename 原子落盘; 404 容忍(不重试); 网络错误/429/5xx 指数重试, 其余 4xx 永久失败
```