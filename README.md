# dukascopy-data-downloader

从 Dukascopy Datafeed 下载历史 K 线原始 `.bi5` 文件(LZMA 压缩,不做解码)。每文件按
`symbol × timeframe × 周期(年/月/日)` 组织,周期由 timeframe 决定:>`D1/1d`=年文件、`H1/1h`=月文件、`M1/1m`=日文件。

## 文件结构

```
config/
  config.toml              # 实际运行配置(gitignore, 不提交)
  config.example.toml      # 配置模板
data/                      # 下载产物: {dir}/dukascopy/{symbol}/{tf}/{symbol}-{tf}-{period}.bi5 (gitignore)
CHANGELOG.md               # 变更记录
docs/dev.md                # 开发笔记
src/
  main.rs                  # 入口: tracing → dkdd::cli::parse → dkdd::app::run
  lib.rs                   # #[path] 模块声明 + DownloadTask/PeriodKind 定义
  error.rs                 # 支撑: AppError (thiserror)
  cache.rs                 # 支撑: redb 缓存 (read/write/TTL)
  backoff.rs               # 支撑: 默认指数退避 (max 30s)
  steps/                   # 按流水线步骤归档 (磁盘文件名带数字前缀, 模块名 = 可读步骤名)
    01_cli.rs                  cli:  解析 --config/-c (可选, 缺省 crate 内 config/config.toml)
    02_config.rs               config: Config::load + 空 symbol/timeframe 默认值兜底
    03_build_client.rs         build_client: TCP 探测直连 → SOCKS5(127.0.0.1:7890) 回退
    04_build_tasks.rs          build_tasks: tf→周期映射 + URL/本地路径构建
    05_run_downloads/
      mod.rs                   run_downloads: Semaphore(30) 并发 + 已存在/缓存过滤 + 404 标记 + 聚合
      stream.rs                stream: GET → .bi5.tmp 原子写 → rename, backoff 重试
    06_app.rs                  app: 编排 (config → 日期校验 → client → tasks → run_downloads)
```

## 配置

`config/config.toml`(示例见 `config/config.example.toml`):

```toml
symbol = ["EURUSD", "GBPUSD"]   # 品种, 缺省 ["EURUSD"]
timeframe = ["D1"]              # M1/1m(日文件) H1/1h(月文件) D1/1d(年文件), 缺省 ["D1"]
price_type = "BID"              # BID / ASK, 缺省 "BID"
start = "2020-01-01"            # 起始日期, 缺省 "2020-01-01"
end = "2025-12-31"              # 结束日期, 缺省 "2025-12-31"
dir = "data"                    # 输出目录, 缺省 "data"; 最终路径: {dir}/dukascopy/{symbol}/{tf}/...
```

全部字段有默认值;配置缺省走 crate 内 `config/config.toml`;`start > end` 会在启动时报错退出。

## 用法

```bash
cargo run --release -p dukascopy-data-downloader -- -c config/config.toml
# 不带 -c: 使用 crate 内默认配置
```

网络策略:先 5s 探测直连 `www.dukascopy.com:443`,失败再试 SOCKS5 `127.0.0.1:7890`。
并发:固定 `Semaphore(30)` 有界并发,配合 redb 缓存跳过已下载/已确认 404 的文件;
文件级失败不中止同批其余任务,404 计为预期不重试。

## 测试

```bash
cargo test
```

单元测试 + mock 测试覆盖配置加载、任务构建(年/月/日 URL 与路径)、并发聚合(OK/404/失败)、
单文件下载(200/404/500)、redb 缓存、指数退避(使用独立 tempdir,无路径冲突)。

## License

MIT