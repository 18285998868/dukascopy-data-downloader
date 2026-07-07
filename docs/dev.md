# 逻辑:
读取配置
for symbol 并行
  for timeframe 串行
    根据 timeframe 周期(D1-年/H1-月/M1-日)构建 URL
    检查本地文件 + redb 缓存过滤已下载的
    Semaphore(30) 并发下载 .bi5 原始 LZMA 文件, 原子写入 .bi5.tmp → rename .bi5
    404 标记缓存(数据不存在), 其他错误重试(backoff 30s)

```toml
symbol = ["EURUSD", "GBPUSD"]  # 外汇/金属/加密货币/指数, 不指定默认 EURUSD
timeframe = ["D1"]              # M1(日文件) / H1(月文件) / D1(年文件)
price_type = "BID"              # BID / ASK
start = "2020-01-01"            # 起始日期
end = "2025-12-31"              # 结束日期
dir = "data"                    # 存储目录, 最终路径: {dir}/dukascopy/{symbol}/{tf}/{symbol}-{tf}-{period}.bi5
```

```
src/
  main.rs             # 入口
  cli.rs              # --config/-c
  lib.rs              # 模块导出
  error.rs            # AppError (thiserror)
  config.rs           # Config, Config::load()
  app.rs              # 编排: 加载配置 → parse 日期 → build_tasks → run_downloads
  cache.rs            # redb 本地缓存 (read/write, TTL)
  backoff.rs          # default_backoff() ExponentialBackoff
  download/
    mod.rs            # PeriodKind {Year,Month,Day}, DownloadTask
                      # build_tasks/build_url: 按周期构建 URL 和文件路径
                      # run_downloads: Semaphore(30) 并发, aggregate_results
    stream.rs         # download_to_file: HTTP → .bi5.tmp → rename .bi5
                      # map_timeframe/tf_period/build_url: 时间粒度 → URL 模式映射
```

## 时间粒度与 URL 模式

| 别名 | 后缀 | 周期 | URL 模式 |
|------|------|------|----------|
| M1/1m | `candles_min_1` | 日 | `/{YYYY}/{MM}/{DD}/{price}_candles_min_1.bi5` |
| H1/1h | `candles_hour_1` | 月 | `/{YYYY}/{MM}/{price}_candles_hour_1.bi5` |
| D1/1d | `candles_day_1` | 年 | `/{YYYY}/{price}_candles_day_1.bi5` |

Base URL: `https://www.dukascopy.com/datafeed`

## 支持品种

- 外汇: 标准对 (EURUSD, GBPJPY, AUDNZD 等)
- 金属: XAUUSD, XAGUSD
- 加密货币: BTCUSD, ETHUSD, XRPUSD
- 指数: USA30IDXUSD, USA500IDXUSD, GBRIDXGBP 等
- **不包含**: 个股、铂金(XPTUSD)

## 输出格式

原始 `.bi5` 文件 (LZMA 压缩), 不做解码。文件名:
- `{symbol}-D1-{year}.bi5`
- `{symbol}-H1-{year}-{month}.bi5`
- `{symbol}-M1-{year}-{month}-{day}.bi5`
