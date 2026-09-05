# 开发笔记:每个步骤做了什么

源码按流水线步骤归档于 `src/steps/NN_*.rs`,模块名经 `lib.rs` 的 `#[path]` 映射为可读步骤名
(`cli`/`config`/`build_client`/`build_tasks`/`run_downloads`/`app`)。`01_cli`/`02_config`/`06_app`
保留原名,其余模块名 = 磁盘名去掉数字前缀。步骤间通过函数调用串联,`06_app.rs` 是顶层编排。

```text
src/main.rs                     入口, anyhow context
src/lib.rs                      #[path] 模块声明 + DownloadTask/PeriodKind 定义 + re-export
src/error.rs                    支撑: AppError(thiserror)
src/cache.rs                    支撑: redb 通用缓存(read/write/TTL)
src/backoff.rs                  支撑: 默认指数退避(max 30s)

steps/01_cli.rs                 cli:  clap: --config/-c (可选, 缺省 crate 内 config/config.toml)
steps/02_config.rs              config: Config::load + #[serde(default)] 全字段默认值
steps/03_build_client.rs        build_client: TCP 探测直连 443 → SOCKS5 127.0.0.1:7890 回退
steps/04_build_tasks.rs         build_tasks: tf→周期(D1-年/H1-月/M1-日) + URL/路径构建
steps/05_run_downloads/mod.rs   run_downloads: 已存在/缓存过滤 → Semaphore(30) 并发 → 404 标记 → 聚合
steps/05_run_downloads/stream.rs stream: GET → .bi5.tmp 原子写 → rename; 网络/5xx 重试, 404 永久
steps/06_app.rs                 app: 编排(config → 日期解析校验 → client → build_tasks → run_downloads)
```

## 01_cli.rs (`cli`)

clap 解析命令行,`--config/-c` 可选;为 `None` 时由 `config::load` 回退到 crate 内默认路径。

## 02_config.rs (`config`)

`Config::load`:`#[serde(default)]` + `Default` 全字段兜底,读 TOML;空 `symbol`/`timeframe` 回落默认值。
日期字符串合法性不在配置层校验,由 `app` 解析。

## 03_build_client.rs (`build_client`)

`build_client`:5s 内先 TCP 探测 `www.dukascopy.com:443`(直连),失败再探测 SOCKS5
`127.0.0.1:7890`;直连超时 30s,代理超时 60s;都失败则 `anyhow::bail!("no network")`。

## 04_build_tasks.rs (`build_tasks`)

`map_timeframe`/`tf_period`:tf 别名 → 数据后缀 + 周期(年/月/日)。`build_date_range` 按周期枚举
日期代表点;`build_url` 拼 Datafeed URL(`/datafeed/{symbol}/{y}/{m}/{d}/{price}_{suffix}.bi5`);
`dest_filename` 生成 `{symbol}-{tf}-{period}.bi5`。`build_tasks` 三层循环 `symbol × tf × date`
产出全部 `DownloadTask`(O(日数) 内存)。

## 05_run_downloads/ (`run_downloads`)

- `mod.rs`:`run_downloads` 先过滤(`dest.exists()` 或 redb 缓存命中 → skip);并发用
  `Semaphore(30)` + `join_all`,`force=true` 可跳过过滤。`aggregate_results`:404 计 `not_found`
  (预期),`failed > 0` 才返回 `AppError::Data`;成功与 404 都 `mark_cached` 写缓存,
  保证幂等、404 不重复请求。
- `stream.rs`:`download_to_file` → GET → 非 2xx:404 `Permanent(NotFound)`、其余 `Transient`;
  网络错误/读流 `Transient`;写 `.bi5.tmp` → flush → rename 原子落盘。`backoff` 上限 30s。

## 06_app.rs (`app`)

顶层编排:load 配置 → `parse_date`(YYYY-MM-DD)+ `start > end` 校验 → `build_tasks` →
`build_client` → `run_downloads(client, tasks, force=false)`。

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
`{symbol}-D1-{year}.bi5` / `{symbol}-H1-{year}-{month}.bi5` / `{symbol}-M1-{year}-{month}-{day}.bi5`。