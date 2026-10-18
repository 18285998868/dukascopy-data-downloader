# dukascopy-data-downloader

从 [Dukascopy Datafeed](https://www.dukascopy.com/datafeed) 下载历史 K 线原始 `.bi5` 文件（LZMA 压缩，**保持原始格式不解码**），并按官方目录结构镜像落盘。

程序直接请求重定向链的终端主机 `https://datafeed.dukascopy.com/datafeed`，以省去每个文件的 2 次重定向往返（详见[重定向链与终端主机](#重定向链与终端主机已实测确认-2026-10-18)）。

## 站点数据布局

```
https://datafeed.dukascopy.com/datafeed/
  {symbol}/
    {YYYY}/
      {PRICE}_candles_day_1.bi5     # D1/1d  一个文件覆盖一年
      {MM}/
        {PRICE}_candles_hour_1.bi5   # H1/1h  一个文件覆盖一月
        {DD}/
          {PRICE}_candles_min_1.bi5  # M1/1m  一个文件覆盖一天
```

`{PRICE}` 取 `BID` / `ASK`。落盘路径严格镜像服务器路径，程序自动在 `output_dir` 下追加 `dukascopy/` 前缀：

```
output_dir=/data  →  /data/dukascopy/{symbol}/{YYYY}[/{MM}[/{DD}]]/{PRICE}_candles_*.bi5
```

> ⚠️ **不要**把 `dukascopy` 写进 `output_dir`，否则会产生重复嵌套目录。

## 快速开始

```bash
# 1) 复制并编辑配置
cp config.example.toml config.toml

# 2) 运行
cargo run --release -- -c config.toml

# 3) 增量更新（再次运行同一命令即可，已存在的文件会被跳过）
cargo run --release -- -c config.toml
```

日志由 `RUST_LOG` 控制，缺省 `info`：

```bash
RUST_LOG=debug cargo run --release -- -c config.toml
```

## 配置

完整字段说明见 `config.example.toml`，冒烟配置见 `config.test.toml`。

| 字段 | 类型 | 必填 | 缺省 | 说明 |
|---|---|---|---|---|
| `symbol` | `string[]` | ✅ | — | 交易品种。**只允许 3..=20 位大写 ASCII 字母与数字**，如 `EURUSD`、`XAUUSD`、`CRUDEOIL`。自动排序去重 |
| `timeframe` | `string[]` | ✅ | — | `["M1","H1","D1","1m","1h","1d"]`，别名归一（`1m`≡`M1`）。自动排序去重 |
| `price_type` | `string` | | `"BID"` | `"BID"` / `"ASK"` |
| `start` | `YYYY-MM-DD` | | `2020-01-01` | 起始日期 |
| `end` | `YYYY-MM-DD` | | 昨天 | 截止日期，未来值自动截到昨天 |
| `output_dir` | `path` | | `"data"` | 数据根目录（**不含** `dukascopy`） |
| `concurrency` | `usize` | | `30` | **全局**最大并发下载数（不是每 pair），取值 `1..=256` |

配置校验规则：

- **未知键拒收**（`deny_unknown_fields`），拼错即报错，错误信息带配置文件路径。
- `symbol` / `timeframe` 不能为空。
- `symbol` 必须匹配 `[A-Z0-9]{3,20}` —— 它会被原样插入 URL 路径与本地目录路径，放宽会引入路径穿越与 URL 查询/片段注入。
- `concurrency` 必须 `1..=256`。
- `start` 与 `end` 同时出现时，`start > end` 报错。
- 列表自动 `sort + dedup`。

### 生效日期区间

`AppConfig::effective_range(today)` 是唯一计算区间的地方：

```
start = config.start           或 2020-01-01
end   = min(config.end, 昨天)
天数 > 366 × 15 (15 年) → 报错退出
```

> 这一步刻意在**解析完默认值之后**再做上限校验。如果只校验"用户显式写了 start 和 end"的情况，省略 `end`（它是可选项！）就能开出无上限区间——例如 `start = "1900-01-01"` 会展开出 46000+ 个任务。

## 架构

```
main.rs                      仅入口：初始化 tracing、解析 -c/--config
lib.rs                       模块导出 + lint 配置
cli.rs                       clap 参数
config.rs                    TOML 解析 / 校验 / 区间归一 + Symbol、Timeframe、PriceType、DownloadTask
error.rs                     AppError（thiserror）+ chain() 错误链展开
app/
  mod.rs                     编排：JoinSet 并发跑 pair，共享下载闸门
  build_urls.rs              ① pair -> 惰性 URL 迭代器
  build_download_tasks.rs    ② 剔除本地缓存 + 绑定目标路径
  download/
    mod.rs                   ③ 有界并发下载、统计、熔断
    stream.rs                单文件流式下载 + 退避重试 + 原子落盘
utils/
  scan_local_files.rs        扫描本地 .bi5 缓存（同遍清理 .bi5.tmp）
  backoff.rs                 指数退避策略
```

### 执行流程

```
main
 └─ app::run(config.toml)
     ├─ AppConfig::load            读文件 → toml 解析 → 校验
     ├─ build_client               连接超时 10s / 请求超时 30s / 连接池每主机 256 空闲
     ├─ scan_local_files           一次全量扫描，得到相对路径集合；同时删除残留 *.bi5.tmp
     ├─ effective_range            归一起止日期并施加 15 年上限
     ├─ Semaphore(concurrency)     全局下载闸门，整个 run 共享
     └─ JoinSet: for symbol × for timeframe 并发 spawn
          └─ run_one
              ├─ start > end        → warn 跳过（不联网）
              ├─ build_download_tasks  ① → ②
              ├─ 无任务              → debug 跳过（全部命中缓存）
              └─ run_downloads      ③
```

## 关键设计

### 全局并发闸门

所有 `symbol × timeframe` 组合通过 `JoinSet` **并发**执行，但共享同一个 `Semaphore`。
因此 `concurrency` 是**整个 run 的在途请求上限**，而不是每个 pair 各算一份 —— 5 个品种 × 3 个周期（15 个 pair）同时跑，在途请求仍然不超过 `concurrency`。

若某个品种完全不可达（例如拼写错误），每个文件都会各自耗尽退避窗口。该 pair **连续失败 5 次**后触发熔断，剩余文件标记为 `skipped` 不再尝试，让其它 pair 尽快跑完。`skipped` 不会被算进 `failed`。
成功或 404（节假日属正常）都会重置连续失败计数。

### 落盘原子性与持久性

```
GET → 校验状态码 → 建父目录 → 删残留 .bi5.tmp
    → 流式写 .bi5.tmp → flush → sync_all → rename → .bi5
```

- 写入期间只存在 `.bi5.tmp`，最终文件名下永远不会出现半截文件。
- `sync_all()` 而非仅 `flush()`：只 `flush` 只能把数据交给内核，断电后仍可能留下一个"已 rename 但为空"的 `.bi5`，而缓存扫描只看文件名不看大小，会把它永久当成完整数据 —— 该文件从此再也不重下。
- 每次重试前先删除残留 `.bi5.tmp`，避免 `File::create` 在旧内容后追加。

### 重试策略

| 响应 | 处理 |
|---|---|
| 2xx | 成功 |
| 404 | 永久失败，记 `not_found`，**不重试**（节假日/边界日期属正常） |
| 429 / 5xx | 瞬态错误，指数退避重试 |
| 其他 4xx | 永久失败 |

`Retry-After` 响应头（delta-seconds 形式）会被解析并透传给退避策略；HTTP-date 形式需要可信时钟与额外解析器，未做处理，降级为指数退避。

**超时与退避刻意设在不同量级**：单次请求超时 30s，整个文件的重试预算 120s。两者若相等，一次卡死的请求就能吃掉全部重试预算，重试策略会静默退化成"只试一次"。

### 缓存

`scan_local_files` 遍历 `output_dir`，收集所有 `.bi5` 相对路径构建 `HashSet`，建 task 时直接查表剔除。重复运行即增量更新。

只删除 `*.bi5.tmp`。**不会**扫荡其它 `.tmp` 文件 —— 若 `output_dir` 指向共享目录，全量扫荡会误删他人文件。

### 重定向链与终端主机（已实测确认 2026-10-18）

程序**直接请求终端主机** `datafeed.dukascopy.com`，而不是公开入口 `www.dukascopy.com`：

| 请求入口 | 重定向次数 | 耗时 |
|---|---|---|
| `https://www.dukascopy.com/datafeed/...` | 2 | 3.51s |
| `https://datafeed.dukascopy.com/datafeed/...`（本程序采用） | **0** | **2.03s** |

完整链条：

```
https://www.dukascopy.com/datafeed/...        301 → http://datafeed.dukascopy.com/datafeed/...
http://datafeed.dukascopy.com/datafeed/...    301 → https://datafeed.dukascopy.com/datafeed/...
https://datafeed.dukascopy.com/datafeed/...   200 | 429    ← 行情数据始终经 TLS 传输
```

两点结论：

1. **数据安全无差异**。入口主机第 1 跳会降级为明文，但该请求只携带 `Location` 头，不含任何行情数据；K 线负载**无论从哪个主机请求，始终在 HTTPS 上传输**。因此无需自定义重定向策略，reqwest 默认策略即可到达负载。
2. **每个文件省 2 次往返**。走入口主机等于每个文件多 2 次 HTTP 往返，全量区间下就是 `2 × N` 次额外请求（D1 区间 N 较小，M1 全区间可达数千）。对下载密集场景这是可观损耗。

> ⚠️ **权衡与回退**：`datafeed.dukascopy.com` 是重定向链的**终端主机**，而非 Dukascopy 官方文档化的公开入口，对方理论上可能改名。已用单元测试 `feed_base_targets_the_terminal_host` 固定该主机。**若出现批量 HTTP 404，把 `src/app/build_urls.rs` 中的 `FEED_BASE` 改回 `https://www.dukascopy.com/datafeed` 即可恢复**（代价是退回 2 次重定向）。

### 无代理回退

客户端**直连**，不做代理回退探测。若目标网络需代理，请在环境侧自行配置（如 `HTTPS_PROXY` 环境变量，或系统级透明代理）。历史上曾计划实现"直连失败后回退代理"，当前**尚未实现**，此处如实记录以免误导。

## 错误处理

`AppError`（thiserror）跨层统一：

| 变体 | 含义 |
|---|---|
| `Http` | reqwest 传输错误（保留 `source`） |
| `Io` | 文件系统错误 |
| `Config` | 配置读取 / 解析 / 校验失败 |
| `Data` | 数据处理失败（含整批汇总） |
| `DownloadFailed` | 单文件下载失败 |
| `NotFound` | HTTP 404 |
| `BatchFailed` | 批次内有文件失败，携带精确计数 + 有上限的原因样本 |
| `Join` | tokio 任务 join 错误 |

日志与汇总一律使用 `AppError::chain()`，它会沿 `Error::source()` 展开完整因果链。仅用 `Display` 会丢掉根因（表现为只剩 `http error: ...`，看不到底层的 `operation timed out`）。

单文件失败原因保留上限 20 条，计数始终精确。

## 测试

```bash
cargo test              # 123 个单元测试 + 9 个 doctest
cargo clippy --all-targets -- -D warnings
```

覆盖范围：

- `config` —— 解析、别名归一、符号字符集/长度、区间归一与上限、15 年封顶回归
- `build_urls` —— 年/月/日三种分桶、跨月跨年、惰性（`take(3)` 不展开全量）、rel_path 与 URL 严格镜像
- `build_download_tasks` —— 缓存剔除、price_type 参与缓存键、URL 与目标路径一致性
- `download` —— 计数口径、熔断触发/重置、原因上限、**跨 pair 的全局闸门**（并发 4 个 pair 峰值仍为 3）
- `download::stream` —— 状态码分类、`Retry-After` 解析、wiremock 端到端（成功/覆盖/404 不重试/5xx 重试恢复/大文件/空体）、`TempDir` 隔离
- `scan_local_files` —— 嵌套扫描、扩展名过滤、`.bi5.tmp` 清理、**不误删无关 `.tmp`**

## 已知限制

- 只支持 D1 / H1 / M1，不支持 Dukascopy 的 tick 与其他周期。
- 不校验 `.bi5` 内容完整性（不做 LZMA 解码校验），按原始字节镜像保存。
- 下载前不查询交易日历，直接按自然日展开；非交易日由 404 容忍（计 `not_found`）。
- 无断点续传：中断后重跑依赖 `.bi5.tmp` 清理 + 整体重下该文件。
