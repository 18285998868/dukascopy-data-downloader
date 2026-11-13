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
cp config/00.example.toml config.toml

# 2) 运行
cargo run --release -p dukascopy-data-downloader-cli -- -c config.toml

# 3) 增量更新（再次运行同一命令即可，已存在的文件会被跳过）
cargo run --release -p dukascopy-data-downloader-cli -- -c config.toml
```

日志由 `RUST_LOG` 控制，缺省 `info`：

```bash
RUST_LOG=debug cargo run --release -p dukascopy-data-downloader-cli -- -c config.toml
```

### 命令行参数

| 参数 | 说明 |
|---|---|
| `-c, --config <PATH>` | TOML 配置文件路径（**必填**） |
| `--allow-missing` | 豁免"请求区间不在 feed 里"的三条判据（404 占比 / 连续缺失段 / 全部 404）：不再因此非零退出，只打 `warn`。文件**真的下载失败**仍然让 run 失败。详见"错误处理" |

```
Download raw Dukascopy historical kline (.bi5) files from the public Datafeed

Usage: dukascopy-data-downloader [OPTIONS] --config <CONFIG>

Options:
  -c, --config <CONFIG>
          Path to the TOML configuration file

      --allow-missing
          Tolerate a requested range that the data feed does not have.

          Without this flag the run fails when the feed is missing the requested range
          (too many 404s, or a long stretch of consecutive missing files). With it, the
          run finishes successfully with whatever the feed did have. Files that actually
          failed to download still fail the run.

  -h, --help
          Print help (see a summary with '-h')
```

按 Ctrl+C 可以安全中断：程序取消 run、释放运行锁，并以非零状态退出（一次被中断的 run 并没有交付请求的区间）。

## 配置

完整字段说明见 `config/00.example.toml`，冒烟配置见 `config/00.smoke.toml`。

| 字段 | 类型 | 必填 | 缺省 | 说明 |
|---|---|---|---|---|
| `symbol` | `string[]` | ✅ | — | 交易品种。**只允许 3..=20 位大写 ASCII 字母、数字与 `.`**，如 `EURUSD`、`XAUUSD`、`LIGHT.CMD`。点不能在首尾或连续出现。自动排序去重 |
| `timeframe` | `string[]` | ✅ | — | `["M1","H1","D1","1m","1h","1d"]`，别名归一（`1m`≡`M1`）。自动排序去重 |
| `price_type` | `string` | | `"BID"` | `"BID"` / `"ASK"` |
| `start` | `YYYY-MM-DD` | | `2020-01-01` | 起始日期。若该默认值已超出 15 年窗口，自动顺延到窗口内并 `warn` |
| `end` | `YYYY-MM-DD` | | 昨天 | 截止日期，未来值自动截到昨天 |
| `output_dir` | `path` | | `"data"` | 数据根目录（**不含** `dukascopy`） |
| `concurrency` | `usize` | | `30` | **全局**最大并发下载数（不是每 pair），取值 `1..=256` |

配置校验规则：

- **未知键拒收**（`deny_unknown_fields`），拼错即报错，错误信息带配置文件路径。
- `symbol` / `timeframe` 不能为空。
- `symbol` 必须匹配 `[A-Z0-9.]{3,20}`，且点不能在首尾或连续出现（`.` / `..` 因此被排除）—— 它会被原样插入 URL 路径与本地目录路径，放宽会引入路径穿越与 URL 查询/片段注入。允许点是 Dukascopy 的 `LIGHT.CMD` / `BRENT.CMD` 等品种所需。长度按**字符**计（不是字节）：`"欧元"` 是 2 个字符 6 字节，按字节算会从"字符数"这条规则里漏过去。
- `concurrency` 必须 `1..=256`。
- `start` 与 `end` 同时出现时，`start > end` 报错。
- 列表自动 `sort + dedup`。

### 生效日期区间

`AppConfig::effective_range(today)` 是唯一计算区间的地方：

```
start = config.start           或 max(2020-01-01, 昨天 - (15年 - 1天))
end   = min(config.end, 昨天)
显式 start 时，天数 > 366 × 15 (15 年) → 报错退出
```

> 这一步刻意在**解析完默认值之后**再做上限校验。如果只校验"用户显式写了 start 和 end"的情况，省略 `end`（它是可选项！）就能开出无上限区间——例如 `start = "1900-01-01"` 会展开出 46000+ 个任务。
>
> 天数按**闭区间**计：`start..=end` 覆盖的天数是两个日期间隔 + 1，所以上限 5490 天对应的是"含首尾共 5490 个自然日"。区间校验是纯算术，排在所有 I/O 之前；区间反序（`start > end`）则直接 warn 返回，连磁盘都不扫。
>
> 默认起点固定为 `2020-01-01` 会在 2035-01-13 之后超出 15 年窗口。为了避免"用户什么都没配错却报错"，省略 `start` 时程序会把默认起点顺延到窗口内最早的一天并打 `warn`；**显式**写出超限的 `start` 仍然报错。上限以公开常量 `ddd::MAX_RANGE_DAYS`（= 366 × 15）暴露，调用方不必把它抄一遍。

## 架构

```
main.rs                      仅入口：初始化 tracing、解析 -c/--config
lib.rs                       模块导出
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
  backoff.rs                 墙钟预算 + 指数退避策略
  run_lock.rs                output_dir 独占运行锁（拒绝并发实例）
tests/
  public_api.rs              公开 API 契约（配置 / URL / 缓存 / 错误）
  download_flow.rs           wiremock 端到端（下载 → 落盘 → 404 语义）
```

> lint 策略统一放在 `Cargo.toml` 的 `[workspace.lints]`：lib、bin、集成测试三者受同一套约束（此前 crate 属性只覆盖声明它的那个 crate，`src/main.rs` 不受约束）。
>
> | 组 | 级别 | 作用 |
> |---|---|---|
> | `rust::missing_docs` / `missing_debug_implementations` | warn | 公开项必须有文档；公开类型必须能 `Debug` |
> | `rust::unsafe_code` | warn | 全 crate 零 `unsafe`：运行锁刻意用原子 `create_dir` 而不是 `flock`，正是为了不引入 C 依赖与 `unsafe` |
> | `clippy::correctness` | **deny** | 明确会出错的写法，直接编译失败 |
> | `clippy::suspicious` / `style` / `complexity` / `perf` | warn | 常规可疑写法 |
> | `clippy::pedantic` | warn | 补齐上面几组漏掉的：公开项缺 `#[must_use]`、文档里的标识符没加反引号、浮点转换丢精度、函数长到无法评审 |
>
> `clippy::pedantic` 之前没开，开启后它一次抓出了 10 处问题（4 处 `usize → f64` 精度损失、1 处超长函数、1 处 `HashSet` 未泛型化 hasher、2 处文档反引号、1 处多余生命周期、1 处缺 `#[must_use]`）—— 这些都已修掉，现在 `cargo clippy --all-targets --all-features -- -D warnings` 零告警。
>
> 占比判定一律用**整数比较**（`part * den` vs `whole * num`），不用浮点除法：阈值是精确分数（1/20 = 5%、1/2 = 50%），浮点只会给一个本不需要舍入的比较加上舍入，并让"恰好 5%"这种边界依赖二进制的表示方式。

### 执行流程

```
main
  └─ app::run(config.toml)
     ├─ termination_signal       注册 SIGINT / SIGTERM handler（取锁之前！）
     ├─ AppConfig::load            spawn_blocking：读文件 → toml 解析 → 校验
     ├─ acquire_run_lock           output_dir 独占锁（拒绝并发实例）
     └─ select! { run_pipeline(...) | SIGINT / SIGTERM }
        ├─ effective_range            归一起止日期并施加 15 年上限（纯计算，先于任何 I/O）
        ├─ start > end                → warn 直接返回（不扫盘、不联网）
        ├─ build_client               spawn_blocking：连接超时 30s / 请求超时 30s / 连接池每主机 256 空闲
        ├─ scan_local_files           一次全量扫描，得到相对路径集合；同时删除残留 *.bi5.tmp
        ├─ Semaphore(concurrency)     全局下载闸门，整个 run 共享
        └─ JoinSet: for symbol × for timeframe 并发 spawn
             └─ run_pair                tracing span：symbol / timeframe
                 └─ run_one
                     ├─ start > end        → warn 跳过（不联网）
                     ├─ 无已闭合桶         → debug 跳过（见"只下载已闭合的桶"）
                     ├─ build_download_tasks  ① → ②
                     ├─ 无任务              → debug 跳过（全部命中缓存）
                     └─ run_downloads      ③
```

> 阻塞调用（读配置、建客户端、扫盘、取锁）一律走 `spawn_blocking`，不占住 async worker 线程；文件读写用 `tokio::fs`。区间归一与上限校验是纯算术，排在最前 —— 配置写错时代价是零，而不是一次全盘扫描。
>
> **信号与锁的时序**：信号 handler 在**取锁之前**注册，取锁发生在 `select!` 之外，锁 guard 则**由 pipeline future 持有**。中断时 future 被丢弃 → guard `Drop` → 锁目录删除。取锁本身走 `spawn_blocking`，该任务不可取消；即使 future 先被丢弃，任务仍会完成，但它返回的 guard 会立即被丢弃，因此无论哪边先结束都不会漏锁。代价是中断的 run 以非零状态退出（`AppError::Interrupted`）。
>
> **运行锁**：`output_dir/.ddd.lock/` 目录即锁。同一 `output_dir` 上并发跑两个实例不安全 —— 后启动的那个会在扫描阶段删掉前一个正在写的 `.bi5.tmp`，让那个文件以 "No such file or directory" 失败。锁用目录实现（创建目录是原子操作），不需要 C 依赖或 `unsafe`；代价是进程被 `kill -9` 强杀后锁目录会残留，需要手动删除（错误信息里会给出路径）。
>
> **Ctrl+C 会正常释放锁**：程序注册了 `SIGINT` / `SIGTERM` handler，收到信号即取消 run —— pipeline future 被丢弃，锁 guard 随之 `Drop`，锁目录被删除。锁的获取放在 `select!` 之外，因此中断只会取消已经持有锁的 pipeline，不会把"正在取锁"本身丢在半途。代价是中断时程序**以非零状态退出**（`AppError::Interrupted`）—— 一次被中断的 run 并没有交付请求的区间，只看退出码的调用方不能把它当成"下载完成"。

## 关键设计

### 只下载已闭合的桶

一个"桶"就是一个文件覆盖的时间跨度：**M1 一天 / H1 一月 / D1 一年**。服务端会持续往桶里追加数据，直到它覆盖的最后一个日历日结束 —— 桶在那之后才定稿。

程序**只下载已定稿的桶**。原因很直接：缓存一旦命中就不会重下（缓存判定只看文件名），所以下载一个还没定稿的桶，等于把一份"将来还会变"的文件永久冻结在当前状态，缺口永远补不上。

按周期折算（`end` 已被钳到"昨天"）：

| 周期 | 桶 | 桶定稿时刻 | 实际下载范围 |
|---|---|---|---|
| M1 | 1 天 | 该日 24:00 后 | ≤ 昨天（天然满足） |
| H1 | 1 月 | 该月末 24:00 后 | ≤ 上月末 |
| D1 | 1 年 | 该年末 24:00 后 | ≤ 上年末 |

**代价**：H1 要等月末、D1 要等年末才能拿到当期的完整数据。
**收益**：不会把半截数据永久缓存。

> ⚠️ **对当前 feed 的实际影响**（重新实测于 2026-10-20）
>
> Dukascopy 的数据 feed 本身有延迟：实测最新只发布到 **2026-09-19**（约 31 天前）。与"只下已闭合桶"叠加后的结果是：
>
> | 周期 | 实际能拿到的最新数据 | 原因 |
> |---|---|---|
> | M1 | 2026-09-19 | feed 边界；日桶当天即定稿，不受闭合规则限制 |
> | H1 | 2026-08 | 9 月的月文件尚未发布 → 404（10 月的月文件因"未闭合"根本不会去请求） |
> | D1 | **2025 年** | 2026 年的年文件在 feed 落后期间从未存在过 → 404 |
>
> 即使 feed 恢复正常，**D1 仍要等到 12-31 之后**才能拿到 2026 年的完整年文件 —— 这是"年桶"的固有粒度，不是本程序能绕开的。
>
> 这些缺失**不会**被当作成功：程序会因为 404 占比或连续缺失段超阈值而以非零状态退出（见"错误处理"），你不会在毫不知情的情况下拿到一份"看起来完整、实际缺一大段"的数据。
>
> 例：请求 `M1 / 2026-08-25 .. 2026-10-19`，实测下到 25 个文件、31 个 404，最长连续缺失段 30 天 → 以 `RangeUnavailable` 非零退出。

### 全局并发闸门

所有 `symbol × timeframe` 组合通过 `JoinSet` **并发**执行，但共享同一个 `Semaphore`。
因此 `concurrency` 是**整个 run 的在途请求上限**，而不是每个 pair 各算一份 —— 5 个品种 × 3 个周期（15 个 pair）同时跑，在途请求仍然不超过 `concurrency`。

### 熔断：探针 + 滑动窗口失败率

若某个品种完全不可达（例如网络不通），每个文件都会各自耗尽 120s 重试窗口。熔断的作用是止损，机制分两层：

**第一层 · 探针（串行）**

每个 pair 的**第一个文件单独下载**，其余文件在它成功之后才放量。

- 探针遇到**传输层错误**（DNS / 连接 / 超时）→ 判定该 pair 不可达 → 剩余文件全部 `skipped`，**一个请求都不再发**
- 探针遇到 **HTTP 状态错误**（5xx / 429，重试预算耗尽后仍失败）→ 不判定不可达（那是服务端的问题，不是"这个品种不存在"的证据）→ 正常放量

这一层把"完全不可达"的代价从 `concurrency` 个请求 × 120s 降到 **1 个请求 × 120s**。

**第二层 · 滑动窗口失败率**

熔断器记录最近 **32** 次尝试的成败，窗口内失败率 ≥ **50%** 时开闸，之后该 pair 的剩余文件全部 `skipped`。

为什么不用"连续失败计数"：任何一次成功都会把它清零，于是**服务端一半请求失败时熔断永远不触发**，程序会在每个文件上重复烧掉整个重试预算。窗口没有这个盲区。

`skipped` 永远不会计入 `failed`。

### 落盘原子性与持久性

```
GET → 校验状态码 → 建父目录 → 删残留 .bi5.tmp
    → 流式写 .bi5.tmp（统计字节数） → 写入 0 字节？删 .tmp 并失败
    → sync_all → rename → .bi5
    → fsync 父目录
```

- 写入期间只存在 `.bi5.tmp`，最终文件名下永远不会出现半截文件。
- **HTTP 200 但 body 为空**（`Content-Length: 0` 或直接结束）会被拒绝：统计写入字节数为 0 时删除 `.tmp` 并返回 `DownloadFailed`（`empty response body (HTTP 200 with no payload)`），且不重试。合法的 `.bi5` 是 LZMA 容器，至少带文件头，不可能是空文件；而缓存扫描只看文件名不看大小，提交空文件会让它被永久当成完整数据，此后再也不重下。
- `sync_all()` 而非仅 `flush()`：只 `flush` 只能把数据交给内核，断电后仍可能留下一个"已 rename 但为空"的 `.bi5`，而缓存扫描只看文件名不看大小，会把它永久当成完整数据 —— 该文件从此再也不重下。（`File::flush` 本身是空操作，保留它只会白白多走一趟 blocking pool，所以直接省掉。）
- `rename` 之后额外 `fsync` 父目录：文件本体的 `sync_all` 只保证**内容**持久，rename 创建的**目录项**仍在 page cache 里，不刷目录的话断电可能撤销这次 rename，载荷退回 `.bi5.tmp` 甚至消失。目录 fsync 失败（个别文件系统不支持）时忽略，不因此判定下载失败。
- 每次重试前先删除残留 `.bi5.tmp`，避免 `File::create` 在旧内容后追加。

### 重试策略

| 响应 | 处理 |
|---|---|
| 2xx | 成功 |
| 404 | 永久失败，记 `not_found`，**不重试**（节假日/边界日期属正常） |
| 429 / 5xx | 瞬态错误，指数退避重试 |
| 其他 4xx | 永久失败 |

**超时与退避刻意设在不同量级**：单次请求超时 30s，整个文件的重试预算 120s。两者若相等，一次卡死的请求就能吃掉全部重试预算，重试策略会静默退化成"只试一次"。

#### 预算是绝对墙钟截止时刻，不是"间隔之和"

`ExponentialBackoff` 自带的 `max_elapsed_time` 只统计**退避间隔**的累计，忽略请求本身花掉的时间。本项目改为一个绝对截止时刻（`RetryBudget`），请求耗时、服务端要求等待的时间都从里面扣。

这个改动不是洁癖，而是**必需的**：`backoff::future::retry` 在失败携带 `retry_after` 提示时，会**优先用提示值而不再询问退避策略**（`retry_after.or_else(|| backoff.next_backoff())`，`Option::or_else` 是短路的）。而"是否该停止重试"的判断恰好只存在于 `next_backoff()` 内部 —— 提示值一旦出现，预算检查就永远不会被执行。

后果是致命的：Dukascopy 限流时确实会返回 429 + `Retry-After`，只要每次响应都带这个头，单个文件就会**无限重试**，该 pair 的任务永不结束，整个 run 永久挂起；熔断也救不了场（熔断统计的是"已完成的失败"，挂起的任务根本没完成）。

因此现在的做法是：

- `Retry-After`（delta-seconds 形式）由**下载操作自身**消费（`sleep` 后再交给退避策略），**不再**交给 `backoff` crate；
- 等待时长被钳制在**剩余预算**之内，`Retry-After: 86400` 最多也只等到预算耗尽；
- 退避策略每次都会被询问，预算检查永不失效；
- HTTP-date 形式需要可信时钟与额外解析器，未做处理，降级为指数退避。

| 现象 | 处理 |
|---|---|
| 429 带 `Retry-After: 60`，预算只剩 200ms | 等待 200ms 后放弃（不睡满 60s） |
| 429 带 `Retry-After: 1` | 老实等 1s 再重试，不硬刚限流器 |
| 连续 5xx，预算 120s | 预算耗尽即放弃该文件 |

### 缓存

`scan_local_files` 遍历 `output_dir`，收集所有 `.bi5` 相对路径构建 `HashSet`，建 task 时直接查表剔除。重复运行即增量更新。

扫描时删除两类文件：`*.bi5.tmp` 残留，以及**零字节的 `.bi5`**（空文件不可能是合法 LZMA 容器，留在缓存里等于永久冻结缺口）。**不会**扫荡其它 `.tmp` 文件 —— 若 `output_dir` 指向共享目录，全量扫荡会误删他人文件。

缓存键统一用 `/` 分隔（与生成 URL 相对路径时的拼法一致），因此在 Windows 上缓存同样有效。

> **404 不是节假日**。实测 Dukascopy 为**每个日历日**都提供数据文件（含周末与节假日）：`2025/01/04`（周六）、`2026/08/30`（周日）都返回 200；D1 年文件 2025 年有完整 365 条、日线 candle 的时间戳就落在 UTC 00:00。所以 404 只有一个含义：**该文件不存在**，通常是请求区间超出了 feed 的覆盖范围。程序据此分级处理（见"错误处理"）。

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
| `Http` | reqwest 传输错误（保留 `source`）—— 连接不通、DNS 失败、超时 |
| `Io` | 文件系统错误（不含路径） |
| `FileIo` | 带路径的文件系统错误 —— 批量汇总里 20 条裸 io 错误无从区分，带路径才定位得到 |
| `Config` | 配置读取 / 解析 / 校验失败（含"另一个实例正持有运行锁"） |
| `DownloadFailed` | 单文件下载失败（HTTP 状态错误，重试预算耗尽后仍失败） |
| `NotFound` | HTTP 404 —— 该文件不存在 |
| `BatchFailed` | 单个 pair 批次内有文件失败，携带精确计数 + 有上限的原因样本 |
| `PairsFailed` | 整个 run 有 pair 失败，携带精确计数 + 有上限的原因样本 |
| `RangeUnavailable` | 请求区间不存在（404 占比过高、存在长段连续缺失，或批次内**全部** 404），很可能是超出了 feed 的覆盖范围（`--allow-missing` 可豁免） |
| `Interrupted` | run 被 `SIGINT`（Ctrl+C）/ `SIGTERM` 取消 —— 锁已释放，区间未交付，程序非零退出 |
| `Join` | tokio 任务 join 错误 |

两级失败分开表达，不混用一个变体：`BatchFailed`（pair 内的**文件**级）与 `PairsFailed`（run 内的 **pair** 级）各自计数，"failed" 指代哪一单位一目了然。

**404 占比分级**：由于 404 只意味着"文件不存在"，占比过高说明请求区间超出了 feed 的覆盖范围。程序按占比分级处理：

| 404 占比 | 处理 |
|---|---|
| ≤ 5% | 正常，记 `not_found` |
| > 5% | `warn` 提示区间可能超出覆盖范围 |
| > 50%（且样本 ≥ 8） | 判定失败，返回 `RangeUnavailable`，**run 以非零状态退出** |
| 全部 404（`ok == 0`，任意样本数） | 判定失败，返回 `RangeUnavailable`，**run 以非零状态退出** |

占比用整数交叉相乘比较（`not_found × 20` vs `total × 1`），不用浮点除法 —— 阈值是精确分数，"恰好 5%"必须落在"不告警"这一侧，且不受二进制表示影响。

最后一条不设最小样本数：1 个文件也好、1000 个文件也好，"一个都没下到"是明确的失败，不需要用比例来证明。此前这条路径（例如只请求 2 个文件、全 404）会**成功退出却一个文件都没下**，用户以为数据已完整下载；现在它与占比、连续缺失段一样由 `RangeUnavailable` 报错，`--allow-missing` 下则降级为 `warn`。

**连续缺失段检测**（与占比判据独立）：把缺失的文件按**桶**（M1 一天 / H1 一月 / D1 一年）归位、排序，找出**日历上连续**的最长缺失段。连续 ≥ **5** 个桶缺失即判定失败 —— 这条不看占比，所以"整段缺失、其余正常"（占比可能只有 10~20%）也能被抓住。阈值定在 5 是因为一个长周末加一个法定假日最多连续 3 天。

> 缺失段**按周期分组计算**：`D1` 的桶键是年份（`2025`），`M1` 的桶键是完整日期（`2025/06/15`），两套键空间不可比。混在一个列表里排序会让 `2023` 与 `2023/06/15` 挨在一起，把连续的年份缺失段切成几截 —— 明明"连续缺 5 年"却因最长段只有 3 而判定通过。因此先按周期分组，组内各自算最长段，最后取各组最大值。
>
> 公开 API `run_downloads` 允许一个批次里混合周期，分组逻辑正是为此存在（`tests/public_api.rs` 有对应回归测试）。

三条判据都通过 `RangeUnavailable` 报错，消息里带上最长连续缺失段的长度。

> ⚠️ **对工作流的影响**
>
> 默认（严格）行为把"数据源落后"变成**显式失败**：任何跨越 feed 落后区间的 run 都会**以非零状态退出** —— 即使它已经成功下载了区间内能拿到的全部文件。
>
> - 工作流若是"**要么完整、要么报错**"（CI、定时全量同步），默认行为正合需要。
> - 工作流若是"**跑一次、拿到能拿到的、其余忽略**"（例如在 feed 落后期间做增量维护），加 **`--allow-missing`** 即可豁免这三条判据。
>
> ```
> # 严格模式：区间超出 feed 覆盖范围 → 以 RangeUnavailable 非零退出
> dukascopy-data-downloader -c config.toml
>
> # 尽力而为：同样的区间 → 只打 warn，退出码 0
> dukascopy-data-downloader -c config.toml --allow-missing
> ```
>
> `--allow-missing` **只豁免"区间不在 feed 里"这三条判据**。文件**真的下载失败**（连接错误、超时、5xx 耗尽重试预算）仍然会让 run 失败 —— 那是"下载坏了"，不是"feed 没有这段区间"。实测：feed 限流返回 503 时，带不带这个开关都会非零退出。
>
> 豁免时程序会打三条 `warn` 说清楚发生了什么，不会静默吞掉：可下载的占比、最长连续缺失段、以及"全部文件都没找到"。

日志与汇总一律使用 `AppError::chain()`，它会沿 `Error::source()` 展开完整因果链。仅用 `Display` 会丢掉根因（表现为只剩 `http error: ...`，看不到底层的 `operation timed out`）。

单文件失败原因保留上限 20 条，计数始终精确。

**panic 诊断**：pair 任务若 panic，`JoinError` 的 `Display` 只说"某个任务 panic 了"，**不含 panic 消息** —— 消息只存在于 panic payload 里。程序会取回 payload 一并打进日志，否则排障时只能看到一句没有信息量的 "task panicked"。

## 测试

```bash
cargo test              # 216 个单元测试 + 14 个集成测试 + 16 个 doctest
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
cargo doc --no-deps     # 公开 API 文档（无断链）

# 冒烟：真实下载 1 个品种 × D1 × 2025 年，输出到 /tmp
cargo run --release -p dukascopy-data-downloader-cli -- -c config/00.smoke.toml
```

单元测试（`src/` 内 `#[cfg(test)]` 模块）覆盖范围：

- `config` —— 解析、别名归一、符号字符集/长度（含**多字节字符按字符数计**、**带点品种**、**危险点位置拒绝**、**`Symbol` 克隆零拷贝**）、区间归一与上限（含闭区间天数边界、**默认起点在 2035 年后顺延**）、15 年封顶回归
- `build_urls` —— 年/月/日三种分桶、跨月跨年、惰性（`take(3)` 不展开全量）、rel_path 与 URL 严格镜像、**已闭合桶边界**
- `build_download_tasks` —— 缓存剔除、price_type 参与缓存键、URL 与目标路径一致性
- `download` —— 计数口径（四个计数器互斥且合计等于 total）、**探针先行**（传输失败即放弃整对）、**滑动窗口失败率**（50% 交错失败也能熔断、恰好半窗口即熔断）、低失败率不熔断、原因上限、**404 连续缺失段检测**（**按周期分组**、**无法定位的键不打断真实缺口**、**越界日历键不可定位**、**元旦跨年边界**）、**占比比较的整数精确性**、**全部 404 的小批次也失败**（含 `--allow-missing` 豁免）、**`--allow-missing` 只豁免区间判据**（真实下载失败仍然失败）、**跨 pair 的全局闸门**（并发 4 个 pair 峰值仍为 3）
- `download::stream` —— 状态码分类、`Retry-After` 解析（含 **u64 溢出值当作文法错误**）、wiremock 端到端（成功/覆盖/404 不重试/5xx 重试恢复/大文件/**空 body 拒绝且不留 `.tmp`**）、**预算不被 `Retry-After` 绕过**、IO 失败带路径
- `scan_local_files` —— 嵌套扫描、扩展名过滤、`.bi5.tmp` 清理、**零字节 `.bi5` 删除且不缓存**、**不误删无关 `.tmp`**、缓存键分隔符归一、**运行锁目录不计入缓存**
- `run_lock` —— 锁的获取/释放、第二个实例被拒、**拒绝信息给出待删目录**、输出目录按需创建、**中止持有者会释放锁**（中断路径的关键不变量）
- `backoff` —— 预算耗尽即停、间隔不超预算、`reset` 重置间隔增长
- `error` —— 错误链展开、两级失败变体不混淆、空原因列表不留悬空分隔符、**传输错误保留根因**、**中断错误点名信号**
- `cli` —— 参数解析、**帮助文本包含 `--config` 与 `--allow-missing`**、opt-out 默认关闭
- `app` —— panic payload 恢复、**未闭合桶导致整对跳过**

集成测试（`tests/`）从**外部使用者视角**验证公开契约，只能用 `pub` API：

- `public_api.rs` —— 配置加载契约、URL/目标路径/任务元数据三者同步、缓存键一致性（含 price_type）、错误链、**混合周期批次按周期分别分析缺失段**
- `download_flow.rs` —— wiremock 端到端：落盘内容正确、scratch 文件被 rename 掉、404 不留痕、批次里 404 与成功混排、**小批量全 404 返回 `RangeUnavailable`**、**空 200 body 不落盘**

> 集成测试**不覆盖 5xx 重试路径**：公开的 `download_to_file` 用的是生产退避预算（120s），会让测试挂住。5xx 由单元测试覆盖 —— 那里可以注入退避策略。

## 已知限制

- 只支持 D1 / H1 / M1，不支持 Dukascopy 的 tick 与其他周期。
- 不校验 `.bi5` 内容完整性（不做 LZMA 解码校验），按原始字节镜像保存。
- **只下载已闭合的桶**（见"只下载已闭合的桶"）：H1 要等月末、D1 要等年末才能拿到当期完整数据；feed 落后期间 D1 实际只能拿到**上一年**。
- **数据 feed 有延迟**：实测（2026-10-20）最新只发布到 2026-09-19，更早的区间一律 404。
- **跨越 feed 落后区间的 run 会非零退出**：404 占比或连续缺失段超阈值即判定失败，即使区间内可下载的文件都已下完（见"错误处理"）。需要"尽力而为"语义时加 **`--allow-missing`**。
- **无断点续传**：中断后重跑依赖 `.bi5.tmp` 清理 + 整体重下该文件。
- **运行锁不是内核级的**：进程被 `kill -9` 强杀后 `output_dir/.ddd.lock/` 会残留，需要手动删除（错误信息里会给出路径）。**Ctrl+C / SIGTERM 不会留下残留** —— 信号会取消 run 并触发锁的 `Drop` 释放。锁的 `Drop` 释放失败时现在会打 `warn` 日志提示手动删除 —— 否则下一次 run 拒绝启动时看起来像是"有进程卡住了"，而不是"上次留下了空目录"。

## 本次审查修订（2026-10-22）

按"逻辑正确性优先、外科手术式改动"完成一轮审查，实际修改如下：

| 位置 | 修订 |
|---|---|
| `src/app/download/mod.rs` `range_verdict` | 新增"全部 404"判据：`ok == 0 && not_found == total` 时严格模式返回 `RangeUnavailable`，不再因为样本数 < 8 而成功退出；`--allow-missing` 下只 `warn` |
| `src/app/download/stream.rs` `write_body_to_file` | HTTP 200 空 body 不再落盘：统计写入字节数，为 0 时删除 `.bi5.tmp` 并返回 `DownloadFailed`，避免空文件被缓存永久当成完整数据 |
| `src/app/download/mod.rs` `bucket_key` | 只在 404 分支构建桶键；成功路径不再为每个任务付出字符串/组件分配 |
| `src/app/download/mod.rs` `bucket_position` | 用 `NaiveDate::from_ymd_opt` 校验年月日，拒绝越界键，避免 `is_adjacent_bucket` 的 `+1` 溢出 |
| `src/app/mod.rs`、`src/app/download/*` | 补充 `run_with`、`DownloadOptions`、`run_downloads_with`、`write_body_to_file` 等文档与 doctest；修正锁获取时机的注释 |
| `src/utils/scan_local_files.rs` | 零字节 `.bi5` 不再计入缓存：扫描时删除，下一次 run 重下；`metadata` 读取失败则保留为缓存并 `warn` |
| `src/config.rs` `effective_range` | 默认起点超出 15 年窗口时顺延到窗口内并 `warn`；显式写出的超限 `start` 仍然报错 |
| `src/config.rs` `Symbol` | 允许内部 `.`（`LIGHT.CMD` / `BRENT.CMD`），拒绝首尾点与连续点；内部改为 `Arc<str>`，`DownloadTask` 的 `symbol.clone()` 不再复制字符串 |
| `src/app/download/mod.rs` `longest_missing_run` | 直接把 404 桶解析成日历位置，不再为每个缺失键 clone `String` |
| `src/app/download/mod.rs` `run_downloads*` | 文档声明 `gate` 至少要有 1 个 permit，说明零许可会永久等待 |
| `tests/`、各 `#[cfg(test)]` | 新增全 404（单元 + 集成）、`--allow-missing` 全 404、空 body 不落盘、越界日历键、元旦闭合桶边界、`Retry-After` u64 溢出、run 层锁冲突、零字节 `.bi5` 清理、默认起点顺延、带点品种与 `Symbol` 零拷贝 clone 等回归测试 |

验证结果：`cargo fmt --all -- --check`、`cargo clippy --all-targets --all-features -- -D warnings`、`cargo test --all-features`（216 单元 + 14 集成 + 16 doctest）全部通过；冒烟下载 EURUSD D1 2025 成功，二次运行命中缓存、零请求。

以下问题**已识别但仍未改动**（属于设计取舍）：

- **公开 API `run_downloads` 的 `gate` 参数**：已在文档声明“至少 1 个 permit”，但函数仍信任调用方；没有改成内部自建闸门，也没有在零许可时返回错误（共享闸门可能被其它批次暂时占满，启动时检查会产生假阳性）。
- **200 + 非空但非 `.bi5` 内容**：目前只保证非空，不校验 LZMA 头；HTML 错误页或代理页仍可能被原样缓存。这是“按原始字节镜像”的已知限制。
- **非 unix 信号注册时机**：`#[cfg(not(unix))]` 分支在 async 块内创建 `ctrl_c()`；按代码注释自身的说法，handler 可能在取锁之后才注册。不确定 tokio 当前版本的实际行为，若声明支持 Windows 需实测。
- **`RangeUnavailable` 诊断信息**：只有 `not_found/total/longest_run`，无法区分“全 404”与“50% 404 + 长缺口”；可考虑增加 `all_missing` 标记或缺失桶样本。
