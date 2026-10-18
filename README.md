# dukascopy-data-downloader

## 项目目标

> 从 [Dukascopy Datafeed](https://www.dukascopy.com/datafeed) 下载历史 K 线原始 `.bi5` 文件（LZMA 压缩，保持原始格式，不解码）
> 网站数据布局:

- 网址: https://www.dukascopy.com/datafeed
  - {symbol}/
    - {YYYY}/
      - {PRICE}_candles_day_1.bi5（D1/1d 按年一个文件）
      - {MM}/
        - {PRICE}_candles_hour_1.bi5（H1/1h 按月一个文件）
        - {DD}/
          - {PRICE}_candles_min_1.bi5（M1/1m 按天一个文件）

## 技术选型

- rust
- edition = "2024"
- 网络: reqwest（直连）

## 冒烟配置

```toml
symbol = ["EURUSD"] # 必填
timeframe = ["D1"] # ["M1", "H1", "D1", "1m", "1h", "1d"]
# price_type = "BID" # 可选 ["BID", "ASK"]
# start = "2025-01-01" # 可选
# end   = "2025-01-31" # 可选
output_dir = "/tmp/dukascopy-smoke" # 可选
```

## 逻辑编排

```
main.rs         入口, 初始化tracing, 解析参数 --config/-c 并将其传递给 app::(&cli.config)
app/            逻辑处理, 根据配置构建下载 urls -> 执行下载
                01加载配置
                扫描本地已有 .bi5(同遍清 .tmp)
                建 reqwest Client(直连 Dukascopy，30s 超时，连接池每主机 256 空闲连接；不启用代理回退)
                for symbol in config.symbol
                  for timeframe in config.timeframe
                    按 timeframe 映射周期(年/月/日)构建从 start 到 end 的下载链接列表 urls,格式: {symbol}/{YYYY}[/{MM}[/{DD}]]/{PRICE}_{candles_*}.bi5
                    从下载链接列表构建下载 tasks : 剔除本地已有, dest = output_dir.join(dukascopy/{官方path后缀})
                    执行下载
                下载完成时,打印汇总信息


```

## 架构设计

```
main.rs    只做入口
lib.rs     只做模块导出
cli.rs    clap
config.rs config-rs
error.rs  thiserror
app/
  mod.rs app 执行逻辑代码实现
  01_build_urls.rs
  02_build_download_tasks.rs
  03_download/

```

## 实现

### main.rs

- 初始化 tracing 日志
- 传递参数并且调用 app::run()

### app

#### 加载配置&效验

解析层（load + 结构体属性）：

1. 文件可读，失败带路径。
2. TOML 可解析 + 日期格式 YYYY-MM-DD 合法，失败带路径。
3. deny_unknown_fields：未知键拒收。
   语义层（validate）：
4. symbol 非空。
5. timeframe 非空。
6. timeframe 白名单 6 值（VALID_TIMEFRAMES: M1/H1/D1/1m/1h/1d）。
7. price_type 白名单 2 值（BID/ASK）。
8. concurrency≠0。
9. 双 Some 且 start>end 报错；另附归一化：双列表 sort+dedup。

#### 建 reqwest Client（直连，池 256）

直连 Dukascopy，30s 超时，连接池每主机 256 空闲连接；不启用代理回退。

#### end 处理

end=cfg.resolved_end(今天)（min(end,昨天)/缺省昨天）

#### 双层串行循环 symbol × timeframe

逐 symbol 调 run_one；失败只记数+收明细（上限 20），不中断后续 symbol。

##### run_one

1. start>end 直接 Ok 跳过（缺省 start 按 2020-01-01，end 已钳制到昨天）。
2. build_download_tasks 按周期组 URL 并剔本地已有。
3. 有任务则 download::run_downloads 并发下并打统计，空则 debug 跳过。

#### 打印汇总

## 问题:

- 当给的 config.end 超过今天时,数据尚未更新(含今天),会造成网络404,
  - 解决: `AppConfig::resolved_end` 一次性取 min(config.end, 昨天),下游只消费不再算时间
- 当本地已经有下载的数据文件时,会导致重复下载
  - 解决: 在循环开始前扫描本地已有文件一次(同遍清 `.tmp`), 在构建tasks 时剔除本地已有文件实现数据更新而不重复下载
  - 文件: `src/utils/scan_local_files.rs`
- 当遇到网络故障时
  - 解决: 增加重试机制,确保数据下载成功;404 单次即止记容忍,不重试
  - 文件: `src/utils/backoff.rs`
- 当直连被墙或超时时,会全批失败,
  - 现状: 直连 Dukascopy，30s 超时，连接池每主机 256；不启用代理回退（如需代理请自行配置环境）
  - 文件: `src/app/mod.rs:build_client`
- 当配置写重复或写错时,会误下或空跑,
  - 解决: `symbol/timeframe` 排序去重;报错带文件路径;未知键拒收(`deny_unknown_fields`)

## 完整架构

```
main.rs    只做入口
lib.rs     只做模块导出
cli.rs    clap
config.rs toml 直解 + validate
error.rs  thiserror
app/
  mod.rs app 执行逻辑代码实现
  01_build_urls.rs
  02_build_download_tasks.rs
  03_download/ (mod.rs + stream.rs)
utils/
  mod.rs 聚合入口
  scan_local_files.rs
  backoff.rs
```

## 完整配置

```toml
symbol = ["EURUSD"] # 必填，非空，自动排序去重
timeframe = ["D1"] # 必填，非空，6 值白名单 ["M1","H1","D1","1m","1h","1d"]，自动排序去重
# price_type = "BID" # 可选，缺省 "BID"，2 值白名单 ["BID","ASK"]
# start = "2020-01-01" # 可选，缺省无(按 2020-01-01 处理)；与 end 双写且 start>end 直接报错
# end = "2025-01-31" # 可选，缺省昨天；未来值截到昨天
# output_dir = "data" # 可选，缺省 "data"；数据根目录，程序自动追加 dukascopy/...，不要手写进去
# concurrency = 30 # 可选，缺省 30；必须≠0；单个 pair 内最大并发下载数
```

- 未知键拒收（拼错即报错）；读写/解析失败带文件路径。
- 完整示例见 `config.example.toml`，冒烟见 `config.test.toml`（`config.test.toml` 被 `.gitignore` 忽略，不入库）。

## 完整执行逻辑

- main.rs
  - 入口
  - 初始化日志 tracing（`RUST_LOG`，缺省 info）
  - 解析 `--config/-c` 参数给 `app::run()` 并调用它
- cli.rs
  - clap 4 derive 解析，`--config/-c` 必填，返回 `PathBuf`
- config.rs
  - `load`：读文件（失败带路径）→ `toml::from_str`（失败带路径）→ `validate`
  - 缺省：容器级 `Default`（空列表/BID/`data`/30/`None`）
  - 校验：`symbol`/`timeframe` 非空，`timeframe` 6 值白名单，`price_type` 2 值白名单，`concurrency≠0`，双 `Some` 且 `start>end` 报错，未知键拒收
  - 归一化：双列表 `sort+dedup`
  - `resolved_end(today)`：`min(end,昨天)`/缺省昨天，调用方算一次
  - `DownloadTask{url, dest, symbol, timeframe}` 定义在此
- error.rs
  - `AppError`（thiserror）：Config/Api/Data/Date/Http/Io/Json/DownloadFailed/NotFound/Join
- app/mod.rs
  - 建 `reqwest Client`（直连优先/代理回退/池 256，见上）
  - 调 `utils::scan_local_files` 全量扫本地一次（同遍清 `.tmp`）
  - `end = cfg.resolved_end(今天)`，`start = cfg.start.unwrap_or(2020-01-01)`，打启动日志
  - 双层串行 `symbol×timeframe` 逐 pair 调 `run_one`，失败记数收明细（≤20 条）不中断
  - 汇总：零失败 `Ok`，否则 `Err(Data("N pair(s) failed"))`
  - `run_one`：`start>end` 跳过 → 组 tasks → 非空并发下打统计，空则跳过
- app/01_build_urls.rs
  - `DownloadUrl{url, rel_path, symbol, timeframe}` + `PeriodKind(Year/Month/Day)`，`map_timeframe` 映射 `candles_min_1/hour_1/day_1`，`tf_period` 定年/月/日粒度
  - 逐周期拼 `https://www.dukascopy.com/datafeed/{symbol}/{Y}[/{M}[/{D}]]/{PRICE}_{part}.bi5`，`rel_path = dukascopy/{官方path后缀}` 严格镜像
- app/02_build_download_tasks.rs
  - 调 `build_urls`，过滤本地已有（相对路径），`dest = output_dir.join(rel)`，得 `Vec<DownloadTask>`
- app/03_download/mod.rs
  - `buffer_unordered(concurrency)` 有界并发，每 100 个打进度，统计 `ok/not_found/failed`，`failed>0` 返 `Err`
- app/03_download/stream.rs
  - 单文件：GET → 404 永久 `NotFound` 不重试 → 429/5xx/网络瞬态指数退避（30s 上限）→ 其余 4xx 永久失败
  - 建父目录 → 清旧 `.bi5.tmp` → 流式写 `.bi5.tmp` → `flush` → `rename` 原子落盘
- utils/mod.rs
  - 聚合入口：`backoff/scan_local_files`
- utils/scan_local_files.rs
  - `spawn_blocking + walkdir` 遍历，得相对 `.bi5` 集合；目录不存在返空；同遍删 `.tmp`
- utils/backoff.rs
  - 指数退避，`max_elapsed_time = 30s`
