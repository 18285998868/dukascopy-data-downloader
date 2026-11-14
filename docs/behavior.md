# 下载、重试与错误处理

## 桶与已闭合区间

一个文件覆盖一个日历桶：`M1` 一天、`H1` 一月、`D1` 一年。服务端会持续追加，直到桶覆盖的最后一天结束。程序只下载已闭合的桶，避免把半截文件永久缓存。

| 周期 | 桶 | 实际下载上限 |
|---|---|---|
| `M1` | 1 天 | 昨天 |
| `H1` | 1 月 | 上月末 |
| `D1` | 1 年 | 上年末 |

## 重试与超时

```text
单次请求超时 30s
单文件重试预算 120s（绝对墙钟）
404            → 不重试，记 not_found
429 / 5xx      → 指数退避重试
Retry-After    → 下载操作自身等待，且不超过剩余预算
其他 4xx       → 永久失败
```

`Retry-After` 不会绕过预算检查；`HTTP-date` 形式降级为指数退避。

## 原子落盘

```text
GET → 校验状态码 → 建父目录 → 删残留 .bi5.tmp
    → 流式写 .bi5.tmp
    → 0 字节？删除 .bi5.tmp 并返回 DownloadFailed
    → sync_all → rename → .bi5
    → fsync 父目录（失败忽略）
```

最终文件名下不会出现半截文件；空 200 响应不会被缓存。

## 缓存

- 启动时扫描 `output_dir`，收集 `.bi5` 相对路径。
- 删除残留 `*.bi5.tmp`。
- 删除零字节 `.bi5`，让下次运行重新下载。
- 缓存键包含 symbol、price_type 与桶路径，统一使用 `/` 分隔。

## 缺失与错误分级

404 只表示文件不存在（Dukascopy 每个日历日都有文件，含周末与节假日）。程序按以下判据处理：

| 判据 | 处理 |
|---|---|
| 404 占比 ≤ 5% | 正常，记 `not_found` |
| 404 占比 > 5% | `warn` |
| 404 占比 > 50%（样本 ≥ 8） | `RangeUnavailable` |
| 全部 404 | `RangeUnavailable` |
| 同一周期连续缺失 ≥ 5 个桶 | `RangeUnavailable` |

连续缺失段按周期分组计算，`D1` 的年份与 `M1` 的日期不会混排。`--allow-missing` 只豁免这三类区间判据；真实下载失败仍然报错。

错误类型：

| 变体 | 含义 |
|---|---|
| `Http` / `Io` / `FileIo` | 传输、文件系统错误 |
| `Config` | 配置读取、解析、校验失败 |
| `RunLocked` | 同一 `output_dir` 已有实例持锁 |
| `DownloadFailed` | 单文件重试预算耗尽 |
| `NotFound` | HTTP 404 |
| `BatchFailed` | 单 pair 内文件失败 |
| `PairsFailed` | 整个 run 内 pair 失败 |
| `RangeUnavailable` | 请求区间不在 feed 覆盖范围 |
| `Interrupted` | 收到 SIGINT / SIGTERM |
| `GateClosed` | 调用方关闭了共享下载闸门 |

## 终端主机

```text
https://www.dukascopy.com/datafeed/...        301 → http://datafeed.dukascopy.com/...
http://datafeed.dukascopy.com/datafeed/...    301 → https://datafeed.dukascopy.com/...
https://datafeed.dukascopy.com/datafeed/...   200 | 429   ← 数据始终经 TLS
```

程序直接请求终端主机。若出现批量 404，可把 `../crates/dukascopy-data-downloader-core/src/app/build_urls.rs` 中的 `FEED_BASE` 改回 `https://www.dukascopy.com/datafeed`。
