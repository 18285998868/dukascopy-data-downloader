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

| 文件架构(保持顺序)              | 功能简介       | 库依赖                               |
| ------------------------------- | -------------- | ------------------------------------ |
| `main.rs`                       | 只做入口调用   | tokio、tracing-subscriber、anyhow    |
| `lib.rs`                        | 只做模块导入导出 | std(PathBuf)、chrono(NaiveDate)   |
| `error.rs`                      | 错误处理       | thiserror 2                          |
| `cache.rs`                      | 缓存(redb)     | redb 4                               |
| `backoff.rs`                    | 退避策略       | backoff 0.4(tokio)                   |
| `app/`                          | 步骤目录       | —                                    |
| `app/mod.rs`                    | 整体流程编配   | chrono、anyhow、reqwest              |
| `app/01_cli.rs`                 | 命令行解析     | clap 4(derive)                       |
| `app/02_config.rs`              | 配置解析       | config 0.15(config-rs, toml)+ serde  |
| `app/03_build_client.rs`        | 构建网络客户端 | reqwest、tokio                       |
| `app/04_build_tasks.rs`         | 构建下载任务   | chrono                               |
| `app/05_run_downloads/mod.rs`   | 运行下载任务   | tokio(Semaphore)、futures(join_all)、redb |
| `app/05_run_downloads/stream.rs`| 下载任务流     | reqwest、tokio、backoff              |

## 具体逻辑

```text
入口 -> main.rs -> 调用 lib 导出的 app::run

流程编排 -> app/mod.rs -> 加载配置 → 日期校验 → 构建 client → 生成任务 → 并发下载
              -> app/01_cli.rs           -> 解析 --config/-c(可选),缺省 crate 内 config/config.toml
              -> app/02_config.rs        -> 读 TOML,全字段默认值兜底
              -> app/03_build_client.rs  -> TCP 探测直连 → SOCKS5 回退
              -> app/04_build_tasks.rs   -> tf→周期映射,枚举日期,拼 URL/路径 → DownloadTask
              -> app/05_run_downloads/mod.rs   -> 已存在/缓存过滤 → Semaphore(30) 并发 → 404 标记 → 聚合
              -> app/05_run_downloads/stream.rs -> GET 流式写 .bi5.tmp → flush → rename;404 永久/5xx 重试
```

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