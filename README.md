# dukascopy-data-downloader

从 [Dukascopy Datafeed](https://www.dukascopy.com/datafeed) 下载历史 K 线原始 `.bi5` 文件（LZMA 压缩，保持原始格式不解码），按官方目录结构镜像落盘。

程序直接请求终端主机 `https://datafeed.dukascopy.com/datafeed`，省去每个文件的 2 次重定向。详见 [下载行为](./docs/behavior.md)。

## 快速开始

1. 复制配置并修改 `output_dir` / `symbol` / 区间：

```bash
cp config/00.example.toml config.toml
```

2. 运行（首次全量，之后重复运行即增量更新）：

```bash
cargo run --release -p dukascopy-data-downloader-cli -- -c config.toml
```

3. 按需调整日志级别：

```bash
RUST_LOG=debug cargo run --release -p dukascopy-data-downloader-cli -- -c config.toml
```

`output_dir` 是数据根目录，程序自动追加 `dukascopy/` 前缀：

```text
output_dir=/data  →  /data/dukascopy/{symbol}/{YYYY}[/{MM}[/{DD}]]/{PRICE}_candles_*.bi5
```

不要把 `dukascopy` 写进 `output_dir`。

## 配置

全部字段、默认值与校验规则见 [`config/00.example.toml`](./config/00.example.toml)。

| 字段 | 类型 | 必填 | 缺省 | 说明 |
|---|---|---|---|---|
| `symbol` | `string[]` | 是 | — | `3..=20` 个字符，仅大写 ASCII 字母、数字与 `.`；点不能在首尾或连续出现 |
| `timeframe` | `string[]` | 是 | — | `M1` / `H1` / `D1`（别名 `1m` / `1h` / `1d`） |
| `price_type` | `string` | 否 | `"BID"` | `BID` / `ASK` |
| `start` | `YYYY-MM-DD` | 否 | `2020-01-01` | 超出 15 年窗口的默认值会自动顺延 |
| `end` | `YYYY-MM-DD` | 否 | 昨天 | 未来值自动截到昨天 |
| `output_dir` | `path` | 否 | `"data"` | 数据根目录，不含 `dukascopy` |
| `concurrency` | `usize` | 否 | `30` | 整个 run 共享的在途下载上限，`1..=256` |

命令行参数：

```text
-c, --config <PATH>   配置文件路径（必填）
--allow-missing       容忍“请求区间不在 feed 里”，只打 warn 不据此失败
```

## 文档

- [架构与执行流程](./docs/architecture.md)
- [下载、重试与错误处理](./docs/behavior.md)
- [变更记录](./CHANGELOG.md)

## 测试

```bash
./scripts/check.sh          # 本地完整门禁
./scripts/check.sh --ci     # CI 模式：--locked，且要求 cargo-deny/cargo-audit
```

当前覆盖：221 个单元测试、18 个集成测试、22 个 doctest。

冒烟下载（真实网络，输出到 `/tmp/dukascopy-smoke`）：

```bash
cargo run --release -p dukascopy-data-downloader-cli -- -c config/00.smoke.toml
```

## 已知限制

- 只支持 `M1` / `H1` / `D1`；不校验 `.bi5` 的 LZMA 内容。
- 只下载已闭合的桶：H1 等月末、D1 等年末才能拿到当期完整数据。
- feed 可能落后于当前日期；跨越未发布区间的 run 默认非零退出，可用 `--allow-missing` 豁免。
- 无断点续传；运行锁是目录锁，`kill -9` 后可能残留，错误信息会给出待删路径。

## 许可证

MIT，见 [LICENSE](./LICENSE)。
