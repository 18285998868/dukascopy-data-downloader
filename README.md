# dukascopy-data-downloader

从 Dukascopy 下载历史 K 线数据并转换为 Parquet 格式。

## 配置

```toml
# config/config.example.toml
symbol = ["EURUSD", "GBPUSD"]
timeframe = ["D1"]
price_type = "BID"
start = "2020-01-01"
end = "2025-12-31"
dir = "data"
```

## 用法

```bash
cargo run -p dukascopy-data-downloader --release
```

## License

MIT