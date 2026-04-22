# KRAKEN Documentation

## English

Welcome to the KRAKEN documentation for bt_api.

### Quick Start

```bash
pip install bt_api_kraken
```

```python
from bt_api_py import BtApi

api = BtApi(
    exchange_kwargs={
        "KRAKEN___SPOT": {
            "api_key": "your_key",
            "secret": "your_secret",
        }
    }
)

ticker = api.get_tick("KRAKEN___SPOT", "BTCUSDT")
print(ticker)
```

## 中文

欢迎使用 bt_api 的 KRAKEN 文档。

### 快速开始

```bash
pip install bt_api_kraken
```

```python
from bt_api_py import BtApi

api = BtApi(
    exchange_kwargs={
        "KRAKEN___SPOT": {
            "api_key": "your_key",
            "secret": "your_secret",
        }
    }
)

ticker = api.get_tick("KRAKEN___SPOT", "BTCUSDT")
print(ticker)
```

## API Reference

See source code in `src/bt_api_kraken/` for detailed API documentation.
