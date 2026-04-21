from __future__ import annotations

from typing import Any

from bt_api_base.containers.exchanges.exchange_data import ExchangeData
from bt_api_base.logging_factory import get_logger

logger = get_logger("kraken_exchange_data")


class KrakenExchangeData(ExchangeData):
    def __init__(self) -> None:
        super().__init__()
        self.exchange_name = "kraken"
        self.rest_url = "https://api.kraken.com"
        self.acct_wss_url = "wss://ws-auth.kraken.com"
        self.wss_url = "wss://ws.kraken.com"
        self.rest_paths = {}
        self.wss_paths = {}

        self.kline_periods = {
            "1m": "1",
            "5m": "5",
            "15m": "15",
            "30m": "30",
            "1h": "60",
            "2h": "120",
            "4h": "240",
            "6h": "360",
            "12h": "720",
            "1d": "1440",
            "3d": "4320",
            "1w": "10080",
        }
        self.reverse_kline_periods = {v: k for k, v in self.kline_periods.items()}

        self.legal_currency = ["USD", "EUR", "USDT", "BTC", "ETH"]

    def get_symbol(self, symbol: str) -> str:
        result = symbol.replace("/", "").replace("-", "").upper()
        result = result.replace("BTC", "XBT")
        return result

    def get_period(self, period: str) -> str:
        return self.kline_periods.get(period, period)

    def get_rest_path(self, request_type: str, **kwargs) -> str:
        return self.rest_paths.get(request_type, "")

    def get_wss_path(self, channel: Any, **kwargs) -> str:
        return self.wss_paths.get(channel, "")

    def account_wss_symbol(self, symbol: str) -> str:
        return self.get_symbol(symbol)


class KrakenExchangeDataSpot(KrakenExchangeData):
    def __init__(self) -> None:
        super().__init__()
        self.exchange_name = "kraken"
        self.asset_type = "SPOT"

        defaults = {
            "get_server_time": "POST /0/public/Time",
            "get_exchange_info": "POST /0/public/AssetPairs",
            "get_tick": "POST /0/public/Ticker",
            "get_ticker": "POST /0/public/Ticker",
            "get_depth": "POST /0/public/Depth",
            "get_kline": "POST /0/public/OHLC",
            "get_trades": "POST /0/public/Trades",
            "make_order": "POST /0/private/AddOrder",
            "cancel_order": "POST /0/private/CancelOrder",
            "cancel_all_orders": "POST /0/private/CancelAll",
            "get_open_orders": "POST /0/private/OpenOrders",
            "get_closed_orders": "POST /0/private/ClosedOrders",
            "query_order": "POST /0/private/QueryOrders",
            "get_balance": "POST /0/private/Balance",
            "get_trade_balance": "POST /0/private/TradeBalance",
            "get_deals": "POST /0/private/TradesHistory",
            "get_websocket_token": "POST /0/private/GetWebSocketsToken",
        }
        for k, v in defaults.items():
            self.rest_paths.setdefault(k, v)


class KrakenExchangeDataFutures(KrakenExchangeData):
    def __init__(self) -> None:
        super().__init__()
        self.exchange_name = "krakenFutures"
        self.asset_type = "FUTURES"
        self.rest_url = "https://futures.kraken.com"
        self.wss_url = "wss://futures.kraken.com"
        self.acct_wss_url = "wss://futures.kraken.com"

        defaults = {
            "get_server_time": "GET /derivatives/api/v3/time",
            "get_exchange_info": "GET /derivatives/api/v3/instruments",
            "get_tick": "GET /derivatives/api/v3/ticker",
            "get_ticker": "GET /derivatives/api/v3/ticker",
            "get_depth": "GET /derivatives/api/v3/book",
            "get_kline": "GET /derivatives/api/v3/ohlc",
            "get_trades": "GET /derivatives/api/v3/trades",
            "make_order": "POST /derivatives/api/v3/orders",
            "cancel_order": "DELETE /derivatives/api/v3/orders",
            "get_open_orders": "POST /derivatives/api/v3/orders/open",
            "query_order": "POST /derivatives/api/v3/orders",
            "get_balance": "POST /derivatives/api/v3/accounts/all",
            "get_deals": "POST /derivatives/api/v3/fills",
        }
        for k, v in defaults.items():
            self.rest_paths.setdefault(k, v)
