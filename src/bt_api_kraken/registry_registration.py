from __future__ import annotations

from typing import Any

from bt_api_base.balance_utils import nested_balance_handler as _kraken_balance_handler
from bt_api_base.registry import ExchangeRegistry

from bt_api_kraken.exchange_data.kraken_exchange_data import (
    KrakenExchangeDataFutures,
    KrakenExchangeDataSpot,
)
from bt_api_kraken.feeds.live_kraken import (
    KrakenRequestDataFutures,
    KrakenRequestDataSpot,
)


def _kraken_subscribe_handler(data_queue: Any, exchange_params: Any, topics: Any, bt_api: Any) -> None:
    pass


def register_kraken(registry: type[ExchangeRegistry]) -> None:
    registry.register_feed("KRAKEN___SPOT", KrakenRequestDataSpot)
    registry.register_exchange_data("KRAKEN___SPOT", KrakenExchangeDataSpot)
    registry.register_balance_handler("KRAKEN___SPOT", _kraken_balance_handler)
    registry.register_stream("KRAKEN___SPOT", "subscribe", _kraken_subscribe_handler)

    registry.register_feed("KRAKEN___FUTURES", KrakenRequestDataFutures)
    registry.register_exchange_data("KRAKEN___FUTURES", KrakenExchangeDataFutures)
    registry.register_balance_handler("KRAKEN___FUTURES", _kraken_balance_handler)
    registry.register_stream("KRAKEN___FUTURES", "subscribe", _kraken_subscribe_handler)
