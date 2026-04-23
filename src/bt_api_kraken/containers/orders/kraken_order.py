from __future__ import annotations

import time
from typing import Any

from bt_api_base.containers.orders.order import OrderData
from bt_api_base.logging_factory import get_logger


class KrakenRequestOrderData(OrderData):
    def __init__(
        self,
        data: dict[str, Any],
        symbol: str,
        asset_type: str,
        is_response_data: bool = False,
        has_been_json_encoded=False,
    ):
        super().__init__(data, has_been_json_encoded)
        self.symbol = symbol
        self.asset_type = asset_type
        self.average_price: float | None = None
        self.logger = get_logger("kraken_order")
        self.is_response_data = is_response_data
        self._parse_data(data)

    def _parse_data(self, data: dict[str, Any]):
        try:
            if self.is_response_data and "txid" in data:
                self._parse_new_order_response(data)
            elif isinstance(data, dict) and "status" in data:
                self._parse_order_status(data)
            else:
                self._parse_normalized_data(data)

        except Exception as e:
            self.logger.error(f"Error parsing Kraken order data: {e}")
            self.logger.error(f"Raw data: {data}")
            raise

    def _parse_new_order_response(self, data: dict[str, Any]):
        if "txid" in data and len(data) > 1:
            self._parse_order_status(data)
        else:
            self.order_id = data.get("txid", data.get("order_id", str(time.time())))
            self.status = "new"
            self.timestamp = time.time()
            self.datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))
            self.exchange = "kraken"

    def _parse_order_status(self, data: dict[str, Any]):
        self.order_id = data.get("id") or data.get("txid") or str(time.time())
        self.status = data.get("status", "unknown")
        self.timestamp = data.get("opentm", time.time())
        self.datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))
        self.exchange = "kraken"

        descr = data.get("descr", {})
        self.symbol = descr.get("pair", self.symbol)
        self.side = descr.get("type", "unknown")
        self.order_type = descr.get("ordertype", "unknown")
        p = descr.get("price")
        self.price = float(p) if p is not None else None
        v = descr.get("vol")
        self.quantity = float(v) if v is not None else None

        ve = data.get("vol_exec", "0")
        self.executed_quantity = float(ve) if ve is not None else 0.0
        self.remaining_quantity = self.quantity - self.executed_quantity if self.quantity else None
        c = data.get("cost", "0")
        self.cost = float(c) if c is not None else 0.0
        f = data.get("fee", "0")
        self.fee = float(f) if f is not None else 0.0

        self.userref = data.get("userref")
        self.leverage = data.get("leverage", "none")
        self.order_description = descr.get("order")
        self.misc = data.get("misc", "")
        self.oflags = data.get("oflags", "")
        self.start_time = data.get("starttm")
        self.expire_time = data.get("expiretm")

        if self.executed_quantity > 0 and self.cost > 0:
            self.average_price = self.cost / self.executed_quantity
        else:
            self.average_price = None

    def _parse_normalized_data(self, data: dict[str, Any]):
        self.order_id = data.get("id", str(time.time()))
        self.symbol = data.get("symbol", self.symbol)
        self.status = data.get("status", "unknown")
        self.side = data.get("side", "unknown")
        self.type = data.get("type", "unknown")
        self.exchange = data.get("exchange", "kraken")
        self.timestamp = data.get("created_at", time.time())
        if isinstance(self.timestamp, str):
            self.timestamp = time.time()
        self.datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))

        qty = data.get("quantity")
        self.quantity = float(qty) if qty is not None else None
        prc = data.get("price")
        self.price = float(prc) if prc is not None else None
        eq = data.get("executed_quantity", "0")
        self.executed_quantity = float(eq) if eq is not None else 0.0
        self.remaining_quantity = data.get("remaining_quantity")
        if self.remaining_quantity is None and self.quantity:
            self.remaining_quantity = self.quantity - self.executed_quantity

        c = data.get("cost", "0")
        self.cost = float(c) if c is not None else 0.0
        f = data.get("fee", "0")
        self.fee = float(f) if f is not None else 0.0
        ap = data.get("average_price")
        self.average_price = float(ap) if ap is not None else None

        self.userref = data.get("userref")
        self.leverage = data.get("leverage")
        self.order_description = data.get("order_description")
        self.misc = data.get("misc", "")
        self.oflags = data.get("oflags", "")
        self.start_time = data.get("start_time")
        self.expire_time = data.get("expire_time")

    def init_data(self):
        return self

    def get_exchange_name(self):
        return self.exchange

    def get_local_update_time(self):
        return self.timestamp

    def get_asset_type(self):
        return self.asset_type

    def get_symbol_name(self):
        return self.symbol

    def get_server_time(self):
        return self.timestamp

    def get_trade_id(self):
        return

    def get_client_order_id(self):
        return self.userref

    def get_cum_quote(self):
        return self.cost

    def get_executed_qty(self):
        return self.executed_quantity

    def get_order_id(self):
        return self.order_id

    def get_order_size(self):
        return self.quantity

    def get_order_price(self):
        return self.price

    def get_reduce_only(self):
        return

    def get_order_side(self):
        return self.side

    def get_order_status(self):
        return self.status

    def get_order_symbol_name(self):
        return self.symbol

    def get_order_time_in_force(self):
        return

    def get_order_type(self):
        return self.type

    def get_order_avg_price(self):
        return self.average_price

    def get_origin_order_type(self):
        return self.type

    def get_position_side(self):
        return

    def get_trailing_stop_price(self):
        return

    def get_trailing_stop_trigger_price(self):
        return

    def get_trailing_stop_callback_rate(self):
        return

    def get_trailing_stop_trigger_price_type(self):
        return

    def get_stop_loss_price(self):
        return

    def get_stop_loss_trigger_price(self):
        return

    def get_stop_loss_trigger_price_type(self):
        return

    def get_take_profit_price(self):
        return

    def get_take_profit_trigger_price(self):
        return

    def get_take_profit_trigger_price_type(self):
        return

    def get_close_position(self):
        return

    def get_order_offset(self):
        return

    def get_order_exchange_id(self):
        return "kraken"

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "status": self.status,
            "side": self.side,
            "type": self.type,
            "quantity": self.quantity,
            "price": self.price,
            "executed_quantity": self.executed_quantity,
            "remaining_quantity": self.remaining_quantity,
            "average_price": self.average_price,
            "cost": self.cost,
            "fee": self.fee,
            "timestamp": self.timestamp,
            "datetime": self.datetime,
            "exchange": self.exchange,
            "userref": self.userref,
            "leverage": self.leverage,
            "order_description": self.order_description,
            "misc": self.misc,
            "oflags": self.oflags,
            "start_time": self.start_time,
            "expire_time": self.expire_time,
            "asset_type": self.asset_type,
        }

    def validate(self) -> bool:
        if not self.order_id:
            return False
        if self.status not in ["new", "open", "closed", "canceled", "expired", "unknown"]:
            return False
        if self.side not in ["buy", "sell"]:
            return False
        if self.type not in [
            "market",
            "limit",
            "stop-loss",
            "take-profit",
            "stop-loss-limit",
            "take-profit-limit",
        ]:
            return False
        if self.type in ["limit", "stop-loss-limit", "take-profit-limit"] and self.price is None:
            return False
        if self.quantity is None or self.quantity <= 0:
            return False
        if self.executed_quantity is None:
            return True
        return 0 <= self.executed_quantity <= self.quantity

    def is_open(self) -> bool:
        return self.status in ["open", "new"]

    def is_filled(self) -> bool:
        return self.status == "closed" and self.executed_quantity == self.quantity

    def get_fill_percentage(self) -> float:
        if self.quantity is None or self.quantity == 0:
            return 0.0
        return (self.executed_quantity / self.quantity) * 100

    def update_from_trade(self, trade_data: dict[str, Any]):
        if "executed_quantity" in trade_data:
            self.executed_quantity = float(trade_data["executed_quantity"])
        if "cost" in trade_data:
            self.cost = float(trade_data["cost"])
        if "fee" in trade_data:
            self.fee = float(trade_data["fee"])
        if self.quantity is not None:
            self.remaining_quantity = self.quantity - self.executed_quantity
        if self.executed_quantity == self.quantity and self.quantity > 0:
            self.status = "closed"

    def cancel(self):
        self.status = "canceled"
        self.remaining_quantity = 0

    def __str__(self) -> str:
        return f"KrakenOrder({self.order_id}: {self.side} {self.quantity} {self.symbol} @ {self.price} [{self.status}])"

    def __repr__(self) -> str:
        return (
            f"KrakenRequestOrderData(order_id='{self.order_id}', "
            f"symbol='{self.symbol}', side='{self.side}', "
            f"quantity={self.quantity}, price={self.price}, "
            f"status='{self.status}')"
        )


class KrakenSpotWssOrderData(OrderData):
    def __init__(self, data: dict[str, Any], symbol: str, asset_type: str, has_been_json_encoded=False):
        super().__init__(data, has_been_json_encoded)
        self.symbol = symbol
        self.asset_type = asset_type
        self.logger = get_logger("kraken_wss_order")
        self._parse_wss_data(data)

    def _parse_wss_data(self, data: dict[str, Any]):
        self.order_id = data.get("orderId")
        self.symbol = data.get("symbol", self.symbol)
        self.status = data.get("status", "unknown")
        self.side = data.get("side", "unknown")
        self.type = data.get("type", "unknown")
        qty = data.get("qty")
        self.quantity = float(qty) if qty is not None else None
        prc = data.get("price")
        self.price = float(prc) if prc is not None else None
        eq = data.get("executedQty", "0")
        self.executed_quantity = float(eq) if eq is not None else 0.0
        rq = data.get("remainingQty")
        self.remaining_quantity = float(rq) if rq is not None else None
        self.timestamp = data.get("time", time.time())
        self.datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))
        self.exchange = "kraken"

    def to_dict(self) -> dict[str, Any]:
        return {
            "order_id": self.order_id,
            "symbol": self.symbol,
            "status": self.status,
            "side": self.side,
            "type": self.type,
            "quantity": self.quantity,
            "price": self.price,
            "executed_quantity": self.executed_quantity,
            "remaining_quantity": self.remaining_quantity,
            "timestamp": self.timestamp,
            "datetime": self.datetime,
            "exchange": self.exchange,
            "asset_type": self.asset_type,
        }
