from __future__ import annotations

import time
from typing import Any

from bt_api_base.containers.orderbooks.orderbook import OrderBookData
from bt_api_base.logging_factory import get_logger


class KrakenRequestOrderBookData(OrderBookData):
    def __init__(self, data: dict[str, Any], symbol: str, asset_type: str, has_been_json_encoded=False):
        super().__init__(data, has_been_json_encoded)
        self.symbol = symbol
        self.asset_type = asset_type
        self.logger = get_logger("kraken_orderbook")
        self._parse_data(data)

    def init_data(self):
        return self

    def get_exchange_name(self):
        return self.exchange

    def get_local_update_time(self):
        return self.timestamp

    def get_symbol_name(self):
        return self.symbol

    def get_asset_type(self):
        return self.asset_type

    def get_server_time(self):
        return self.timestamp

    def get_bid_price_list(self):
        return [bid["price"] for bid in self.bids]

    def get_ask_price_list(self):
        return [ask["price"] for ask in self.asks]

    def get_bid_volume_list(self):
        return [bid["quantity"] for bid in self.bids]

    def get_ask_volume_list(self):
        return [ask["quantity"] for ask in self.asks]

    def get_bid_trade_nums(self):
        return

    def get_ask_trade_nums(self):
        return

    def _parse_data(self, data: dict[str, Any]):
        try:
            result = data.get("result", {})
            book_data = result.get(self.symbol, {})
            if not book_data and result:
                first_key = next(iter(result.keys()))
                book_data = result.get(first_key, {})

            self.symbol = data.get("symbol", self.symbol)
            self.exchange = data.get("exchange", "kraken")
            self.timestamp = data.get("timestamp", time.time())
            self.datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))

            self.asks = []
            for ask in book_data.get("asks", []):
                if len(ask) >= 2:
                    price = float(ask[0])
                    quantity = float(ask[1])
                    timestamp = ask[2] if len(ask) > 2 else self.timestamp
                    self.asks.append({"price": price, "quantity": quantity, "timestamp": timestamp})

            self.bids = []
            for bid in book_data.get("bids", []):
                if len(bid) >= 2:
                    price = float(bid[0])
                    quantity = float(bid[1])
                    timestamp = bid[2] if len(bid) > 2 else self.timestamp
                    self.bids.append({"price": price, "quantity": quantity, "timestamp": timestamp})

            self.asks.sort(key=lambda x: x["price"])
            self.bids.sort(key=lambda x: x["price"], reverse=True)

            self._calculate_stats()

        except Exception as e:
            self.logger.error(f"Error parsing Kraken order book data: {e}")
            self.logger.error(f"Raw data: {data}")
            raise

    @property
    def symbol_name(self):
        return self.symbol

    @symbol_name.setter
    def symbol_name(self, value):
        pass

    def _calculate_stats(self):
        self.best_bid = self.bids[0]["price"] if self.bids else None
        self.best_ask = self.asks[0]["price"] if self.asks else None

        if self.best_bid and self.best_ask:
            self.mid_price = (self.best_bid + self.best_ask) / 2
        else:
            self.mid_price = None

        if self.best_bid and self.best_ask:
            self.spread = self.best_ask - self.best_bid
            self.spread_percentage = (self.spread / self.best_bid) * 100 if self.best_bid else None
        else:
            self.spread = None
            self.spread_percentage = None

        self.total_volume_bid = sum(level["quantity"] for level in self.bids)
        self.total_volume_ask = sum(level["quantity"] for level in self.asks)
        self.bid_depth_10 = sum(level["quantity"] for level in self.bids[:10])
        self.ask_depth_10 = sum(level["quantity"] for level in self.asks[:10])
        self.bid_weighted_depth = sum(level["price"] * level["quantity"] for level in self.bids)
        self.ask_weighted_depth = sum(level["price"] * level["quantity"] for level in self.asks)

    def get_levels(self, depth: int = 10, side: str = "both") -> dict[str, list[dict]]:
        levels = {}
        if side in ["bid", "both"]:
            levels["bids"] = self.bids[:depth]
        if side in ["ask", "both"]:
            levels["asks"] = self.asks[:depth]
        return levels

    def get_total_depth(self, side: str, price_range: tuple[float, float] | None = None) -> float:
        levels = self.bids if side == "bid" else self.asks
        total = 0.0
        for level in levels:
            if price_range:
                min_price, max_price = price_range
                if not (min_price <= level["price"] <= max_price):
                    continue
            total += level["quantity"]
        return total

    def get_price_impact(self, side: str, volume: float) -> dict[str, Any]:
        levels = self.bids if side == "buy" else self.asks
        cumulative_volume = 0.0
        estimated_price = None
        slippage = 0.0

        for level in levels:
            cumulative_volume += level["quantity"]
            if cumulative_volume >= volume:
                estimated_price = level["price"]
                break

        if estimated_price and side == "buy" and self.best_ask:
            slippage = estimated_price - self.best_ask
        elif estimated_price and side == "sell" and self.best_bid:
            slippage = self.best_bid - estimated_price

        return {
            "estimated_price": estimated_price,
            "slippage": slippage,
            "slippage_percentage": (slippage / (estimated_price or 1)) * 100,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "timestamp": self.timestamp,
            "datetime": self.datetime,
            "bids": self.bids,
            "asks": self.asks,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "mid_price": self.mid_price,
            "spread": self.spread,
            "spread_percentage": self.spread_percentage,
            "total_volume_bid": self.total_volume_bid,
            "total_volume_ask": self.total_volume_ask,
            "bid_depth_10": self.bid_depth_10,
            "ask_depth_10": self.ask_depth_10,
            "bid_weighted_depth": self.bid_weighted_depth,
            "ask_weighted_depth": self.ask_weighted_depth,
            "asset_type": self.asset_type,
        }

    def validate(self) -> bool:
        if not self.symbol:
            return False
        if self.bids and self.asks and self.bids[0]["price"] > self.asks[0]["price"]:
            return False
        return all(level["quantity"] >= 0 for level in self.bids + self.asks)

    def update_from_delta(self, delta_bids: list[dict], delta_asks: list[dict], timestamp: float | None = None):
        if timestamp is None:
            timestamp = time.time()

        for delta in delta_bids:
            price = delta["price"]
            quantity = delta["quantity"]
            found = False
            for i, bid in enumerate(self.bids):
                if bid["price"] == price:
                    if quantity > 0:
                        self.bids[i]["quantity"] = quantity
                        self.bids[i]["timestamp"] = timestamp
                    else:
                        self.bids.pop(i)
                    found = True
                    break
            if quantity > 0 and not found:
                self.bids.append({"price": price, "quantity": quantity, "timestamp": timestamp})

        for delta in delta_asks:
            price = delta["price"]
            quantity = delta["quantity"]
            found = False
            for i, ask in enumerate(self.asks):
                if ask["price"] == price:
                    if quantity > 0:
                        self.asks[i]["quantity"] = quantity
                        self.asks[i]["timestamp"] = timestamp
                    else:
                        self.asks.pop(i)
                    found = True
                    break
            if quantity > 0 and not found:
                self.asks.append({"price": price, "quantity": quantity, "timestamp": timestamp})

        self.bids.sort(key=lambda x: x["price"])
        self.asks.sort(key=lambda x: x["price"])
        self._calculate_stats()

    def get_liquidation_price(self, side: str, position_size: float, leverage: float = 1.0) -> float | None:
        if side == "long":
            if not self.bids:
                return None
            price = self.bids[0]["price"]
            return float(price) * (1 - 0.01 * leverage)
        else:
            if not self.asks:
                return None
            price = self.asks[0]["price"]
            return float(price) * (1 + 0.01 * leverage)

    def get_vwap(self, side: str, volume: float) -> float | None:
        levels = self.bids if side == "buy" else self.asks
        cumulative_volume = 0.0
        cumulative_value = 0.0

        for level in levels:
            level_volume = min(level["quantity"], volume - cumulative_volume)
            cumulative_volume += level_volume
            cumulative_value += level_volume * level["price"]
            if cumulative_volume >= volume:
                break

        if cumulative_volume > 0:
            return cumulative_value / cumulative_volume
        return None

    def __str__(self) -> str:
        bid_str = f"{self.best_bid}" if self.best_bid else "N/A"
        ask_str = f"{self.best_ask}" if self.best_ask else "N/A"
        return (
            f"KrakenOrderBook({self.symbol}: BID:{bid_str} ASK:{ask_str} "
            f"Spread:{self.spread:.4f} Levels:{len(self.bids)}b/{len(self.asks)}a)"
        )

    def __repr__(self) -> str:
        return (
            f"KrakenRequestOrderBookData(symbol='{self.symbol}', "
            f"bid_levels={len(self.bids)}, ask_levels={len(self.asks)}, "
            f"best_bid={self.best_bid}, best_ask={self.best_ask})"
        )
