from __future__ import annotations

import time
from typing import Any

from bt_api_base.containers.balances.balance import BalanceData
from bt_api_base.logging_factory import get_logger

_CURRENCY_DATA: dict[str, dict[str, Any]] = {
    "XXBT": {"display_name": "Bitcoin", "decimal_places": 8, "is_stakable": True, "is_fiat": False},
    "XBT": {"display_name": "Bitcoin", "decimal_places": 8, "is_stakable": True, "is_fiat": False},
    "ETH": {"display_name": "Ethereum", "decimal_places": 8, "is_stakable": True, "is_fiat": False},
    "XRP": {"display_name": "Ripple", "decimal_places": 6, "is_stakable": False, "is_fiat": False},
    "LTC": {"display_name": "Litecoin", "decimal_places": 8, "is_stakable": True, "is_fiat": False},
    "DASH": {"display_name": "Dash", "decimal_places": 8, "is_stakable": True, "is_fiat": False},
    "ETC": {
        "display_name": "Ethereum Classic",
        "decimal_places": 8,
        "is_stakable": True,
        "is_fiat": False,
    },
    "XMR": {"display_name": "Monero", "decimal_places": 12, "is_stakable": True, "is_fiat": False},
    "ZEC": {"display_name": "Zcash", "decimal_places": 8, "is_stakable": True, "is_fiat": False},
    "USDT": {
        "display_name": "Tether USD",
        "decimal_places": 6,
        "is_stakable": False,
        "is_fiat": True,
    },
    "USDC": {"display_name": "USD Coin", "decimal_places": 6, "is_stakable": True, "is_fiat": True},
    "DAI": {"display_name": "Dai", "decimal_places": 18, "is_stakable": False, "is_fiat": True},
    "ZUSD": {
        "display_name": "US Dollar",
        "decimal_places": 4,
        "is_stakable": False,
        "is_fiat": True,
    },
    "ZEUR": {"display_name": "Euro", "decimal_places": 4, "is_stakable": False, "is_fiat": True},
    "ZJPY": {
        "display_name": "Japanese Yen",
        "decimal_places": 2,
        "is_stakable": False,
        "is_fiat": True,
    },
    "ZCAD": {
        "display_name": "Canadian Dollar",
        "decimal_places": 4,
        "is_stakable": False,
        "is_fiat": True,
    },
    "ZGBP": {
        "display_name": "British Pound",
        "decimal_places": 4,
        "is_stakable": False,
        "is_fiat": True,
    },
    "REP": {"display_name": "Augur", "decimal_places": 8, "is_stakable": False, "is_fiat": False},
    "NMC": {
        "display_name": "Namecoin",
        "decimal_places": 8,
        "is_stakable": False,
        "is_fiat": False,
    },
    "XLM": {"display_name": "Stellar", "decimal_places": 7, "is_stakable": True, "is_fiat": False},
    "LSK": {"display_name": "Lisk", "decimal_places": 8, "is_stakable": False, "is_fiat": False},
    "FCN": {
        "display_name": "FairCoin",
        "decimal_places": 8,
        "is_stakable": False,
        "is_fiat": False,
    },
    "FCT": {"display_name": "Factom", "decimal_places": 6, "is_stakable": False, "is_fiat": False},
    "VTC": {
        "display_name": "Vertcoin",
        "decimal_places": 8,
        "is_stakable": False,
        "is_fiat": False,
    },
    "DGB": {
        "display_name": "Dogecoin",
        "decimal_places": 8,
        "is_stakable": False,
        "is_fiat": False,
    },
    "SC": {"display_name": "Siacoin", "decimal_places": 12, "is_stakable": False, "is_fiat": False},
    "XBC": {
        "display_name": "BlackCoin",
        "decimal_places": 8,
        "is_stakable": False,
        "is_fiat": False,
    },
    "BTG": {
        "display_name": "Bitcoin Gold",
        "decimal_places": 8,
        "is_stakable": False,
        "is_fiat": False,
    },
    "BCH": {
        "display_name": "Bitcoin Cash",
        "decimal_places": 8,
        "is_stakable": True,
        "is_fiat": False,
    },
    "BSV": {
        "display_name": "Bitcoin SV",
        "decimal_places": 8,
        "is_stakable": False,
        "is_fiat": False,
    },
    "XDG": {
        "display_name": "Dogecoin",
        "decimal_places": 8,
        "is_stakable": False,
        "is_fiat": False,
    },
}

_DEFAULT_PRICES: dict[str, float] = {
    "XXBT": 45000.0,
    "XBT": 45000.0,
    "ETH": 3000.0,
    "XRP": 0.5,
    "LTC": 100.0,
    "DASH": 150.0,
    "ETC": 20.0,
    "XMR": 200.0,
    "ZEC": 50.0,
    "USDT": 1.0,
    "USDC": 1.0,
    "DAI": 1.0,
    "ZUSD": 1.0,
    "ZEUR": 1.0,
    "ZJPY": 0.0067,
    "ZCAD": 0.75,
    "ZGBP": 0.8,
    "REP": 10.0,
    "NMC": 5.0,
    "XLM": 0.1,
    "LSK": 2.0,
    "FCN": 0.01,
    "FCT": 15.0,
    "VTC": 0.1,
    "DGB": 0.005,
    "SC": 0.001,
    "XBC": 1.0,
    "BTG": 50.0,
    "BCH": 400.0,
    "BSV": 100.0,
    "XDG": 0.0001,
}

_DEFAULT_CURRENCY_INFO = {
    "display_name": "",
    "decimal_places": 8,
    "is_stakable": False,
    "is_fiat": False,
}


class KrakenRequestBalanceData(BalanceData):
    def __init__(self, data: dict[str, Any], asset_type: str, has_been_json_encoded=False):
        super().__init__(data, has_been_json_encoded)
        self.asset_type = asset_type
        self.logger = get_logger("kraken_balance")
        self._parse_data(data)

    def _parse_data(self, data: dict[str, Any]):
        try:
            self.timestamp = time.time()
            self.datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))
            self.exchange = "kraken"

            result = data.get("result", {})
            if not result:
                raise ValueError("No balance data found in response")

            self.balances = {}
            total_value_usd = 0.0
            self.total_crypto_balance = 0.0

            for currency, balance_str in result.items():
                try:
                    balance = float(balance_str)
                    if balance > 0:
                        balance_info = self._get_balance_info(currency, balance)
                        self.balances[currency] = balance_info
                        if balance_info["usd_value"] is not None:
                            total_value_usd += balance_info["usd_value"]
                        if balance_info["crypto_amount"] is not None:
                            self.total_crypto_balance += balance_info["crypto_amount"]

                except ValueError as e:
                    self.logger.warning(f"Error parsing balance for {currency}: {e}")

            self.total_value_usd = total_value_usd
            self.currency_balances = self._group_by_currency()

        except Exception as e:
            self.logger.error(f"Error parsing Kraken balance data: {e}")
            self.logger.error(f"Raw data: {data}")
            raise

    def _get_balance_info(self, currency: str, balance: float) -> dict[str, Any]:
        currency_info = self._get_currency_info(currency)

        balance_info = {
            "currency": currency,
            "free": balance,
            "used": 0.0,
            "total": balance,
            "crypto_amount": balance,
            "usd_value": None,
            "eur_value": None,
            "btc_value": None,
            "is_stakable": currency_info["is_stakable"],
            "is_fiat": currency_info["is_fiat"],
            "decimal_places": currency_info["decimal_places"],
            "display_name": currency_info["display_name"],
        }

        if currency_info["usd_price"] and balance > 0:
            usd_value = balance * currency_info["usd_price"]
            balance_info["usd_value"] = usd_value

        if currency_info["eur_price"] and balance > 0:
            eur_value = balance * currency_info["eur_price"]
            balance_info["eur_value"] = eur_value

        if currency_info["btc_price"] and balance > 0:
            btc_value = balance * currency_info["btc_price"]
            balance_info["btc_value"] = btc_value

        return balance_info

    def _get_currency_info(self, currency: str) -> dict[str, Any]:
        info = _CURRENCY_DATA.get(currency, {**_DEFAULT_CURRENCY_INFO, "display_name": currency})
        prices = self._get_prices()
        return {
            **info,
            "usd_price": prices.get(currency),
            "eur_price": prices.get(f"{currency}EUR") if currency != "EUR" else None,
            "btc_price": prices.get(f"{currency}XBT") if currency != "XBT" else None,
        }

    def _get_prices(self) -> dict[str, float]:
        return _DEFAULT_PRICES

    def _group_by_currency(self) -> dict[str, float]:
        grouped = {}
        for currency, info in self.balances.items():
            if info["total"] > 0:
                grouped[currency] = info["total"]
        return grouped

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "datetime": self.datetime,
            "exchange": self.exchange,
            "asset_type": self.asset_type,
            "balances": self.balances,
            "currency_balances": self.currency_balances,
            "total_value_usd": self.total_value_usd,
            "total_crypto_balance": self.total_crypto_balance,
        }

    def validate(self) -> bool:
        if not self.balances:
            return False
        for info in self.balances.values():
            if info["total"] < 0:
                return False
        return not self.total_value_usd < 0

    def get_currency_balance(self, currency: str) -> float | None:
        if currency in self.balances:
            return self.balances[currency]["total"]
        return None

    def get_fiat_balance(self, currency: str = "USD") -> float:
        total = 0.0
        for info in self.balances.values():
            if info["is_fiat"]:
                if currency == "USD" and info["usd_value"]:
                    total += info["usd_value"]
                elif currency == "EUR" and info["eur_value"]:
                    total += info["eur_value"]
            else:
                if currency == "USD" and info["usd_value"]:
                    total += info["usd_value"]
                elif currency == "EUR" and info["eur_value"]:
                    total += info["eur_value"]
        return total

    def get_crypto_balance(self) -> float:
        return self.total_crypto_balance

    def get_stakable_balance(self) -> dict[str, float]:
        stakable = {}
        for currency, info in self.balances.items():
            if info["is_stakable"] and info["total"] > 0:
                stakable[currency] = info["total"]
        return stakable

    def get_biggest_holding(self) -> tuple | None:
        if not self.balances:
            return None
        biggest = None
        max_value = 0.0
        for currency, info in self.balances.items():
            if info["usd_value"] and info["usd_value"] > max_value:
                max_value = info["usd_value"]
                biggest = (currency, info["total"], max_value)
        return biggest

    def update_balance(self, currency: str, delta: float):
        if currency in self.balances:
            old_balance = self.balances[currency]["total"]
            new_balance = old_balance + delta
            if new_balance >= 0:
                self.balances[currency]["total"] = new_balance
                self.balances[currency]["free"] = new_balance
                self.balances[currency]["crypto_amount"] = new_balance
                currency_info = self._get_currency_info(currency)
                if currency_info["usd_price"]:
                    self.balances[currency]["usd_value"] = new_balance * currency_info["usd_price"]
                self._update_totals()
                self.currency_balances = self._group_by_currency()
            else:
                self.logger.warning(f"Cannot update balance for {currency}: would be negative")

    def _update_totals(self):
        self.total_value_usd = sum(info["usd_value"] or 0 for info in self.balances.values())
        self.total_crypto_balance = sum(info["crypto_amount"] or 0 for info in self.balances.values())

    def __str__(self) -> str:
        return f"KrakenBalance({self.total_value_usd:.2f} USD, {len(self.balances)} currencies)"

    def __repr__(self) -> str:
        return (
            f"KrakenRequestBalanceData(timestamp={self.timestamp}, "
            f"total_value_usd={self.total_value_usd}, "
            f"currency_count={len(self.balances)})"
        )


class KrakenSpotWssBalanceData(BalanceData):
    def __init__(self, data: dict[str, Any], asset_type: str, has_been_json_encoded=False):
        super().__init__(data, has_been_json_encoded)
        self.asset_type = asset_type
        self.logger = get_logger("kraken_wss_balance")
        self._parse_wss_data(data)

    def _parse_wss_data(self, data: dict[str, Any]):
        if isinstance(data, dict) and "currency" in data:
            self.currency = data.get("currency")
            self.free = float(data.get("free", 0))
            self.used = float(data.get("used", 0))
            self.total = self.free + self.used
            self.timestamp = data.get("time", time.time())
        elif isinstance(data, dict):
            self.currency = None
            self.free = 0.0
            self.used = 0.0
            self.total = 0.0
            for curr, amount in data.items():
                try:
                    amt = float(amount)
                    if amt > 0 and self.currency is None:
                        self.currency = curr
                        self.free = amt
                        self.total = amt
                        break
                except (ValueError, TypeError):
                    continue
            if self.currency is None and data:
                self.currency = list(data.keys())[0]
            self.timestamp = time.time()
        else:
            self.currency = "UNKNOWN"
            self.free = 0.0
            self.used = 0.0
            self.total = 0.0
            self.timestamp = time.time()

        self.datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))
        self.exchange = "kraken"

    def to_dict(self) -> dict[str, Any]:
        return {
            "currency": self.currency,
            "free": self.free,
            "used": self.used,
            "total": self.total,
            "timestamp": self.timestamp,
            "datetime": self.datetime,
            "exchange": self.exchange,
            "asset_type": self.asset_type,
        }
