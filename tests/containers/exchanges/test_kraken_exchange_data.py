"""Tests for KrakenExchangeData container."""

from __future__ import annotations

from bt_api_kraken.exchange_data import KrakenExchangeData


class TestKrakenExchangeData:
    """Tests for KrakenExchangeData."""

    def test_init(self):
        """Test initialization."""
        exchange = KrakenExchangeData()

        assert exchange.exchange_name == "kraken"
