from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Any
from urllib.parse import urlencode

import requests as req_lib
from bt_api_base.containers.requestdatas.request_data import RequestData
from bt_api_base.feeds.capability import Capability
from bt_api_base.feeds.feed import Feed
from bt_api_base.logging_factory import get_logger

from bt_api_kraken.exchange_data.kraken_exchange_data import KrakenExchangeDataSpot


class KrakenRequestData(Feed):
    @classmethod
    def _capabilities(cls) -> set[Capability]:
        return {
            Capability.GET_TICK,
            Capability.GET_DEPTH,
            Capability.GET_KLINE,
            Capability.MAKE_ORDER,
            Capability.CANCEL_ORDER,
            Capability.QUERY_ORDER,
            Capability.QUERY_OPEN_ORDERS,
            Capability.GET_DEALS,
            Capability.GET_BALANCE,
            Capability.GET_ACCOUNT,
            Capability.GET_SERVER_TIME,
            Capability.GET_EXCHANGE_INFO,
        }

    def __init__(self, data_queue: Any = None, **kwargs: Any) -> None:
        super().__init__(data_queue, **kwargs)
        self.public_key = kwargs.get("public_key") or kwargs.get("api_key")
        self.private_key = kwargs.get("private_key") or kwargs.get("secret_key") or kwargs.get("api_secret")
        self.asset_type = kwargs.get("asset_type", "SPOT")
        self.exchange_name = kwargs.get("exchange_name", "KRAKEN")
        self.logger_name = kwargs.get("logger_name", "kraken_feed.log")
        self._params = KrakenExchangeDataSpot()
        self.request_logger = get_logger("request")
        self.async_logger = get_logger("async_request")
        self._nonce = int(time.time() * 1000)

    def _generate_nonce(self):
        self._nonce += 1
        return str(self._nonce)

    def _sign_request(self, url_path, data):
        if not self.private_key or not self.public_key:
            return {}

        nonce = self._generate_nonce()
        data["nonce"] = nonce

        postdata = urlencode(data)
        encoded = (nonce + postdata).encode("utf-8")
        message = url_path.encode("utf-8") + hashlib.sha256(encoded).digest()

        mac = hmac.new(base64.b64decode(self.private_key), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())

        return {
            "API-Key": self.public_key,
            "API-Sign": sigdigest.decode("utf-8"),
        }

    def request(self, path, params=None, body=None, extra_data=None, timeout=10):
        if params is None:
            params = {}
        if body is None:
            body = {}
        if extra_data is None:
            extra_data = {}

        parts = path.split(" ", 1) if " " in path else ["POST", path]
        method = parts[0].upper()
        endpoint = parts[1] if len(parts) > 1 else parts[0]

        base_url = self._params.rest_url
        url = base_url + endpoint

        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        response_data = None

        try:
            if method == "POST":
                post_data = {**params, **body}

                if "/private/" in endpoint:
                    auth_headers = self._sign_request(endpoint, post_data)
                    headers.update(auth_headers)

                self.request_logger.info(f"POST {url}")
                req_kwargs = {
                    "headers": headers,
                    "data": urlencode(post_data) if post_data else "",
                }
                if self.proxies:
                    req_kwargs["proxies"] = self.proxies
                res = req_lib.post(url, timeout=timeout, **req_kwargs)
                response_data = res.json()
            else:
                self.request_logger.info(f"{method} {url}")
                if params:
                    url = f"{url}?{urlencode(params)}"
                response_data = self.http_request(method=method, url=url, headers={}, timeout=timeout)
        except Exception as e:
            self.request_logger.error(f"Request error: {e}")

        request_data = RequestData(response_data, extra_data)
        request_data.init_data()
        return request_data

    async def async_request(self, path, params=None, body=None, extra_data=None, timeout=5):
        if params is None:
            params = {}
        if body is None:
            body = {}
        if extra_data is None:
            extra_data = {}

        parts = path.split(" ", 1) if " " in path else ["POST", path]
        method = parts[0].upper()
        endpoint = parts[1] if len(parts) > 1 else parts[0]

        base_url = self._params.rest_url
        url = base_url + endpoint

        response_data = None
        try:
            if method == "POST":
                import asyncio

                post_data = {**params, **body}
                loop = asyncio.get_event_loop()
                response_data = await loop.run_in_executor(
                    None, lambda: self._sync_post(url, post_data, endpoint, timeout)
                )
            else:
                if params:
                    url = f"{url}?{urlencode(params)}"
                response_data = await self.async_http_request(method=method, url=url, headers={}, timeout=timeout)
        except Exception as e:
            self.async_logger.error(f"Async request error: {e}")

        return RequestData(response_data, extra_data)

    def _sync_post(self, url, post_data, endpoint, timeout):
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        if "/private/" in endpoint:
            auth_headers = self._sign_request(endpoint, post_data)
            headers.update(auth_headers)
        try:
            req_kwargs = {
                "headers": headers,
                "data": urlencode(post_data) if post_data else "",
            }
            if self.proxies:
                req_kwargs["proxies"] = self.proxies
            res = req_lib.post(url, timeout=timeout, **req_kwargs)
            return res.json()
        except Exception as e:
            self.async_logger.error(f"Sync POST error: {e}")
            return None

    def async_callback(self, future):
        try:
            request_data = future.result()
            if request_data is not None:
                request_data.init_data()
                self.push_data_to_queue(request_data)
        except Exception as e:
            self.async_logger.error(f"async_callback error: {e}")

    def push_data_to_queue(self, request_data):
        if self.data_queue is not None:
            self.data_queue.put(request_data)

    @staticmethod
    def _extract_data_normalize_function(input_data, extra_data):
        if input_data is None:
            return [], False
        if isinstance(input_data, dict):
            errors = input_data.get("error", [])
            if errors:
                return [input_data], False
            result = input_data.get("result")
            if result is not None:
                if isinstance(result, list):
                    return result, True
                return [result], True
        if isinstance(input_data, list):
            return input_data, True
        return [input_data], True
