import asyncio
import hashlib
import hmac
import json as _json
import time
from decimal import Decimal
from typing import Literal

import httpx

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position
from app.helper.key_vault import vault

_BASE_URL = "https://api.bybit.com"
_RECV_WINDOW = "5000"


class BybitConnector(BaseExchangeConnector):
    name = "bybit"

    def __init__(self) -> None:
        super().__init__()
        self._api_key = vault.get("BYBIT_API_KEY")
        self._api_secret = vault.get("BYBIT_API_SECRET")
        self._client = httpx.AsyncClient(base_url=_BASE_URL)

    def _sign(self, query_string: str) -> dict:
        """Формирует заголовки с HMAC-SHA256 подписью для приватных эндпоинтов Bybit v5."""
        timestamp = str(int(time.time() * 1000))
        payload = timestamp + self._api_key + _RECV_WINDOW + query_string
        signature = hmac.new(
            self._api_secret.encode(),
            payload.encode(),
            hashlib.sha256,
        ).hexdigest()
        return {
            "X-BAPI-API-KEY": self._api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": _RECV_WINDOW,
            "X-BAPI-SIGN": signature,
        }

    async def _fetch_positions_by_settle(self, settle_coin: str) -> list:
        query = f"category=linear&settleCoin={settle_coin}"
        headers = self._sign(query)
        resp = await self._client.get(f"/v5/position/list?{query}", headers=headers)
        resp.raise_for_status()
        return resp.json()["result"]["list"]

    async def fetch_positions(self) -> list[Position]:
        # запрашиваем USDT и USDC позиции параллельно
        usdt_items, usdc_items = await asyncio.gather(
            self._fetch_positions_by_settle("USDT"),
            self._fetch_positions_by_settle("USDC"),
        )
        positions = []
        for item in usdt_items + usdc_items:
            size = Decimal(item["size"])
            if size == 0:
                continue
            direction = "long" if item["side"] == "Buy" else "short"
            positions.append(Position(
                ticker=item["symbol"],
                exchange_name=self.name,
                direction=direction,
                amount=size,
                avg_price=Decimal(item["avgPrice"]),
                current_price=Decimal(item["markPrice"]),
            ))
        return positions

    def _sign_body(self, body: dict) -> dict:
        """Формирует заголовки с HMAC-SHA256 подписью для POST-запросов Bybit v5 (JSON body)."""
        timestamp = str(int(time.time() * 1000))
        body_str = _json.dumps(body, separators=(",", ":"))
        payload = timestamp + self._api_key + _RECV_WINDOW + body_str
        signature = hmac.new(
            self._api_secret.encode(),
            payload.encode(),
            hashlib.sha256,
        ).hexdigest()
        return {
            "X-BAPI-API-KEY": self._api_key,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": _RECV_WINDOW,
            "X-BAPI-SIGN": signature,
            "Content-Type": "application/json",
        }

    async def place_order(
            self,
            ticker: str,
            direction: Literal["long", "short"],
            amount: Decimal,
            order_type: Literal["market", "limit"] = "market",
            limit_price: Decimal | None = None,
    ) -> bool:
        if order_type == "limit" and limit_price is None:
            raise ValueError("limit_price required for limit orders")
        snapshot = await self.fetch_positions()
        side = "Buy" if direction == "long" else "Sell"
        body = {
            "category": "linear",
            "symbol": ticker,
            "side": side,
            "orderType": "Market" if order_type == "market" else "Limit",
            "qty": str(amount),
        }
        if order_type == "limit":
            body["price"] = str(limit_price)
        headers = self._sign_body(body)
        resp = await self._client.post("/v5/order/create", headers=headers,
                                       content=_json.dumps(body, separators=(",", ":")))
        if not resp.is_success:
            return False
        data = resp.json()
        if data.get("retCode") != 0:
            return False
        if order_type == "market":
            return await self._verify_position_changed(ticker, snapshot)
        return True

    async def close_position(
            self,
            ticker: str,
            amount: Decimal,
            order_type: Literal["market", "limit"] = "market",
            limit_price: Decimal | None = None,
    ) -> bool:
        if order_type == "limit" and limit_price is None:
            raise ValueError("limit_price required for limit orders")
        snapshot = await self.fetch_positions()
        pos = self.state.positions.get(ticker)
        side = "Sell" if (pos and pos.direction == "long") else "Buy"
        body = {
            "category": "linear",
            "symbol": ticker,
            "side": side,
            "orderType": "Market" if order_type == "market" else "Limit",
            "qty": str(amount),
            "reduceOnly": True,
        }
        if order_type == "limit":
            body["price"] = str(limit_price)
        headers = self._sign_body(body)
        resp = await self._client.post("/v5/order/create", headers=headers,
                                       content=_json.dumps(body, separators=(",", ":")))
        if not resp.is_success:
            return False
        data = resp.json()
        if data.get("retCode") != 0:
            return False
        if order_type == "market":
            return await self._verify_position_changed(ticker, snapshot)
        return True

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        query = "accountType=UNIFIED"
        headers = self._sign(query)
        resp = await self._client.get(f"/v5/account/wallet-balance?{query}", headers=headers)
        resp.raise_for_status()
        data = resp.json()
        account = data["result"]["list"][0]
        maintenance_margin = Decimal(account["totalMaintenanceMargin"])
        current_margin = Decimal(account["totalMarginBalance"])
        return maintenance_margin, current_margin

