import asyncio
import hashlib
import hmac
import json as _json
import os
import time
from decimal import Decimal

import httpx

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position

_BASE_URL = "https://api.bybit.com"
_RECV_WINDOW = "5000"


class BybitConnector(BaseExchangeConnector):
    name = "bybit"

    def __init__(self) -> None:
        super().__init__()
        self._api_key = os.environ["BYBIT_API_KEY"]
        self._api_secret = os.environ["BYBIT_API_SECRET"]
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

    async def close_position(self, ticker: str, amount: Decimal) -> None:
        pos = self.state.positions.get(ticker)
        side = "Sell" if (pos and pos.direction == "long") else "Buy"
        body = {
            "category": "linear",
            "symbol": ticker,
            "side": side,
            "orderType": "Market",
            "qty": str(amount),
            "reduceOnly": True,
        }
        headers = self._sign_body(body)
        resp = await self._client.post("/v5/order/create", headers=headers,
                                       content=_json.dumps(body, separators=(",", ":")))
        resp.raise_for_status()
        data = resp.json()
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit close_position error: {data.get('retMsg')} (retCode={data.get('retCode')})")

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

