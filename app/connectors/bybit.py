import asyncio
import hashlib
import hmac
import os
import time
from decimal import Decimal

import httpx
from dotenv import load_dotenv

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position

load_dotenv()

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
                funding_rate=None,
            ))
        return positions

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

    async def get_funding(self, ticker: str) -> Decimal:
        resp = await self._client.get(
            f"/v5/market/tickers?category=linear&symbol={ticker}"
        )
        resp.raise_for_status()
        data = resp.json()
        item = data["result"]["list"][0]
        return Decimal(item["fundingRate"])
