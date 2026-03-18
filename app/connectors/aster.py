import hashlib
import hmac
import os
import time
from decimal import Decimal

import httpx

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position

_BASE_URL = "https://fapi.asterdex.com"


class AsterConnector(BaseExchangeConnector):
    name = "aster"

    def __init__(self) -> None:
        super().__init__()
        self._api_key = os.environ["ASTER_API_KEY"]
        self._api_secret = os.environ["ASTER_API_SECRET"]
        self._client = httpx.AsyncClient(base_url=_BASE_URL, headers={"X-MBX-APIKEY": self._api_key})

    def _sign(self, params: dict) -> str:
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 5000
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        signature = hmac.new(
            self._api_secret.encode(),
            query_string.encode(),
            hashlib.sha256,
        ).hexdigest()
        return query_string + f"&signature={signature}"

    async def fetch_positions(self) -> list[Position]:
        query = self._sign({})
        resp = await self._client.get(f"/fapi/v2/positionRisk?{query}")
        resp.raise_for_status()
        positions = []
        for item in resp.json():
            if Decimal(item["positionAmt"]) == 0:
                continue
            amount = Decimal(item["positionAmt"])
            direction = "long" if amount > 0 else "short"
            positions.append(Position(
                ticker=item["symbol"],
                exchange_name=self.name,
                direction=direction,
                amount=abs(amount),
                avg_price=Decimal(item["entryPrice"]),
                current_price=Decimal(item["markPrice"]),
            ))
        return positions

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        query = self._sign({})
        resp = await self._client.get(f"/fapi/v4/account?{query}")
        resp.raise_for_status()
        data = resp.json()
        return Decimal(data["totalMaintMargin"]), Decimal(data["totalWalletBalance"])

    async def close_position(self, ticker: str, amount: Decimal) -> None:
        pos = self.state.positions.get(ticker)
        params = {
            "symbol": ticker,
            "side": "SELL" if pos and pos.direction == "long" else "BUY",
            "type": "MARKET",
            "quantity": str(amount),
            "reduceOnly": "true",
        }
        query = self._sign(params)
        resp = await self._client.post(f"/fapi/v1/order?{query}")
        resp.raise_for_status()
