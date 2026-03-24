import hashlib
import hmac
import time
from decimal import Decimal
from typing import Literal

import httpx

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position
from app.helper.key_vault import vault

_BASE_URL = "https://fapi.asterdex.com"


class AsterConnector(BaseExchangeConnector):
    name = "aster"

    def __init__(self) -> None:
        super().__init__()
        self._api_key = vault.get("ASTER_API_KEY")
        self._api_secret = vault.get("ASTER_API_SECRET")
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
        maint = data.get("totalMaintMargin") or data.get("maintMargin") or "0"
        current = data.get("totalMarginBalance") or data.get("marginBalance") or "0"
        return Decimal(maint), Decimal(current)

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
        side = "BUY" if direction == "long" else "SELL"
        params = {
            "symbol": ticker,
            "side": side,
            "type": order_type.upper(),
            "quantity": str(amount),
        }
        if order_type == "limit":
            params["price"] = str(limit_price)
            params["timeInForce"] = "GTC"
        query = self._sign(params)
        resp = await self._client.post(f"/fapi/v1/order?{query}")
        if not resp.is_success:
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
        pos = self.state.positions.get(ticker)
        params = {
            "symbol": ticker,
            "side": "SELL" if pos and pos.direction == "long" else "BUY",
            "type": order_type.upper(),
            "quantity": str(amount),
            "reduceOnly": "true",
        }
        if order_type == "limit":
            params["price"] = str(limit_price)
            params["timeInForce"] = "GTC"
        query = self._sign(params)
        resp = await self._client.post(f"/fapi/v1/order?{query}")
        resp.raise_for_status()
        return resp.is_success
