import hashlib
import hmac
import time
from decimal import Decimal
from typing import Literal

import httpx

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position
from app.helper.key_vault import vault

_BASE_URL = "https://open-api.bingx.com"


class BingXConnector(BaseExchangeConnector):
    name = "bingx"

    def __init__(self) -> None:
        super().__init__()
        self._api_key = vault.get("BINGX_API_KEY")
        self._api_secret = vault.get("BINGX_SECRET_KEY")
        self._client = httpx.AsyncClient(base_url=_BASE_URL)

    def _sign(self, params: dict) -> str:
        params["timestamp"] = int(time.time() * 1000)
        query_string = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        signature = hmac.new(
            self._api_secret.encode(),
            query_string.encode(),
            hashlib.sha256,
        ).hexdigest()
        return query_string + f"&signature={signature}"

    async def fetch_positions(self) -> list[Position]:
        query = self._sign({})
        resp = await self._client.get(
            f"/openApi/swap/v2/user/positions?{query}",
            headers={"X-BX-APIKEY": self._api_key},
        )
        resp.raise_for_status()
        data = resp.json()["data"]
        items = data if isinstance(data, list) else data.get("positions", [])
        positions = []
        for item in items:
            amt = Decimal(item["positionAmt"])
            if amt == 0:
                continue
            position_side = item.get("positionSide", "")
            if position_side == "SHORT":
                direction = "short"
            else:
                direction = "long" if amt > 0 else "short"
            positions.append(Position(
                ticker=item["symbol"],
                exchange_name=self.name,
                direction=direction,
                amount=abs(amt),
                avg_price=Decimal(item["avgPrice"]),
                current_price=Decimal(item["markPrice"]),
            ))
        return positions

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        query = self._sign({})
        resp = await self._client.get(
            f"/openApi/swap/v2/user/balance?{query}",
            headers={"X-BX-APIKEY": self._api_key},
        )
        resp.raise_for_status()
        data = resp.json()["data"]["balance"]
        maintenance_margin = Decimal(data["usedMargin"])
        current_margin = Decimal(data["balance"])
        return maintenance_margin, current_margin

    async def _send_order(self, params: dict) -> bool:
        query = self._sign(params)
        resp = await self._client.post(
            f"/openApi/swap/v2/trade/order?{query}",
            headers={"X-BX-APIKEY": self._api_key},
        )
        if not resp.is_success:
            return False
        data = resp.json()
        return data.get("code") == 0

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
        position_side = "LONG" if direction == "long" else "SHORT"
        params: dict = {
            "symbol": ticker,
            "side": side,
            "positionSide": position_side,
            "type": order_type.upper(),
            "quantity": str(amount),
        }
        if order_type == "limit":
            params["price"] = str(limit_price)
        ok = await self._send_order(params)
        if not ok:
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
        is_long = pos and pos.direction == "long"
        side = "SELL" if is_long else "BUY"
        position_side = "LONG" if is_long else "SHORT"
        params: dict = {
            "symbol": ticker,
            "side": side,
            "positionSide": position_side,
            "type": order_type.upper(),
            "quantity": str(amount),
        }
        if order_type == "limit":
            params["price"] = str(limit_price)
        ok = await self._send_order(params)
        if not ok:
            return False
        if order_type == "market":
            return await self._verify_position_changed(ticker, snapshot)
        return True
