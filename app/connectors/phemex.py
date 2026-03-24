import hashlib
import hmac
import json
import time
from decimal import Decimal
from typing import Literal

import httpx

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position
from app.helper.key_vault import vault

_BASE_URL = "https://api.phemex.com"


class PhemexConnector(BaseExchangeConnector):
    name = "phemex"

    def __init__(self) -> None:
        super().__init__()
        self._api_key = vault.get("PHEMEX_API_KEY")
        self._api_secret = vault.get("PHEMEX_SECRET_KEY")
        self._client = httpx.AsyncClient(base_url=_BASE_URL)

    def _sign_query(self, path: str, query: str = "") -> dict:
        """Sign a GET request (params in query string)."""
        expiry = str(int(time.time()) + 60)
        msg = path + query + expiry
        sig = hmac.new(self._api_secret.encode(), msg.encode(), hashlib.sha256).hexdigest()
        return {
            "x-phemex-access-token": self._api_key,
            "x-phemex-request-expiry": expiry,
            "x-phemex-request-signature": sig,
        }

    def _sign_body(self, path: str, body: str = "") -> dict:
        """Sign a POST/PUT request with JSON body."""
        expiry = str(int(time.time()) + 60)
        msg = path + expiry + body
        sig = hmac.new(self._api_secret.encode(), msg.encode(), hashlib.sha256).hexdigest()
        return {
            "x-phemex-access-token": self._api_key,
            "x-phemex-request-expiry": expiry,
            "x-phemex-request-signature": sig,
            "Content-Type": "application/json",
        }

    async def fetch_positions(self) -> list[Position]:
        path = "/g-accounts/positions"
        query = "currency=USDT"
        headers = self._sign_query(path, query)
        resp = await self._client.get(f"{path}?{query}", headers=headers)
        resp.raise_for_status()
        positions = []
        for item in resp.json().get("data", {}).get("positions", []):
            size = Decimal(item.get("sizeRq", "0"))
            if size == 0:
                continue
            pos_side = item.get("posSide", "")
            direction = "long" if pos_side == "Long" else "short"
            avg_price = Decimal(item.get("avgEntryPriceRp", "0"))
            mark_price = Decimal(item.get("markPriceRp", "0"))
            positions.append(Position(
                ticker=item["symbol"],
                exchange_name=self.name,
                direction=direction,
                amount=size,
                avg_price=avg_price,
                current_price=mark_price,
            ))
        return positions

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        path = "/g-accounts/accountPositions"
        query = "currency=USDT"
        headers = self._sign_query(path, query)
        resp = await self._client.get(f"{path}?{query}", headers=headers)
        resp.raise_for_status()
        account = resp.json().get("data", {}).get("account", {})
        maintenance_margin = Decimal(account.get("totalUsedBalanceRv", "0"))
        current_margin = Decimal(account.get("accountBalanceRv", "0"))
        return maintenance_margin, current_margin

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
        pos_side = "Long" if direction == "long" else "Short"
        ord_type = "Market" if order_type == "market" else "Limit"
        path = "/g-orders"
        body: dict = {
            "symbol": ticker,
            "ordType": ord_type,
            "side": side,
            "posSide": pos_side,
            "orderQtyRq": str(amount),
        }
        if order_type == "limit":
            body["priceRp"] = str(limit_price)
        body_str = json.dumps(body)
        headers = self._sign_body(path, body_str)
        resp = await self._client.post(path, headers=headers, content=body_str)
        if not resp.is_success:
            return False
        data = resp.json()
        if data.get("code") != 0:
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
        pos_side = "Long" if (pos and pos.direction == "long") else "Short"
        ord_type = "Market" if order_type == "market" else "Limit"
        path = "/g-orders"
        body: dict = {
            "symbol": ticker,
            "ordType": ord_type,
            "side": side,
            "posSide": pos_side,
            "orderQtyRq": str(amount),
            "reduceOnly": True,
        }
        if order_type == "limit":
            body["priceRp"] = str(limit_price)
        body_str = json.dumps(body)
        headers = self._sign_body(path, body_str)
        resp = await self._client.post(path, headers=headers, content=body_str)
        if not resp.is_success:
            return False
        data = resp.json()
        if data.get("code") != 0:
            return False
        if order_type == "market":
            return await self._verify_position_changed(ticker, snapshot)
        return True
