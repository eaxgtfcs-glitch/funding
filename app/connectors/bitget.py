import base64
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

_BASE_URL = "https://api.bitget.com"


class BitgetConnector(BaseExchangeConnector):
    name = "bitget"

    def __init__(self) -> None:
        super().__init__()
        self._api_key = vault.get("BITGET_API_KEY")
        self._api_secret = vault.get("BITGET_SECRET_KEY")
        self._passphrase = vault.get("BITGET_PASSPHRASE")
        self._client = httpx.AsyncClient(base_url=_BASE_URL)

    def _sign(self, method: str, path: str, body: str = "") -> dict:
        timestamp = str(int(time.time() * 1000))
        message = timestamp + method.upper() + path + body
        signature = base64.b64encode(
            hmac.new(
                self._api_secret.encode(),
                message.encode(),
                hashlib.sha256,
            ).digest()
        ).decode()
        return {
            "ACCESS-KEY": self._api_key,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type": "application/json",
        }

    async def fetch_positions(self) -> list[Position]:
        path = "/api/v2/mix/position/all-position?productType=USDT-FUTURES&marginCoin=USDT"
        headers = self._sign("GET", path)
        resp = await self._client.get(path, headers=headers)
        resp.raise_for_status()
        positions = []
        for item in resp.json()["data"]:
            amount = Decimal(item["total"])
            if amount == 0:
                continue
            positions.append(Position(
                ticker=item["symbol"],
                exchange_name=self.name,
                direction=item["holdSide"],
                amount=amount,
                avg_price=Decimal(item["openPriceAvg"]),
                current_price=Decimal(item["markPrice"]),
            ))
        return positions

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        params = "productType=USDT-FUTURES"
        path = f"/api/v2/mix/account/accounts?{params}"
        headers = self._sign("GET", path)
        resp = await self._client.get(path, headers=headers)
        resp.raise_for_status()
        data = resp.json()["data"]
        account = next((a for a in data if a.get("marginCoin") == "USDT"), data[0])
        maintenance_margin = Decimal(account.get("unionMm", "0"))
        current_margin = Decimal(account["accountEquity"])
        return maintenance_margin, current_margin

    async def _send_order(self, payload: dict) -> bool:
        body = _json.dumps(payload)
        path = "/api/v2/mix/order/place-order"
        headers = self._sign("POST", path, body)
        resp = await self._client.post(path, headers=headers, content=body)
        if not resp.is_success:
            return False
        data = resp.json()
        return data.get("code") == "00000"

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
        side = "buy" if direction == "long" else "sell"
        payload: dict = {
            "symbol": ticker,
            "productType": "USDT-FUTURES",
            "marginCoin": "USDT",
            "marginMode": "crossed",
            "orderType": order_type,
            "side": side,
            "tradeSide": "open",
            "size": str(amount),
        }
        if order_type == "limit":
            payload["price"] = str(limit_price)
        ok = await self._send_order(payload)
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
        side = "sell" if (pos and pos.direction == "long") else "buy"
        payload: dict = {
            "symbol": ticker,
            "productType": "USDT-FUTURES",
            "marginCoin": "USDT",
            "marginMode": "crossed",
            "orderType": order_type,
            "side": side,
            "tradeSide": "close",
            "size": str(amount),
        }
        if order_type == "limit":
            payload["price"] = str(limit_price)
        ok = await self._send_order(payload)
        if not ok:
            return False
        if order_type == "market":
            return await self._verify_position_changed(ticker, snapshot)
        return True
