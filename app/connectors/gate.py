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

_BASE_URL = "https://api.gateio.ws/api/v4"


class GateConnector(BaseExchangeConnector):
    name = "gate"

    def __init__(self) -> None:
        super().__init__()
        self._api_key = vault.get("GATE_API_KEY")
        self._api_secret = vault.get("GATE_SECRET_KEY")
        self._client = httpx.AsyncClient(base_url=_BASE_URL)

    def _sign(self, method: str, path: str, query: str = "", body: str = "") -> dict:
        t = str(time.time())
        body_hash = hashlib.sha512(body.encode()).hexdigest()
        full_path = f"/api/v4{path}"
        msg = f"{method}\n{full_path}\n{query}\n{body_hash}\n{t}"
        sig = hmac.new(self._api_secret.encode(), msg.encode(), hashlib.sha512).hexdigest()
        return {"KEY": self._api_key, "Timestamp": t, "SIGN": sig}

    def _sign_body(self, method: str, path: str, body_str: str) -> dict:
        t = str(time.time())
        body_hash = hashlib.sha512(body_str.encode()).hexdigest()
        full_path = f"/api/v4{path}"
        msg = f"{method}\n{full_path}\n\n{body_hash}\n{t}"
        sig = hmac.new(self._api_secret.encode(), msg.encode(), hashlib.sha512).hexdigest()
        return {"KEY": self._api_key, "Timestamp": t, "SIGN": sig, "Content-Type": "application/json"}

    async def fetch_positions(self) -> list[Position]:
        path = "/futures/usdt/positions"
        headers = self._sign("GET", path)
        resp = await self._client.get(path, headers=headers)
        resp.raise_for_status()
        positions = []
        for item in resp.json():
            size = item["size"]
            if size == 0:
                continue
            direction = "long" if size > 0 else "short"
            positions.append(Position(
                ticker=item["contract"],
                exchange_name=self.name,
                direction=direction,
                amount=Decimal(abs(size)),
                avg_price=Decimal(item["entry_price"]),
                current_price=Decimal(item["mark_price"]),
            ))
        return positions

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        path = "/futures/usdt/accounts"
        headers = self._sign("GET", path)
        resp = await self._client.get(path, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        maintenance_margin = Decimal(data["cross_maintenance_margin"])
        current_margin = Decimal(data["total"])
        return maintenance_margin, current_margin

    async def _amount_to_contracts(self, ticker: str, amount: Decimal) -> int:
        """Convert base-currency amount to Gate.io contract count using quanto_multiplier."""
        resp = await self._client.get(f"/futures/usdt/contracts/{ticker}")
        resp.raise_for_status()
        multiplier = Decimal(resp.json().get("quanto_multiplier", "1"))
        if multiplier <= 0:
            multiplier = Decimal("1")
        return max(1, int(amount / multiplier))

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
        contracts = await self._amount_to_contracts(ticker, amount)
        size = contracts if direction == "long" else -contracts
        body: dict = {
            "contract": ticker,
            "size": size,
        }
        if order_type == "market":
            body["price"] = "0"
            body["tif"] = "ioc"
        else:
            body["price"] = str(limit_price)
            body["tif"] = "gtc"
        path = "/futures/usdt/orders"
        body_str = _json.dumps(body)
        headers = self._sign_body("POST", path, body_str)
        resp = await self._client.post(path, headers=headers, content=body_str)
        if not resp.is_success:
            return False
        if order_type == "market":
            data = resp.json()
            return data.get("finish_as") == "filled" or data.get("left") == 0
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
        direction = pos.direction if pos else "long"
        body: dict = {
            "contract": ticker,
            "size": 0,
            "reduce_only": True,
            "auto_size": "close_long" if direction == "long" else "close_short",
        }
        if order_type == "market":
            body["price"] = "0"
            body["tif"] = "ioc"
        else:
            body["price"] = str(limit_price)
            body["tif"] = "gtc"
        path = "/futures/usdt/orders"
        body_str = _json.dumps(body)
        headers = self._sign_body("POST", path, body_str)
        resp = await self._client.post(path, headers=headers, content=body_str)
        if not resp.is_success:
            return False
        if order_type == "market":
            data = resp.json()
            return data.get("finish_as") == "filled" or data.get("left") == 0
        return True
