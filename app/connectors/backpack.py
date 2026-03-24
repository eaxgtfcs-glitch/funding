import base64
import time
from decimal import Decimal
from typing import Literal

import httpx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position
from app.helper.key_vault import vault

_BASE_URL = "https://api.backpack.exchange"
_WINDOW = "5000"


class BackpackConnector(BaseExchangeConnector):
    name = "backpack"

    def __init__(self) -> None:
        super().__init__()
        self._api_key = vault.get("BACKPACK_API_KEY")
        self._api_secret = vault.get("BACKPACK_API_SECRET")
        self._client = httpx.AsyncClient(base_url=_BASE_URL)

    def _sign(self, instruction: str, params: dict) -> dict:
        timestamp = str(int(time.time() * 1000))
        sorted_params = dict(sorted(params.items()))
        parts = [f"instruction={instruction}"]
        for k, v in sorted_params.items():
            parts.append(f"{k}={v}")
        parts.append(f"timestamp={timestamp}")
        parts.append(f"window={_WINDOW}")
        message = "&".join(parts)

        private_key = Ed25519PrivateKey.from_private_bytes(base64.b64decode(self._api_secret))
        signature = base64.b64encode(private_key.sign(message.encode())).decode()

        return {
            "X-API-Key": self._api_key,
            "X-Signature": signature,
            "X-Timestamp": timestamp,
            "X-Window": _WINDOW,
        }

    async def fetch_positions(self) -> list[Position]:
        headers = self._sign("positionQuery", {})
        resp = await self._client.get("/api/v1/position", headers=headers)
        resp.raise_for_status()
        positions = []
        for item in resp.json():
            net_qty = Decimal(item["netQuantity"])
            if net_qty == 0:
                continue
            direction = "long" if net_qty > 0 else "short"
            positions.append(Position(
                ticker=item["symbol"],
                exchange_name=self.name,
                direction=direction,
                amount=abs(net_qty),
                avg_price=Decimal(item["entryPrice"]),
                current_price=Decimal(item["markPrice"]),
            ))
        return positions

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        headers = self._sign("collateralQuery", {})
        resp = await self._client.get("/api/v1/capital/collateral", headers=headers)
        resp.raise_for_status()
        data = resp.json()
        maintenance_margin = abs(Decimal(data["mmf"]) * Decimal(data["netExposureFutures"]))
        current_margin = Decimal(data["netEquity"])
        return maintenance_margin, current_margin

    def _build_order_body(
            self,
            ticker: str,
            side: str,
            amount: Decimal,
            order_type: str,
            limit_price: Decimal | None,
            reduce_only: bool,
    ) -> dict:
        body: dict = {
            "orderType": "Market" if order_type == "market" else "Limit",
            "quantity": str(amount),
            "reduceOnly": reduce_only,
            "side": side,
            "symbol": ticker,
        }
        if order_type == "limit":
            body["price"] = str(limit_price)
            body["timeInForce"] = "GTC"
        return body

    async def _send_order(self, body: dict) -> bool:
        def _bool_to_str(v):
            if isinstance(v, bool):
                return "true" if v else "false"
            return str(v)

        params_for_sign = {k: _bool_to_str(v) for k, v in body.items()}
        headers = self._sign("orderExecute", params_for_sign)
        headers["Content-Type"] = "application/json"
        resp = await self._client.post("/api/v1/order", headers=headers, json=body)
        if not resp.is_success:
            raise RuntimeError(f"HTTP {resp.status_code}")
        data = resp.json()
        if isinstance(data, dict) and "error" in data:
            raise RuntimeError(data["error"])
        return True

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
        side = "Bid" if direction == "long" else "Ask"
        body = self._build_order_body(ticker, side, amount, order_type, limit_price, False)
        ok = await self._send_order(body)
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
        pos = self.state.positions.get(ticker)
        side = "Ask" if (pos and pos.direction == "long") else "Bid"
        body = self._build_order_body(ticker, side, amount, order_type, limit_price, True)
        return await self._send_order(body)
