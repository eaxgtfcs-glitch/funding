import base64
import hashlib
import hmac
import json as _json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

import httpx

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position
from app.helper.key_vault import vault

_BASE_URL = "https://www.okx.com"

_QUOTE_CURRENCIES = ("USDT", "USDC", "USD", "BTC", "ETH")


def _to_inst_id(symbol: str) -> str:
    """Convert a flat symbol like 'HYPEUSDT' to OKX instId format 'HYPE-USDT-SWAP'.

    If the symbol already contains '-', it is returned unchanged.
    """
    if "-" in symbol:
        return symbol
    for quote in _QUOTE_CURRENCIES:
        if symbol.endswith(quote):
            base = symbol[: -len(quote)]
            return f"{base}-{quote}-SWAP"
    return symbol


def _from_inst_id(inst_id: str) -> str:
    """Convert an OKX instId like 'HYPE-USDT-SWAP' back to a flat symbol like 'HYPEUSDT'.

    Strips the '-SWAP' suffix and removes the dash between base and quote.
    If the format is unexpected, returns inst_id unchanged.
    """
    if not inst_id.endswith("-SWAP"):
        return inst_id
    parts = inst_id[:-5].split("-")  # strip '-SWAP', then split 'HYPE-USDT'
    if len(parts) == 2:
        return parts[0] + parts[1]
    return inst_id


class OKXConnector(BaseExchangeConnector):
    name = "okx"

    def __init__(self) -> None:
        super().__init__()
        self._api_key = vault.get("OKX_API_KEY")
        self._api_secret = vault.get("OKX_SECRET_KEY")
        self._passphrase = vault.get("OKX_PASSPHRASE")
        self._client = httpx.AsyncClient(base_url=_BASE_URL)
        self._ct_val_cache: dict[str, Decimal] = {}

    async def _get_ct_val(self, inst_id: str) -> Decimal:
        """Return the contract value (ctVal) for a SWAP instrument, cached per session."""
        if inst_id in self._ct_val_cache:
            return self._ct_val_cache[inst_id]
        resp = await self._client.get(
            "/api/v5/public/instruments",
            params={"instType": "SWAP", "instId": inst_id},
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])
        ct_val = Decimal(data[0]["ctVal"]) if data else Decimal("1")
        self._ct_val_cache[inst_id] = ct_val
        return ct_val

    async def _amount_to_sz(self, inst_id: str, amount: Decimal) -> str:
        """Convert a base-currency amount to the integer number of contracts OKX expects."""
        ct_val = await self._get_ct_val(inst_id)
        contracts = amount / ct_val
        # Round to nearest integer (lot size is always 1 contract for linear SWAPs)
        return str(int(contracts.to_integral_value()))

    def _sign(self, method: str, path: str, body: str = "") -> dict:
        t = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        msg = t + method.upper() + path + body
        sig = base64.b64encode(
            hmac.new(self._api_secret.encode(), msg.encode(), hashlib.sha256).digest()
        ).decode()
        return {
            "OK-ACCESS-KEY": self._api_key,
            "OK-ACCESS-SIGN": sig,
            "OK-ACCESS-TIMESTAMP": t,
            "OK-ACCESS-PASSPHRASE": self._passphrase,
            "Content-Type": "application/json",
        }

    async def fetch_positions(self) -> list[Position]:
        path = "/api/v5/account/positions?instType=SWAP"
        headers = self._sign("GET", path)
        resp = await self._client.get(path, headers=headers)
        resp.raise_for_status()
        positions = []
        for item in resp.json()["data"]:
            pos_str = item.get("pos", "0")
            if not pos_str or pos_str == "0":
                continue
            pos_val = Decimal(pos_str)
            if pos_val == 0:
                continue
            pos_side = item.get("posSide", "net")
            if pos_side == "net":
                direction = "long" if pos_val > 0 else "short"
            else:
                direction = pos_side
            avg_px = item.get("avgPx", "0") or "0"
            mark_px = item.get("markPx", "0") or "0"
            positions.append(Position(
                ticker=_from_inst_id(item["instId"]),
                exchange_name=self.name,
                direction=direction,
                amount=abs(pos_val),
                avg_price=Decimal(avg_px),
                current_price=Decimal(mark_px),
            ))
        return positions

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        path = "/api/v5/account/balance"
        headers = self._sign("GET", path)
        resp = await self._client.get(path, headers=headers)
        resp.raise_for_status()
        account = resp.json()["data"][0]
        maintenance_margin = Decimal(account.get("imr", "0") or "0")
        current_margin = Decimal(account.get("totalEq", "0") or "0")
        return maintenance_margin, current_margin

    async def _send_order(self, body: dict) -> bool:
        path = "/api/v5/trade/order"
        body_str = _json.dumps(body)
        headers = self._sign("POST", path, body_str)
        resp = await self._client.post(path, headers=headers, content=body_str)
        if not resp.is_success:
            return False
        data = resp.json()
        return data.get("code") == "0"

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
        inst_id = _to_inst_id(ticker)
        snapshot = await self.fetch_positions()
        body: dict = {
            "instId": inst_id,
            "tdMode": "cross",
            "side": "buy" if direction == "long" else "sell",
            "ordType": order_type,
            "sz": await self._amount_to_sz(inst_id, amount),
        }
        if order_type == "limit":
            body["px"] = str(limit_price)
        ok = await self._send_order(body)
        if not ok:
            return False
        if order_type == "market":
            return await self._verify_position_changed(_from_inst_id(inst_id), snapshot)
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
        inst_id = _to_inst_id(ticker)
        flat_ticker = _from_inst_id(inst_id)
        snapshot = await self.fetch_positions()
        pos = self.state.positions.get(flat_ticker) or self.state.positions.get(ticker)
        direction = pos.direction if pos else "long"
        body: dict = {
            "instId": inst_id,
            "tdMode": "cross",
            "side": "sell" if direction == "long" else "buy",
            "ordType": order_type,
            "sz": await self._amount_to_sz(inst_id, amount),
            "reduceOnly": True,
        }
        if order_type == "limit":
            body["px"] = str(limit_price)
        ok = await self._send_order(body)
        if not ok:
            return False
        if order_type == "market":
            return await self._verify_position_changed(flat_ticker, snapshot)
        return True
