import math
import time
from decimal import Decimal, ROUND_DOWN
from typing import Literal

import httpx
import msgpack
from eth_account import Account
from eth_hash.auto import keccak

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position
from app.helper.key_vault import vault


class HyperliquidConnector(BaseExchangeConnector):
    name = "hyperliquid"

    def __init__(self) -> None:
        super().__init__()
        self._account_address = vault.get("HYPERLIQUID_ACCOUNT_ADDRESS")
        self._secret_key = vault.get("HYPERLIQUID_SECRET_KEY")
        self._client = httpx.AsyncClient(base_url="https://api.hyperliquid.xyz")
        self._asset_index: dict[str, int] = {}

    def _sign_action(self, action: dict, nonce: int) -> dict:
        nonce_bytes = nonce.to_bytes(8, "big")
        vault_flag = b"\x00"
        connection_id = keccak(msgpack.packb(action, use_bin_type=True) + nonce_bytes + vault_flag)

        domain_data = {
            "chainId": 1337,
            "name": "Exchange",
            "verifyingContract": "0x0000000000000000000000000000000000000000",
            "version": "1",
        }
        message_types = {
            "Agent": [
                {"name": "source", "type": "string"},
                {"name": "connectionId", "type": "bytes32"},
            ],
        }
        message_data = {"source": "a", "connectionId": connection_id}
        signed = Account.sign_typed_data(self._secret_key, domain_data, message_types, message_data)
        return {"r": hex(signed.r), "s": hex(signed.s), "v": signed.v}

    async def _fetch_state(self) -> dict:
        resp = await self._client.post("/info", json={
            "type": "clearinghouseState",
            "user": self._account_address,
        })
        resp.raise_for_status()
        return resp.json()

    async def _fetch_spot_usdc(self) -> Decimal:
        resp = await self._client.post("/info", json={
            "type": "spotClearinghouseState",
            "user": self._account_address,
        })
        resp.raise_for_status()
        for balance in resp.json().get("balances", []):
            if balance.get("coin") == "USDC" or balance.get("token") == 0:
                return Decimal(balance["total"])
        return Decimal(0)

    async def fetch_positions(self) -> list[Position]:
        data = await self._fetch_state()
        positions = []
        for entry in data["assetPositions"]:
            pos = entry["position"]
            szi = Decimal(pos["szi"])
            if szi == 0:
                continue
            direction = "long" if szi > 0 else "short"
            amount = abs(szi)
            ticker = pos["coin"]
            avg_price = Decimal(pos["entryPx"])
            current_price = Decimal(pos["positionValue"]) / amount
            positions.append(Position(
                ticker=ticker,
                exchange_name=self.name,
                direction=direction,
                amount=amount,
                avg_price=avg_price,
                current_price=current_price,
            ))
        return positions

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        data = await self._fetch_state()
        maintenance_margin = Decimal(data["crossMaintenanceMarginUsed"])
        current_margin = await self._fetch_spot_usdc()
        return maintenance_margin, current_margin

    def _price_to_wire(self, price: Decimal) -> str:
        magnitude = int(math.floor(math.log10(float(price)))) + 1
        decimals = max(0, 5 - magnitude)
        if decimals == 0:
            return str(int(price.quantize(Decimal("1"), rounding=ROUND_DOWN)))
        return str(price.quantize(Decimal("0." + "0" * decimals), rounding=ROUND_DOWN))

    async def _fetch_mid_price(self, ticker: str) -> Decimal:
        resp = await self._client.post("/info", json={"type": "allMids"})
        resp.raise_for_status()
        return Decimal(resp.json()[ticker])

    async def _ensure_asset_index(self) -> None:
        if not self._asset_index:
            resp = await self._client.post("/info", json={"type": "meta"})
            data = resp.json()
            self._asset_index = {asset["name"]: i for i, asset in enumerate(data["universe"])}

    async def _send_hl_order(
            self,
            ticker: str,
            is_buy: bool,
            amount: Decimal,
            order_type: str,
            limit_price: Decimal | None,
            reduce_only: bool,
    ) -> bool:
        await self._ensure_asset_index()
        if order_type == "market":
            mid = await self._fetch_mid_price(ticker)
            slippage = Decimal("1.05") if is_buy else Decimal("0.95")
            price = self._price_to_wire(mid * slippage)
            order_spec = {"limit": {"tif": "Ioc"}}
        else:
            order_spec = {"limit": {"tif": "Gtc"}}
            price = str(limit_price)
        action = {
            "type": "order",
            "orders": [{
                "a": self._asset_index[ticker],
                "b": is_buy,
                "p": price,
                "s": str(amount),
                "r": reduce_only,
                "t": order_spec,
            }],
            "grouping": "na",
        }
        nonce = int(time.time() * 1000)
        signature = self._sign_action(action, nonce)
        resp = await self._client.post("/exchange", json={
            "action": action,
            "nonce": nonce,
            "signature": signature,
        })
        resp.raise_for_status()
        data = resp.json()
        if data.get("status") == "err":
            raise RuntimeError(f"Hyperliquid order error: {data.get('response')}")
        statuses = data.get("response", {}).get("data", {}).get("statuses", [])
        if statuses and "error" in statuses[0]:
            raise RuntimeError(f"Hyperliquid order rejected: {statuses[0]['error']}")
        return data.get("status") == "ok"

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
        is_buy = direction == "long"
        ok = await self._send_hl_order(ticker, is_buy, amount, order_type, limit_price, False)
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
        is_buy = not (pos and pos.direction == "long")
        return await self._send_hl_order(ticker, is_buy, amount, order_type, limit_price, True)
