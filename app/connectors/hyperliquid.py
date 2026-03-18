import os
import time
from decimal import Decimal

import httpx
import msgpack
from eth_account import Account
from eth_hash.auto import keccak

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position


class HyperliquidConnector(BaseExchangeConnector):
    name = "hyperliquid"

    def __init__(self) -> None:
        super().__init__()
        self._account_address = os.environ["HYPERLIQUID_ACCOUNT_ADDRESS"]
        self._secret_key = os.environ["HYPERLIQUID_SECRET_KEY"]
        self._client = httpx.AsyncClient(base_url="https://api.hyperliquid.xyz")
        self._asset_index: dict[str, int] = {}

    def _sign_action(self, action: dict, nonce: int) -> dict:
        nonce_bytes = nonce.to_bytes(8, "big")
        vault_flag = b"\x00"
        connection_id = keccak(msgpack.packb(action, use_bin_type=True) + nonce_bytes + vault_flag)

        structured_data = {
            "domain": {
                "chainId": 1337,
                "name": "Exchange",
                "verifyingContract": "0x0000000000000000000000000000000000000000",
                "version": "1",
            },
            "primaryType": "Agent",
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Agent": [
                    {"name": "source", "type": "string"},
                    {"name": "connectionId", "type": "bytes32"},
                ],
            },
            "message": {"source": "a", "connectionId": connection_id},
        }
        signed = Account.sign_typed_data(self._secret_key, structured_data)
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

    async def close_position(self, ticker: str, amount: Decimal) -> None:
        if not self._asset_index:
            resp = await self._client.post("/info", json={"type": "meta"})
            data = resp.json()
            self._asset_index = {asset["name"]: i for i, asset in enumerate(data["universe"])}

        pos = self.state.positions.get(ticker)
        is_buy = not (pos and pos.direction == "long")

        action = {
            "type": "order",
            "orders": [{
                "a": self._asset_index[ticker],
                "b": is_buy,
                "p": "0",
                "s": str(amount),
                "r": True,
                "t": {"trigger": {"isMarket": True, "triggerPx": "0"}},
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
        if data.get("status") != "ok":
            raise RuntimeError(f"Hyperliquid close_position error: {data}")
