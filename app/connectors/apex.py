import asyncio
from decimal import Decimal
from typing import Literal

import httpx

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position
from app.helper.key_vault import vault

_BASE_URL = "https://omni.apex.exchange"


class ApexConnector(BaseExchangeConnector):
    name = "apex"

    def __init__(self) -> None:
        super().__init__()
        self._api_key = vault.get("APEX_API_KEY")
        self._api_secret = vault.get("APEX_API_SECRET")
        self._passphrase = vault.get("APEX_PASSPHRASE")
        self._zk_seeds = vault.get("APEX_ZK_SEEDS")
        self._client = httpx.AsyncClient(base_url=_BASE_URL)
        self._sdk_client = None  # lazy-initialised HttpPrivateSign
        self._sdk_ready = False

    def _get_sdk_client(self):
        """Return a fully initialised HttpPrivateSign client (cached)."""
        if self._sdk_client is not None:
            return self._sdk_client
        from apexomni.http_private_sign import HttpPrivateSign
        from apexomni.constants import NETWORKID_OMNI_MAIN_ARB
        client = HttpPrivateSign(
            endpoint=_BASE_URL,
            network_id=NETWORKID_OMNI_MAIN_ARB,
            api_key_credentials={
                "key": self._api_key,
                "secret": self._api_secret,
                "passphrase": self._passphrase,
            },
            zk_seeds=self._zk_seeds,
        )
        self._sdk_client = client
        return client

    def _ensure_sdk_ready(self) -> None:
        """Load configs_v3 and account_v3 into the SDK client (sync, call once)."""
        if self._sdk_ready:
            return
        client = self._get_sdk_client()
        client.configs_v3()
        client.get_account_v3()
        # Build bidirectional symbol maps and step-size map
        self._symbol_map: dict[str, str] = {}  # display → internal
        self._display_map: dict[str, str] = {}  # internal → display
        self._step_map: dict[str, Decimal] = {}  # internal → stepSize
        for section in ("perpetualContract", "prelaunchContract", "predictionContract"):
            for c in (client.configV3.get("contractConfig", {}).get(section) or []):
                display = c.get("symbolDisplayName") or c.get("symbol")
                internal = c.get("symbol")
                if display and internal:
                    self._symbol_map[display] = internal
                    self._display_map[internal] = display
                    step = c.get("stepSize")
                    if step:
                        self._step_map[internal] = Decimal(str(step))
        self._sdk_ready = True

    def _internal_symbol(self, ticker: str) -> str:
        """Translate display name (e.g. APEXUSDT) to internal name (e.g. APEX-USDT)."""
        return getattr(self, "_symbol_map", {}).get(ticker, ticker)

    def _display_symbol(self, ticker: str) -> str:
        """Translate internal name (e.g. APEX-USDT) to display name (e.g. APEXUSDT)."""
        return getattr(self, "_display_map", {}).get(ticker, ticker)

    def _quantize_size(self, internal: str, amount: Decimal) -> Decimal:
        """Round amount down to the instrument's stepSize precision."""
        step = getattr(self, "_step_map", {}).get(internal)
        if step and step > 0:
            return (amount // step) * step
        return amount

    async def fetch_positions(self) -> list[Position]:
        await asyncio.to_thread(self._ensure_sdk_ready)
        resp = await asyncio.to_thread(
            lambda: self._get_sdk_client()._get(
                endpoint="/api/v3/account",
                params={},
            )
        )
        positions = []
        for item in (resp.get("data", {}).get("positions") or []):
            size = Decimal(str(item.get("size", "0")))
            if size == 0:
                continue
            side_val = item.get("side") or item.get("positionSide", "LONG")
            direction = "long" if side_val == "LONG" else "short"
            current_price = Decimal(str(item.get("markPrice") or item.get("indexPrice") or "0"))
            # Normalize the symbol to display name (e.g. APEX-USDT → APEXUSDT)
            ticker = self._display_symbol(item["symbol"])
            positions.append(Position(
                ticker=ticker,
                exchange_name=self.name,
                direction=direction,
                amount=abs(size),
                avg_price=Decimal(str(item.get("entryPrice", "0"))),
                current_price=current_price,
            ))
        return positions

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        await asyncio.to_thread(self._ensure_sdk_ready)
        resp = await asyncio.to_thread(
            lambda: self._get_sdk_client()._get(
                endpoint="/api/v3/account-balance",
                params={},
            )
        )
        data = resp.get("data") or {}
        current_margin = Decimal(str(data.get("totalEquityValue") or "0"))
        maintenance_margin = Decimal(str(data.get("maintenanceMargin") or "0"))
        return maintenance_margin, current_margin

    async def _get_worst_price(self, ticker: str, side: str, amount: Decimal) -> str:
        """Fetch the worst acceptable price for a market order."""
        internal = self._internal_symbol(ticker)
        resp = await asyncio.to_thread(
            lambda: self._get_sdk_client()._get(
                endpoint="/api/v3/get-worst-price",
                params={"symbol": internal, "side": side, "size": str(amount)},
            )
        )
        data = resp.get("data") or {}
        # API returns 'worstPrice'; fall back to ask/bid
        price = data.get("worstPrice") or data.get("price") or data.get("askOnePrice") or data.get("bidOnePrice")
        if not price:
            raise ValueError(f"get-worst-price returned no price: {resp}")
        return str(price)

    async def _send_apex_order(
            self,
            ticker: str,
            side: str,
            amount: Decimal,
            order_type: str,
            limit_price: Decimal | None,
            reduce_only: bool,
    ) -> bool:
        await asyncio.to_thread(self._ensure_sdk_ready)

        internal = self._internal_symbol(ticker)
        amount = self._quantize_size(internal, amount)

        if order_type == "market":
            price = await self._get_worst_price(ticker, side, amount)
            apex_type = "MARKET"
        else:
            price = str(limit_price)
            apex_type = "LIMIT"
        result = await asyncio.to_thread(
            lambda: self._get_sdk_client().create_order_v3(
                symbol=internal,
                side=side,
                type=apex_type,
                size=str(amount),
                price=price,
                reduceOnly=reduce_only,
                timeInForce="IMMEDIATE_OR_CANCEL" if order_type == "market" else "GOOD_TIL_CANCEL",
            )
        )
        # Success: response contains a 'data' dict (order object).
        # Failure: response has a non-zero 'code' and 'msg' field.
        if "data" in result and result["data"]:
            return True
        return result.get("code") == "0" or result.get("code") == 0

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
        ok = await self._send_apex_order(ticker, side, amount, order_type, limit_price, False)
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
        direction = pos.direction if pos else "long"
        side = "BUY" if direction == "short" else "SELL"
        ok = await self._send_apex_order(ticker, side, amount, order_type, limit_price, True)
        if not ok:
            return False
        if order_type == "market":
            return await self._verify_position_changed(ticker, snapshot)
        return True
