import sys
from decimal import Decimal
from typing import Literal

import httpx

if sys.platform == "win32":
    import lighter.signer_client as _sc

    _sc.free = lambda ptr: None  # Go manages its own memory; ucrtbase.free() on Go heap crashes Windows

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position
from app.helper.key_vault import vault

_BASE_URL = "https://mainnet.zklighter.elliot.ai"


class LighterConnector(BaseExchangeConnector):
    name = "lighter"

    def __init__(self) -> None:
        super().__init__()
        self._account_index = int(vault.get("LIGHTER_ACCOUNT_INDEX"))
        self._api_private_key = vault.get("LIGHTER_API_PRIVATE_KEY")
        self._api_key_index = int(vault.get("LIGHTER_API_KEY_INDEX"))
        self._client = httpx.AsyncClient(base_url=_BASE_URL)
        # Кэш maintenance_margin_fraction по symbol (из orderBookDetails)
        self._maint_fracs: dict[str, int] = {}
        # Кэш market details: lighter_symbol -> {market_id, size_decimals, price_decimals}
        self._market_details: dict[str, dict] = {}

    @staticmethod
    def _to_lighter_symbol(ticker: str) -> str:
        """Strip _USDT suffix: 'HYPE_USDT' -> 'HYPE'."""
        if ticker.endswith("_USDT"):
            return ticker[:-5]
        return ticker

    @staticmethod
    def _to_user_symbol(lighter_symbol: str) -> str:
        """Add _USDT suffix: 'HYPE' -> 'HYPE_USDT'."""
        if "_" not in lighter_symbol:
            return lighter_symbol + "_USDT"
        return lighter_symbol

    async def _fetch_account(self) -> dict:
        """Публичный эндпоинт — авторизация не нужна."""
        resp = await self._client.get(
            "/api/v1/account",
            params={"by": "index", "value": self._account_index},
        )
        resp.raise_for_status()
        return resp.json()["accounts"][0]

    async def _ensure_market_details(self, lighter_symbols: list[str]) -> None:
        """Load market_id, size_decimals, price_decimals for missing symbols."""
        missing = [s for s in lighter_symbols if s not in self._market_details]
        if not missing:
            return
        resp = await self._client.get("/api/v1/orderBookDetails")
        resp.raise_for_status()
        for entry in resp.json().get("order_book_details", []):
            sym = entry["symbol"]
            self._market_details[sym] = {
                "market_id": int(entry["market_id"]),
                "size_decimals": int(entry.get("size_decimals", 2)),
                "price_decimals": int(entry.get("price_decimals", 4)),
            }
            self._maint_fracs[sym] = int(entry.get("maintenance_margin_fraction", 120))

    async def _ensure_maint_fracs(self, symbols: list[str]) -> None:
        """Загружает maintenance_margin_fraction для недостающих символов."""
        await self._ensure_market_details(symbols)

    async def fetch_positions(self) -> list[Position]:
        account = await self._fetch_account()
        positions = []
        for item in account.get("positions", []):
            pos_str = item.get("position", "0")
            amount = Decimal(pos_str)
            if amount == 0:
                continue
            sign = item.get("sign", 1)
            direction = "long" if sign == 1 else "short"
            avg_price = Decimal(str(item.get("avg_entry_price", "0") or "0"))
            pos_value = Decimal(str(item.get("position_value", "0") or "0"))
            current_price = pos_value / amount if amount else Decimal(0)
            # Lighter returns symbols like "HYPE"; normalize to "HYPE_USDT" to
            # match the user-facing symbol format expected by the test framework.
            ticker = self._to_user_symbol(item["symbol"])
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
        account = await self._fetch_account()
        current_margin = Decimal(str(account.get("available_balance", "0") or "0"))

        positions = account.get("positions", [])
        symbols = [p["symbol"] for p in positions if Decimal(str(p.get("position", "0"))) != 0]
        await self._ensure_maint_fracs(symbols)

        maintenance_margin = Decimal(0)
        for item in positions:
            amount = Decimal(str(item.get("position", "0")))
            if amount == 0:
                continue
            pos_value = Decimal(str(item.get("position_value", "0") or "0"))
            frac = self._maint_fracs.get(item["symbol"], 120)
            maintenance_margin += pos_value * Decimal(frac) / Decimal(10000)

        return maintenance_margin, current_margin

    def _get_lighter_client(self):
        try:
            import lighter  # type: ignore[import]
        except ImportError as e:
            raise RuntimeError(
                "lighter-sdk не установлен или не поддерживается на этой ОС "
                "(требует Linux/macOS). pip install lighter-sdk"
            ) from e
        return lighter.SignerClient(
            url=_BASE_URL,
            api_private_keys={self._api_key_index: self._api_private_key},
            account_index=self._account_index,
        )

    async def _get_best_price_int(self, client, market_id: int, is_ask: bool) -> int:
        """Fetch current best bid/ask price as a scaled integer (decimal dot stripped)."""
        ob = await client.order_api.order_book_orders(market_id, 1)
        # is_ask=True (selling) → use best bid; is_ask=False (buying) → use best ask
        price_str = ob.bids[0].price if is_ask else ob.asks[0].price
        return int(price_str.replace(".", ""))

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

        lighter_symbol = self._to_lighter_symbol(ticker)
        await self._ensure_market_details([lighter_symbol])
        mkt = self._market_details[lighter_symbol]
        market_id = mkt["market_id"]
        size_decimals = mkt["size_decimals"]
        price_decimals = mkt["price_decimals"]

        snapshot = await self.fetch_positions()
        client = self._get_lighter_client()

        # is_ask=True means sell/short; is_ask=False means buy/long
        is_ask = direction == "short"
        # Scale base_amount to integer (e.g. 0.1 with size_decimals=2 -> 10)
        base_amount_int = int(amount * Decimal(10 ** size_decimals))

        if order_type == "market":
            price_int = await self._get_best_price_int(client, market_id, is_ask)
            _, resp, err = await client.create_market_order(
                market_index=market_id,
                client_order_index=0,
                base_amount=base_amount_int,
                avg_execution_price=price_int,
                is_ask=is_ask,
                reduce_only=False,
            )
            if err:
                raise RuntimeError(f"create_market_order failed: {err}")
            return await self._verify_position_changed(ticker, snapshot)
        else:
            # Limit order: scale limit_price
            price_int = int(limit_price * Decimal(10 ** price_decimals))
            tif = 1  # GTC
            _, resp, err = await client.create_order(
                market_index=market_id,
                client_order_index=0,
                base_amount=base_amount_int,
                price=price_int,
                is_ask=is_ask,
                order_type=client.ORDER_TYPE_LIMIT,
                time_in_force=tif,
                reduce_only=False,
            )
            if err:
                raise RuntimeError(f"create_order failed: {err}")
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

        lighter_symbol = self._to_lighter_symbol(ticker)
        await self._ensure_market_details([lighter_symbol])
        mkt = self._market_details[lighter_symbol]
        market_id = mkt["market_id"]
        size_decimals = mkt["size_decimals"]
        price_decimals = mkt["price_decimals"]

        snapshot = await self.fetch_positions()
        client = self._get_lighter_client()

        pos = self.state.positions.get(ticker)
        is_long = pos.direction == "long" if pos else True
        # To close a long → sell → is_ask=True; to close a short → buy → is_ask=False
        is_ask = is_long
        base_amount_int = int(amount * Decimal(10 ** size_decimals))

        if order_type == "market":
            price_int = await self._get_best_price_int(client, market_id, is_ask)
            _, resp, err = await client.create_market_order(
                market_index=market_id,
                client_order_index=0,
                base_amount=base_amount_int,
                avg_execution_price=price_int,
                is_ask=is_ask,
                reduce_only=True,
            )
            if err:
                raise RuntimeError(f"create_market_order failed: {err}")
            return await self._verify_position_changed(ticker, snapshot)
        else:
            price_int = int(limit_price * Decimal(10 ** price_decimals))
            tif = 1  # GTC
            _, resp, err = await client.create_order(
                market_index=market_id,
                client_order_index=0,
                base_amount=base_amount_int,
                price=price_int,
                is_ask=is_ask,
                order_type=client.ORDER_TYPE_LIMIT,
                time_in_force=tif,
                reduce_only=True,
            )
            if err:
                raise RuntimeError(f"create_order failed: {err}")
            return True
