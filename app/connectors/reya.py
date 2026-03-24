import time
from decimal import Decimal
from typing import Literal

import httpx
from eth_abi import encode
from eth_account import Account

from app.connectors.base import BaseExchangeConnector
from app.connectors.model.position import Position
from app.helper.key_vault import vault

_BASE_URL = "https://api.reya.xyz/v2"

# Mainnet constants
_CHAIN_ID = 1729
_DEX_ID = 2
_POOL_ACCOUNT_ID = 2
_ORDERS_GATEWAY = "0xfc8c96be87da63cecddbf54abfa7b13ee8044739"

# Market order type (IOC)
_ORDER_TYPE_MARKET = 3


def _normalize_symbol(symbol: str) -> str:
    """Convert user-facing symbol like 'HYPE-USDT' or 'ETHUSDT' to Reya's BASERUSDPERP format."""
    # Remove common separators and quote currencies
    s = symbol.upper()
    for sep in ["-", "_", "/"]:
        s = s.replace(sep, "")
    for quote in ["USDT", "USDC", "USD", "RUSD"]:
        if s.endswith(quote):
            s = s[: -len(quote)]
            break
    if not s.endswith("PERP"):
        s = s + "RUSDPERP"
    return s


class ReyaConnector(BaseExchangeConnector):
    name = "reya"

    def __init__(self) -> None:
        super().__init__()
        self._wallet_address: str = vault.get("REYA_WALLET_ADDRESS")
        self._private_key: str = vault.get("REYA_PRIVATE_KEY")
        self._client = httpx.AsyncClient(base_url=_BASE_URL)
        self._market_definitions: dict[str, dict] = {}
        # map: reya symbol -> user-facing symbol and vice-versa
        self._reya_to_user: dict[str, str] = {}
        self._user_to_reya: dict[str, str] = {}
        self._account_id: int | None = None

    async def _load_market_definitions(self) -> None:
        if self._market_definitions:
            return
        resp = await self._client.get("/marketDefinitions")
        resp.raise_for_status()
        for m in resp.json():
            reya_sym = m["symbol"]
            self._market_definitions[reya_sym] = m

    async def _ensure_account_id(self) -> int:
        if self._account_id is not None:
            return self._account_id
        resp = await self._client.get(f"/wallet/{self._wallet_address}/accountBalances")
        resp.raise_for_status()
        balances = resp.json()
        if balances:
            self._account_id = int(balances[0]["accountId"])
        else:
            raise RuntimeError("No account found for wallet")
        return self._account_id

    def _reya_symbol(self, ticker: str) -> str:
        """Return the Reya API symbol for a user-facing ticker, and cache the mapping."""
        # If it's already in our market definitions as-is, return it
        if ticker in self._market_definitions:
            self._reya_to_user.setdefault(ticker, ticker)
            return ticker
        normalized = _normalize_symbol(ticker)
        if normalized in self._market_definitions:
            # Cache the bidirectional mapping
            self._reya_to_user[normalized] = ticker
            self._user_to_reya[ticker] = normalized
            return normalized
        # fallback: return normalized even if not found (API will reject)
        return normalized

    # ------------------------------------------------------------------ #
    # Signing helpers                                                     #
    # ------------------------------------------------------------------ #

    def _encode_inputs(self, is_buy: bool, qty: Decimal, limit_px: Decimal) -> str:
        factor = 10 ** 18
        signed_qty = int(qty * factor) if is_buy else -int(qty * factor)
        limit_int = int(limit_px * factor)
        encoded = encode(["int256", "uint256"], [signed_qty, limit_int])
        hex_str = encoded.hex()
        return hex_str if hex_str.startswith("0x") else f"0x{hex_str}"

    def _create_nonce(self, account_id: int, market_id: int) -> int:
        timestamp_ms = int(time.time_ns() / 1_000_000)
        return (account_id << 98) | (timestamp_ms << 32) | market_id

    def _sign_order(
            self,
            account_id: int,
            market_id: int,
            order_type: int,
            inputs: str,
            deadline: int,
            nonce: int,
    ) -> str:
        signer = Account.from_key(self._private_key).address
        domain = {
            "name": "Reya",
            "version": "1",
            "verifyingContract": _ORDERS_GATEWAY,
        }
        types = {
            "ConditionalOrder": [
                {"name": "verifyingChainId", "type": "uint256"},
                {"name": "deadline", "type": "uint256"},
                {"name": "order", "type": "ConditionalOrderDetails"},
            ],
            "ConditionalOrderDetails": [
                {"name": "accountId", "type": "uint128"},
                {"name": "marketId", "type": "uint128"},
                {"name": "exchangeId", "type": "uint128"},
                {"name": "counterpartyAccountIds", "type": "uint128[]"},
                {"name": "orderType", "type": "uint8"},
                {"name": "inputs", "type": "bytes"},
                {"name": "signer", "type": "address"},
                {"name": "nonce", "type": "uint256"},
            ],
        }
        message = {
            "verifyingChainId": _CHAIN_ID,
            "deadline": deadline,
            "order": {
                "accountId": account_id,
                "marketId": market_id,
                "exchangeId": _DEX_ID,
                "counterpartyAccountIds": [_POOL_ACCOUNT_ID],
                "orderType": order_type,
                "inputs": inputs,
                "signer": signer,
                "nonce": nonce,
            },
        }
        signed = Account.sign_typed_data(self._private_key, domain, types, message)
        sig = signed.signature.hex()
        return sig if sig.startswith("0x") else f"0x{sig}"

    # ------------------------------------------------------------------ #
    # Public interface                                                    #
    # ------------------------------------------------------------------ #

    async def fetch_positions(self) -> list[Position]:
        await self._load_market_definitions()
        resp = await self._client.get(f"/wallet/{self._wallet_address}/positions")
        resp.raise_for_status()
        positions = []
        for item in resp.json():
            qty = Decimal(str(item.get("qty", "0")))
            if qty == 0:
                continue
            direction = "long" if item.get("side") == "B" else "short"
            reya_sym = item["symbol"]
            # Use user-facing ticker if we have a mapping, otherwise use the raw Reya symbol
            user_ticker = self._reya_to_user.get(reya_sym, reya_sym)
            avg_price = Decimal(str(item.get("avgEntryPrice", "0") or "0"))
            market = self._market_definitions.get(reya_sym, {})
            liq_param = Decimal(str(market.get("liquidationMarginParameter", "0") or "0"))
            current_price = avg_price
            positions.append(Position(
                ticker=user_ticker,
                exchange_name=self.name,
                direction=direction,
                amount=qty,
                avg_price=avg_price,
                current_price=current_price,
            ))
        return positions

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        await self._load_market_definitions()
        resp = await self._client.get(f"/wallet/{self._wallet_address}/accountBalances")
        resp.raise_for_status()
        current_margin = sum(
            Decimal(str(a.get("realBalance", "0") or "0"))
            for a in resp.json()
        )
        maintenance_margin = Decimal(0)
        for ticker, pos in self.state.positions.items():
            # ticker may be user-facing; resolve to reya symbol for market definitions lookup
            reya_sym = self._user_to_reya.get(ticker, ticker)
            market = self._market_definitions.get(reya_sym, {})
            liq_param = Decimal(str(market.get("liquidationMarginParameter", "0") or "0"))
            maintenance_margin += pos.amount * pos.current_price * liq_param
        return maintenance_margin, current_margin

    async def place_order(
            self,
            ticker: str,
            direction: Literal["long", "short"],
            amount: Decimal,
            order_type: Literal["market", "limit"] = "market",
            limit_price: Decimal | None = None,
    ) -> bool:
        await self._load_market_definitions()
        account_id = await self._ensure_account_id()
        reya_sym = self._reya_symbol(ticker)

        market = self._market_definitions.get(reya_sym)
        if market is None:
            raise ValueError(f"Unknown symbol '{ticker}' (resolved to '{reya_sym}'). "
                             f"Available: {list(self._market_definitions.keys())}")
        market_id = int(market["marketId"])

        is_buy = direction == "long"

        # For market orders we use a very low/high limit price (like IOC with wide spread)
        # Reya market orders use orderType=3 (MARKET_ORDER) with limit_px as price limit.
        # Use a generous price limit: 2x for buys, 0.01 for sells.
        # Get tick size and qty step size for rounding
        tick_size = Decimal(str(market.get("tickSize", "0.001") or "0.001"))
        qty_step = Decimal(str(market.get("qtyStepSize", "0.1") or "0.1"))
        # Round qty to step size
        from decimal import ROUND_DOWN as _ROUND_DOWN
        amount = amount.quantize(qty_step, rounding=_ROUND_DOWN)

        if limit_price is None:
            # Fetch current price from market summary
            try:
                price_resp = await self._client.get(f"/prices/{reya_sym}")
                price_resp.raise_for_status()
                price_data = price_resp.json()
                mark_price = Decimal(str(
                    price_data.get("oraclePrice")
                    or price_data.get("poolPrice")
                    or price_data.get("markPrice")
                    or price_data.get("price")
                    or "1000000"
                ))
            except Exception:
                mark_price = Decimal("1000000")
            # Round limit price to tick size to avoid signature mismatch
            if is_buy:
                from decimal import ROUND_UP
                limit_px = (mark_price * Decimal("2")).quantize(tick_size, rounding=ROUND_UP)
            else:
                from decimal import ROUND_DOWN
                limit_px = (mark_price * Decimal("0.01")).quantize(tick_size, rounding=ROUND_DOWN)
        else:
            # Round provided limit price to tick size
            from decimal import ROUND_HALF_UP
            limit_px = limit_price.quantize(tick_size, rounding=ROUND_HALF_UP)

        nonce = self._create_nonce(account_id, market_id)
        deadline = int(time.time()) + 30  # 30 second window for IOC

        inputs = self._encode_inputs(is_buy, amount, limit_px)
        signature = self._sign_order(
            account_id=account_id,
            market_id=market_id,
            order_type=_ORDER_TYPE_MARKET,
            inputs=inputs,
            deadline=deadline,
            nonce=nonce,
        )

        signer_wallet = Account.from_key(self._private_key).address
        payload = {
            "accountId": account_id,
            "symbol": reya_sym,
            "exchangeId": _DEX_ID,
            "isBuy": is_buy,
            "limitPx": str(limit_px),
            "qty": str(amount),
            "orderType": "LIMIT",
            "timeInForce": "IOC",
            "reduceOnly": False,
            "expiresAfter": deadline,
            "signature": signature,
            "nonce": str(nonce),
            "signerWallet": signer_wallet,
        }

        resp = await self._client.post("/createOrder", json=payload)
        if not resp.is_success:
            raise RuntimeError(f"createOrder HTTP {resp.status_code}: {resp.text}")
        result = resp.json()
        status = result.get("status", "")
        # FILLED or PARTIAL_FILLED = success; REJECTED / CANCELLED / EXPIRED = failure
        return status in ("FILLED", "PARTIAL_FILLED", "OPEN", "ACCEPTED", "NEW")

    async def close_position(
            self,
            ticker: str,
            amount: Decimal,
            order_type: Literal["market", "limit"] = "market",
            limit_price: Decimal | None = None,
    ) -> bool:
        import asyncio as _asyncio
        # Determine current direction by looking at open positions.
        # Retry a few times to handle cases where the position API lags behind order fills.
        await self._load_market_definitions()
        reya_sym = self._reya_symbol(ticker)
        user_ticker = self._reya_to_user.get(reya_sym, reya_sym)

        pos = None
        for attempt in range(4):
            positions = await self.fetch_positions()
            pos = next((p for p in positions if p.ticker in (reya_sym, user_ticker, ticker)), None)
            if pos is not None:
                break
            if attempt < 3:
                await _asyncio.sleep(2)

        if pos is None:
            # No open position found after retries — treat as already closed
            return True

        # To close a long we sell; to close a short we buy
        close_direction: Literal["long", "short"] = "short" if pos.direction == "long" else "long"
        return await self.place_order(ticker, close_direction, amount, order_type, limit_price)
