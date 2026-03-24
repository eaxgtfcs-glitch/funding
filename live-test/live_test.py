"""
Live integration test for exchange connectors.

Usage (from project root):
    python scripts/live-test/live_test.py [exchange] [symbol] [amount]

    Args are optional — defaults are read from scripts/live-test/test-params.json.
    CLI args override test-params.json values.

Credentials are read from scripts/live-test/creds (KEY=VALUE, same format as .env_example).
Never reads or writes .env files.

Exit code 0 = all passed, 1 = failures found.
"""

import asyncio
import importlib
import json
import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

CREDS_PATH = ROOT / "live-test" / "creds"
PARAMS_PATH = ROOT / "live-test" / "test-params.json"

# connector name -> (module, class)
CONNECTOR_MAP: dict[str, tuple[str, str]] = {
    "binance": ("app.connectors.binance", "BinanceConnector"),
    "bybit": ("app.connectors.bybit", "BybitConnector"),
    "backpack": ("app.connectors.backpack", "BackpackConnector"),
    "aster": ("app.connectors.aster", "AsterConnector"),
    "hyperliquid": ("app.connectors.hyperliquid", "HyperliquidConnector"),
    "bingx": ("app.connectors.bingx", "BingXConnector"),
    "apex": ("app.connectors.apex", "ApexConnector"),
    "gate": ("app.connectors.gate", "GateConnector"),
    "lighter": ("app.connectors.lighter", "LighterConnector"),
    "phemex": ("app.connectors.phemex", "PhemexConnector"),
    "mexc": ("app.connectors.mexc", "MEXCConnector"),
    "bitget": ("app.connectors.bitget", "BitgetConnector"),
    "okx": ("app.connectors.okx", "OKXConnector"),
    "reya": ("app.connectors.reya", "ReyaConnector"),
}

# fields in test_keys.json -> env var names
CRED_MAP: dict[str, dict[str, str]] = {
    "binance": {"api_key": "BINANCE_API_KEY", "api_secret": "BINANCE_API_SECRET"},
    "bybit": {"api_key": "BYBIT_API_KEY", "api_secret": "BYBIT_API_SECRET"},
    "backpack": {"api_key": "BACKPACK_API_KEY", "api_secret": "BACKPACK_API_SECRET"},
    "aster": {"api_key": "ASTER_API_KEY", "api_secret": "ASTER_API_SECRET"},
    "hyperliquid": {"account_address": "HYPERLIQUID_ACCOUNT_ADDRESS", "secret_key": "HYPERLIQUID_SECRET_KEY"},
    "bingx": {"api_key": "BINGX_API_KEY", "api_secret": "BINGX_SECRET_KEY"},
    "apex": {"api_key": "APEX_API_KEY", "api_secret": "APEX_API_SECRET",
             "passphrase": "APEX_PASSPHRASE", "zk_seeds": "APEX_ZK_SEEDS"},
    "gate": {"api_key": "GATE_API_KEY", "api_secret": "GATE_SECRET_KEY"},
    "lighter": {"account_index": "LIGHTER_ACCOUNT_INDEX", "api_private_key": "LIGHTER_API_PRIVATE_KEY",
                "api_key_index": "LIGHTER_API_KEY_INDEX"},
    "phemex": {"api_key": "PHEMEX_API_KEY", "api_secret": "PHEMEX_SECRET_KEY"},
    "mexc": {"api_key": "MEXC_API_KEY", "api_secret": "MEXC_SECRET_KEY"},
    "bitget": {"api_key": "BITGET_API_KEY", "api_secret": "BITGET_SECRET_KEY",
               "passphrase": "BITGET_PASSPHRASE"},
    "okx": {"api_key": "OKX_API_KEY", "api_secret": "OKX_SECRET_KEY",
            "passphrase": "OKX_PASSPHRASE"},
    "reya": {"wallet_address": "REYA_WALLET_ADDRESS", "private_key": "REYA_PRIVATE_KEY"},
}

VERIFY_DELAY = 4  # seconds to wait before verifying an order executed

# Per-exchange overrides for exchanges with slower position API settlement
VERIFY_DELAY_OVERRIDES: dict[str, int] = {
    "reya": 12,
}


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------

class Step:
    def __init__(self, name: str, status: str, detail: str = "", error: str = ""):
        self.name = name
        self.status = status
        self.detail = detail
        self.error = error

    def __str__(self) -> str:
        if self.status == "PASS":
            suffix = f"  ({self.detail})" if self.detail else ""
            return f"  [PASS] {self.name}{suffix}"
        if self.status == "FAIL":
            return f"  [FAIL] {self.name}  ->  {self.error}"
        suffix = f"  ({self.detail})" if self.detail else ""
        return f"  [SKIP] {self.name}{suffix}"


class TestReport:
    def __init__(self, exchange: str, symbol: str, amount: Decimal):
        self.exchange = exchange
        self.symbol = symbol
        self.amount = amount
        self.steps: list[Step] = []

    def ok(self, name: str, detail: str = "") -> None:
        s = Step(name, "PASS", detail=detail)
        self.steps.append(s)
        print(str(s))

    def fail(self, name: str, error: str) -> None:
        s = Step(name, "FAIL", error=error)
        self.steps.append(s)
        print(str(s))

    def skip(self, name: str, reason: str = "") -> None:
        s = Step(name, "SKIP", detail=reason)
        self.steps.append(s)
        print(str(s))

    def info(self, label: str, value) -> None:
        print(f"         {label}: {value}")

    @property
    def bugs(self) -> list[Step]:
        return [s for s in self.steps if s.status == "FAIL"]

    def print_summary(self) -> None:
        passed = sum(1 for s in self.steps if s.status == "PASS")
        skipped = sum(1 for s in self.steps if s.status == "SKIP")
        failed = len(self.bugs)
        total = len(self.steps)

        print()
        print("=" * 52)
        print(f"  {self.exchange.upper()}  |  {self.symbol}  |  amount={self.amount}")
        print(f"  PASS {passed}   FAIL {failed}   SKIP {skipped}   TOTAL {total}")
        if self.bugs:
            print()
            print(f"  BUGS ({failed}):")
            for b in self.bugs:
                print(f"    [{b.name}]  {b.error}")
        else:
            print()
            print("  All checks passed.")
        print("=" * 52)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sync_state(connector, positions: list) -> None:
    """Push fetched positions into connector.state so close_position can read direction."""
    connector.state.positions = {p.ticker: p for p in positions}


async def _fetch_and_sync(connector) -> list:
    positions = await connector.fetch_positions()
    _sync_state(connector, positions)
    return positions


def _exc(e: Exception) -> str:
    return f"{type(e).__name__}: {e}"


# ---------------------------------------------------------------------------
# Test sequence
# ---------------------------------------------------------------------------

async def _poll_until_open(connector, symbol: str, initial_delay: int, poll_interval: int = 3,
                           max_wait: int = 60) -> list:
    """Wait initial_delay, then poll until symbol appears in positions or max_wait is reached."""
    await asyncio.sleep(initial_delay)
    positions = await _fetch_and_sync(connector)
    elapsed = initial_delay
    while elapsed < max_wait and not any(p.ticker == symbol for p in positions):
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
        positions = await _fetch_and_sync(connector)
    return positions


async def _poll_until_closed(connector, symbol: str, initial_delay: int, poll_interval: int = 3,
                             max_wait: int = 60) -> list:
    """Wait initial_delay, then poll until symbol disappears from positions or max_wait is reached."""
    await asyncio.sleep(initial_delay)
    positions = await _fetch_and_sync(connector)
    elapsed = initial_delay
    while elapsed < max_wait:
        pos = next((p for p in positions if p.ticker == symbol), None)
        if pos is None or pos.amount == Decimal(0):
            break
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval
        positions = await _fetch_and_sync(connector)
    return positions


async def run_tests(connector, symbol: str, amount: Decimal) -> TestReport:
    report = TestReport(connector.name, symbol, amount)
    verify_delay = VERIFY_DELAY_OVERRIDES.get(connector.name, VERIFY_DELAY)

    # 1. fetch_margin
    print("\n--- fetch_margin ---")
    try:
        maint, curr = await connector.fetch_margin()
        report.ok("fetch_margin", f"maintenance={maint}  current={curr}")
    except Exception as e:
        report.fail("fetch_margin", _exc(e))

    # 2. fetch_positions — initial snapshot
    print("\n--- fetch_positions (initial) ---")
    try:
        init_positions = await _fetch_and_sync(connector)
        report.ok("fetch_positions_initial", f"{len(init_positions)} open position(s)")
        for p in init_positions:
            report.info(p.ticker, f"{p.direction}  amt={p.amount}  entry={p.avg_price}")
    except Exception as e:
        report.fail("fetch_positions_initial", _exc(e))

    # 3. place_order LONG
    print(f"\n--- place_order LONG {amount} {symbol} ---")
    long_placed = False
    try:
        ok = await connector.place_order(symbol, "long", amount)
        if ok:
            report.ok("place_order_long", "accepted")
            long_placed = True
        else:
            report.fail("place_order_long", "returned False")
    except Exception as e:
        report.fail("place_order_long", _exc(e))

    # 4. verify long opened
    if long_placed:
        print(f"\n--- verify long opened (wait up to {verify_delay}s+) ---")
        try:
            positions = await _poll_until_open(connector, symbol, initial_delay=verify_delay)
            pos = next((p for p in positions if p.ticker == symbol), None)
            if pos and pos.direction == "long":
                report.ok("verify_long_open", f"amt={pos.amount}  entry={pos.avg_price}  mark={pos.current_price}")
            elif pos:
                report.fail("verify_long_open", f"direction={pos.direction!r} (expected 'long')")
            else:
                report.fail("verify_long_open", f"{symbol!r} not in positions {[p.ticker for p in positions]}")
        except Exception as e:
            report.fail("verify_long_open", _exc(e))
    else:
        report.skip("verify_long_open", "place_order_long failed")

    # 5. close_position LONG
    if long_placed:
        print(f"\n--- close_position LONG {amount} {symbol} ---")
        try:
            ok = await connector.close_position(symbol, amount)
            if ok:
                report.ok("close_position_long", "accepted")
            else:
                report.fail("close_position_long", "returned False")
        except Exception as e:
            report.fail("close_position_long", _exc(e))

        print(f"\n--- verify long closed (wait up to {verify_delay}s+) ---")
        try:
            positions = await _poll_until_closed(connector, symbol, initial_delay=verify_delay)
            pos = next((p for p in positions if p.ticker == symbol), None)
            if pos is None or pos.amount == Decimal(0):
                report.ok("verify_long_closed")
            else:
                report.fail("verify_long_closed", f"still open: dir={pos.direction} amt={pos.amount}")
        except Exception as e:
            report.fail("verify_long_closed", _exc(e))
    else:
        report.skip("close_position_long", "place_order_long failed")
        report.skip("verify_long_closed", "place_order_long failed")

    # 6. place_order SHORT
    print(f"\n--- place_order SHORT {amount} {symbol} ---")
    short_placed = False
    try:
        ok = await connector.place_order(symbol, "short", amount)
        if ok:
            report.ok("place_order_short", "accepted")
            short_placed = True
        else:
            report.fail("place_order_short", "returned False")
    except Exception as e:
        report.fail("place_order_short", _exc(e))

    # 7. verify short opened
    if short_placed:
        print(f"\n--- verify short opened (wait up to {verify_delay}s+) ---")
        try:
            positions = await _poll_until_open(connector, symbol, initial_delay=verify_delay)
            pos = next((p for p in positions if p.ticker == symbol), None)
            if pos and pos.direction == "short":
                report.ok("verify_short_open", f"amt={pos.amount}  entry={pos.avg_price}  mark={pos.current_price}")
            elif pos:
                report.fail("verify_short_open", f"direction={pos.direction!r} (expected 'short')")
            else:
                report.fail("verify_short_open", f"{symbol!r} not in positions {[p.ticker for p in positions]}")
        except Exception as e:
            report.fail("verify_short_open", _exc(e))
    else:
        report.skip("verify_short_open", "place_order_short failed")

    # 8. close_position SHORT
    if short_placed:
        print(f"\n--- close_position SHORT {amount} {symbol} ---")
        try:
            ok = await connector.close_position(symbol, amount)
            if ok:
                report.ok("close_position_short", "accepted")
            else:
                report.fail("close_position_short", "returned False")
        except Exception as e:
            report.fail("close_position_short", _exc(e))

        print(f"\n--- verify short closed (wait up to {verify_delay}s+) ---")
        try:
            positions = await _poll_until_closed(connector, symbol, initial_delay=verify_delay)
            pos = next((p for p in positions if p.ticker == symbol), None)
            if pos is None or pos.amount == Decimal(0):
                report.ok("verify_short_closed")
            else:
                report.fail("verify_short_closed", f"still open: dir={pos.direction} amt={pos.amount}")
        except Exception as e:
            report.fail("verify_short_closed", _exc(e))
    else:
        report.skip("close_position_short", "place_order_short failed")
        report.skip("verify_short_closed", "place_order_short failed")

    # 9. fetch_positions — final
    print("\n--- fetch_positions (final) ---")
    try:
        final_positions = await _fetch_and_sync(connector)
        report.ok("fetch_positions_final", f"{len(final_positions)} open position(s)")
        for p in final_positions:
            report.info(p.ticker, f"{p.direction}  amt={p.amount}")
    except Exception as e:
        report.fail("fetch_positions_final", _exc(e))

    return report


# ---------------------------------------------------------------------------
# Credentials loader
# ---------------------------------------------------------------------------

def load_credentials(exchange: str) -> None:
    """Read scripts/live-test/creds (KEY=VALUE format, same as .env_example) and set env vars."""
    if not CREDS_PATH.exists():
        print(f"ERROR: {CREDS_PATH} not found.")
        print(f"       Copy scripts/live-test/creds.example to scripts/live-test/creds and fill in your keys.")
        sys.exit(1)

    with open(CREDS_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            value = value.split("#")[0].strip()  # strip inline comments
            os.environ[key.strip()] = value

    if exchange not in CRED_MAP:
        print(f"ERROR: no credential mapping defined for '{exchange}'.")
        sys.exit(1)

    mapping = CRED_MAP[exchange]
    missing = [env_var for env_var in mapping.values() if not os.environ.get(env_var)]
    if missing:
        print(f"ERROR: missing credentials for '{exchange}' in {CREDS_PATH}:")
        for var in missing:
            print(f"       {var}=")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def make_connector(exchange: str):
    if exchange not in CONNECTOR_MAP:
        available = ", ".join(sorted(CONNECTOR_MAP.keys()))
        print(f"ERROR: unknown exchange '{exchange}'. Available: {available}")
        sys.exit(1)
    module_path, class_name = CONNECTOR_MAP[exchange]
    try:
        module = importlib.import_module(module_path)
    except ImportError as e:
        print(f"ERROR: cannot import {module_path}: {e}")
        sys.exit(1)
    return getattr(module, class_name)()


def load_params() -> list[tuple[str, str, Decimal]]:
    """Read exchange/symbol/amount from test-params.json (dict or list), overridable via CLI args."""
    args = sys.argv[1:]

    # CLI args override everything: <exchange> <symbol> <amount>
    if len(args) >= 3:
        exchange = args[0].lower()
        symbol = args[1]
        amount = Decimal(args[2])
        return [(exchange, symbol, amount)]

    raw: list[dict] = []
    if PARAMS_PATH.exists():
        with open(PARAMS_PATH, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            raw = data
        else:
            raw = [data]

    # Apply partial CLI overrides to every entry
    result = []
    for params in raw:
        exchange = (args[0] if len(args) > 0 else params.get("exchange", "")).lower()
        symbol = args[1] if len(args) > 1 else params.get("symbol", "")
        amount = Decimal(args[2] if len(args) > 2 else params.get("amount", "0"))

        missing = [n for n, v in [("exchange", exchange), ("symbol", symbol)] if not v] + \
                  (["amount"] if amount == 0 else [])
        if missing:
            print(f"ERROR: missing params: {', '.join(missing)}")
            print(f"       Set them in {PARAMS_PATH} or pass as CLI args: <exchange> <symbol> <amount>")
            sys.exit(1)

        result.append((exchange, symbol, amount))

    if not result:
        print(f"ERROR: no test entries found in {PARAMS_PATH}")
        sys.exit(1)

    return result


async def main() -> None:
    entries = load_params()

    all_reports: list[TestReport] = []
    for exchange, symbol, amount in entries:
        load_credentials(exchange)

        print("=" * 52)
        print(f"  LIVE TEST: {exchange.upper()}")
        print(f"  Symbol:  {symbol}  |  Amount: {amount}")
        print("=" * 52)

        connector = make_connector(exchange)
        report = await run_tests(connector, symbol, amount)
        report.print_summary()
        all_reports.append(report)

    any_bugs = any(r.bugs for r in all_reports)
    sys.exit(1 if any_bugs else 0)


if __name__ == "__main__":
    asyncio.run(main())
