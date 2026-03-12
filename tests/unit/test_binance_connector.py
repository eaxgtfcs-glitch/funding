"""
Тесты для BinanceConnector (app/connectors/binance.py).

Все HTTP-запросы мокируются через AsyncMock на self.connector._client.get.
Реальных сетевых обращений нет.
"""
import hashlib
import hmac
import os
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Вспомогательная фабрика ответа httpx
# ---------------------------------------------------------------------------

def _make_response(json_data, status_code: int = 200) -> MagicMock:
    """Создаёт фиктивный объект ответа httpx с нужными атрибутами."""
    resp = MagicMock()
    resp.json.return_value = json_data
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()  # не бросает исключений при 200
    return resp


# ===========================================================================
# TestBinanceFetchPositions
# ===========================================================================

class TestBinanceFetchPositions:
    pytestmark = pytest.mark.asyncio

    def setup_method(self):
        with patch.dict(os.environ, {"BINANCE_API_KEY": "test_key", "BINANCE_API_SECRET": "test_secret"}):
            from app.connectors.binance import BinanceConnector
            self.connector = BinanceConnector()

    async def test_fetch_positions_returns_position_objects(self):
        payload = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "0.5",
                "entryPrice": "60000",
                "markPrice": "61000",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert len(positions) == 1
        pos = positions[0]
        assert pos.ticker == "BTCUSDT"

    async def test_fetch_positions_filters_zero_amount(self):
        payload = [
            {
                "symbol": "ETHUSDT",
                "positionAmt": "0",
                "entryPrice": "3000",
                "markPrice": "3100",
            },
            {
                "symbol": "BTCUSDT",
                "positionAmt": "1.0",
                "entryPrice": "60000",
                "markPrice": "61000",
            },
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        tickers = [p.ticker for p in positions]
        assert "ETHUSDT" not in tickers
        assert "BTCUSDT" in tickers

    async def test_fetch_positions_positive_amt_is_long(self):
        payload = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "2.5",
                "entryPrice": "60000",
                "markPrice": "61000",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].direction == "long"

    async def test_fetch_positions_negative_amt_is_short(self):
        payload = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "-1.5",
                "entryPrice": "60000",
                "markPrice": "61000",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].direction == "short"

    async def test_fetch_positions_amount_is_abs_value(self):
        payload = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "-3.0",
                "entryPrice": "60000",
                "markPrice": "61000",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].amount == Decimal("3.0")
        assert positions[0].amount >= 0

    async def test_fetch_positions_exchange_name_is_binance(self):
        payload = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "1.0",
                "entryPrice": "60000",
                "markPrice": "61000",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].exchange_name == "binance"

    async def test_fetch_positions_avg_price_from_entry_price(self):
        payload = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "1.0",
                "entryPrice": "59500.25",
                "markPrice": "61000",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].avg_price == Decimal("59500.25")

    async def test_fetch_positions_current_price_from_mark_price(self):
        payload = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "1.0",
                "entryPrice": "60000",
                "markPrice": "61234.56",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].current_price == Decimal("61234.56")

    async def test_fetch_positions_empty_response_returns_empty_list(self):
        self.connector._client.get = AsyncMock(return_value=_make_response([]))

        positions = await self.connector.fetch_positions()

        assert positions == []

    async def test_fetch_positions_multiple_nonzero_positions_returned(self):
        payload = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "1.0",
                "entryPrice": "60000",
                "markPrice": "61000",
            },
            {
                "symbol": "ETHUSDT",
                "positionAmt": "-5.0",
                "entryPrice": "3000",
                "markPrice": "3100",
            },
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert len(positions) == 2

    @pytest.mark.parametrize("pos_amt,expected_direction,expected_amount", [
        ("10.0", "long", Decimal("10.0")),
        ("-10.0", "short", Decimal("10.0")),
        ("0.001", "long", Decimal("0.001")),
        ("-0.001", "short", Decimal("0.001")),
    ])
    async def test_fetch_positions_direction_and_amount_parametrized(
            self, pos_amt: str, expected_direction: str, expected_amount: Decimal
    ):
        payload = [
            {
                "symbol": "XYZUSDT",
                "positionAmt": pos_amt,
                "entryPrice": "100",
                "markPrice": "101",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].direction == expected_direction
        assert positions[0].amount == expected_amount


# ===========================================================================
# TestBinanceFetchMargin
# ===========================================================================

class TestBinanceFetchMargin:
    pytestmark = pytest.mark.asyncio

    def setup_method(self):
        with patch.dict(os.environ, {"BINANCE_API_KEY": "test_key", "BINANCE_API_SECRET": "test_secret"}):
            from app.connectors.binance import BinanceConnector
            self.connector = BinanceConnector()

    async def test_fetch_margin_returns_tuple_of_decimals(self):
        payload = {
            "totalMaintMargin": "1500.75",
            "totalMarginBalance": "8000.00",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        result = await self.connector.fetch_margin()

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], Decimal)
        assert isinstance(result[1], Decimal)

    async def test_fetch_margin_maintenance_margin_from_total_maint_margin(self):
        payload = {
            "totalMaintMargin": "1234.56",
            "totalMarginBalance": "9999.99",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, _ = await self.connector.fetch_margin()

        assert maintenance_margin == Decimal("1234.56")

    async def test_fetch_margin_current_margin_from_total_margin_balance(self):
        payload = {
            "totalMaintMargin": "500",
            "totalMarginBalance": "7777.77",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        _, current_margin = await self.connector.fetch_margin()

        assert current_margin == Decimal("7777.77")

    @pytest.mark.parametrize("maint,balance", [
        ("0", "0"),
        ("0.01", "100000"),
        ("99999.99", "99999.99"),
    ])
    async def test_fetch_margin_parses_various_values(self, maint: str, balance: str):
        payload = {
            "totalMaintMargin": maint,
            "totalMarginBalance": balance,
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, current_margin = await self.connector.fetch_margin()

        assert maintenance_margin == Decimal(maint)
        assert current_margin == Decimal(balance)


# ===========================================================================
# TestBinanceSign
# ===========================================================================

class TestBinanceSign:

    def setup_method(self):
        with patch.dict(os.environ, {"BINANCE_API_KEY": "test_key", "BINANCE_API_SECRET": "test_secret"}):
            from app.connectors.binance import BinanceConnector
            self.connector = BinanceConnector()

    def test_sign_result_contains_timestamp(self):
        result = self.connector._sign({})

        assert "timestamp=" in result

    def test_sign_result_contains_recv_window_5000(self):
        result = self.connector._sign({})

        assert "recvWindow=5000" in result

    def test_sign_result_contains_signature(self):
        result = self.connector._sign({})

        assert "signature=" in result

    def test_sign_signature_is_valid_hmac_sha256(self):
        params: dict = {}
        result = self.connector._sign(params)

        # Разбираем строку: всё до &signature= — подписываемые данные
        assert "&signature=" in result
        query_without_sig, sig_value = result.rsplit("&signature=", 1)

        expected_sig = hmac.new(
            b"test_secret",
            query_without_sig.encode(),
            hashlib.sha256,
        ).hexdigest()

        assert sig_value == expected_sig

    def test_sign_with_extra_params_includes_them_in_query(self):
        result = self.connector._sign({"symbol": "BTCUSDT"})

        assert "symbol=BTCUSDT" in result

    def test_sign_signature_changes_with_different_secret(self):
        result_a = self.connector._sign({})

        with patch.dict(os.environ, {"BINANCE_API_KEY": "k2", "BINANCE_API_SECRET": "other_secret"}):
            from app.connectors.binance import BinanceConnector
            connector_b = BinanceConnector()

        result_b = connector_b._sign({})

        sig_a = result_a.rsplit("&signature=", 1)[1]
        sig_b = result_b.rsplit("&signature=", 1)[1]
        assert sig_a != sig_b
