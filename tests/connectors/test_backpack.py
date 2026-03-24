"""
Тесты для BackpackConnector (app/connectors/backpack.py).

Все HTTP-запросы мокируются через AsyncMock на self.connector._client.
Реальных сетевых обращений и реального крипто-ключа нет.
"""
import base64
import json
import os
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _generate_test_key_b64() -> str:
    """Генерирует случайный Ed25519 приватный ключ и возвращает base64-строку."""
    private_key = Ed25519PrivateKey.generate()
    raw_bytes = private_key.private_bytes_raw()
    return base64.b64encode(raw_bytes).decode()


def _make_response(json_data, status_code: int = 200) -> MagicMock:
    """Создаёт фиктивный объект ответа httpx."""
    resp = MagicMock()
    resp.json.return_value = json_data
    resp.status_code = status_code
    resp.is_success = (200 <= status_code < 300)
    resp.text = json.dumps(json_data)
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    return resp


_TEST_SECRET = _generate_test_key_b64()
_ENV = {"BACKPACK_API_KEY": "test_api_key", "BACKPACK_API_SECRET": _TEST_SECRET}


def _make_connector():
    with patch.dict(os.environ, _ENV):
        from app.connectors.backpack import BackpackConnector
        return BackpackConnector()


# ===========================================================================
# TestBackpackSign
# ===========================================================================

class TestBackpackSign:
    """Проверяет формат строки подписи метода _sign."""

    def setup_method(self):
        self.connector = _make_connector()

    def test_sign_returns_dict_with_required_headers(self):
        result = self.connector._sign("positionQuery", {})

        assert "X-API-Key" in result
        assert "X-Signature" in result
        assert "X-Timestamp" in result
        assert "X-Window" in result

    def test_sign_api_key_matches_env(self):
        result = self.connector._sign("positionQuery", {})

        assert result["X-API-Key"] == "test_api_key"

    def test_sign_window_is_5000(self):
        result = self.connector._sign("positionQuery", {})

        assert result["X-Window"] == "5000"

    def test_sign_timestamp_is_numeric_string(self):
        result = self.connector._sign("positionQuery", {})

        assert result["X-Timestamp"].isdigit()

    def test_sign_message_starts_with_instruction(self):
        """Строка подписи должна начинаться с instruction."""
        captured_messages = []

        original_sign_method = self.connector._sign.__func__

        private_key = Ed25519PrivateKey.from_private_bytes(
            base64.b64decode(_TEST_SECRET)
        )

        # Перехватываем вызов sign у приватного ключа
        with patch(
                "app.connectors.backpack.Ed25519PrivateKey.from_private_bytes",
                return_value=MagicMock(
                    sign=lambda msg: (captured_messages.append(msg), private_key.sign(msg))[1]
                ),
        ):
            self.connector._sign("orderExecute", {"symbol": "BTCUSDT"})

        assert len(captured_messages) == 1
        decoded = captured_messages[0].decode()
        assert decoded.startswith("instruction=orderExecute&")

    def test_sign_params_are_sorted_alphabetically(self):
        """Параметры в строке подписи должны идти в алфавитном порядке."""
        captured_messages = []
        private_key = Ed25519PrivateKey.from_private_bytes(base64.b64decode(_TEST_SECRET))

        with patch(
                "app.connectors.backpack.Ed25519PrivateKey.from_private_bytes",
                return_value=MagicMock(
                    sign=lambda msg: (captured_messages.append(msg), private_key.sign(msg))[1]
                ),
        ):
            self.connector._sign("orderExecute", {"symbol": "BTCUSDT", "orderType": "Market"})

        decoded = captured_messages[0].decode()
        # После instruction: orderType=Market должен идти раньше symbol=BTCUSDT
        idx_order_type = decoded.index("orderType=Market")
        idx_symbol = decoded.index("symbol=BTCUSDT")
        assert idx_order_type < idx_symbol

    def test_sign_timestamp_and_window_are_at_the_end(self):
        """timestamp и window должны быть последними частями строки подписи."""
        captured_messages = []
        private_key = Ed25519PrivateKey.from_private_bytes(base64.b64decode(_TEST_SECRET))

        with patch(
                "app.connectors.backpack.Ed25519PrivateKey.from_private_bytes",
                return_value=MagicMock(
                    sign=lambda msg: (captured_messages.append(msg), private_key.sign(msg))[1]
                ),
        ):
            self.connector._sign("positionQuery", {"foo": "bar"})

        decoded = captured_messages[0].decode()
        parts = decoded.split("&")
        # Последние два — timestamp=... и window=5000
        assert parts[-1] == "window=5000"
        assert parts[-2].startswith("timestamp=")

    def test_sign_message_full_structure(self):
        """Полная структура: instruction&sorted_params&timestamp=...&window=5000."""
        captured_messages = []
        private_key = Ed25519PrivateKey.from_private_bytes(base64.b64decode(_TEST_SECRET))

        with patch(
                "app.connectors.backpack.Ed25519PrivateKey.from_private_bytes",
                return_value=MagicMock(
                    sign=lambda msg: (captured_messages.append(msg), private_key.sign(msg))[1]
                ),
        ):
            self.connector._sign("collateralQuery", {})

        decoded = captured_messages[0].decode()
        # Без параметров структура: collateralQuery&timestamp=...&window=5000
        parts = decoded.split("&")
        assert parts[0] == "instruction=collateralQuery"
        assert parts[-1] == "window=5000"
        assert parts[-2].startswith("timestamp=")


# ===========================================================================
# TestBackpackFetchPositions
# ===========================================================================

class TestBackpackFetchPositions:
    pytestmark = pytest.mark.asyncio

    def setup_method(self):
        self.connector = _make_connector()

    async def test_fetch_positions_returns_position_objects(self):
        payload = [
            {
                "symbol": "BTC_USDC_PERP",
                "netQuantity": "0.5",
                "entryPrice": "60000",
                "markPrice": "61000",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert len(positions) == 1
        pos = positions[0]
        assert pos.ticker == "BTC_USDC_PERP"

    async def test_fetch_positions_empty_response_returns_empty_list(self):
        self.connector._client.get = AsyncMock(return_value=_make_response([]))

        positions = await self.connector.fetch_positions()

        assert positions == []

    async def test_fetch_positions_filters_zero_net_quantity(self):
        payload = [
            {
                "symbol": "ETH_USDC_PERP",
                "netQuantity": "0",
                "entryPrice": "3000",
                "markPrice": "3100",
            },
            {
                "symbol": "BTC_USDC_PERP",
                "netQuantity": "1.0",
                "entryPrice": "60000",
                "markPrice": "61000",
            },
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        tickers = [p.ticker for p in positions]
        assert "ETH_USDC_PERP" not in tickers
        assert "BTC_USDC_PERP" in tickers

    async def test_fetch_positions_positive_net_quantity_is_long(self):
        payload = [
            {
                "symbol": "BTC_USDC_PERP",
                "netQuantity": "2.5",
                "entryPrice": "60000",
                "markPrice": "61000",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].direction == "long"

    async def test_fetch_positions_negative_net_quantity_is_short(self):
        payload = [
            {
                "symbol": "BTC_USDC_PERP",
                "netQuantity": "-1.5",
                "entryPrice": "60000",
                "markPrice": "61000",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].direction == "short"

    async def test_fetch_positions_amount_is_abs_value_of_net_quantity(self):
        payload = [
            {
                "symbol": "BTC_USDC_PERP",
                "netQuantity": "-3.0",
                "entryPrice": "60000",
                "markPrice": "61000",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].amount == Decimal("3.0")
        assert positions[0].amount >= 0

    async def test_fetch_positions_avg_price_from_entry_price(self):
        payload = [
            {
                "symbol": "BTC_USDC_PERP",
                "netQuantity": "1.0",
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
                "symbol": "BTC_USDC_PERP",
                "netQuantity": "1.0",
                "entryPrice": "60000",
                "markPrice": "61234.56",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].current_price == Decimal("61234.56")

    async def test_fetch_positions_exchange_name_is_backpack(self):
        payload = [
            {
                "symbol": "BTC_USDC_PERP",
                "netQuantity": "1.0",
                "entryPrice": "60000",
                "markPrice": "61000",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].exchange_name == "backpack"

    async def test_fetch_positions_multiple_nonzero_positions_returned(self):
        payload = [
            {
                "symbol": "BTC_USDC_PERP",
                "netQuantity": "1.0",
                "entryPrice": "60000",
                "markPrice": "61000",
            },
            {
                "symbol": "ETH_USDC_PERP",
                "netQuantity": "-5.0",
                "entryPrice": "3000",
                "markPrice": "3100",
            },
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert len(positions) == 2

    @pytest.mark.parametrize("net_qty,expected_direction,expected_amount", [
        ("10.0", "long", Decimal("10.0")),
        ("-10.0", "short", Decimal("10.0")),
        ("0.001", "long", Decimal("0.001")),
        ("-0.001", "short", Decimal("0.001")),
    ])
    async def test_fetch_positions_direction_and_amount_parametrized(
            self, net_qty: str, expected_direction: str, expected_amount: Decimal
    ):
        payload = [
            {
                "symbol": "XYZUSDT",
                "netQuantity": net_qty,
                "entryPrice": "100",
                "markPrice": "101",
            }
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].direction == expected_direction
        assert positions[0].amount == expected_amount


# ===========================================================================
# TestBackpackFetchMargin
# ===========================================================================

class TestBackpackFetchMargin:
    pytestmark = pytest.mark.asyncio

    def setup_method(self):
        self.connector = _make_connector()

    async def test_fetch_margin_returns_tuple_of_two_decimals(self):
        payload = {
            "mmf": "0.05",
            "netExposureFutures": "10000",
            "netEquity": "8000.00",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        result = await self.connector.fetch_margin()

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], Decimal)
        assert isinstance(result[1], Decimal)

    async def test_fetch_margin_maintenance_margin_is_abs_mmf_times_exposure(self):
        payload = {
            "mmf": "0.05",
            "netExposureFutures": "10000",
            "netEquity": "8000.00",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, _ = await self.connector.fetch_margin()

        # abs(0.05 * 10000) = 500
        assert maintenance_margin == Decimal("500.00")

    async def test_fetch_margin_current_margin_from_net_equity(self):
        payload = {
            "mmf": "0.05",
            "netExposureFutures": "10000",
            "netEquity": "7777.77",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        _, current_margin = await self.connector.fetch_margin()

        assert current_margin == Decimal("7777.77")

    async def test_fetch_margin_zero_exposure_returns_zero_maintenance_margin(self):
        """При нулевой экспозиции (пустой аккаунт) maintenance_margin должен быть 0."""
        payload = {
            "mmf": "0.05",
            "netExposureFutures": "0",
            "netEquity": "5000",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, _ = await self.connector.fetch_margin()

        assert maintenance_margin == Decimal("0")

    async def test_fetch_margin_zero_mmf_returns_zero_maintenance_margin(self):
        """При нулевом mmf (пустой аккаунт) maintenance_margin должен быть 0."""
        payload = {
            "mmf": "0",
            "netExposureFutures": "50000",
            "netEquity": "5000",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, _ = await self.connector.fetch_margin()

        assert maintenance_margin == Decimal("0")

    async def test_fetch_margin_negative_exposure_gives_positive_maintenance_margin(self):
        """abs() гарантирует положительный maintenance_margin при отрицательной экспозиции."""
        payload = {
            "mmf": "0.05",
            "netExposureFutures": "-10000",
            "netEquity": "6000",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, _ = await self.connector.fetch_margin()

        assert maintenance_margin == Decimal("500.00")

    @pytest.mark.parametrize("mmf,exposure,equity,expected_maint", [
        ("0.1", "20000", "15000", Decimal("2000.0")),
        ("0", "0", "0", Decimal("0")),
        ("0.025", "8000", "3000", Decimal("200.0")),
    ])
    async def test_fetch_margin_parametrized(
            self, mmf: str, exposure: str, equity: str, expected_maint: Decimal
    ):
        payload = {"mmf": mmf, "netExposureFutures": exposure, "netEquity": equity}
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, current_margin = await self.connector.fetch_margin()

        assert maintenance_margin == expected_maint
        assert current_margin == Decimal(equity)


# ===========================================================================
# TestBackpackClosePosition
# ===========================================================================

class TestBackpackClosePosition:
    pytestmark = pytest.mark.asyncio

    def setup_method(self):
        self.connector = _make_connector()

    async def test_close_position_long_uses_ask_side(self):
        """При закрытии long-позиции side должен быть 'Ask'."""
        from app.connectors.model.position import Position

        self.connector.state.positions["BTC_USDC_PERP"] = Position(
            ticker="BTC_USDC_PERP",
            exchange_name="backpack",
            direction="long",
            amount=Decimal("1.0"),
            avg_price=Decimal("60000"),
            current_price=Decimal("61000"),
        )
        captured_bodies = []
        mock_resp = _make_response({"status": "Filled"})

        async def fake_post(url, headers, json):
            captured_bodies.append(json)
            return mock_resp

        self.connector._client.post = fake_post

        await self.connector.close_position("BTC_USDC_PERP", Decimal("1.0"))

        assert captured_bodies[0]["side"] == "Ask"

    async def test_close_position_short_uses_bid_side(self):
        """При закрытии short-позиции side должен быть 'Bid'."""
        from app.connectors.model.position import Position

        self.connector.state.positions["ETH_USDC_PERP"] = Position(
            ticker="ETH_USDC_PERP",
            exchange_name="backpack",
            direction="short",
            amount=Decimal("5.0"),
            avg_price=Decimal("3000"),
            current_price=Decimal("3100"),
        )
        captured_bodies = []
        mock_resp = _make_response({"status": "Filled"})

        async def fake_post(url, headers, json):
            captured_bodies.append(json)
            return mock_resp

        self.connector._client.post = fake_post

        await self.connector.close_position("ETH_USDC_PERP", Decimal("5.0"))

        assert captured_bodies[0]["side"] == "Bid"

    async def test_close_position_unknown_ticker_uses_bid_as_default(self):
        """Если позиция не найдена в state, side по умолчанию 'Bid'."""
        captured_bodies = []
        mock_resp = _make_response({"status": "Filled"})

        async def fake_post(url, headers, json):
            captured_bodies.append(json)
            return mock_resp

        self.connector._client.post = fake_post

        await self.connector.close_position("UNKNOWN_PERP", Decimal("1.0"))

        assert captured_bodies[0]["side"] == "Bid"

    async def test_close_position_reduce_only_is_bool_true(self):
        """reduceOnly должен быть булевым True, а не строкой 'true'."""
        from app.connectors.model.position import Position

        self.connector.state.positions["BTC_USDC_PERP"] = Position(
            ticker="BTC_USDC_PERP",
            exchange_name="backpack",
            direction="long",
            amount=Decimal("1.0"),
            avg_price=Decimal("60000"),
            current_price=Decimal("61000"),
        )
        captured_bodies = []
        mock_resp = _make_response({"status": "Filled"})

        async def fake_post(url, headers, json):
            captured_bodies.append(json)
            return mock_resp

        self.connector._client.post = fake_post

        await self.connector.close_position("BTC_USDC_PERP", Decimal("1.0"))

        assert captured_bodies[0]["reduceOnly"] is True
        assert captured_bodies[0]["reduceOnly"] is not "true"

    async def test_close_position_body_contains_correct_fields(self):
        """Тело запроса содержит все обязательные поля."""
        from app.connectors.model.position import Position

        self.connector.state.positions["BTC_USDC_PERP"] = Position(
            ticker="BTC_USDC_PERP",
            exchange_name="backpack",
            direction="long",
            amount=Decimal("2.5"),
            avg_price=Decimal("60000"),
            current_price=Decimal("61000"),
        )
        captured_bodies = []
        mock_resp = _make_response({"status": "Filled"})

        async def fake_post(url, headers, json):
            captured_bodies.append(json)
            return mock_resp

        self.connector._client.post = fake_post

        await self.connector.close_position("BTC_USDC_PERP", Decimal("2.5"))

        body = captured_bodies[0]
        assert body["orderType"] == "Market"
        assert body["quantity"] == "2.5"
        assert body["symbol"] == "BTC_USDC_PERP"

    async def test_close_position_http_error_raises_runtime_error(self):
        """HTTP-ошибка (non-2xx) должна выбрасывать RuntimeError."""
        mock_resp = _make_response({"message": "Unauthorized"}, status_code=401)

        async def fake_post(url, headers, json):
            return mock_resp

        self.connector._client.post = fake_post

        with pytest.raises(RuntimeError, match="HTTP 401"):
            await self.connector.close_position("BTC_USDC_PERP", Decimal("1.0"))

    async def test_close_position_response_with_error_field_raises_runtime_error(self):
        """Ответ 200 с полем 'error' должен выбрасывать RuntimeError."""
        mock_resp = _make_response({"error": "InsufficientFunds"}, status_code=200)

        async def fake_post(url, headers, json):
            return mock_resp

        self.connector._client.post = fake_post

        with pytest.raises(RuntimeError, match="InsufficientFunds"):
            await self.connector.close_position("BTC_USDC_PERP", Decimal("1.0"))

    async def test_close_position_http_500_raises_runtime_error(self):
        """HTTP 500 тоже должен выбрасывать RuntimeError."""
        mock_resp = _make_response({"message": "Internal Server Error"}, status_code=500)

        async def fake_post(url, headers, json):
            return mock_resp

        self.connector._client.post = fake_post

        with pytest.raises(RuntimeError, match="HTTP 500"):
            await self.connector.close_position("BTC_USDC_PERP", Decimal("1.0"))

    async def test_close_position_success_does_not_raise(self):
        """Успешный ответ не должен выбрасывать исключений."""
        mock_resp = _make_response({"orderId": "abc123", "status": "Filled"})

        async def fake_post(url, headers, json):
            return mock_resp

        self.connector._client.post = fake_post

        # Не должно бросить ничего
        await self.connector.close_position("BTC_USDC_PERP", Decimal("0.1"))
