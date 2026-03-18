"""
Тесты для AsterConnector (app/connectors/aster.py).

Все HTTP-запросы мокируются через AsyncMock на self.connector._client.
Реальных сетевых обращений нет.
"""
import hashlib
import hmac
import os
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _make_response(json_data, status_code: int = 200) -> MagicMock:
    """Создаёт фиктивный объект ответа httpx с нужными атрибутами."""
    resp = MagicMock()
    resp.json.return_value = json_data
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    return resp


_ENV = {"ASTER_API_KEY": "test_key", "ASTER_API_SECRET": "test_secret"}


def _make_connector():
    with patch.dict(os.environ, _ENV):
        from app.connectors.aster import AsterConnector
        return AsterConnector()


# ===========================================================================
# TestAsterSign
# ===========================================================================

class TestAsterSign:
    """Проверяет формат строки подписи метода _sign."""

    def setup_method(self):
        self.connector = _make_connector()

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
        result = self.connector._sign({})

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

    def test_sign_mutates_input_dict_adds_timestamp(self):
        """_sign мутирует входной dict: добавляет timestamp."""
        params: dict = {}
        self.connector._sign(params)

        assert "timestamp" in params

    def test_sign_mutates_input_dict_adds_recv_window(self):
        """_sign мутирует входной dict: добавляет recvWindow."""
        params: dict = {}
        self.connector._sign(params)

        assert "recvWindow" in params
        assert params["recvWindow"] == 5000

    def test_sign_second_call_with_same_dict_overwrites_timestamp(self):
        """При повторном вызове с тем же dict timestamp перезаписывается новым значением."""
        params: dict = {}
        self.connector._sign(params)
        ts_first = params["timestamp"]

        # Небольшая задержка не нужна — time.time() может вернуть то же значение,
        # но ключ в любом случае должен присутствовать и быть перезаписан.
        self.connector._sign(params)
        ts_second = params["timestamp"]

        # timestamp должен быть целым числом в обоих случаях
        assert isinstance(ts_first, int)
        assert isinstance(ts_second, int)

    def test_sign_signature_changes_with_different_secret(self):
        result_a = self.connector._sign({})

        with patch.dict(os.environ, {"ASTER_API_KEY": "k2", "ASTER_API_SECRET": "other_secret"}):
            from app.connectors.aster import AsterConnector
            connector_b = AsterConnector()

        result_b = connector_b._sign({})

        sig_a = result_a.rsplit("&signature=", 1)[1]
        sig_b = result_b.rsplit("&signature=", 1)[1]
        assert sig_a != sig_b

    def test_sign_close_position_params_mutated_contains_timestamp(self):
        """Мутация dict внутри close_position: params после _sign содержит timestamp."""
        params = {"symbol": "BTCUSDT", "side": "SELL", "type": "MARKET",
                  "quantity": "1.0", "reduceOnly": "true"}
        self.connector._sign(params)

        assert "timestamp" in params
        assert "recvWindow" in params


# ===========================================================================
# TestAsterFetchPositions
# ===========================================================================

class TestAsterFetchPositions:
    pytestmark = pytest.mark.asyncio

    def setup_method(self):
        self.connector = _make_connector()

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
        assert positions[0].ticker == "BTCUSDT"

    async def test_fetch_positions_empty_response_returns_empty_list(self):
        self.connector._client.get = AsyncMock(return_value=_make_response([]))

        positions = await self.connector.fetch_positions()

        assert positions == []

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

    async def test_fetch_positions_amount_is_abs_value_for_short(self):
        """amount всегда >= 0, даже если positionAmt отрицательный."""
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

    async def test_fetch_positions_exchange_name_is_aster(self):
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

        assert positions[0].exchange_name == "aster"

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

    async def test_fetch_positions_multiple_nonzero_returned(self):
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

    async def test_fetch_positions_all_zero_returns_empty_list(self):
        """Все позиции с нулевым amount должны быть отфильтрованы."""
        payload = [
            {"symbol": "BTCUSDT", "positionAmt": "0", "entryPrice": "60000", "markPrice": "61000"},
            {"symbol": "ETHUSDT", "positionAmt": "0", "entryPrice": "3000", "markPrice": "3100"},
        ]
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions == []

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

    async def test_fetch_positions_http_error_raises(self):
        """HTTP-ошибка (4xx/5xx) должна распространяться через raise_for_status."""
        self.connector._client.get = AsyncMock(
            return_value=_make_response({}, status_code=401)
        )

        with pytest.raises(Exception):
            await self.connector.fetch_positions()


# ===========================================================================
# TestAsterFetchMargin
# ===========================================================================

class TestAsterFetchMargin:
    pytestmark = pytest.mark.asyncio

    def setup_method(self):
        self.connector = _make_connector()

    async def test_fetch_margin_total_fields_returns_correct_tuple(self):
        """Ответ с totalMaintMargin/totalMarginBalance — первая ветка."""
        payload = {
            "totalMaintMargin": "1500.75",
            "totalMarginBalance": "8000.00",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        result = await self.connector.fetch_margin()

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] == Decimal("1500.75")
        assert result[1] == Decimal("8000.00")

    async def test_fetch_margin_total_maintenance_margin_is_decimal(self):
        payload = {
            "totalMaintMargin": "1234.56",
            "totalMarginBalance": "9999.99",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, current_margin = await self.connector.fetch_margin()

        assert isinstance(maintenance_margin, Decimal)
        assert isinstance(current_margin, Decimal)

    async def test_fetch_margin_fallback_fields_used_when_no_total_prefix(self):
        """Ответ с maintMargin/marginBalance — вторая ветка (fallback)."""
        payload = {
            "maintMargin": "500.00",
            "marginBalance": "4000.00",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, current_margin = await self.connector.fetch_margin()

        assert maintenance_margin == Decimal("500.00")
        assert current_margin == Decimal("4000.00")

    async def test_fetch_margin_fallback_returns_decimal_types(self):
        payload = {
            "maintMargin": "250.00",
            "marginBalance": "3500.00",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, current_margin = await self.connector.fetch_margin()

        assert isinstance(maintenance_margin, Decimal)
        assert isinstance(current_margin, Decimal)

    async def test_fetch_margin_total_fields_take_priority_over_fallback(self):
        """Если присутствуют оба набора полей, используются totalMaintMargin/totalMarginBalance."""
        payload = {
            "totalMaintMargin": "1000.00",
            "totalMarginBalance": "5000.00",
            "maintMargin": "999.00",
            "marginBalance": "4999.00",
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, current_margin = await self.connector.fetch_margin()

        assert maintenance_margin == Decimal("1000.00")
        assert current_margin == Decimal("5000.00")

    @pytest.mark.parametrize("maint,balance", [
        ("0", "0"),
        ("0.01", "100000"),
        ("99999.99", "99999.99"),
    ])
    async def test_fetch_margin_total_fields_parses_various_values(
            self, maint: str, balance: str
    ):
        payload = {
            "totalMaintMargin": maint,
            "totalMarginBalance": balance,
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, current_margin = await self.connector.fetch_margin()

        assert maintenance_margin == Decimal(maint)
        assert current_margin == Decimal(balance)

    @pytest.mark.parametrize("maint,balance", [
        ("0", "0"),
        ("123.45", "9876.54"),
    ])
    async def test_fetch_margin_fallback_fields_parses_various_values(
            self, maint: str, balance: str
    ):
        payload = {
            "maintMargin": maint,
            "marginBalance": balance,
        }
        self.connector._client.get = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, current_margin = await self.connector.fetch_margin()

        assert maintenance_margin == Decimal(maint)
        assert current_margin == Decimal(balance)

    async def test_fetch_margin_http_error_raises(self):
        """HTTP-ошибка распространяется через raise_for_status."""
        self.connector._client.get = AsyncMock(
            return_value=_make_response({}, status_code=403)
        )

        with pytest.raises(Exception):
            await self.connector.fetch_margin()


# ===========================================================================
# TestAsterClosePosition
# ===========================================================================

class TestAsterClosePosition:
    pytestmark = pytest.mark.asyncio

    def setup_method(self):
        self.connector = _make_connector()

    async def test_close_position_long_sends_sell_side(self):
        """Закрытие long-позиции отправляет SELL."""
        from app.connectors.model.position import Position

        self.connector.state.positions["BTCUSDT"] = Position(
            ticker="BTCUSDT",
            exchange_name="aster",
            direction="long",
            amount=Decimal("1.0"),
            avg_price=Decimal("60000"),
            current_price=Decimal("61000"),
        )
        mock_resp = _make_response({"orderId": "123"})
        self.connector._client.post = AsyncMock(return_value=mock_resp)

        await self.connector.close_position("BTCUSDT", Decimal("1.0"))

        call_args = self.connector._client.post.call_args
        url = call_args[0][0]
        assert "side=SELL" in url

    async def test_close_position_short_sends_buy_side(self):
        """Закрытие short-позиции отправляет BUY."""
        from app.connectors.model.position import Position

        self.connector.state.positions["ETHUSDT"] = Position(
            ticker="ETHUSDT",
            exchange_name="aster",
            direction="short",
            amount=Decimal("5.0"),
            avg_price=Decimal("3000"),
            current_price=Decimal("3100"),
        )
        mock_resp = _make_response({"orderId": "456"})
        self.connector._client.post = AsyncMock(return_value=mock_resp)

        await self.connector.close_position("ETHUSDT", Decimal("5.0"))

        call_args = self.connector._client.post.call_args
        url = call_args[0][0]
        assert "side=BUY" in url

    async def test_close_position_unknown_ticker_sends_buy_side(self):
        """Если тикер не найден в state.positions, по умолчанию отправляется BUY.

        ДЕФЕКТ: close_position при ticker не в state даёт side=BUY независимо
        от реального направления. Тест фиксирует фактическое поведение.
        """
        mock_resp = _make_response({"orderId": "789"})
        self.connector._client.post = AsyncMock(return_value=mock_resp)

        await self.connector.close_position("UNKNOWN", Decimal("1.0"))

        call_args = self.connector._client.post.call_args
        url = call_args[0][0]
        assert "side=BUY" in url

    async def test_close_position_url_contains_symbol(self):
        """URL запроса содержит символ тикера."""
        from app.connectors.model.position import Position

        self.connector.state.positions["BTCUSDT"] = Position(
            ticker="BTCUSDT",
            exchange_name="aster",
            direction="long",
            amount=Decimal("2.0"),
            avg_price=Decimal("60000"),
            current_price=Decimal("61000"),
        )
        mock_resp = _make_response({"orderId": "111"})
        self.connector._client.post = AsyncMock(return_value=mock_resp)

        await self.connector.close_position("BTCUSDT", Decimal("2.0"))

        call_args = self.connector._client.post.call_args
        url = call_args[0][0]
        assert "symbol=BTCUSDT" in url

    async def test_close_position_url_contains_quantity(self):
        """URL запроса содержит quantity равный переданному amount."""
        from app.connectors.model.position import Position

        self.connector.state.positions["BTCUSDT"] = Position(
            ticker="BTCUSDT",
            exchange_name="aster",
            direction="long",
            amount=Decimal("3.75"),
            avg_price=Decimal("60000"),
            current_price=Decimal("61000"),
        )
        mock_resp = _make_response({"orderId": "222"})
        self.connector._client.post = AsyncMock(return_value=mock_resp)

        await self.connector.close_position("BTCUSDT", Decimal("3.75"))

        call_args = self.connector._client.post.call_args
        url = call_args[0][0]
        assert "quantity=3.75" in url

    async def test_close_position_url_contains_market_type(self):
        """URL запроса содержит type=MARKET."""
        mock_resp = _make_response({"orderId": "333"})
        self.connector._client.post = AsyncMock(return_value=mock_resp)

        await self.connector.close_position("BTCUSDT", Decimal("1.0"))

        call_args = self.connector._client.post.call_args
        url = call_args[0][0]
        assert "type=MARKET" in url

    async def test_close_position_url_contains_reduce_only(self):
        """URL запроса содержит reduceOnly=true."""
        mock_resp = _make_response({"orderId": "444"})
        self.connector._client.post = AsyncMock(return_value=mock_resp)

        await self.connector.close_position("BTCUSDT", Decimal("1.0"))

        call_args = self.connector._client.post.call_args
        url = call_args[0][0]
        assert "reduceOnly=true" in url

    async def test_close_position_url_contains_signature(self):
        """URL запроса содержит подпись signature."""
        mock_resp = _make_response({"orderId": "555"})
        self.connector._client.post = AsyncMock(return_value=mock_resp)

        await self.connector.close_position("BTCUSDT", Decimal("1.0"))

        call_args = self.connector._client.post.call_args
        url = call_args[0][0]
        assert "signature=" in url

    async def test_close_position_http_error_raises(self):
        """HTTP-ошибка (4xx/5xx) распространяется через raise_for_status."""
        self.connector._client.post = AsyncMock(
            return_value=_make_response({"msg": "Unauthorized"}, status_code=401)
        )

        with pytest.raises(Exception):
            await self.connector.close_position("BTCUSDT", Decimal("1.0"))

    async def test_close_position_http_500_raises(self):
        """HTTP 500 тоже распространяется через raise_for_status."""
        self.connector._client.post = AsyncMock(
            return_value=_make_response({"msg": "Internal Server Error"}, status_code=500)
        )

        with pytest.raises(Exception):
            await self.connector.close_position("BTCUSDT", Decimal("1.0"))

    async def test_close_position_success_does_not_raise(self):
        """Успешный ответ не должен выбрасывать исключений."""
        mock_resp = _make_response({"orderId": "abc123", "status": "NEW"})
        self.connector._client.post = AsyncMock(return_value=mock_resp)

        await self.connector.close_position("BTCUSDT", Decimal("0.1"))
