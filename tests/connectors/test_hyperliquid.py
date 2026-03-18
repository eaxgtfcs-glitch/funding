"""
Тесты для HyperliquidConnector (app/connectors/hyperliquid.py).

Все HTTP-запросы мокируются через AsyncMock на self.connector._client.
eth_account и eth_hash мокируются через sys.modules (версия 0.2.x несовместима
с Python 3.12 — удалён collections.Mapping).
Реальных сетевых обращений нет.
"""
import os
import sys
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Патч eth_account / eth_hash на уровне модуля — до любого импорта коннектора
# ---------------------------------------------------------------------------

def _install_eth_mocks():
    """Регистрирует фиктивные модули eth_account и eth_hash в sys.modules."""
    if "eth_account" not in sys.modules:
        mock_eth_account = MagicMock()
        mock_eth_account.Account = MagicMock()
        sys.modules["eth_account"] = mock_eth_account

    if "eth_hash" not in sys.modules:
        sys.modules["eth_hash"] = MagicMock()
    if "eth_hash.auto" not in sys.modules:
        mock_keccak_mod = MagicMock()
        mock_keccak_mod.keccak = MagicMock(return_value=b"\x00" * 32)
        sys.modules["eth_hash.auto"] = mock_keccak_mod

    if "msgpack" not in sys.modules:
        mock_msgpack = MagicMock()
        mock_msgpack.packb = MagicMock(return_value=b"\x00")
        sys.modules["msgpack"] = mock_msgpack


_install_eth_mocks()


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


# Валидный приватный ключ Ethereum для тестов (64 hex-символа после 0x).
_TEST_SECRET = "0x" + "a" * 64
_ENV = {
    "HYPERLIQUID_ACCOUNT_ADDRESS": "0xTestAddress",
    "HYPERLIQUID_SECRET_KEY": _TEST_SECRET,
}


def _make_connector():
    with patch.dict(os.environ, _ENV):
        from app.connectors.hyperliquid import HyperliquidConnector
        return HyperliquidConnector()


def _make_state_payload(asset_positions=None, cross_margin_summary=None):
    """Собирает стандартный ответ /info clearinghouseState."""
    return {
        "assetPositions": asset_positions if asset_positions is not None else [],
        "crossMarginSummary": cross_margin_summary if cross_margin_summary is not None else {
            "crossMaintenanceMarginUsed": "0",
            "accountValue": "0",
        },
    }


def _make_meta_payload(names=None):
    """Собирает стандартный ответ /info meta."""
    names = names or ["BTC", "ETH", "SOL"]
    return {"universe": [{"name": n} for n in names]}


# ===========================================================================
# TestHyperliquidSignAction
# ===========================================================================

class TestHyperliquidSignAction:
    """Проверяет структуру и свойства результата _sign_action.

    eth_account замокирован, поэтому проверяем контракт интерфейса:
    метод должен возвращать dict с ключами r, s, v нужных типов.
    """

    def setup_method(self):
        self.connector = _make_connector()

    def _mock_signed(self, r: int, s: int, v: int) -> MagicMock:
        signed = MagicMock()
        signed.r = r
        signed.s = s
        signed.v = v
        return signed

    def test_sign_action_returns_dict(self):
        sys.modules["eth_account"].Account.sign_typed_data.return_value = (
            self._mock_signed(r=0xAB, s=0xCD, v=27)
        )
        action = {"type": "order"}

        result = self.connector._sign_action(action, nonce=1000)

        assert isinstance(result, dict)

    def test_sign_action_returns_dict_with_r_s_v_keys(self):
        sys.modules["eth_account"].Account.sign_typed_data.return_value = (
            self._mock_signed(r=0xAB, s=0xCD, v=27)
        )
        action = {"type": "order"}

        result = self.connector._sign_action(action, nonce=1000)

        assert "r" in result
        assert "s" in result
        assert "v" in result

    def test_sign_action_r_starts_with_0x(self):
        sys.modules["eth_account"].Account.sign_typed_data.return_value = (
            self._mock_signed(r=0xDEADBEEF, s=0xCAFEBABE, v=28)
        )
        action = {"type": "order"}

        result = self.connector._sign_action(action, nonce=1000)

        assert result["r"].startswith("0x")

    def test_sign_action_s_starts_with_0x(self):
        sys.modules["eth_account"].Account.sign_typed_data.return_value = (
            self._mock_signed(r=0x1, s=0x2, v=27)
        )
        action = {"type": "order"}

        result = self.connector._sign_action(action, nonce=1000)

        assert result["s"].startswith("0x")

    def test_sign_action_v_is_integer(self):
        sys.modules["eth_account"].Account.sign_typed_data.return_value = (
            self._mock_signed(r=0xFF, s=0xAA, v=27)
        )
        action = {"type": "order"}

        result = self.connector._sign_action(action, nonce=1000)

        assert isinstance(result["v"], int)

    def test_sign_action_v_value_matches_signed_v(self):
        sys.modules["eth_account"].Account.sign_typed_data.return_value = (
            self._mock_signed(r=0x10, s=0x20, v=28)
        )
        action = {"type": "order"}

        result = self.connector._sign_action(action, nonce=1000)

        assert result["v"] == 28

    def test_sign_action_r_value_is_hex_of_r(self):
        sys.modules["eth_account"].Account.sign_typed_data.return_value = (
            self._mock_signed(r=0xABCDEF, s=0x1, v=27)
        )
        action = {"type": "order"}

        result = self.connector._sign_action(action, nonce=1000)

        assert result["r"] == hex(0xABCDEF)

    def test_sign_action_s_value_is_hex_of_s(self):
        sys.modules["eth_account"].Account.sign_typed_data.return_value = (
            self._mock_signed(r=0x1, s=0x987654, v=27)
        )
        action = {"type": "order"}

        result = self.connector._sign_action(action, nonce=1000)

        assert result["s"] == hex(0x987654)

    def test_sign_action_called_with_secret_key(self):
        """sign_typed_data вызывается с секретным ключом коннектора."""
        mock_sign = MagicMock(return_value=self._mock_signed(r=1, s=2, v=27))
        sys.modules["eth_account"].Account.sign_typed_data = mock_sign
        action = {"type": "order"}

        self.connector._sign_action(action, nonce=5000)

        call_args = mock_sign.call_args
        assert call_args[0][0] == _TEST_SECRET

    def test_sign_action_different_nonces_produce_different_connection_ids(self):
        """Разные nonce передают разные structured_data в sign_typed_data.

        keccak патчится через модуль hyperliquid (where used), потому что
        from-import создаёт локальную ссылку, независимую от sys.modules.
        """
        import app.connectors.hyperliquid as hl_mod

        captured_calls = []

        def tracking_keccak(data):
            captured_calls.append(data)
            return b"\x00" * 32

        sys.modules["eth_account"].Account.sign_typed_data.return_value = (
            self._mock_signed(r=1, s=2, v=27)
        )
        action = {"type": "order", "grouping": "na"}

        with patch.object(hl_mod, "keccak", side_effect=tracking_keccak):
            self.connector._sign_action(action, nonce=1000)
            self.connector._sign_action(action, nonce=2000)

        # Два вызова keccak с разными nonce дают разные входные данные
        assert len(captured_calls) == 2
        assert captured_calls[0] != captured_calls[1]


# ===========================================================================
# TestHyperliquidFetchPositions
# ===========================================================================

class TestHyperliquidFetchPositions:
    pytestmark = pytest.mark.asyncio

    def setup_method(self):
        self.connector = _make_connector()

    async def test_fetch_positions_returns_list(self):
        payload = _make_state_payload(asset_positions=[
            {"position": {"coin": "ETH", "szi": "2.5", "entryPx": "3000.0", "positionValue": "7750.0"}},
        ])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert isinstance(positions, list)

    async def test_fetch_positions_returns_position_objects(self):
        from app.connectors.model.position import Position

        payload = _make_state_payload(asset_positions=[
            {"position": {"coin": "ETH", "szi": "2.5", "entryPx": "3000.0", "positionValue": "7750.0"}},
        ])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert len(positions) == 1
        assert isinstance(positions[0], Position)

    async def test_fetch_positions_ticker_is_coin_without_suffix(self):
        payload = _make_state_payload(asset_positions=[
            {"position": {"coin": "ETH", "szi": "1.0", "entryPx": "3000.0", "positionValue": "3000.0"}},
        ])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].ticker == "ETH"

    async def test_fetch_positions_ticker_btc_has_no_usdt_suffix(self):
        payload = _make_state_payload(asset_positions=[
            {"position": {"coin": "BTC", "szi": "0.1", "entryPx": "60000.0", "positionValue": "6000.0"}},
        ])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].ticker == "BTC"
        assert "USDT" not in positions[0].ticker

    async def test_fetch_positions_positive_szi_is_long(self):
        payload = _make_state_payload(asset_positions=[
            {"position": {"coin": "ETH", "szi": "2.5", "entryPx": "3000.0", "positionValue": "7750.0"}},
        ])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].direction == "long"

    async def test_fetch_positions_negative_szi_is_short(self):
        payload = _make_state_payload(asset_positions=[
            {"position": {"coin": "ETH", "szi": "-1.5", "entryPx": "3000.0", "positionValue": "4500.0"}},
        ])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].direction == "short"

    async def test_fetch_positions_amount_is_abs_szi(self):
        payload = _make_state_payload(asset_positions=[
            {"position": {"coin": "ETH", "szi": "-3.0", "entryPx": "3000.0", "positionValue": "9000.0"}},
        ])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].amount == Decimal("3.0")
        assert positions[0].amount >= 0

    async def test_fetch_positions_avg_price_from_entry_px(self):
        payload = _make_state_payload(asset_positions=[
            {"position": {"coin": "BTC", "szi": "0.5", "entryPx": "61234.5", "positionValue": "30617.25"}},
        ])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].avg_price == Decimal("61234.5")

    async def test_fetch_positions_current_price_is_position_value_divided_by_amount(self):
        payload = _make_state_payload(asset_positions=[
            {"position": {"coin": "ETH", "szi": "2.5", "entryPx": "3000.0", "positionValue": "7750.0"}},
        ])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        expected = Decimal("7750.0") / Decimal("2.5")
        assert positions[0].current_price == expected

    async def test_fetch_positions_zero_szi_is_filtered(self):
        payload = _make_state_payload(asset_positions=[
            {"position": {"coin": "ETH", "szi": "0", "entryPx": "3000.0", "positionValue": "0"}},
            {"position": {"coin": "BTC", "szi": "0.1", "entryPx": "60000.0", "positionValue": "6000.0"}},
        ])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        tickers = [p.ticker for p in positions]
        assert "ETH" not in tickers
        assert "BTC" in tickers

    async def test_fetch_positions_empty_asset_positions_returns_empty_list(self):
        payload = _make_state_payload(asset_positions=[])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions == []

    async def test_fetch_positions_multiple_positions_all_returned(self):
        payload = _make_state_payload(asset_positions=[
            {"position": {"coin": "BTC", "szi": "0.1", "entryPx": "60000.0", "positionValue": "6000.0"}},
            {"position": {"coin": "ETH", "szi": "-5.0", "entryPx": "3000.0", "positionValue": "15000.0"}},
            {"position": {"coin": "SOL", "szi": "10.0", "entryPx": "150.0", "positionValue": "1500.0"}},
        ])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert len(positions) == 3

    async def test_fetch_positions_exchange_name_is_hyperliquid(self):
        payload = _make_state_payload(asset_positions=[
            {"position": {"coin": "BTC", "szi": "0.1", "entryPx": "60000.0", "positionValue": "6000.0"}},
        ])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].exchange_name == "hyperliquid"

    async def test_fetch_positions_http_error_raises(self):
        self.connector._client.post = AsyncMock(
            return_value=_make_response({}, status_code=500)
        )

        with pytest.raises(Exception):
            await self.connector.fetch_positions()

    @pytest.mark.parametrize("szi,expected_direction,expected_amount", [
        ("10.0", "long", Decimal("10.0")),
        ("-10.0", "short", Decimal("10.0")),
        ("0.001", "long", Decimal("0.001")),
        ("-0.001", "short", Decimal("0.001")),
    ])
    async def test_fetch_positions_direction_and_amount_parametrized(
            self, szi: str, expected_direction: str, expected_amount: Decimal
    ):
        pos_value = str(abs(Decimal(szi)) * Decimal("100"))
        payload = _make_state_payload(asset_positions=[
            {"position": {"coin": "XYZ", "szi": szi, "entryPx": "100.0", "positionValue": pos_value}},
        ])
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        positions = await self.connector.fetch_positions()

        assert positions[0].direction == expected_direction
        assert positions[0].amount == expected_amount


# ===========================================================================
# TestHyperliquidFetchMargin
# ===========================================================================

class TestHyperliquidFetchMargin:
    pytestmark = pytest.mark.asyncio

    def setup_method(self):
        self.connector = _make_connector()

    async def test_fetch_margin_returns_tuple(self):
        payload = _make_state_payload(cross_margin_summary={
            "crossMaintenanceMarginUsed": "150.5",
            "accountValue": "5000.0",
        })
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        result = await self.connector.fetch_margin()

        assert isinstance(result, tuple)
        assert len(result) == 2

    async def test_fetch_margin_first_element_is_maintenance_margin(self):
        payload = _make_state_payload(cross_margin_summary={
            "crossMaintenanceMarginUsed": "150.5",
            "accountValue": "5000.0",
        })
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, _ = await self.connector.fetch_margin()

        assert maintenance_margin == Decimal("150.5")

    async def test_fetch_margin_second_element_is_account_value(self):
        payload = _make_state_payload(cross_margin_summary={
            "crossMaintenanceMarginUsed": "150.5",
            "accountValue": "5000.0",
        })
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        _, current_margin = await self.connector.fetch_margin()

        assert current_margin == Decimal("5000.0")

    async def test_fetch_margin_both_values_are_decimal(self):
        payload = _make_state_payload(cross_margin_summary={
            "crossMaintenanceMarginUsed": "200.0",
            "accountValue": "10000.0",
        })
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, current_margin = await self.connector.fetch_margin()

        assert isinstance(maintenance_margin, Decimal)
        assert isinstance(current_margin, Decimal)

    async def test_fetch_margin_http_error_raises(self):
        self.connector._client.post = AsyncMock(
            return_value=_make_response({}, status_code=403)
        )

        with pytest.raises(Exception):
            await self.connector.fetch_margin()

    @pytest.mark.parametrize("maint,account_value", [
        ("0", "0"),
        ("0.01", "100000"),
        ("99999.99", "99999.99"),
        ("150.5", "5000.0"),
    ])
    async def test_fetch_margin_parses_various_values(self, maint: str, account_value: str):
        payload = _make_state_payload(cross_margin_summary={
            "crossMaintenanceMarginUsed": maint,
            "accountValue": account_value,
        })
        self.connector._client.post = AsyncMock(return_value=_make_response(payload))

        maintenance_margin, current_margin = await self.connector.fetch_margin()

        assert maintenance_margin == Decimal(maint)
        assert current_margin == Decimal(account_value)


# ===========================================================================
# TestHyperliquidClosePosition
# ===========================================================================

class TestHyperliquidClosePosition:
    pytestmark = pytest.mark.asyncio

    def setup_method(self):
        self.connector = _make_connector()
        # Подменяем sign_typed_data чтобы не зависеть от реализации eth_account
        mock_signed = MagicMock()
        mock_signed.r = 0xDEAD
        mock_signed.s = 0xBEEF
        mock_signed.v = 27
        sys.modules["eth_account"].Account.sign_typed_data.return_value = mock_signed

    def _ok_response(self):
        return _make_response({"status": "ok", "response": {}})

    def _meta_response(self):
        return _make_response(_make_meta_payload(["BTC", "ETH", "SOL"]))

    async def test_close_position_first_call_fetches_meta(self):
        """При пустом _asset_index первый вызов делает два POST: meta и exchange."""
        assert self.connector._asset_index == {}

        calls_json = []

        async def fake_post(url, **kwargs):
            body = kwargs.get("json", {})
            calls_json.append(body)
            if body.get("type") == "meta":
                return self._meta_response()
            return self._ok_response()

        self.connector._client.post = fake_post

        await self.connector.close_position("BTC", Decimal("0.1"))

        assert len(calls_json) == 2

    async def test_close_position_second_call_does_not_fetch_meta(self):
        """После первого вызова _asset_index заполнен — второй вызов делает один POST."""
        self.connector._asset_index = {"BTC": 0, "ETH": 1, "SOL": 2}

        calls_json = []

        async def fake_post(url, **kwargs):
            body = kwargs.get("json", {})
            calls_json.append(body)
            return self._ok_response()

        self.connector._client.post = fake_post

        await self.connector.close_position("BTC", Decimal("0.1"))

        assert len(calls_json) == 1

    async def test_close_position_meta_loaded_lazily_populates_asset_index(self):
        """_asset_index пустой до вызова и заполнен после первого вызова."""
        assert self.connector._asset_index == {}

        async def fake_post(url, **kwargs):
            body = kwargs.get("json", {})
            if body.get("type") == "meta":
                return self._meta_response()
            return self._ok_response()

        self.connector._client.post = fake_post

        await self.connector.close_position("ETH", Decimal("1.0"))

        assert "ETH" in self.connector._asset_index
        assert "BTC" in self.connector._asset_index

    async def test_close_position_short_uses_is_buy_true(self):
        """Закрытие short-позиции: is_buy=True (b=True) в ордере."""
        from app.connectors.model.position import Position

        self.connector._asset_index = {"ETH": 1}
        self.connector.state.positions["ETH"] = Position(
            ticker="ETH",
            exchange_name="hyperliquid",
            direction="short",
            amount=Decimal("5.0"),
            avg_price=Decimal("3000"),
            current_price=Decimal("3100"),
        )

        captured_actions = []

        async def fake_post(url, **kwargs):
            body = kwargs.get("json", {})
            if "action" in body:
                captured_actions.append(body["action"])
            return self._ok_response()

        self.connector._client.post = fake_post

        await self.connector.close_position("ETH", Decimal("5.0"))

        order = captured_actions[0]["orders"][0]
        assert order["b"] is True

    async def test_close_position_long_uses_is_buy_false(self):
        """Закрытие long-позиции: is_buy=False (b=False) в ордере."""
        from app.connectors.model.position import Position

        self.connector._asset_index = {"BTC": 0}
        self.connector.state.positions["BTC"] = Position(
            ticker="BTC",
            exchange_name="hyperliquid",
            direction="long",
            amount=Decimal("0.5"),
            avg_price=Decimal("60000"),
            current_price=Decimal("61000"),
        )

        captured_actions = []

        async def fake_post(url, **kwargs):
            body = kwargs.get("json", {})
            if "action" in body:
                captured_actions.append(body["action"])
            return self._ok_response()

        self.connector._client.post = fake_post

        await self.connector.close_position("BTC", Decimal("0.5"))

        order = captured_actions[0]["orders"][0]
        assert order["b"] is False

    async def test_close_position_unknown_ticker_in_state_uses_is_buy_true(self):
        """Тикер не найден в state.positions — по умолчанию is_buy=True."""
        self.connector._asset_index = {"SOL": 2}

        captured_actions = []

        async def fake_post(url, **kwargs):
            body = kwargs.get("json", {})
            if "action" in body:
                captured_actions.append(body["action"])
            return self._ok_response()

        self.connector._client.post = fake_post

        await self.connector.close_position("SOL", Decimal("10.0"))

        order = captured_actions[0]["orders"][0]
        assert order["b"] is True

    async def test_close_position_action_contains_correct_asset_index(self):
        """Поле 'a' в order соответствует индексу тикера из _asset_index."""
        self.connector._asset_index = {"BTC": 0, "ETH": 1, "SOL": 2}

        captured_actions = []

        async def fake_post(url, **kwargs):
            body = kwargs.get("json", {})
            if "action" in body:
                captured_actions.append(body["action"])
            return self._ok_response()

        self.connector._client.post = fake_post

        await self.connector.close_position("ETH", Decimal("1.0"))

        order = captured_actions[0]["orders"][0]
        assert order["a"] == 1

    async def test_close_position_order_has_reduce_only_true(self):
        """Поле 'r' (reduceOnly) в order должно быть True."""
        self.connector._asset_index = {"BTC": 0}

        captured_actions = []

        async def fake_post(url, **kwargs):
            body = kwargs.get("json", {})
            if "action" in body:
                captured_actions.append(body["action"])
            return self._ok_response()

        self.connector._client.post = fake_post

        await self.connector.close_position("BTC", Decimal("0.1"))

        order = captured_actions[0]["orders"][0]
        assert order["r"] is True

    async def test_close_position_order_s_matches_amount(self):
        """Поле 's' в order соответствует строковому представлению amount."""
        self.connector._asset_index = {"SOL": 2}

        captured_actions = []

        async def fake_post(url, **kwargs):
            body = kwargs.get("json", {})
            if "action" in body:
                captured_actions.append(body["action"])
            return self._ok_response()

        self.connector._client.post = fake_post

        await self.connector.close_position("SOL", Decimal("7.5"))

        order = captured_actions[0]["orders"][0]
        assert order["s"] == "7.5"

    async def test_close_position_action_grouping_is_na(self):
        """Поле 'grouping' в action должно быть 'na'."""
        self.connector._asset_index = {"BTC": 0}

        captured_actions = []

        async def fake_post(url, **kwargs):
            body = kwargs.get("json", {})
            if "action" in body:
                captured_actions.append(body["action"])
            return self._ok_response()

        self.connector._client.post = fake_post

        await self.connector.close_position("BTC", Decimal("0.1"))

        assert captured_actions[0]["grouping"] == "na"

    async def test_close_position_error_status_raises_runtime_error(self):
        """Ответ {'status': 'error'} должен выбрасывать RuntimeError."""
        self.connector._asset_index = {"BTC": 0}

        async def fake_post(url, **kwargs):
            return _make_response({"status": "error", "response": "some error"})

        self.connector._client.post = fake_post

        with pytest.raises(RuntimeError, match="Hyperliquid close_position error"):
            await self.connector.close_position("BTC", Decimal("0.1"))

    async def test_close_position_http_error_raises(self):
        """HTTP-ошибка (4xx/5xx) распространяется через raise_for_status."""
        self.connector._asset_index = {"BTC": 0}
        self.connector._client.post = AsyncMock(
            return_value=_make_response({}, status_code=401)
        )

        with pytest.raises(Exception):
            await self.connector.close_position("BTC", Decimal("0.1"))

    async def test_close_position_success_does_not_raise(self):
        """Успешный ответ не должен выбрасывать исключений."""
        self.connector._asset_index = {"ETH": 1}

        async def fake_post(url, **kwargs):
            return self._ok_response()

        self.connector._client.post = fake_post

        await self.connector.close_position("ETH", Decimal("2.0"))

    async def test_close_position_meta_first_request_type_is_meta(self):
        """Первый запрос при пустом _asset_index имеет type='meta'."""
        assert self.connector._asset_index == {}

        captured_bodies = []

        async def fake_post(url, **kwargs):
            body = kwargs.get("json", {})
            captured_bodies.append(body)
            if body.get("type") == "meta":
                return self._meta_response()
            return self._ok_response()

        self.connector._client.post = fake_post

        await self.connector.close_position("BTC", Decimal("0.1"))

        assert captured_bodies[0].get("type") == "meta"

    async def test_close_position_exchange_request_contains_action_nonce_signature(self):
        """POST /exchange содержит ключи action, nonce, signature."""
        self.connector._asset_index = {"BTC": 0}

        captured_bodies = []

        async def fake_post(url, **kwargs):
            body = kwargs.get("json", {})
            captured_bodies.append(body)
            return self._ok_response()

        self.connector._client.post = fake_post

        await self.connector.close_position("BTC", Decimal("0.1"))

        exchange_body = captured_bodies[0]
        assert "action" in exchange_body
        assert "nonce" in exchange_body
        assert "signature" in exchange_body
