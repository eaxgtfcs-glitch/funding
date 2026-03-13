"""
Тесты для фичи автозакрытия контрпозиции в Structure.

Покрываемые области:
  1. close_position присутствует в базовом классе и конкретных коннекторах
  2. READ_ONLY_MODE читается из env
  3. _get_connector возвращает нужный коннектор по имени / None если не найден
  4. Логика автозакрытия в _on_positions_updated:
       - READ_ONLY_MODE=True  → close_position НЕ вызывается
       - READ_ONLY_MODE=False → вычисляется close_amount и запускается задача
  5. _auto_close_structure_leg:
       - успех с первой попытки → send_reduction_alert_raw
       - первая попытка бросила исключение → вторая попытка
       - первая успешна по коду, но факт не подтверждён → вторая попытка
       - вторая попытка успешна → send_reduction_alert_raw
       - обе попытки провалились → send_critical_alert
  6. Критерий успеха: amount_after <= (amount_b_before - close_amount) / 0.995
  7. Форматтеры format_auto_close_success и format_auto_close_failed
"""
import asyncio
import inspect
import os
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.connectors.base import BaseExchangeConnector
from app.connectors.config import ConnectorConfig
from app.connectors.model.position import Position
from app.engine.model.structure import Structure, StructureLeg
from app.telegram.formatters import format_auto_close_failed, format_auto_close_success


# ---------------------------------------------------------------------------
# Тестовые дубли коннекторов
# ---------------------------------------------------------------------------


class AlphaConnector(BaseExchangeConnector):
    """Дубль коннектора «alpha» — сторона A."""

    name = "alpha"
    config = ConnectorConfig(positions_interval=0.0, margin_interval=0.0)

    async def fetch_positions(self) -> list[Position]:
        return []

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        return Decimal(0), Decimal(0)

    async def close_position(self, ticker: str, amount: Decimal) -> None:
        pass


class BetaConnector(BaseExchangeConnector):
    """Дубль коннектора «beta» — сторона B."""

    name = "beta"
    config = ConnectorConfig(positions_interval=0.0, margin_interval=0.0)

    async def fetch_positions(self) -> list[Position]:
        return []

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        return Decimal(0), Decimal(0)

    async def close_position(self, ticker: str, amount: Decimal) -> None:
        pass


# ---------------------------------------------------------------------------
# Вспомогательные фабрики
# ---------------------------------------------------------------------------


def _make_position(
        ticker: str = "BTCUSDT",
        exchange_name: str = "alpha",
        direction: str = "long",
        amount: str = "1.0",
) -> Position:
    return Position(
        ticker=ticker,
        exchange_name=exchange_name,
        direction=direction,
        amount=Decimal(amount),
        avg_price=Decimal("50000"),
        current_price=Decimal("51000"),
    )


def make_engine_with_two_connectors():
    """
    Создаёт MonitoringEngine с двумя изолированными коннекторами (alpha, beta).
    Telegram и Broadcaster отключены (нет токена).
    """
    from app.engine.engine import MonitoringEngine

    with patch("pkgutil.iter_modules", return_value=[]):
        with patch.object(
                BaseExchangeConnector,
                "__subclasses__",
                return_value=[AlphaConnector, BetaConnector],
        ):
            engine = MonitoringEngine()
    return engine


def _make_structure(
        exchange_a: str = "alpha",
        ticker_a: str = "BTCUSDT",
        exchange_b: str = "beta",
        ticker_b: str = "BTCUSDT",
) -> Structure:
    return Structure(
        legs=[
            StructureLeg(exchange=exchange_a, ticker=ticker_a),
            StructureLeg(exchange=exchange_b, ticker=ticker_b),
        ],
        is_active=True,
    )


# ---------------------------------------------------------------------------
# Минимальная заглушка TelegramAlertService
# ---------------------------------------------------------------------------


def _attach_mock_telegram(engine) -> MagicMock:
    """Подвешивает мок-telegram к engine и возвращает его."""
    tg = MagicMock()
    tg.send_alert_tracked = AsyncMock(return_value=42)
    engine._telegram = tg
    return tg


# ===========================================================================
# 1. close_position присутствует в базовом классе и конкретных коннекторах
# ===========================================================================


class TestClosePositionAbstractMethod:

    def test_close_position_declared_abstract_in_base(self):
        """BaseExchangeConnector.close_position обязан быть абстрактным методом."""
        method = BaseExchangeConnector.__dict__.get("close_position")
        assert method is not None, "close_position не найден в BaseExchangeConnector"
        assert getattr(method, "__isabstractmethod__", False), (
            "close_position должен быть абстрактным"
        )

    def test_binance_connector_has_close_position(self):
        with patch.dict(
                os.environ,
                {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"},
        ):
            from app.connectors.binance import BinanceConnector

            assert hasattr(BinanceConnector, "close_position")
            assert callable(BinanceConnector.close_position)

    def test_bybit_connector_has_close_position(self):
        with patch.dict(
                os.environ,
                {"BYBIT_API_KEY": "k", "BYBIT_API_SECRET": "s"},
        ):
            from app.connectors.bybit import BybitConnector

            assert hasattr(BybitConnector, "close_position")
            assert callable(BybitConnector.close_position)

    def test_binance_close_position_is_coroutine_function(self):
        with patch.dict(
                os.environ,
                {"BINANCE_API_KEY": "k", "BINANCE_API_SECRET": "s"},
        ):
            from app.connectors.binance import BinanceConnector

            assert inspect.iscoroutinefunction(BinanceConnector.close_position)

    def test_bybit_close_position_is_coroutine_function(self):
        with patch.dict(
                os.environ,
                {"BYBIT_API_KEY": "k", "BYBIT_API_SECRET": "s"},
        ):
            from app.connectors.bybit import BybitConnector

            assert inspect.iscoroutinefunction(BybitConnector.close_position)


# ===========================================================================
# 2. READ_ONLY_MODE читается из env
# ===========================================================================


class TestReadOnlyModeConfig:

    def test_read_only_mode_false_when_env_not_set(self):
        env = {k: v for k, v in os.environ.items() if k != "READ_ONLY_MODE"}
        with patch.dict(os.environ, env, clear=True):
            import importlib

            import app.connectors.config as cfg_module

            importlib.reload(cfg_module)
            assert cfg_module.READ_ONLY_MODE is False

    def test_read_only_mode_true_when_env_is_true_lowercase(self):
        with patch.dict(os.environ, {"READ_ONLY_MODE": "true"}):
            import importlib

            import app.connectors.config as cfg_module

            importlib.reload(cfg_module)
            assert cfg_module.READ_ONLY_MODE is True

    def test_read_only_mode_true_when_env_is_TRUE_uppercase(self):
        with patch.dict(os.environ, {"READ_ONLY_MODE": "TRUE"}):
            import importlib

            import app.connectors.config as cfg_module

            importlib.reload(cfg_module)
            assert cfg_module.READ_ONLY_MODE is True

    def test_read_only_mode_false_when_env_is_false_string(self):
        with patch.dict(os.environ, {"READ_ONLY_MODE": "false"}):
            import importlib

            import app.connectors.config as cfg_module

            importlib.reload(cfg_module)
            assert cfg_module.READ_ONLY_MODE is False

    def test_read_only_mode_false_when_env_is_random_string(self):
        with patch.dict(os.environ, {"READ_ONLY_MODE": "yes"}):
            import importlib

            import app.connectors.config as cfg_module

            importlib.reload(cfg_module)
            # "yes".lower() == "true" → False; только "true" → True
            assert cfg_module.READ_ONLY_MODE is False


# ===========================================================================
# 3. _get_connector возвращает нужный коннектор по имени / None
# ===========================================================================


class TestGetConnector:

    def test_get_connector_returns_correct_connector_by_name(self):
        engine = make_engine_with_two_connectors()
        connector = engine._get_connector("alpha")
        assert connector is not None
        assert connector.name == "alpha"

    def test_get_connector_returns_second_connector_by_name(self):
        engine = make_engine_with_two_connectors()
        connector = engine._get_connector("beta")
        assert connector is not None
        assert connector.name == "beta"

    def test_get_connector_returns_none_for_unknown_name(self):
        engine = make_engine_with_two_connectors()
        result = engine._get_connector("nonexistent")
        assert result is None

    def test_get_connector_returns_none_for_empty_string(self):
        engine = make_engine_with_two_connectors()
        result = engine._get_connector("")
        assert result is None

    def test_get_connector_returns_same_instance_as_in_connectors_list(self):
        engine = make_engine_with_two_connectors()
        connector_from_list = next(c for c in engine._connectors if c.name == "alpha")
        connector_from_get = engine._get_connector("alpha")
        assert connector_from_list is connector_from_get


# ===========================================================================
# 4. _on_positions_updated — логика автозакрытия через Structure
# ===========================================================================


class TestOnPositionsUpdatedAutoClose:

    def _setup_engine_with_structure(self) -> tuple:
        """
        Возвращает (engine, connector_a, connector_b).
        Structure: alpha/BTCUSDT (long) ↔ beta/BTCUSDT (short).
        """
        engine = make_engine_with_two_connectors()
        connector_a = engine._get_connector("alpha")
        connector_b = engine._get_connector("beta")

        pos_a = _make_position("BTCUSDT", "alpha", "long", "10.0")
        pos_b = _make_position("BTCUSDT", "beta", "short", "10.0")

        structure = _make_structure()
        engine._structures = [structure]

        # Устанавливаем предыдущее состояние — 10 BTC
        engine._prev_amounts["alpha"] = {"BTCUSDT": Decimal("10.0")}

        # Текущее состояние коннектора alpha: позиция уменьшилась до 6
        reduced_pos_a = _make_position("BTCUSDT", "alpha", "long", "6.0")
        connector_a.state.positions = {"BTCUSDT": reduced_pos_a}

        # Текущее состояние коннектора beta: позиция b = 10
        connector_b.state.positions = {"BTCUSDT": pos_b}

        engine._send_reduction_alert = AsyncMock()
        engine._send_reduction_alert_raw = AsyncMock()
        engine._send_critical_alert = AsyncMock()
        engine._send_structure_alert = AsyncMock()

        return engine, connector_a, connector_b

    async def test_read_only_mode_true_does_not_call_close_position(self):
        engine, connector_a, connector_b, = self._setup_engine_with_structure()
        connector_b.close_position = AsyncMock()

        with patch("app.engine.engine.READ_ONLY_MODE", True):
            await engine._on_positions_updated(connector_a)
            await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        connector_b.close_position.assert_not_called()

    async def test_read_only_mode_false_creates_auto_close_task(self):
        engine, connector_a, connector_b = self._setup_engine_with_structure()
        connector_b.close_position = AsyncMock()

        auto_close_calls = []

        async def _mock_auto_close(**kwargs):
            auto_close_calls.append(kwargs)

        engine._auto_close_structure_leg = _mock_auto_close

        with patch("app.engine.engine.READ_ONLY_MODE", False):
            await engine._on_positions_updated(connector_a)
            await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        assert len(auto_close_calls) == 1

    async def test_no_auto_close_when_no_structure(self):
        """Если нет структуры — автозакрытие не запускается."""
        engine, connector_a, connector_b = self._setup_engine_with_structure()
        engine._structures = []

        auto_close_calls = []

        async def _mock_auto_close(**kwargs):
            auto_close_calls.append(kwargs)

        engine._auto_close_structure_leg = _mock_auto_close

        with patch("app.engine.engine.READ_ONLY_MODE", False):
            await engine._on_positions_updated(connector_a)
            await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        assert len(auto_close_calls) == 0

    async def test_no_auto_close_when_no_reduction(self):
        """Если нет сокращения — задача не создаётся."""
        engine, connector_a, connector_b = self._setup_engine_with_structure()
        same_pos = _make_position("BTCUSDT", "alpha", "long", "10.0")
        connector_a.state.positions = {"BTCUSDT": same_pos}

        auto_close_calls = []

        async def _mock_auto_close(**kwargs):
            auto_close_calls.append(kwargs)

        engine._auto_close_structure_leg = _mock_auto_close

        with patch("app.engine.engine.READ_ONLY_MODE", False):
            await engine._on_positions_updated(connector_a)
            await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        assert len(auto_close_calls) == 0


# ===========================================================================
# 5 & 6. _auto_close_structure_leg — логика попыток и критерий успеха
# ===========================================================================


class TestAutoCloseStructureLeg:

    def _make_engine_and_connector_b(self):
        engine = make_engine_with_two_connectors()
        engine._send_reduction_alert_raw = AsyncMock()
        engine._send_critical_alert = AsyncMock()
        connector_b = engine._get_connector("beta")
        return engine, connector_b

    def _leg(self, exchange: str = "beta", ticker: str = "BTCUSDT") -> StructureLeg:
        return StructureLeg(exchange=exchange, ticker=ticker)

    async def test_success_on_first_attempt_sends_success_alert(self):
        engine, connector_b = self._make_engine_and_connector_b()
        connector_b.close_position = AsyncMock()

        # success_threshold = (10 - 4) / 0.995 ≈ 6.030
        pos_after = _make_position("BTCUSDT", "beta", "short", "6.0")
        connector_b.fetch_positions = AsyncMock(return_value=[pos_after])

        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="BTCUSDT",
            leg=self._leg(),
            close_exchange_units=Decimal("4.0"),
            amount_before=Decimal("10.0"),
        )

        engine._send_reduction_alert_raw.assert_awaited_once()
        engine._send_critical_alert.assert_not_called()

    async def test_success_on_first_attempt_does_not_retry(self):
        engine, connector_b = self._make_engine_and_connector_b()
        connector_b.close_position = AsyncMock()

        pos_after = _make_position("BTCUSDT", "beta", "short", "6.0")
        connector_b.fetch_positions = AsyncMock(return_value=[pos_after])

        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="BTCUSDT",
            leg=self._leg(),
            close_exchange_units=Decimal("4.0"),
            amount_before=Decimal("10.0"),
        )

        connector_b.close_position.assert_awaited_once()

    async def test_first_attempt_exception_triggers_retry(self):
        engine, connector_b = self._make_engine_and_connector_b()
        connector_b.close_position = AsyncMock(
            side_effect=[RuntimeError("network error"), None]
        )

        pos_after = _make_position("BTCUSDT", "beta", "short", "6.0")
        connector_b.fetch_positions = AsyncMock(return_value=[pos_after])

        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="BTCUSDT",
            leg=self._leg(),
            close_exchange_units=Decimal("4.0"),
            amount_before=Decimal("10.0"),
        )

        assert connector_b.close_position.await_count == 2

    async def test_first_attempt_exception_then_second_success_sends_success_alert(self):
        engine, connector_b = self._make_engine_and_connector_b()
        connector_b.close_position = AsyncMock(
            side_effect=[RuntimeError("timeout"), None]
        )

        pos_after = _make_position("BTCUSDT", "beta", "short", "6.0")
        connector_b.fetch_positions = AsyncMock(return_value=[pos_after])

        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="BTCUSDT",
            leg=self._leg(),
            close_exchange_units=Decimal("4.0"),
            amount_before=Decimal("10.0"),
        )

        engine._send_reduction_alert_raw.assert_awaited_once()
        engine._send_critical_alert.assert_not_called()

    async def test_first_success_but_fact_not_confirmed_triggers_retry(self):
        """
        close_position не бросает, но amount_after > success_threshold → False.
        Ожидаем вторую попытку.
        """
        engine, connector_b = self._make_engine_and_connector_b()
        connector_b.close_position = AsyncMock()

        # success_threshold = (10 - 4) / 0.995 ≈ 6.0301
        pos_not_reduced = _make_position("BTCUSDT", "beta", "short", "7.0")
        pos_confirmed = _make_position("BTCUSDT", "beta", "short", "6.0")
        connector_b.fetch_positions = AsyncMock(
            side_effect=[[pos_not_reduced], [pos_confirmed]]
        )

        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="BTCUSDT",
            leg=self._leg(),
            close_exchange_units=Decimal("4.0"),
            amount_before=Decimal("10.0"),
        )

        assert connector_b.close_position.await_count == 2

    async def test_first_unconfirmed_then_second_confirmed_sends_success_alert(self):
        engine, connector_b = self._make_engine_and_connector_b()
        connector_b.close_position = AsyncMock()

        pos_not_reduced = _make_position("BTCUSDT", "beta", "short", "7.0")
        pos_confirmed = _make_position("BTCUSDT", "beta", "short", "6.0")
        connector_b.fetch_positions = AsyncMock(
            side_effect=[[pos_not_reduced], [pos_confirmed]]
        )

        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="BTCUSDT",
            leg=self._leg(),
            close_exchange_units=Decimal("4.0"),
            amount_before=Decimal("10.0"),
        )

        engine._send_reduction_alert_raw.assert_awaited_once()
        engine._send_critical_alert.assert_not_called()

    async def test_both_attempts_failed_sends_critical_alert(self):
        engine, connector_b = self._make_engine_and_connector_b()
        connector_b.close_position = AsyncMock(
            side_effect=[RuntimeError("fail1"), RuntimeError("fail2")]
        )
        connector_b.fetch_positions = AsyncMock(return_value=[])

        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="BTCUSDT",
            leg=self._leg(),
            close_exchange_units=Decimal("4.0"),
            amount_before=Decimal("10.0"),
        )

        await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)
        engine._send_critical_alert.assert_called_once()

    async def test_both_attempts_unconfirmed_sends_critical_alert(self):
        """Оба вызова close_position прошли без исключений, но факт не подтверждён."""
        engine, connector_b = self._make_engine_and_connector_b()
        connector_b.close_position = AsyncMock()

        pos_still_open = _make_position("BTCUSDT", "beta", "short", "9.0")
        connector_b.fetch_positions = AsyncMock(
            side_effect=[[pos_still_open], [pos_still_open]]
        )

        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="BTCUSDT",
            leg=self._leg(),
            close_exchange_units=Decimal("4.0"),
            amount_before=Decimal("10.0"),
        )

        await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)
        engine._send_critical_alert.assert_called_once()

    async def test_both_failed_does_not_send_success_alert(self):
        engine, connector_b = self._make_engine_and_connector_b()
        connector_b.close_position = AsyncMock(
            side_effect=[RuntimeError("e1"), RuntimeError("e2")]
        )
        connector_b.fetch_positions = AsyncMock(return_value=[])

        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="BTCUSDT",
            leg=self._leg(),
            close_exchange_units=Decimal("4.0"),
            amount_before=Decimal("10.0"),
        )

        await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)
        engine._send_reduction_alert_raw.assert_not_awaited()


# ===========================================================================
# 6. Критерий успеха: amount_after <= (amount_before - close_amount) / 0.995
# ===========================================================================


class TestSuccessThreshold:

    def _make_engine_and_connector_b(self):
        engine = make_engine_with_two_connectors()
        engine._send_reduction_alert_raw = AsyncMock()
        engine._send_critical_alert = AsyncMock()
        connector_b = engine._get_connector("beta")
        return engine, connector_b

    def _leg(self, ticker: str = "XYZUSDT") -> StructureLeg:
        return StructureLeg(exchange="beta", ticker=ticker)

    @pytest.mark.parametrize(
        "amount_before,close_amount,amount_after,expect_success",
        [
            # threshold = (10 - 4) / 0.995 = 6.030150…
            ("10", "4", "6.0", True),
            ("10", "4", "6.030", True),
            ("10", "4", "6.031", False),
            ("10", "4", "0", True),
            # threshold = (100 - 50) / 0.995 = 50.251…
            ("100", "50", "50.0", True),
            ("100", "50", "50.251", True),
            ("100", "50", "50.252", False),
        ],
    )
    async def test_success_threshold_parametrized(
            self,
            amount_before: str,
            close_amount: str,
            amount_after: str,
            expect_success: bool,
    ):
        engine, connector_b = self._make_engine_and_connector_b()
        connector_b.close_position = AsyncMock()

        if amount_after != "0":
            pos_after = _make_position("XYZUSDT", "beta", "short", amount_after)
            connector_b.fetch_positions = AsyncMock(return_value=[pos_after])
        else:
            connector_b.fetch_positions = AsyncMock(return_value=[])

        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="BTCUSDT",
            leg=self._leg(),
            close_exchange_units=Decimal(close_amount),
            amount_before=Decimal(amount_before),
        )

        await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        if expect_success:
            engine._send_reduction_alert_raw.assert_awaited()
            engine._send_critical_alert.assert_not_called()
        else:
            engine._send_critical_alert.assert_called()


# ===========================================================================
# 7. Форматтеры format_auto_close_success и format_auto_close_failed
# ===========================================================================


class TestFormatAutoCloseSuccess:

    def test_returns_non_empty_string(self):
        result = format_auto_close_success(
            "binance", "BTCUSDT", "bybit", "BTCUSDT", Decimal("4.0")
        )
        assert isinstance(result, str)
        assert len(result) > 0

    def test_contains_auto_close_ok_header(self):
        result = format_auto_close_success(
            "binance", "BTCUSDT", "bybit", "BTCUSDT", Decimal("4.0")
        )
        assert "AUTO CLOSE OK" in result

    def test_contains_trigger_exchange(self):
        result = format_auto_close_success(
            "binance", "BTCUSDT", "bybit", "BTCUSDT", Decimal("4.0")
        )
        assert "binance" in result

    def test_contains_trigger_ticker(self):
        result = format_auto_close_success(
            "binance", "BTCUSDT", "bybit", "ETHUSDT", Decimal("4.0")
        )
        assert "BTCUSDT" in result

    def test_contains_close_exchange(self):
        result = format_auto_close_success(
            "binance", "BTCUSDT", "bybit", "ETHUSDT", Decimal("4.0")
        )
        assert "bybit" in result

    def test_contains_close_ticker(self):
        result = format_auto_close_success(
            "binance", "BTCUSDT", "bybit", "ETHUSDT", Decimal("4.0")
        )
        assert "ETHUSDT" in result

    def test_contains_close_amount(self):
        result = format_auto_close_success(
            "binance", "BTCUSDT", "bybit", "BTCUSDT", Decimal("4.0")
        )
        assert "4.0" in result

    @pytest.mark.parametrize("amount", ["0.001", "100", "9999.99"])
    def test_amount_present_in_output_parametrized(self, amount: str):
        result = format_auto_close_success(
            "exch_a", "TICKA", "exch_b", "TICKB", Decimal(amount)
        )
        assert amount in result


class TestFormatAutoCloseFailed:

    def test_returns_non_empty_string(self):
        result = format_auto_close_failed(
            "binance", "BTCUSDT", "bybit", "BTCUSDT", Decimal("4.0")
        )
        assert isinstance(result, str)
        assert len(result) > 0

    def test_contains_auto_close_failed_header(self):
        result = format_auto_close_failed(
            "binance", "BTCUSDT", "bybit", "BTCUSDT", Decimal("4.0")
        )
        assert "AUTO CLOSE FAILED" in result

    def test_contains_trigger_exchange(self):
        result = format_auto_close_failed(
            "binance", "BTCUSDT", "bybit", "BTCUSDT", Decimal("4.0")
        )
        assert "binance" in result

    def test_contains_trigger_ticker(self):
        result = format_auto_close_failed(
            "binance", "BTCUSDT", "bybit", "ETHUSDT", Decimal("4.0")
        )
        assert "BTCUSDT" in result

    def test_contains_close_exchange(self):
        result = format_auto_close_failed(
            "binance", "BTCUSDT", "bybit", "ETHUSDT", Decimal("4.0")
        )
        assert "bybit" in result

    def test_contains_close_ticker(self):
        result = format_auto_close_failed(
            "binance", "BTCUSDT", "bybit", "ETHUSDT", Decimal("4.0")
        )
        assert "ETHUSDT" in result

    def test_contains_manual_intervention_message(self):
        result = format_auto_close_failed(
            "binance", "BTCUSDT", "bybit", "BTCUSDT", Decimal("4.0")
        )
        assert "Manual intervention" in result

    def test_contains_close_amount(self):
        result = format_auto_close_failed(
            "binance", "BTCUSDT", "bybit", "BTCUSDT", Decimal("7.5")
        )
        assert "7.5" in result

    @pytest.mark.parametrize("amount", ["0.001", "100", "9999.99"])
    def test_amount_present_in_output_parametrized(self, amount: str):
        result = format_auto_close_failed(
            "exch_a", "TICKA", "exch_b", "TICKB", Decimal(amount)
        )
        assert amount in result


# ===========================================================================
# 8. _get_structure — находит Structure по бирже+тикеру / возвращает None
# ===========================================================================


class TestGetStructure:

    def test_get_structure_returns_structure_containing_matching_leg(self):
        engine = make_engine_with_two_connectors()
        structure = _make_structure(
            exchange_a="alpha", ticker_a="BTCUSDT",
            exchange_b="beta", ticker_b="BTCUSDT",
        )
        engine._structures = [structure]

        result = engine._get_structure("alpha", "BTCUSDT")

        assert result is structure

    def test_get_structure_returns_structure_for_second_leg(self):
        engine = make_engine_with_two_connectors()
        structure = _make_structure(
            exchange_a="alpha", ticker_a="BTCUSDT",
            exchange_b="beta", ticker_b="ETHUSDT",
        )
        engine._structures = [structure]

        result = engine._get_structure("beta", "ETHUSDT")

        assert result is structure

    def test_get_structure_returns_none_when_exchange_not_matched(self):
        engine = make_engine_with_two_connectors()
        structure = _make_structure(exchange_a="alpha", ticker_a="BTCUSDT")
        engine._structures = [structure]

        result = engine._get_structure("gamma", "BTCUSDT")

        assert result is None

    def test_get_structure_returns_none_when_ticker_not_matched(self):
        engine = make_engine_with_two_connectors()
        structure = _make_structure(exchange_a="alpha", ticker_a="BTCUSDT")
        engine._structures = [structure]

        result = engine._get_structure("alpha", "ETHUSDT")

        assert result is None

    def test_get_structure_returns_none_when_structures_empty(self):
        engine = make_engine_with_two_connectors()
        engine._structures = []

        result = engine._get_structure("alpha", "BTCUSDT")

        assert result is None

    def test_get_structure_returns_correct_structure_among_multiple(self):
        engine = make_engine_with_two_connectors()
        structure_btc = _make_structure(
            exchange_a="alpha", ticker_a="BTCUSDT",
            exchange_b="beta", ticker_b="BTCUSDT",
        )
        structure_eth = Structure(
            legs=[
                StructureLeg(exchange="alpha", ticker="ETHUSDT"),
                StructureLeg(exchange="beta", ticker="ETHUSDT"),
            ],
            is_active=True,
        )
        engine._structures = [structure_btc, structure_eth]

        result = engine._get_structure("alpha", "ETHUSDT")

        assert result is structure_eth


# ===========================================================================
# 9. _check_leg_not_found_alerts — алерт отправляется ровно один раз
# ===========================================================================


class TestCheckLegNotFoundAlerts:
    """
    _check_leg_not_found_alerts создаёт asyncio.Task внутри,
    поэтому тесты должны быть async — pytest-asyncio запускает их в event loop.
    Вместо проверки что alert был отправлен (awaited), проверяем что задача
    была создана через _pending_tasks или что алерт вызывался.
    """

    def _make_engine_with_state(self) -> tuple:
        """
        Возвращает (engine, connector_a).
        State alpha содержит BTCUSDT, ETHUSDT отсутствует.
        """
        engine = make_engine_with_two_connectors()
        connector_a = engine._get_connector("alpha")
        pos_btc = _make_position("BTCUSDT", "alpha", "long", "1.0")
        connector_a.state.positions = {"BTCUSDT": pos_btc}
        engine._send_structure_alert = AsyncMock()
        return engine, connector_a

    async def test_alert_sent_once_when_leg_ticker_not_in_positions(self):
        """Первый вызов _check_leg_not_found_alerts при ненайденной ноге создаёт задачу алерта."""
        engine, _ = self._make_engine_with_state()
        structure = Structure(
            legs=[StructureLeg(exchange="alpha", ticker="ETHUSDT")],
            is_active=True,
        )

        engine._check_leg_not_found_alerts([structure])
        await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        engine._send_structure_alert.assert_awaited_once()

    async def test_alert_not_sent_twice_for_same_missing_leg(self):
        """Повторный вызов при той же ненайденной ноге НЕ должен создавать второй алерт."""
        engine, _ = self._make_engine_with_state()
        structure = Structure(
            legs=[StructureLeg(exchange="alpha", ticker="ETHUSDT")],
            is_active=True,
        )

        engine._check_leg_not_found_alerts([structure])
        await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)
        engine._pending_tasks.clear()

        engine._check_leg_not_found_alerts([structure])
        await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        engine._send_structure_alert.assert_awaited_once()

    async def test_alert_key_added_to_alerted_set_after_first_detection(self):
        engine, _ = self._make_engine_with_state()
        structure = Structure(
            legs=[StructureLeg(exchange="alpha", ticker="ETHUSDT")],
            is_active=True,
        )

        engine._check_leg_not_found_alerts([structure])
        await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        assert "alpha/ETHUSDT" in engine._leg_not_found_alerted

    async def test_alert_key_removed_from_alerted_set_when_leg_appears(self):
        """Если нога появляется в позициях — ключ убирается из alerted."""
        engine, connector_a = self._make_engine_with_state()
        structure = Structure(
            legs=[StructureLeg(exchange="alpha", ticker="ETHUSDT")],
            is_active=True,
        )

        # Первый вызов: ETHUSDT отсутствует — ключ добавляется
        engine._check_leg_not_found_alerts([structure])
        await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)
        assert "alpha/ETHUSDT" in engine._leg_not_found_alerted

        # Добавляем позицию
        pos_eth = _make_position("ETHUSDT", "alpha", "long", "2.0")
        connector_a.state.positions["ETHUSDT"] = pos_eth

        # Второй вызов: нога найдена — ключ убирается
        engine._check_leg_not_found_alerts([structure])
        assert "alpha/ETHUSDT" not in engine._leg_not_found_alerted

    async def test_no_alert_for_found_leg(self):
        """Если тикер есть в позициях — алерт не отправляется."""
        engine, _ = self._make_engine_with_state()
        structure = Structure(
            legs=[StructureLeg(exchange="alpha", ticker="BTCUSDT")],
            is_active=True,
        )

        engine._check_leg_not_found_alerts([structure])
        await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        engine._send_structure_alert.assert_not_awaited()

    async def test_no_alert_for_inactive_structure(self):
        """Для неактивных структур алерты не отправляются."""
        engine, _ = self._make_engine_with_state()
        structure = Structure(
            legs=[StructureLeg(exchange="alpha", ticker="ETHUSDT")],
            is_active=False,
        )

        engine._check_leg_not_found_alerts([structure])
        await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        engine._send_structure_alert.assert_not_awaited()

    async def test_no_alert_when_exchange_state_not_found(self):
        """Если exchange не известен движку — пропускаем без алерта."""
        engine, _ = self._make_engine_with_state()
        structure = Structure(
            legs=[StructureLeg(exchange="unknown_exchange", ticker="BTCUSDT")],
            is_active=True,
        )

        engine._check_leg_not_found_alerts([structure])
        await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        engine._send_structure_alert.assert_not_awaited()


# ===========================================================================
# 10. Расчёт доли (share) с multiplier — несколько ног одной стороны
# ===========================================================================


class TestShareCalculationWithMultiplier:
    """
    Проверяет расчёт доли сокращения с учётом multiplier.

    Structure: alpha/BTCUSDT (long, mult=2) + alpha/ETHUSDT (long, mult=1)
               vs beta/XRPUSDT (short, mult=1).

    alpha/BTCUSDT уменьшается с 10 до 6 (delta=4).
    real_delta = 4 * 2 = 8.
    total_real_same_side = 10 * 2 + 5 * 1 = 25.
    share = 8 / 25 = 0.32.
    close_real_y = 20 * 1 * 0.32 = 6.4  →  close_exchange_units = 6.4 / 1 = 6.4.
    """

    def _make_engine_multileg(self):
        """
        Engine с тремя коннекторами: alpha (два тикера), beta.
        Используем AlphaConnector + BetaConnector.
        """
        from app.engine.engine import MonitoringEngine
        from unittest.mock import patch

        with patch("pkgutil.iter_modules", return_value=[]):
            with patch.object(
                    BaseExchangeConnector,
                    "__subclasses__",
                    return_value=[AlphaConnector, BetaConnector],
            ):
                engine = MonitoringEngine()

        engine._send_reduction_alert = AsyncMock()
        engine._send_reduction_alert_raw = AsyncMock()
        engine._send_critical_alert = AsyncMock()
        engine._send_structure_alert = AsyncMock()
        return engine

    async def test_share_calculated_with_multileg_same_side(self):
        """
        Структура: alpha/BTC (long, mult=2) + alpha/ETH (long, mult=1) vs beta/XRP (short, mult=1).
        BTC сокращается с 10 до 6 (delta=4).

        _on_positions_updated делает shallow copy prev_snapshot ПЕРЕД обновлением снимка,
        поэтому _handle_structure_reduction получает старые amounts.

        Ожидаемое поведение:
          prev_snapshot["alpha"]["BTC"] == 10 (старый снимок)
          delta_real = (10-6)*2 = 8
          total_real_same = 10*2 + 5*1 = 25  (оба берутся из старого снимка)
          share = 8/25
          close_XRP = 20 * 1 * (8/25) / 1 = 160/25 = 6.4
        """
        engine = self._make_engine_multileg()
        connector_a = engine._get_connector("alpha")
        connector_b = engine._get_connector("beta")

        structure = Structure(
            legs=[
                StructureLeg(exchange="alpha", ticker="BTCUSDT", multiplier=Decimal("2")),
                StructureLeg(exchange="alpha", ticker="ETHUSDT", multiplier=Decimal("1")),
                StructureLeg(exchange="beta", ticker="XRPUSDT", multiplier=Decimal("1")),
            ],
            is_active=True,
        )
        engine._structures = [structure]

        connector_a.state.positions = {
            "BTCUSDT": _make_position("BTCUSDT", "alpha", "long", "6.0"),
            "ETHUSDT": _make_position("ETHUSDT", "alpha", "long", "5.0"),
        }
        connector_b.state.positions = {
            "XRPUSDT": _make_position("XRPUSDT", "beta", "short", "20.0"),
        }
        engine._prev_amounts["alpha"] = {
            "BTCUSDT": Decimal("10.0"),
            "ETHUSDT": Decimal("5.0"),
        }
        engine._prev_amounts["beta"] = {"XRPUSDT": Decimal("20.0")}

        captured_calls: list[dict] = []

        async def _mock_auto_close(**kwargs):
            captured_calls.append(kwargs)

        engine._auto_close_structure_leg = _mock_auto_close

        with patch("app.engine.engine.READ_ONLY_MODE", False):
            await engine._on_positions_updated(connector_a)
            await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        assert len(captured_calls) == 1
        call = captured_calls[0]
        assert call["leg"].exchange == "beta"
        assert call["leg"].ticker == "XRPUSDT"
        # Движок передаёт prev_snapshot (shallow copy) в _handle_structure_reduction.
        # prev_snapshot["alpha"] указывает на старый inner dict: BTC=10, ETH=5.
        # total_real_same = 10*2 + 5*1 = 25; share = 8/25; close = 20 * 8/25 = 6.4
        expected = Decimal("20") * Decimal("8") / Decimal("25")
        assert call["close_exchange_units"] == expected

    @pytest.mark.parametrize(
        "mult_a,mult_b,prev_a,new_a,prev_b",
        [
            # mult=1 для обоих: share = delta / prev_a; close = prev_b * delta / prev_a
            # delta=4, prev_a=10: close = 10 * 4/10 = 4
            ("1", "1", "10", "6", "10"),
            # mult_a=2: delta_real=4*2=8; total_real_same=prev_a*mult_a=10*2=20
            # share=8/20=0.4; close_real=10*1*0.4=4; close_exchange=4/1=4
            ("2", "1", "10", "6", "10"),
            # mult_b=2: share=4/10=0.4; close_real=10*2*0.4=8
            # close_exchange=8/2=4
            ("1", "2", "10", "6", "10"),
        ],
    )
    async def test_share_with_multiplier_parametrized(
            self,
            mult_a: str,
            mult_b: str,
            prev_a: str,
            new_a: str,
            prev_b: str,
    ):
        """
        Параметрический тест расчёта close_exchange_units при разных multiplier.

        _on_positions_updated делает shallow copy prev_snapshot перед обновлением снимка.
        Поэтому _handle_structure_reduction всегда получает старые (prev) значения amounts.
        share = delta_real / total_real_same, где total_real_same = prev_a * mult_a.

        Ожидаемое значение вычисляется через то же выражение что использует движок,
        чтобы избежать расхождений из-за точности Decimal.
        """
        engine = self._make_engine_multileg()
        connector_a = engine._get_connector("alpha")
        connector_b = engine._get_connector("beta")

        d_prev_a = Decimal(prev_a)
        d_new_a = Decimal(new_a)
        d_prev_b = Decimal(prev_b)
        d_mult_a = Decimal(mult_a)
        d_mult_b = Decimal(mult_b)

        structure = Structure(
            legs=[
                StructureLeg(exchange="alpha", ticker="BTCUSDT", multiplier=d_mult_a),
                StructureLeg(exchange="beta", ticker="XRPUSDT", multiplier=d_mult_b),
            ],
            is_active=True,
        )
        engine._structures = [structure]

        connector_a.state.positions = {
            "BTCUSDT": _make_position("BTCUSDT", "alpha", "long", new_a),
        }
        connector_b.state.positions = {
            "XRPUSDT": _make_position("XRPUSDT", "beta", "short", prev_b),
        }
        engine._prev_amounts["alpha"] = {"BTCUSDT": d_prev_a}
        engine._prev_amounts["beta"] = {"XRPUSDT": d_prev_b}

        captured_calls: list[dict] = []

        async def _mock_auto_close(**kwargs):
            captured_calls.append(kwargs)

        engine._auto_close_structure_leg = _mock_auto_close

        with patch("app.engine.engine.READ_ONLY_MODE", False):
            await engine._on_positions_updated(connector_a)
            await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        assert len(captured_calls) == 1
        # Движок передаёт prev_snapshot (shallow copy) в _handle_structure_reduction.
        # prev_snapshot[trigger_exchange] указывает на СТАРЫЙ inner dict (prev_a).
        # delta_real = (prev_a - new_a) * mult_a
        # total_real_same = prev_a * mult_a  (старый снимок)
        # share = delta_real / total_real_same = (prev_a - new_a) / prev_a
        # close_real_y = prev_b * mult_b * share
        # close_exchange = close_real_y / mult_b = prev_b * (prev_a - new_a) / prev_a
        delta_real = (d_prev_a - d_new_a) * d_mult_a
        total_real_same = d_prev_a * d_mult_a
        share = delta_real / total_real_same
        real_amount_y = d_prev_b * d_mult_b
        close_real_y = real_amount_y * share
        expected = close_real_y / d_mult_b
        assert captured_calls[0]["close_exchange_units"] == expected


# ===========================================================================
# 11. Параллельное закрытие нескольких ног через asyncio.gather
# ===========================================================================


class TestParallelAutoClose:
    """
    Проверяет что при нескольких ногах противоположной стороны
    close_position вызывается для каждой параллельно (через asyncio.gather).
    """

    def _make_engine_three_legs(self):
        from app.engine.engine import MonitoringEngine

        with patch("pkgutil.iter_modules", return_value=[]):
            with patch.object(
                    BaseExchangeConnector,
                    "__subclasses__",
                    return_value=[AlphaConnector, BetaConnector],
            ):
                engine = MonitoringEngine()

        engine._send_reduction_alert = AsyncMock()
        engine._send_reduction_alert_raw = AsyncMock()
        engine._send_critical_alert = AsyncMock()
        engine._send_structure_alert = AsyncMock()
        return engine

    async def test_parallel_close_called_for_each_opposite_leg(self):
        """
        Structure: alpha/BTC (long) vs beta/ETH (short) + beta/XRP (short).
        При сокращении alpha/BTC оба шортовых плеча должны быть закрыты.
        """
        engine = self._make_engine_three_legs()
        connector_a = engine._get_connector("alpha")
        connector_b = engine._get_connector("beta")

        structure = Structure(
            legs=[
                StructureLeg(exchange="alpha", ticker="BTCUSDT"),
                StructureLeg(exchange="beta", ticker="ETHUSDT"),
                StructureLeg(exchange="beta", ticker="XRPUSDT"),
            ],
            is_active=True,
        )
        engine._structures = [structure]

        connector_a.state.positions = {
            "BTCUSDT": _make_position("BTCUSDT", "alpha", "long", "6.0"),
        }
        connector_b.state.positions = {
            "ETHUSDT": _make_position("ETHUSDT", "beta", "short", "10.0"),
            "XRPUSDT": _make_position("XRPUSDT", "beta", "short", "10.0"),
        }
        engine._prev_amounts["alpha"] = {"BTCUSDT": Decimal("10.0")}
        engine._prev_amounts["beta"] = {
            "ETHUSDT": Decimal("10.0"),
            "XRPUSDT": Decimal("10.0"),
        }

        captured_calls: list[dict] = []

        async def _mock_auto_close(**kwargs):
            captured_calls.append(kwargs)

        engine._auto_close_structure_leg = _mock_auto_close

        with patch("app.engine.engine.READ_ONLY_MODE", False):
            await engine._on_positions_updated(connector_a)
            await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        closed_tickers = {c["leg"].ticker for c in captured_calls}
        assert "ETHUSDT" in closed_tickers
        assert "XRPUSDT" in closed_tickers
        assert len(captured_calls) == 2

    async def test_parallel_close_both_legs_receive_correct_amount(self):
        """
        Оба закрытия должны получить одинаковую долю от своих объёмов.

        BTC сокращается с 10 до 6 (delta=4, mult=1).
        prev_snapshot["alpha"] указывает на старый inner dict (BTC prev=10):
          total_real_same = 10, share = 4/10 = 0.4
          close_ETH = 10 * 0.4 = 4, close_XRP = 10 * 0.4 = 4
        """
        engine = self._make_engine_three_legs()
        connector_a = engine._get_connector("alpha")
        connector_b = engine._get_connector("beta")

        structure = Structure(
            legs=[
                StructureLeg(exchange="alpha", ticker="BTCUSDT"),
                StructureLeg(exchange="beta", ticker="ETHUSDT"),
                StructureLeg(exchange="beta", ticker="XRPUSDT"),
            ],
            is_active=True,
        )
        engine._structures = [structure]

        connector_a.state.positions = {
            "BTCUSDT": _make_position("BTCUSDT", "alpha", "long", "6.0"),
        }
        connector_b.state.positions = {
            "ETHUSDT": _make_position("ETHUSDT", "beta", "short", "10.0"),
            "XRPUSDT": _make_position("XRPUSDT", "beta", "short", "10.0"),
        }
        engine._prev_amounts["alpha"] = {"BTCUSDT": Decimal("10.0")}
        engine._prev_amounts["beta"] = {
            "ETHUSDT": Decimal("10.0"),
            "XRPUSDT": Decimal("10.0"),
        }

        captured_calls: list[dict] = []

        async def _mock_auto_close(**kwargs):
            captured_calls.append(kwargs)

        engine._auto_close_structure_leg = _mock_auto_close

        with patch("app.engine.engine.READ_ONLY_MODE", False):
            await engine._on_positions_updated(connector_a)
            await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        # Движок использует prev_snapshot["alpha"] = старый dict (BTC prev=10).
        # total_real_same = 10, share = delta / total_real_same = 4/10 = 0.4
        # close_ETH = 10 * 0.4 = 4, close_XRP = 10 * 0.4 = 4
        expected = Decimal("10") * Decimal("4") / Decimal("10")
        by_ticker = {c["leg"].ticker: c["close_exchange_units"] for c in captured_calls}
        assert by_ticker["ETHUSDT"] == expected
        assert by_ticker["XRPUSDT"] == expected

    async def test_read_only_mode_blocks_all_parallel_closes(self):
        """READ_ONLY_MODE=True блокирует закрытие всех ног."""
        engine = self._make_engine_three_legs()
        connector_a = engine._get_connector("alpha")
        connector_b = engine._get_connector("beta")

        structure = Structure(
            legs=[
                StructureLeg(exchange="alpha", ticker="BTCUSDT"),
                StructureLeg(exchange="beta", ticker="ETHUSDT"),
                StructureLeg(exchange="beta", ticker="XRPUSDT"),
            ],
            is_active=True,
        )
        engine._structures = [structure]

        connector_a.state.positions = {
            "BTCUSDT": _make_position("BTCUSDT", "alpha", "long", "6.0"),
        }
        connector_b.state.positions = {
            "ETHUSDT": _make_position("ETHUSDT", "beta", "short", "10.0"),
            "XRPUSDT": _make_position("XRPUSDT", "beta", "short", "10.0"),
        }
        engine._prev_amounts["alpha"] = {"BTCUSDT": Decimal("10.0")}
        engine._prev_amounts["beta"] = {
            "ETHUSDT": Decimal("10.0"),
            "XRPUSDT": Decimal("10.0"),
        }

        connector_b.close_position = AsyncMock()

        with patch("app.engine.engine.READ_ONLY_MODE", True):
            await engine._on_positions_updated(connector_a)
            await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        connector_b.close_position.assert_not_called()


# ===========================================================================
# 12. Расчёт share с multiplier — воспроизведение описанного дефекта prev_amounts
# ===========================================================================


class TestPrevAmountsShareWithMultiplier:
    """
    Воспроизводящий тест для дефекта расчёта share с multiplier.

    Structure: alpha/BTC (mult=2, prev=10) vs beta/XRP (mult=1, prev=20).
    BTC сокращается: 10 -> 6, delta=4 exchange = 4*2=8 real.
    total_real_longs (prev) = 10*2 = 20.
    share = 8/20 = 0.4.
    close_real_xrp = 20 * 1 * 0.4 = 8.
    close_exchange_xrp = 8 / 1 = 8.
    """

    def _make_engine(self):
        from app.engine.engine import MonitoringEngine

        with patch("pkgutil.iter_modules", return_value=[]):
            with patch.object(
                    BaseExchangeConnector,
                    "__subclasses__",
                    return_value=[AlphaConnector, BetaConnector],
            ):
                engine = MonitoringEngine()

        engine._send_reduction_alert = AsyncMock()
        engine._send_reduction_alert_raw = AsyncMock()
        engine._send_critical_alert = AsyncMock()
        engine._send_structure_alert = AsyncMock()
        return engine

    async def test_share_uses_prev_amounts_with_multiplier(self):
        """
        alpha/BTC (mult=2, prev=10) сокращается до 6 vs beta/XRP (mult=1, prev=20).

        delta_exchange = 10 - 6 = 4
        delta_real     = 4 * 2 = 8
        total_real_longs (prev) = 10 * 2 = 20
        share = 8 / 20 = 0.4
        close_real_xrp = 20 * 1 * 0.4 = 8
        close_exchange_xrp = 8 / 1 = 8
        """
        engine = self._make_engine()
        connector_a = engine._get_connector("alpha")
        connector_b = engine._get_connector("beta")

        from app.engine.model.structure import Structure, StructureLeg

        structure = Structure(
            legs=[
                StructureLeg(exchange="alpha", ticker="BTCUSDT", multiplier=Decimal("2")),
                StructureLeg(exchange="beta", ticker="XRPUSDT", multiplier=Decimal("1")),
            ],
            is_active=True,
        )
        engine._structures = [structure]

        connector_a.state.positions = {
            "BTCUSDT": _make_position("BTCUSDT", "alpha", "long", "6"),
        }
        connector_b.state.positions = {
            "XRPUSDT": _make_position("XRPUSDT", "beta", "short", "20"),
        }
        engine._prev_amounts["alpha"] = {"BTCUSDT": Decimal("10")}
        engine._prev_amounts["beta"] = {"XRPUSDT": Decimal("20")}

        captured_calls: list[dict] = []

        async def _mock_auto_close(**kwargs):
            captured_calls.append(kwargs)

        engine._auto_close_structure_leg = _mock_auto_close

        with patch("app.engine.engine.READ_ONLY_MODE", False):
            await engine._on_positions_updated(connector_a)
            await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)

        assert len(captured_calls) == 1
        call = captured_calls[0]
        assert call["leg"].exchange == "beta"
        assert call["leg"].ticker == "XRPUSDT"
        assert call["close_exchange_units"] == Decimal("8")
