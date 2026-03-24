"""
Тесты для place_order, close_position и _verify_position_changed.

Покрываемые области:
  1. place_order — ValueError при limit без цены, вызов _verify_position_changed для market,
     возврат True/False, NotImplementedError у VariationalConnector
  2. close_position — ValueError при limit без цены, корректная передача параметров,
     VariationalConnector принимает сигнатуру без ошибок
  3. _verify_position_changed — ровно 2 попытки с интервалом 2 с, возврат True/False
  4. Движок — close_position(False) → Telegram-алерт, order_type="market" передаётся в close_position
"""
import asyncio
from decimal import Decimal
from typing import Literal
from unittest.mock import AsyncMock, patch

import pytest

from app.connectors.base import BaseExchangeConnector
from app.connectors.config import ConnectorConfig
from app.connectors.model.position import Position
from app.engine.model.structure import StructureLeg
from tests.unit.conftest import make_position


# ---------------------------------------------------------------------------
# Тестовый дубль коннектора с полной сигнатурой
# ---------------------------------------------------------------------------

class FakeOrderConnector(BaseExchangeConnector):
    """Дубль коннектора для тестирования place_order / close_position."""

    name = "fake_order"
    config = ConnectorConfig(positions_interval=0.0, margin_interval=0.0)

    async def fetch_positions(self) -> list[Position]:
        raise NotImplementedError

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        return Decimal(0), Decimal(0)

    async def place_order(
            self,
            ticker: str,
            direction: Literal["long", "short"],
            amount: Decimal,
            order_type: Literal["market", "limit"] = "limit",
            limit_price: Decimal | None = None,
    ) -> bool:
        if order_type == "limit" and limit_price is None:
            raise ValueError("limit_price required for limit orders")
        snapshot = await self.fetch_positions()
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
        if order_type == "market":
            return await self._verify_position_changed(ticker, snapshot)
        return True


class VariationalConnector(BaseExchangeConnector):
    """Дубль коннектора у которого place_order и close_position не реализованы."""

    name = "variational"
    config = ConnectorConfig(positions_interval=0.0, margin_interval=0.0)

    async def fetch_positions(self) -> list[Position]:
        return []

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        return Decimal(0), Decimal(0)

    async def place_order(self, ticker, direction, amount, order_type="market", limit_price=None) -> bool:
        raise NotImplementedError

    async def close_position(self, ticker, amount, order_type="market", limit_price=None) -> bool:
        raise NotImplementedError


def _make_variational() -> VariationalConnector:
    return VariationalConnector()


# ===========================================================================
# Тесты: place_order
# ===========================================================================

class TestPlaceOrderValidation:

    def setup_method(self):
        self.connector = FakeOrderConnector()

    async def test_place_order_limit_without_price_raises_value_error(self):
        with pytest.raises(ValueError, match="limit_price"):
            await self.connector.place_order(
                ticker="BTCUSDT",
                direction="long",
                amount=Decimal("1"),
                order_type="limit",
                limit_price=None,
            )

    async def test_place_order_market_without_price_does_not_raise(self):
        self.connector.fetch_positions = AsyncMock(return_value=[])
        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            # _verify_position_changed вернёт False (позиций нет до и после)
            self.connector.fetch_positions = AsyncMock(return_value=[])
            # Не должно бросать исключений
            result = await self.connector.place_order(
                ticker="BTCUSDT",
                direction="long",
                amount=Decimal("1"),
                order_type="market",
                limit_price=None,
            )
        assert isinstance(result, bool)

    async def test_place_order_limit_with_price_returns_true(self):
        self.connector.fetch_positions = AsyncMock(return_value=[])
        result = await self.connector.place_order(
            ticker="BTCUSDT",
            direction="long",
            amount=Decimal("1"),
            order_type="limit",
            limit_price=Decimal("50000"),
        )
        assert result is True

    async def test_place_order_market_calls_verify_position_changed(self):
        snapshot = [make_position(ticker="BTCUSDT", amount="1.0")]
        # первый вызов — snapshot, последующие — fresh (после sleep)
        self.connector.fetch_positions = AsyncMock(
            side_effect=[snapshot, snapshot, snapshot]
        )
        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            await self.connector.place_order(
                ticker="BTCUSDT",
                direction="long",
                amount=Decimal("1"),
                order_type="market",
            )
        # fetch_positions должен быть вызван: 1 (snapshot) + 2 (попытки) = 3 раза
        assert self.connector.fetch_positions.await_count == 3

    async def test_place_order_market_returns_true_when_position_changed(self):
        snapshot = [make_position(ticker="BTCUSDT", amount="1.0")]
        fresh = [make_position(ticker="BTCUSDT", amount="2.0")]
        self.connector.fetch_positions = AsyncMock(
            side_effect=[snapshot, fresh]
        )
        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            result = await self.connector.place_order(
                ticker="BTCUSDT",
                direction="long",
                amount=Decimal("1"),
                order_type="market",
            )
        assert result is True

    async def test_place_order_market_returns_false_when_position_unchanged(self):
        snapshot = [make_position(ticker="BTCUSDT", amount="1.0")]
        self.connector.fetch_positions = AsyncMock(
            side_effect=[snapshot, snapshot, snapshot]
        )
        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            result = await self.connector.place_order(
                ticker="BTCUSDT",
                direction="long",
                amount=Decimal("1"),
                order_type="market",
            )
        assert result is False


class TestPlaceOrderVariational:

    async def test_variational_place_order_raises_not_implemented(self):
        connector = _make_variational()
        with pytest.raises(NotImplementedError):
            await connector.place_order(
                ticker="BTCUSDT",
                direction="long",
                amount=Decimal("1"),
                order_type="market",
            )

    async def test_variational_place_order_limit_raises_not_implemented(self):
        """NotImplementedError должен подниматься даже при корректном limit_price."""
        connector = _make_variational()
        with pytest.raises(NotImplementedError):
            await connector.place_order(
                ticker="BTCUSDT",
                direction="short",
                amount=Decimal("0.5"),
                order_type="limit",
                limit_price=Decimal("50000"),
            )


# ===========================================================================
# Тесты: close_position
# ===========================================================================

class TestClosePositionValidation:

    def setup_method(self):
        self.connector = FakeOrderConnector()

    async def test_close_position_limit_without_price_raises_value_error(self):
        with pytest.raises(ValueError, match="limit_price"):
            await self.connector.close_position(
                ticker="BTCUSDT",
                amount=Decimal("1"),
                order_type="limit",
                limit_price=None,
            )

    async def test_close_position_market_without_price_does_not_raise(self):
        self.connector.fetch_positions = AsyncMock(return_value=[])
        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            result = await self.connector.close_position(
                ticker="BTCUSDT",
                amount=Decimal("1"),
                order_type="market",
                limit_price=None,
            )
        assert isinstance(result, bool)

    async def test_close_position_limit_with_price_returns_true(self):
        self.connector.fetch_positions = AsyncMock(return_value=[])
        result = await self.connector.close_position(
            ticker="BTCUSDT",
            amount=Decimal("1"),
            order_type="limit",
            limit_price=Decimal("48000"),
        )
        assert result is True

    async def test_close_position_passes_amount_parameter(self):
        """amount передаётся корректно — не поднимается TypeError при вызове."""
        self.connector.fetch_positions = AsyncMock(return_value=[])
        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            # Должно выполниться без ошибок
            await self.connector.close_position(
                ticker="ETHUSDT",
                amount=Decimal("0.25"),
                order_type="market",
            )

    async def test_close_position_passes_order_type_parameter(self):
        """order_type='limit' не вызывает TypeError при передаче."""
        self.connector.fetch_positions = AsyncMock(return_value=[])
        result = await self.connector.close_position(
            ticker="BTCUSDT",
            amount=Decimal("1"),
            order_type="limit",
            limit_price=Decimal("50000"),
        )
        assert result is True

    async def test_close_position_default_order_type_is_market(self):
        """По умолчанию order_type='market' → вызывается _verify_position_changed."""
        snapshot = [make_position(ticker="BTCUSDT", amount="1.0")]
        fresh = [make_position(ticker="BTCUSDT", amount="0.5")]
        self.connector.fetch_positions = AsyncMock(side_effect=[snapshot, fresh])
        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            result = await self.connector.close_position(
                ticker="BTCUSDT",
                amount=Decimal("0.5"),
            )
        assert result is True

    async def test_close_position_market_returns_false_when_unchanged(self):
        snapshot = [make_position(ticker="BTCUSDT", amount="1.0")]
        self.connector.fetch_positions = AsyncMock(
            side_effect=[snapshot, snapshot, snapshot]
        )
        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            result = await self.connector.close_position(
                ticker="BTCUSDT",
                amount=Decimal("1"),
                order_type="market",
            )
        assert result is False


class TestClosePositionVariational:

    async def test_variational_close_position_raises_not_implemented(self):
        connector = _make_variational()
        with pytest.raises(NotImplementedError):
            await connector.close_position(
                ticker="BTCUSDT",
                amount=Decimal("1"),
                order_type="market",
            )

    async def test_variational_close_position_accepts_new_signature(self):
        """Новая сигнатура (ticker, amount, order_type, limit_price) принимается без TypeError."""
        connector = _make_variational()
        # TypeError при неверной сигнатуре возникает до вызова тела метода,
        # поэтому NotImplementedError означает, что сигнатура принята корректно
        with pytest.raises(NotImplementedError):
            await connector.close_position(
                ticker="BTCUSDT",
                amount=Decimal("0.5"),
                order_type="limit",
                limit_price=Decimal("50000"),
            )


# ===========================================================================
# Тесты: _verify_position_changed
# ===========================================================================

class TestVerifyPositionChanged:

    def setup_method(self):
        self.connector = FakeOrderConnector()

    async def test_verify_makes_exactly_two_fetch_calls_when_unchanged(self):
        """Если позиция не менялась — делается ровно 2 вызова fetch_positions."""
        snapshot = [make_position(ticker="BTCUSDT", amount="1.0")]
        self.connector.fetch_positions = AsyncMock(return_value=snapshot)

        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await self.connector._verify_position_changed("BTCUSDT", snapshot)

        assert self.connector.fetch_positions.await_count == 2
        assert result is False

    async def test_verify_sleeps_twice_with_two_seconds(self):
        """_verify_position_changed вызывает asyncio.sleep(2) ровно 2 раза."""
        snapshot = [make_position(ticker="BTCUSDT", amount="1.0")]
        self.connector.fetch_positions = AsyncMock(return_value=snapshot)

        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await self.connector._verify_position_changed("BTCUSDT", snapshot)

        assert mock_sleep.await_count == 2
        mock_sleep.assert_awaited_with(2)

    async def test_verify_returns_true_on_first_attempt_when_changed(self):
        """Если позиция изменилась на первой попытке — возвращает True сразу."""
        snapshot = [make_position(ticker="BTCUSDT", amount="1.0")]
        fresh_changed = [make_position(ticker="BTCUSDT", amount="0.5")]

        self.connector.fetch_positions = AsyncMock(
            side_effect=[fresh_changed]  # первая попытка
        )

        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            result = await self.connector._verify_position_changed("BTCUSDT", snapshot)

        assert result is True
        # Сразу вернул True после первой попытки — второго вызова fetch не было
        assert self.connector.fetch_positions.await_count == 1

    async def test_verify_returns_true_on_second_attempt_when_changed(self):
        """Если на первой попытке изменений нет, а на второй — есть, возвращает True."""
        snapshot = [make_position(ticker="BTCUSDT", amount="1.0")]
        fresh_unchanged = [make_position(ticker="BTCUSDT", amount="1.0")]
        fresh_changed = [make_position(ticker="BTCUSDT", amount="0.3")]

        self.connector.fetch_positions = AsyncMock(
            side_effect=[fresh_unchanged, fresh_changed]
        )

        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            result = await self.connector._verify_position_changed("BTCUSDT", snapshot)

        assert result is True
        assert self.connector.fetch_positions.await_count == 2

    async def test_verify_returns_false_when_no_attempt_confirms_change(self):
        """Если обе попытки не зафиксировали изменение — возвращает False."""
        snapshot = [make_position(ticker="BTCUSDT", amount="1.0")]
        self.connector.fetch_positions = AsyncMock(return_value=snapshot)

        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            result = await self.connector._verify_position_changed("BTCUSDT", snapshot)

        assert result is False

    async def test_verify_returns_true_when_position_disappears(self):
        """Если позиция исчезла (amount=0 через отсутствие в списке) — изменение засчитывается."""
        snapshot = [make_position(ticker="BTCUSDT", amount="1.0")]
        # После закрытия позиция отсутствует в списке → amount=0 != 1.0
        self.connector.fetch_positions = AsyncMock(return_value=[])

        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            result = await self.connector._verify_position_changed("BTCUSDT", snapshot)

        assert result is True

    async def test_verify_returns_false_for_unknown_ticker_in_snapshot(self):
        """Если тикера нет ни в snapshot, ни в fresh — оба amount=0, изменений нет."""
        snapshot = []  # тикера нет в снапшоте → snapshot_amount=0
        # fresh тоже без тикера → fresh_amount=0
        self.connector.fetch_positions = AsyncMock(return_value=[])

        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            result = await self.connector._verify_position_changed("XYZUSDT", snapshot)

        assert result is False


# ===========================================================================
# Тесты: движок — close_position(False) → Telegram-алерт, order_type="market"
# ===========================================================================

class AlphaConnector(BaseExchangeConnector):
    """Дубль коннектора для тестов движка."""

    name = "alpha"
    config = ConnectorConfig(positions_interval=0.0, margin_interval=0.0)

    async def fetch_positions(self) -> list[Position]:
        return []

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        return Decimal(0), Decimal(0)

    async def place_order(
            self,
            ticker: str,
            direction: Literal["long", "short"],
            amount: Decimal,
            order_type: Literal["market", "limit"] = "limit",
            limit_price: Decimal | None = None,
    ) -> bool:
        return True

    async def close_position(
            self,
            ticker: str,
            amount: Decimal,
            order_type: Literal["market", "limit"] = "market",
            limit_price: Decimal | None = None,
    ) -> bool:
        return True


def _make_engine_with_alpha():
    """Создаёт MonitoringEngine только с AlphaConnector (без реальных коннекторов)."""
    from app.engine.engine import MonitoringEngine
    with patch("pkgutil.iter_modules", return_value=[]):
        with patch.object(
                BaseExchangeConnector,
                "__subclasses__",
                return_value=[AlphaConnector],
        ):
            engine = MonitoringEngine()
    return engine


async def _drain_pending_tasks(engine) -> None:
    """Ожидает завершения всех pending_tasks движка, дренируя event loop."""
    # Даём event loop несколько тиков, чтобы create_task-задачи успели запуститься
    for _ in range(5):
        await asyncio.sleep(0)
    # Ждём все зарегистрированные pending_tasks
    if engine._pending_tasks:
        await asyncio.gather(*list(engine._pending_tasks), return_exceptions=True)


class TestEngineAutoCloseAlert:

    async def test_auto_close_sends_critical_alert_when_close_position_returns_false(self):
        """
        Если close_position возвращает False (ордер не принят биржей),
        движок должен вызвать _send_critical_alert (через create_task).
        """
        engine = _make_engine_with_alpha()
        connector = engine._connectors[0]

        connector.close_position = AsyncMock(return_value=False)
        # fetch_positions не должен вызываться после False — но мокируем на всякий случай
        connector.fetch_positions = AsyncMock(return_value=[
            make_position(ticker="BTCUSDT", amount="1.0", exchange_name="alpha")
        ])

        critical_alert_called = []

        async def capture_critical(msg: str) -> None:
            critical_alert_called.append(msg)

        engine._send_critical_alert = capture_critical

        leg = StructureLeg(exchange="alpha", ticker="BTCUSDT", multiplier=Decimal("1"))
        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="ETHUSDT",
            leg=leg,
            close_exchange_units=Decimal("0.5"),
            amount_before=Decimal("1.0"),
        )
        await _drain_pending_tasks(engine)

        # _send_critical_alert вызван хотя бы раз (внутри первой попытки при False)
        assert len(critical_alert_called) >= 1

    async def test_auto_close_passes_market_order_type_to_close_position(self):
        """
        _auto_close_structure_leg должен вызывать close_position с order_type='market'.
        """
        engine = _make_engine_with_alpha()
        connector = engine._connectors[0]

        connector.close_position = AsyncMock(return_value=True)
        # Позиция закрыта → amount_after=0 ≤ success_threshold → подтверждено
        connector.fetch_positions = AsyncMock(return_value=[])

        engine._send_reduction_alert_raw = AsyncMock()

        leg = StructureLeg(exchange="alpha", ticker="BTCUSDT", multiplier=Decimal("1"))
        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="ETHUSDT",
            leg=leg,
            close_exchange_units=Decimal("0.5"),
            amount_before=Decimal("1.0"),
        )

        connector.close_position.assert_awaited_once_with(
            "BTCUSDT",
            Decimal("0.5"),
            order_type="market",
        )

    async def test_auto_close_sends_critical_alert_when_both_attempts_fail_verification(self):
        """
        Если close_position возвращает True, но позиция не уменьшается —
        обе попытки провалены → _send_critical_alert (через create_task в конце метода).
        """
        engine = _make_engine_with_alpha()
        connector = engine._connectors[0]

        # close_position принят биржей (True), но позиция не изменилась
        connector.close_position = AsyncMock(return_value=True)
        # amount_after = 1.0, success_threshold = (1.0 - 0.5) / 0.995 ≈ 0.5025
        # 1.0 > 0.5025 → попытка не подтверждена
        connector.fetch_positions = AsyncMock(return_value=[
            make_position(ticker="BTCUSDT", amount="1.0", exchange_name="alpha")
        ])

        critical_alert_called = []
        reduction_raw_called = []

        async def capture_critical(msg: str) -> None:
            critical_alert_called.append(msg)

        async def capture_reduction_raw(msg: str) -> None:
            reduction_raw_called.append(msg)

        engine._send_critical_alert = capture_critical
        engine._send_reduction_alert_raw = capture_reduction_raw

        leg = StructureLeg(exchange="alpha", ticker="BTCUSDT", multiplier=Decimal("1"))
        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="ETHUSDT",
            leg=leg,
            close_exchange_units=Decimal("0.5"),
            amount_before=Decimal("1.0"),
        )
        await _drain_pending_tasks(engine)

        assert len(critical_alert_called) >= 1
        assert len(reduction_raw_called) == 0

    async def test_auto_close_does_not_send_critical_alert_when_verified_on_first_attempt(self):
        """
        Если close_position возвращает True и позиция уменьшилась —
        _send_critical_alert НЕ вызывается, вызывается _send_reduction_alert_raw.
        """
        engine = _make_engine_with_alpha()
        connector = engine._connectors[0]

        connector.close_position = AsyncMock(return_value=True)
        # amount_after = 0 (позиция закрыта) → success
        connector.fetch_positions = AsyncMock(return_value=[])

        critical_alert_called = []
        reduction_raw_called = []

        async def capture_critical(msg: str) -> None:
            critical_alert_called.append(msg)

        async def capture_reduction_raw(msg: str) -> None:
            reduction_raw_called.append(msg)

        engine._send_critical_alert = capture_critical
        engine._send_reduction_alert_raw = capture_reduction_raw

        leg = StructureLeg(exchange="alpha", ticker="BTCUSDT", multiplier=Decimal("1"))
        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="ETHUSDT",
            leg=leg,
            close_exchange_units=Decimal("0.5"),
            amount_before=Decimal("1.0"),
        )
        await _drain_pending_tasks(engine)

        assert len(critical_alert_called) == 0
        assert len(reduction_raw_called) == 1

    async def test_auto_close_makes_second_attempt_when_first_not_verified(self):
        """
        Если первая попытка close_position прошла (True) но не подтверждена —
        делается вторая попытка (close_position вызван дважды).
        """
        engine = _make_engine_with_alpha()
        connector = engine._connectors[0]

        connector.close_position = AsyncMock(return_value=True)
        # Первый fetch после первого close — позиция не изменилась (не подтверждено)
        # Второй fetch после второго close — позиция закрыта (подтверждено)
        connector.fetch_positions = AsyncMock(side_effect=[
            [make_position(ticker="BTCUSDT", amount="1.0", exchange_name="alpha")],
            [],
        ])

        reduction_raw_called = []
        critical_alert_called = []

        async def capture_reduction_raw(msg: str) -> None:
            reduction_raw_called.append(msg)

        async def capture_critical(msg: str) -> None:
            critical_alert_called.append(msg)

        engine._send_reduction_alert_raw = capture_reduction_raw
        engine._send_critical_alert = capture_critical

        leg = StructureLeg(exchange="alpha", ticker="BTCUSDT", multiplier=Decimal("1"))
        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="ETHUSDT",
            leg=leg,
            close_exchange_units=Decimal("0.5"),
            amount_before=Decimal("1.0"),
        )
        await _drain_pending_tasks(engine)

        assert connector.close_position.await_count == 2
        assert len(reduction_raw_called) == 1
        assert len(critical_alert_called) == 0

    async def test_auto_close_critical_alert_message_contains_exchange_and_ticker(self):
        """
        Сообщение критического алерта при close_position=False содержит имя биржи и тикер.
        """
        engine = _make_engine_with_alpha()
        connector = engine._connectors[0]

        connector.close_position = AsyncMock(return_value=False)
        connector.fetch_positions = AsyncMock(return_value=[
            make_position(ticker="BTCUSDT", amount="1.0", exchange_name="alpha")
        ])

        captured_messages = []

        async def capture_critical(msg: str) -> None:
            captured_messages.append(msg)

        engine._send_critical_alert = capture_critical

        leg = StructureLeg(exchange="alpha", ticker="BTCUSDT", multiplier=Decimal("1"))
        await engine._auto_close_structure_leg(
            trigger_exchange="alpha",
            trigger_ticker="ETHUSDT",
            leg=leg,
            close_exchange_units=Decimal("0.5"),
            amount_before=Decimal("1.0"),
        )
        await _drain_pending_tasks(engine)

        assert len(captured_messages) >= 1
        msg = captured_messages[0]
        assert "alpha" in msg
        assert "BTCUSDT" in msg


# ===========================================================================
# Параметризованные тесты: граничные случаи place_order / close_position
# ===========================================================================

@pytest.mark.parametrize("order_type,limit_price,should_raise", [
    ("limit", None, True),
    ("limit", Decimal("50000"), False),
    ("market", None, False),
    ("market", Decimal("50000"), False),
])
async def test_place_order_validation_parametrized(order_type, limit_price, should_raise):
    connector = FakeOrderConnector()
    connector.fetch_positions = AsyncMock(return_value=[])

    if should_raise:
        with pytest.raises(ValueError):
            await connector.place_order(
                ticker="BTCUSDT",
                direction="long",
                amount=Decimal("1"),
                order_type=order_type,
                limit_price=limit_price,
            )
    else:
        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            await connector.place_order(
                ticker="BTCUSDT",
                direction="long",
                amount=Decimal("1"),
                order_type=order_type,
                limit_price=limit_price,
            )


@pytest.mark.parametrize("order_type,limit_price,should_raise", [
    ("limit", None, True),
    ("limit", Decimal("48000"), False),
    ("market", None, False),
    ("market", Decimal("48000"), False),
])
async def test_close_position_validation_parametrized(order_type, limit_price, should_raise):
    connector = FakeOrderConnector()
    connector.fetch_positions = AsyncMock(return_value=[])

    if should_raise:
        with pytest.raises(ValueError):
            await connector.close_position(
                ticker="BTCUSDT",
                amount=Decimal("1"),
                order_type=order_type,
                limit_price=limit_price,
            )
    else:
        with patch("app.connectors.base.asyncio.sleep", new_callable=AsyncMock):
            await connector.close_position(
                ticker="BTCUSDT",
                amount=Decimal("1"),
                order_type=order_type,
                limit_price=limit_price,
            )
