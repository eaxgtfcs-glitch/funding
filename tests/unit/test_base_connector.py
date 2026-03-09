"""
Тесты для BaseExchangeConnector (app/connectors/base.py).

Конкретный коннектор FakeConnector определён здесь же — это не продовый код,
а тестовый дубль. Все методы fetch_* и get_funding мокируются через AsyncMock,
чтобы не было обращений к сети.
"""
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from app.connectors.base import BaseExchangeConnector
from app.connectors.config import ConnectorConfig
from app.connectors.model.position import Position
from tests.unit.conftest import make_position, make_funding_snapshot


# ---------------------------------------------------------------------------
# Тестовый дубль коннектора
# ---------------------------------------------------------------------------

class FakeConnector(BaseExchangeConnector):
    """Минимальная реализация для тестирования логики базового класса."""

    name = "fake"

    # Переопределяем интервалы на минимальные, чтобы не ждать в тестах
    config = ConnectorConfig(
        positions_interval=0.0,
        margin_interval=0.0,
        funding_interval=0.0,
    )

    async def fetch_positions(self) -> list[Position]:
        raise NotImplementedError  # будет замокан в тестах

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        raise NotImplementedError  # будет замокан в тестах

    async def get_funding(self, ticker: str) -> Decimal:
        raise NotImplementedError  # будет замокан в тестах


# ---------------------------------------------------------------------------
# Вспомогательная функция для одной итерации цикла
# ---------------------------------------------------------------------------

async def run_one_iteration(coro_factory):
    """
    Запускает корутину, которая содержит бесконечный цикл с asyncio.sleep,
    выполняя ровно одну итерацию тела цикла, а затем отменяет задачу.

    Патчим asyncio.sleep внутри модуля base, чтобы первый вызов sleep
    отдал управление event loop (yield), а задача была отменена сразу после
    того, как тело цикла выполнилось. Это гарантирует ровно одну итерацию.
    """
    sleep_entered = asyncio.Event()

    original_sleep = asyncio.sleep

    async def fake_sleep(_delay, **kwargs):
        sleep_entered.set()
        # Реально спим 0, чтобы отдать управление event loop
        await original_sleep(0)

    task = None
    with patch("app.connectors.base.asyncio.sleep", side_effect=fake_sleep):
        task = asyncio.create_task(coro_factory())
        # Ждём, пока тело цикла выполнится и войдёт в sleep
        await sleep_entered.wait()
    # После выхода из patch задача либо уже на sleep(0), либо сразу за ним;
    # отменяем её
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


# ===========================================================================
# Тесты _ensure_funding
# ===========================================================================

class TestEnsureFunding:

    def setup_method(self):
        self.connector = FakeConnector()
        self.connector.get_funding = AsyncMock(return_value=Decimal("0.0002"))

    async def test_ensure_funding_calls_get_funding_when_ticker_not_in_state(self):
        await self.connector._ensure_funding("BTCUSDT")

        self.connector.get_funding.assert_awaited_once_with("BTCUSDT")

    async def test_ensure_funding_does_not_call_get_funding_when_ticker_already_known(self):
        existing = make_funding_snapshot(ticker="BTCUSDT", rate="0.0001")
        self.connector.state.funding_rates["BTCUSDT"] = existing

        await self.connector._ensure_funding("BTCUSDT")

        self.connector.get_funding.assert_not_awaited()

    async def test_ensure_funding_stores_snapshot_in_funding_rates(self):
        await self.connector._ensure_funding("ETHUSDT")

        assert "ETHUSDT" in self.connector.state.funding_rates
        snapshot = self.connector.state.funding_rates["ETHUSDT"]
        assert snapshot.ticker == "ETHUSDT"
        assert snapshot.rate == Decimal("0.0002")

    async def test_ensure_funding_appends_to_history(self):
        await self.connector._ensure_funding("SOLUSDT")

        history = self.connector.state.funding_rates_history.get("SOLUSDT", [])
        assert len(history) == 1
        assert history[0].ticker == "SOLUSDT"

    async def test_ensure_funding_does_not_update_state_when_ticker_already_known(self):
        existing = make_funding_snapshot(ticker="BTCUSDT", rate="0.0001")
        self.connector.state.funding_rates["BTCUSDT"] = existing

        await self.connector._ensure_funding("BTCUSDT")

        # Снимок не заменился
        assert self.connector.state.funding_rates["BTCUSDT"] is existing

    async def test_ensure_funding_swallows_exception_from_get_funding(self):
        self.connector.get_funding = AsyncMock(side_effect=RuntimeError("network error"))

        # Не должно бросать исключение наружу
        await self.connector._ensure_funding("BTCUSDT")

        assert "BTCUSDT" not in self.connector.state.funding_rates

    async def test_ensure_funding_snapshot_has_utc_timestamp(self):
        before = datetime.now(tz=timezone.utc)
        await self.connector._ensure_funding("BTCUSDT")
        after = datetime.now(tz=timezone.utc)

        snapshot = self.connector.state.funding_rates["BTCUSDT"]
        assert before <= snapshot.timestamp <= after


# ===========================================================================
# Тесты _loop_positions
# ===========================================================================

class TestLoopPositions:

    def setup_method(self):
        self.connector = FakeConnector()
        self.connector.get_funding = AsyncMock(return_value=Decimal("0.0001"))

    async def test_loop_positions_calls_fetch_positions(self):
        pos = make_position(ticker="BTCUSDT")
        self.connector.fetch_positions = AsyncMock(return_value=[pos])

        await run_one_iteration(self.connector._loop_positions)

        self.connector.fetch_positions.assert_awaited_once()

    async def test_loop_positions_updates_state_positions(self):
        pos = make_position(ticker="BTCUSDT")
        self.connector.fetch_positions = AsyncMock(return_value=[pos])

        await run_one_iteration(self.connector._loop_positions)

        assert "BTCUSDT" in self.connector.state.positions
        # pos пересоздаётся через dataclasses.replace, поэтому identity не гарантирована
        stored = self.connector.state.positions["BTCUSDT"]
        assert stored.ticker == "BTCUSDT"
        assert stored.amount == pos.amount

    async def test_loop_positions_calls_ensure_funding_for_new_ticker(self):
        pos = make_position(ticker="BTCUSDT")
        self.connector.fetch_positions = AsyncMock(return_value=[pos])

        await run_one_iteration(self.connector._loop_positions)

        self.connector.get_funding.assert_awaited_once_with("BTCUSDT")

    async def test_loop_positions_does_not_call_ensure_funding_for_existing_ticker(self):
        pos = make_position(ticker="BTCUSDT")
        # Позиция уже есть в state
        self.connector.state.positions["BTCUSDT"] = pos
        self.connector.fetch_positions = AsyncMock(return_value=[pos])

        await run_one_iteration(self.connector._loop_positions)

        self.connector.get_funding.assert_not_awaited()

    async def test_loop_positions_removes_closed_positions(self):
        old_pos = make_position(ticker="ETHUSDT")
        self.connector.state.positions["ETHUSDT"] = old_pos
        # fetch вернул пустой список — позиция закрыта
        self.connector.fetch_positions = AsyncMock(return_value=[])

        await run_one_iteration(self.connector._loop_positions)

        assert "ETHUSDT" not in self.connector.state.positions

    async def test_loop_positions_updates_positions_update_time(self):
        before = datetime.now(tz=timezone.utc)
        self.connector.fetch_positions = AsyncMock(return_value=[])

        await run_one_iteration(self.connector._loop_positions)

        assert self.connector.state.positions_update_time >= before

    async def test_loop_positions_keeps_only_fetched_tickers(self):
        pos_btc = make_position(ticker="BTCUSDT")
        pos_eth = make_position(ticker="ETHUSDT")
        self.connector.state.positions["ETHUSDT"] = pos_eth
        self.connector.fetch_positions = AsyncMock(return_value=[pos_btc])

        await run_one_iteration(self.connector._loop_positions)

        assert "BTCUSDT" in self.connector.state.positions
        assert "ETHUSDT" not in self.connector.state.positions

    async def test_loop_positions_swallows_fetch_exception(self):
        self.connector.fetch_positions = AsyncMock(side_effect=RuntimeError("API down"))

        # Не должно бросать исключение наружу — цикл продолжится
        await run_one_iteration(self.connector._loop_positions)

        # state.positions остался пустым — ничего не сломалось
        assert self.connector.state.positions == {}

    async def test_loop_positions_new_ticker_funding_rate_set_from_state(self):
        # get_funding вернёт реальную ставку — она должна оказаться в позиции
        self.connector.get_funding = AsyncMock(return_value=Decimal("0.0005"))
        pos = make_position(ticker="BTCUSDT")  # funding_rate=None, как в реальном коннекторе
        self.connector.fetch_positions = AsyncMock(return_value=[pos])

        await run_one_iteration(self.connector._loop_positions)

        stored = self.connector.state.positions["BTCUSDT"]
        assert stored.funding_rate == Decimal("0.0005")

    async def test_loop_positions_existing_ticker_funding_rate_updated_from_state(self):
        # Позиция уже в стейте, funding_rates тоже заполнен — ставка должна подтянуться
        pos = make_position(ticker="BTCUSDT")  # funding_rate=None, как в реальном коннекторе
        self.connector.state.positions["BTCUSDT"] = pos
        snapshot = make_funding_snapshot(ticker="BTCUSDT", rate="0.0007")
        self.connector.state.funding_rates["BTCUSDT"] = snapshot
        self.connector.fetch_positions = AsyncMock(return_value=[pos])

        await run_one_iteration(self.connector._loop_positions)

        stored = self.connector.state.positions["BTCUSDT"]
        assert stored.funding_rate == Decimal("0.0007")
        # _ensure_funding не должен был вызываться — тикер уже был в positions
        self.connector.get_funding.assert_not_awaited()

    async def test_loop_positions_funding_rate_stays_zero_when_ensure_funding_fails(self):
        # get_funding бросает исключение → _ensure_funding не заполняет funding_rates
        # → ветка `if ticker in state.funding_rates` не выполняется
        # → funding_rate остаётся None (значение из fetch_positions, как в bybit)
        self.connector.get_funding = AsyncMock(side_effect=RuntimeError("network error"))
        pos = make_position(ticker="BTCUSDT")  # funding_rate=None по умолчанию
        self.connector.fetch_positions = AsyncMock(return_value=[pos])

        await run_one_iteration(self.connector._loop_positions)

        stored = self.connector.state.positions["BTCUSDT"]
        assert stored.funding_rate is None


# ===========================================================================
# Тесты _loop_margin
# ===========================================================================

class TestLoopMargin:

    def setup_method(self):
        self.connector = FakeConnector()

    async def test_loop_margin_updates_maintenance_margin(self):
        self.connector.fetch_margin = AsyncMock(
            return_value=(Decimal("1000"), Decimal("5000"))
        )

        await run_one_iteration(self.connector._loop_margin)

        assert self.connector.state.maintenance_margin == Decimal("1000")

    async def test_loop_margin_updates_current_margin(self):
        self.connector.fetch_margin = AsyncMock(
            return_value=(Decimal("1000"), Decimal("5000"))
        )

        await run_one_iteration(self.connector._loop_margin)

        assert self.connector.state.current_margin == Decimal("5000")

    async def test_loop_margin_updates_maintenance_margin_update_time(self):
        before = datetime.now(tz=timezone.utc)
        self.connector.fetch_margin = AsyncMock(
            return_value=(Decimal("0"), Decimal("0"))
        )

        await run_one_iteration(self.connector._loop_margin)

        assert self.connector.state.maintenance_margin_update_time >= before

    async def test_loop_margin_swallows_fetch_exception(self):
        self.connector.fetch_margin = AsyncMock(side_effect=ValueError("bad response"))

        # Не должно бросать исключение
        await run_one_iteration(self.connector._loop_margin)

        # Значения не изменились — остались дефолтными
        assert self.connector.state.maintenance_margin == Decimal(0)
        assert self.connector.state.current_margin == Decimal(0)

    @pytest.mark.parametrize("maint,current", [
        ("0", "0"),
        ("100.5", "200.75"),
        ("99999999", "99999999"),
    ])
    async def test_loop_margin_stores_correct_decimal_values(
            self, maint: str, current: str
    ):
        self.connector.fetch_margin = AsyncMock(
            return_value=(Decimal(maint), Decimal(current))
        )

        await run_one_iteration(self.connector._loop_margin)

        assert self.connector.state.maintenance_margin == Decimal(maint)
        assert self.connector.state.current_margin == Decimal(current)


# ===========================================================================
# Тесты _loop_funding
# ===========================================================================

class TestLoopFunding:

    def setup_method(self):
        self.connector = FakeConnector()

    async def test_loop_funding_calls_get_funding_for_each_open_position(self):
        pos_btc = make_position(ticker="BTCUSDT")
        pos_eth = make_position(ticker="ETHUSDT")
        self.connector.state.positions = {"BTCUSDT": pos_btc, "ETHUSDT": pos_eth}
        self.connector.get_funding = AsyncMock(return_value=Decimal("0.0001"))

        await run_one_iteration(self.connector._loop_funding)

        assert self.connector.get_funding.await_count == 2
        called_tickers = {call.args[0] for call in self.connector.get_funding.await_args_list}
        assert called_tickers == {"BTCUSDT", "ETHUSDT"}

    async def test_loop_funding_does_not_call_get_funding_when_no_positions(self):
        self.connector.state.positions = {}
        self.connector.get_funding = AsyncMock(return_value=Decimal("0.0001"))

        await run_one_iteration(self.connector._loop_funding)

        self.connector.get_funding.assert_not_awaited()

    async def test_loop_funding_updates_funding_rates(self):
        pos = make_position(ticker="BTCUSDT")
        self.connector.state.positions = {"BTCUSDT": pos}
        self.connector.get_funding = AsyncMock(return_value=Decimal("0.0003"))

        await run_one_iteration(self.connector._loop_funding)

        assert "BTCUSDT" in self.connector.state.funding_rates
        assert self.connector.state.funding_rates["BTCUSDT"].rate == Decimal("0.0003")

    async def test_loop_funding_appends_snapshot_to_history(self):
        pos = make_position(ticker="BTCUSDT")
        self.connector.state.positions = {"BTCUSDT": pos}
        self.connector.get_funding = AsyncMock(return_value=Decimal("0.0003"))

        await run_one_iteration(self.connector._loop_funding)

        history = self.connector.state.funding_rates_history.get("BTCUSDT", [])
        assert len(history) == 1
        assert history[0].rate == Decimal("0.0003")

    async def test_loop_funding_accumulates_history_across_iterations(self):
        pos = make_position(ticker="BTCUSDT")
        self.connector.state.positions = {"BTCUSDT": pos}
        # Первый вызов
        self.connector.get_funding = AsyncMock(return_value=Decimal("0.0001"))
        await run_one_iteration(self.connector._loop_funding)
        # Второй вызов с другой ставкой
        self.connector.get_funding = AsyncMock(return_value=Decimal("0.0002"))
        await run_one_iteration(self.connector._loop_funding)

        history = self.connector.state.funding_rates_history["BTCUSDT"]
        assert len(history) == 2
        assert history[0].rate == Decimal("0.0001")
        assert history[1].rate == Decimal("0.0002")

    async def test_loop_funding_updates_funding_rates_update_time(self):
        self.connector.state.positions = {}
        before = datetime.now(tz=timezone.utc)

        await run_one_iteration(self.connector._loop_funding)

        assert self.connector.state.funding_rates_update_time >= before

    async def test_loop_funding_skips_failed_ticker_and_continues(self):
        pos_btc = make_position(ticker="BTCUSDT")
        pos_eth = make_position(ticker="ETHUSDT")
        self.connector.state.positions = {"BTCUSDT": pos_btc, "ETHUSDT": pos_eth}

        async def selective_funding(ticker: str) -> Decimal:
            if ticker == "BTCUSDT":
                raise RuntimeError("rate limit")
            return Decimal("0.0001")

        self.connector.get_funding = AsyncMock(side_effect=selective_funding)

        await run_one_iteration(self.connector._loop_funding)

        # ETHUSDT обновился, BTCUSDT — нет
        assert "ETHUSDT" in self.connector.state.funding_rates
        assert "BTCUSDT" not in self.connector.state.funding_rates
