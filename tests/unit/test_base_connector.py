"""
Тесты для BaseExchangeConnector (app/connectors/base.py).

Конкретный коннектор FakeConnector определён здесь же — это не продовый код,
а тестовый дубль. Все методы fetch_* мокируются через AsyncMock,
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
from tests.unit.conftest import make_position


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
    )

    async def fetch_positions(self) -> list[Position]:
        raise NotImplementedError  # будет замокан в тестах

    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        raise NotImplementedError  # будет замокан в тестах

    async def close_position(self, ticker: str, amount: Decimal) -> None:
        raise NotImplementedError  # будет замокан в тестах при необходимости


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
# Тесты _loop_positions
# ===========================================================================

class TestLoopPositions:

    def setup_method(self):
        self.connector = FakeConnector()

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
        stored = self.connector.state.positions["BTCUSDT"]
        assert stored.ticker == "BTCUSDT"
        assert stored.amount == pos.amount

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
