import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal

from app.connectors.config import DEFAULT_CONFIG, ConnectorConfig
from app.connectors.model.position import Position
from app.connectors.model.state import ExchangeState

logger = logging.getLogger(__name__)


class BaseExchangeConnector(ABC):

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    config: ConnectorConfig = DEFAULT_CONFIG

    def __init__(self) -> None:
        self.state = ExchangeState(
            name=self.name,
            positions={},
            maintenance_margin=Decimal(0),
            current_margin=Decimal(0),
            positions_update_time=datetime.now(tz=timezone.utc),
            maintenance_margin_update_time=datetime.now(tz=timezone.utc),
        )
        self._tasks: list[asyncio.Task] = []
        self.on_margin_updated: Callable[[], Coroutine[Any, Any, None]] | None = None
        self.on_positions_updated: Callable[[], Coroutine[Any, Any, None]] | None = None

    @abstractmethod
    async def fetch_positions(self) -> list[Position]:
        """Fetch open positions from the exchange and return as Position objects."""

    @abstractmethod
    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        """Return from the exchange maintenance_margin and current_margin."""

    @abstractmethod
    async def place_order(
            self,
            ticker: str,
            direction: Literal["long", "short"],
            amount: Decimal,
            order_type: Literal["market", "limit"] = "market",
            limit_price: Decimal | None = None,
    ) -> bool:
        """Открыть позицию. Возвращает True если ордер принят биржей (200 OK).

        Raises:
            ValueError: если order_type="limit" и limit_price=None
        """

    @abstractmethod
    async def close_position(
            self,
            ticker: str,
            amount: Decimal,
            order_type: Literal["market", "limit"] = "market",
            limit_price: Decimal | None = None,
    ) -> bool:
        """Закрыть позицию. Возвращает True если ордер принят биржей (200 OK).

        Raises:
            ValueError: если order_type="limit" и limit_price=None
        """

    async def _verify_position_changed(
            self,
            ticker: str,
            snapshot: list[Position],
    ) -> bool:
        """Проверяет, изменилась ли позиция по ticker относительно snapshot.

        Делает 2 попытки с интервалом 2 секунды. Возвращает True при первом
        зафиксированном изменении, False если ни одна попытка не подтвердила.
        """
        snapshot_amount = next(
            (p.amount for p in snapshot if p.ticker == ticker), Decimal(0)
        )
        for _ in range(2):
            await asyncio.sleep(2)
            fresh = await self.fetch_positions()
            fresh_amount = next(
                (p.amount for p in fresh if p.ticker == ticker), Decimal(0)
            )
            if fresh_amount != snapshot_amount:
                return True
        return False

    async def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._loop_positions()),
            asyncio.create_task(self._loop_margin()),
        ]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _loop_positions(self) -> None:
        while True:
            try:
                positions = await self.fetch_positions()
                self.state.positions = {p.ticker: p for p in positions}
                self.state.positions_update_time = datetime.now(tz=timezone.utc)
                if self.on_positions_updated:
                    await self.on_positions_updated()
            except Exception as e:
                logger.exception("Ошибка при обновлении позиций [%s]", self.name, e)
            await asyncio.sleep(self.config.positions_interval)

    async def _loop_margin(self) -> None:
        while True:
            try:
                maintenance_margin, current_margin = await self.fetch_margin()
                self.state.maintenance_margin = maintenance_margin
                self.state.current_margin = current_margin
                self.state.margin_ratio = (
                    maintenance_margin * 100 / current_margin
                    if current_margin > 0
                    else None
                )
                self.state.maintenance_margin_update_time = datetime.now(tz=timezone.utc)
                if self.on_margin_updated:
                    await self.on_margin_updated()
            except Exception as exc:
                logger.exception("Ошибка при обновлении маржи [%s]", self.name, exc)
            await asyncio.sleep(self.config.margin_interval)
