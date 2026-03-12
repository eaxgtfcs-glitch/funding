import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

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
