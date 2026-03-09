import asyncio
import dataclasses
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from decimal import Decimal

from app.connectors.config import DEFAULT_CONFIG, ConnectorConfig
from app.connectors.model.funding import FundingSnapshot
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
            funding_rates={},
            funding_rates_history={},
            maintenance_margin=Decimal(0),
            current_margin=Decimal(0),
            positions_update_time=datetime.now(tz=timezone.utc),
            maintenance_margin_update_time=datetime.now(tz=timezone.utc),
            funding_rates_update_time=datetime.now(tz=timezone.utc),
        )
        self._tasks: list[asyncio.Task] = []

    @abstractmethod
    async def fetch_positions(self) -> list[Position]:
        """Fetch open positions from the exchange and return as Position objects."""

    @abstractmethod
    async def fetch_margin(self) -> tuple[Decimal, Decimal]:
        """Return from the exchange maintenance_margin and current_margin."""

    @abstractmethod
    async def get_funding(self, ticker: str) -> Decimal:
        """
        Fetch the current funding rate for the given ticker from the exchange.
        Returns the funding rate as a Decimal (e.g. 0.0001 for 0.01%).
        """

    async def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._loop_positions()),
            asyncio.create_task(self._loop_margin()),
            asyncio.create_task(self._loop_funding()),
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
                for pos in positions:
                    # если позиция новая — запрашиваем фандинг немедленно
                    if pos.ticker not in self.state.positions:
                        await self._ensure_funding(pos.ticker)
                    # funding_rate всегда берётся из ExchangeState, не из API позиций
                    if pos.ticker in self.state.funding_rates:
                        pos = dataclasses.replace(pos, funding_rate=self.state.funding_rates[pos.ticker].rate)
                    self.state.positions[pos.ticker] = pos
                # удаляем закрытые позиции
                fetched_tickers = {p.ticker for p in positions}
                for ticker in list(self.state.positions):
                    if ticker not in fetched_tickers:
                        del self.state.positions[ticker]
                self.state.positions_update_time = datetime.now(tz=timezone.utc)
            except Exception:
                logger.exception("Ошибка при обновлении позиций [%s]", self.name)
            await asyncio.sleep(self.config.positions_interval)

    async def _loop_margin(self) -> None:
        while True:
            try:
                maintenance_margin, current_margin = await self.fetch_margin()
                self.state.maintenance_margin = maintenance_margin
                self.state.current_margin = current_margin
                self.state.maintenance_margin_update_time = datetime.now(tz=timezone.utc)
            except Exception:
                logger.exception("Ошибка при обновлении маржи [%s]", self.name)
            await asyncio.sleep(self.config.margin_interval)

    async def _loop_funding(self) -> None:
        while True:
            try:
                now = datetime.now(tz=timezone.utc)
                for ticker in list(self.state.positions):
                    try:
                        rate = await self.get_funding(ticker)
                        snapshot = FundingSnapshot(ticker=ticker, rate=rate, timestamp=now)
                        self.state.funding_rates[ticker] = snapshot
                        self.state.funding_rates_history.setdefault(ticker, []).append(snapshot)
                    except Exception:
                        logger.exception("Ошибка при обновлении фандинга [%s] %s", self.name, ticker)
                self.state.funding_rates_update_time = now
            except Exception:
                logger.exception("Ошибка в цикле фандинга [%s]", self.name)
            await asyncio.sleep(self.config.funding_interval)

    async def _ensure_funding(self, ticker: str) -> None:
        """Запрашивает фандинг для тикера, если он ещё не известен."""
        if ticker in self.state.funding_rates:
            return
        try:
            rate = await self.get_funding(ticker)
            now = datetime.now(tz=timezone.utc)
            snapshot = FundingSnapshot(ticker=ticker, rate=rate, timestamp=now)
            self.state.funding_rates[ticker] = snapshot
            self.state.funding_rates_history.setdefault(ticker, []).append(snapshot)
        except Exception:
            logger.exception("Ошибка при первичном запросе фандинга [%s] %s", self.name, ticker)
