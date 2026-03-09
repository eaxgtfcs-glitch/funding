import asyncio
import importlib
import inspect
import logging
import os
import pkgutil

from dotenv import load_dotenv

import app.connectors as connectors_pkg
from app.connectors.base import BaseExchangeConnector
from app.connectors.model.state import ExchangeState
from app.telegram.service import TelegramAlertService
from app.telegram.state_broadcaster import StateBroadcaster

logger = logging.getLogger(__name__)


class MonitoringEngine:

    def __init__(self) -> None:
        load_dotenv()
        self._connectors: list[BaseExchangeConnector] = []
        self.states: dict[str, ExchangeState] = {}
        self._telegram: TelegramAlertService | None = None
        self._broadcaster: StateBroadcaster | None = None
        self._discover_connectors()
        self._setup_broadcaster()

    def _discover_connectors(self) -> None:
        """Автоматически обнаруживает и инстанциирует все неабстрактные подклассы BaseExchangeConnector."""
        pkg_path = connectors_pkg.__path__
        pkg_name = connectors_pkg.__name__
        for module_info in pkgutil.iter_modules(pkg_path):
            importlib.import_module(f"{pkg_name}.{module_info.name}")

        for subclass in BaseExchangeConnector.__subclasses__():
            if inspect.isabstract(subclass):
                continue
            connector = subclass()
            self._connectors.append(connector)
            self.states[connector.name] = connector.state

    def _setup_broadcaster(self) -> None:
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        state_chat_ids_raw = os.environ.get("STATE_CHAT_IDS", "")
        if not bot_token or not state_chat_ids_raw:
            logger.warning(
                "TELEGRAM_BOT_TOKEN or STATE_CHAT_IDS not set — StateBroadcaster disabled"
            )
            return
        chat_ids = [c.strip() for c in state_chat_ids_raw.split(",") if c.strip()]
        try:
            update_interval = int(os.environ.get("STATE_UPDATE_INTERVAL", "30"))
        except ValueError:
            logger.warning("Invalid STATE_UPDATE_INTERVAL value, using default 30s")
            update_interval = 30
        self._telegram = TelegramAlertService(bot_token)
        self._broadcaster = StateBroadcaster(
            service=self._telegram,
            states=self.states,
            chat_ids=chat_ids,
            update_interval=update_interval,
        )

    async def start(self) -> None:
        await asyncio.gather(*(c.start() for c in self._connectors))
        if self._telegram:
            await self._telegram.start()
        if self._broadcaster:
            await self._broadcaster.start()

    async def stop(self) -> None:
        if self._broadcaster:
            await self._broadcaster.stop()
        if self._telegram:
            await self._telegram.stop()
        await asyncio.gather(*(c.stop() for c in self._connectors))
