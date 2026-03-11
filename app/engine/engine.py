import asyncio
import importlib
import inspect
import logging
import os
import pkgutil
from datetime import datetime, timezone
from decimal import Decimal

import app.connectors as connectors_pkg
from app.connectors.base import BaseExchangeConnector
from app.connectors.config import (
    CRITICAL_ALERT_SEND_COUNT,
    CRITICAL_ALERT_REPEAT_INTERVAL,
)
from app.connectors.model.state import ExchangeState
from app.engine.model.pair import Pair
from app.telegram.formatters import (
    format_high_margin_ratio_alert,
    format_position_reduction_batch,
    format_session_start_separator,
    format_stale_data_alert,
)
from app.telegram.service import TelegramAlertService
from app.telegram.state_broadcaster import StateBroadcaster

logger = logging.getLogger(__name__)

_MARGIN_RATIO_ALERT_THRESHOLD = Decimal("50")


def _parse_chat_ids(raw: str) -> list[str]:
    return [c.strip() for c in raw.split(",") if c.strip()]


class MonitoringEngine:

    def __init__(self) -> None:
        self._connectors: list[BaseExchangeConnector] = []
        self.states: dict[str, ExchangeState] = {}
        self._pairs: list[Pair] = []
        self._telegram: TelegramAlertService | None = None
        self._broadcaster: StateBroadcaster | None = None
        # Предыдущие значения amounts позиций для детектирования сокращений
        self._prev_amounts: dict[str, dict[str, Decimal]] = {}
        # Флаги "алерт уже отправлен" для margin_ratio, чтобы не спамить
        self._margin_ratio_alerted: dict[str, bool] = {}
        # Задачи фоновых циклов движка
        self._engine_tasks: list[asyncio.Task] = []
        self._pending_tasks: set[asyncio.Task] = set()
        self._discover_connectors()
        self._setup_telegram()

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
            self._prev_amounts[connector.name] = {}
            self._margin_ratio_alerted[connector.name] = False
            connector.on_margin_updated = lambda c=connector: self._on_margin_updated(c)
            connector.on_positions_updated = lambda c=connector: self._on_positions_updated(c)

    def _setup_telegram(self) -> None:
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        state_chat_ids_raw = os.environ.get("STATE_CHAT_IDS", "")
        if not bot_token:
            logger.warning("TELEGRAM_BOT_TOKEN not set — Telegram disabled")
            return
        self._telegram = TelegramAlertService(bot_token)
        if not state_chat_ids_raw:
            logger.warning("STATE_CHAT_IDS not set — StateBroadcaster disabled")
            return
        chat_ids = _parse_chat_ids(state_chat_ids_raw)
        try:
            update_interval = int(os.environ.get("STATE_UPDATE_INTERVAL", "30"))
        except ValueError:
            logger.warning("Invalid STATE_UPDATE_INTERVAL value, using default 30s")
            update_interval = 30
        self._broadcaster = StateBroadcaster(
            service=self._telegram,
            states=self.states,
            chat_ids=chat_ids,
            update_interval=update_interval,
        )

    def _critical_chat_ids(self) -> list[str]:
        return _parse_chat_ids(os.environ.get("CRITICAL_ALERT_CHAT_IDS", ""))

    def _alert_chat_ids(self) -> list[str]:
        return _parse_chat_ids(os.environ.get("ALERT_CHAT_IDS", ""))

    def _connector_config(self, exchange_name: str):
        for c in self._connectors:
            if c.name == exchange_name:
                return c.config
        return None

    def _get_counterpart(self, exchange_name: str, ticker: str):
        for pair in self._pairs:
            cp = pair.get_counterpart(exchange_name, ticker)
            if cp is not None:
                return cp
        return None

    # -------------------------------------------------------------------------
    # Фича 1: мониторинг margin_ratio
    # -------------------------------------------------------------------------

    async def _on_margin_updated(self, connector: BaseExchangeConnector) -> None:
        """Вызывается немедленно после получения свежих данных маржи."""
        name = connector.name
        ratio = connector.state.margin_ratio
        if ratio is not None and ratio > _MARGIN_RATIO_ALERT_THRESHOLD:
            if not self._margin_ratio_alerted.get(name):
                msg = format_high_margin_ratio_alert(connector.state)
                task = asyncio.create_task(self._send_critical_alert(msg))
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.discard)
                self._margin_ratio_alerted[name] = True
        else:
            self._margin_ratio_alerted[name] = False

    # -------------------------------------------------------------------------
    # Фича 2: мониторинг работоспособности API
    # -------------------------------------------------------------------------

    async def _loop_stale_check(self) -> None:
        """Проверяет актуальность данных каждого коннектора и шлёт алерт при зависании."""
        alerted: dict[str, dict[str, bool]] = {}
        while True:
            await asyncio.sleep(10)
            if not self._telegram:
                continue
            critical_ids = self._critical_chat_ids()
            if not critical_ids:
                continue
            now = datetime.now(tz=timezone.utc)
            for connector in self._connectors:
                name = connector.name
                cfg = connector.config
                if name not in alerted:
                    alerted[name] = {}
                state = connector.state
                checks = [
                    ("positions", state.positions_update_time, cfg.positions_interval),
                    ("maintenance_margin", state.maintenance_margin_update_time, cfg.margin_interval),
                    ("funding_rates", state.funding_rates_update_time, cfg.funding_interval),
                ]
                for field_name, last_update, interval in checks:
                    threshold = interval * 2.5
                    delta = (now - last_update).total_seconds()
                    if delta > threshold:
                        if not alerted[name].get(field_name):
                            msg = format_stale_data_alert(name, field_name, last_update, now)
                            task = asyncio.create_task(self._send_critical_alert(msg))
                            self._pending_tasks.add(task)
                            task.add_done_callback(self._pending_tasks.discard)
                            alerted[name][field_name] = True
                    else:
                        alerted[name][field_name] = False

    # -------------------------------------------------------------------------
    # Фича 3: мониторинг сокращения позиций
    # -------------------------------------------------------------------------

    async def _send_critical_alert(self, msg: str) -> None:
        critical_ids = self._critical_chat_ids()
        if not critical_ids or not self._telegram:
            return
        for i in range(CRITICAL_ALERT_SEND_COUNT):
            if i > 0:
                await asyncio.sleep(CRITICAL_ALERT_REPEAT_INTERVAL)
            await self._telegram.broadcast(critical_ids, msg)

    async def _send_reduction_alert(self, reductions: list[dict]) -> None:
        """Отправляет батч-алерт о сокращениях с повторами согласно конфигу."""
        if not self._telegram:
            return
        alert_ids = self._alert_chat_ids()
        if not alert_ids:
            return
        msg = format_position_reduction_batch(reductions)
        await self._telegram.broadcast(alert_ids, msg)

    async def _on_positions_updated(self, connector: BaseExchangeConnector) -> None:
        """Вызывается немедленно после получения свежих позиций."""
        name = connector.name
        current_positions = connector.state.positions
        prev = self._prev_amounts[name]

        reductions: list[dict] = []
        for ticker, prev_amount in list(prev.items()):
            current_pos = current_positions.get(ticker)
            current_amount = current_pos.amount if current_pos else Decimal(0)
            if current_amount < prev_amount:
                reductions.append({
                    "exchange_name": name,
                    "ticker": ticker,
                    "old_amount": prev_amount,
                    "new_amount": current_amount,
                    "counterpart": self._get_counterpart(name, ticker),
                })

        # обновляем снимок
        self._prev_amounts[name] = {
            ticker: pos.amount for ticker, pos in current_positions.items()
        }

        if reductions:
            task = asyncio.create_task(self._send_reduction_alert(reductions))
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)

    async def _send_session_start(self) -> None:
        """Отправляет разделитель сессии в ALERT_CHAT_IDS при старте."""
        if not self._telegram:
            return
        alert_ids = self._alert_chat_ids()
        if not alert_ids:
            return
        msg = format_session_start_separator()
        await self._telegram.broadcast(alert_ids, msg)

    async def start(self) -> None:
        await asyncio.gather(*(c.start() for c in self._connectors))
        if self._telegram:
            await self._telegram.start()
            self._telegram.start_polling()
        if self._broadcaster:
            await self._broadcaster.start()
        await self._send_session_start()
        self._engine_tasks = [
            asyncio.create_task(self._loop_stale_check()),
        ]

    async def stop(self) -> None:
        for task in self._engine_tasks:
            task.cancel()
        await asyncio.gather(*self._engine_tasks, return_exceptions=True)
        self._engine_tasks.clear()
        if self._broadcaster:
            await self._broadcaster.stop()
        if self._telegram:
            await self._telegram.stop_polling()
            await self._telegram.stop()
        await asyncio.gather(*(c.stop() for c in self._connectors))
