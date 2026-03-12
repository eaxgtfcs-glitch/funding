import asyncio
import importlib
import inspect
import json
import logging
import os
import pkgutil
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import app.connectors as connectors_pkg
from app.connectors.base import BaseExchangeConnector
from app.connectors.config import (
    CRITICAL_ALERT_SEND_COUNT,
    CRITICAL_ALERT_REPEAT_INTERVAL,
)
from app.connectors.model.position import Position
from app.connectors.model.state import ExchangeState
from app.engine.model.pair import Pair
from app.telegram.formatters import (
    format_high_margin_ratio_alert,
    format_pair_imbalance_batch,
    format_pairs_state,
    format_position_reduction_batch,
    format_session_start_separator,
    format_stale_data_alert,
)
from app.telegram.service import TelegramAlertService
from app.telegram.state_broadcaster import StateBroadcaster

logger = logging.getLogger(__name__)

_MARGIN_RATIO_ALERT_THRESHOLD = Decimal("50")
_PAIRS_FILE = Path("data/pairs.json")
_ALERT_MESSAGES_FILE = Path("data/.alert_messages.json")


def _load_alert_message_ids() -> dict[str, list[int]]:
    try:
        with open(_ALERT_MESSAGES_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_alert_message_ids(ids: dict[str, list[int]]) -> None:
    try:
        with open(_ALERT_MESSAGES_FILE, "w", encoding="utf-8") as f:
            json.dump(ids, f)
    except Exception:
        pass


def _add_alert_message_id(chat_id: str, message_id: int) -> None:
    ids = _load_alert_message_ids()
    ids.setdefault(chat_id, []).append(message_id)
    _save_alert_message_ids(ids)


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
        # Флаги "алерт уже отправлен" для пар, чтобы не спамить
        self._pairs_reduction_alerted: dict[tuple[str, str], bool] = {}
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
            pairs_state_fn=lambda: format_pairs_state(self._pairs, self.states),
        )

    def _critical_chat_ids(self) -> list[str]:
        return _parse_chat_ids(os.environ.get("CRITICAL_ALERT_CHAT_IDS", ""))

    def _alert_chat_ids(self) -> list[str]:
        return _parse_chat_ids(os.environ.get("ALERT_CHAT_IDS", ""))

    def _pairs_alert_chat_ids(self) -> list[str]:
        return _parse_chat_ids(os.environ.get("PAIRS_ALERT_CHAT_IDS", ""))

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
            for chat_id in critical_ids:
                message_id = await self._telegram.send_alert_tracked(chat_id, msg)
                if message_id is not None:
                    _add_alert_message_id(chat_id, message_id)

    async def _send_reduction_alert(self, reductions: list[dict]) -> None:
        """Отправляет батч-алерт о сокращениях с повторами согласно конфигу."""
        if not self._telegram:
            return
        alert_ids = self._alert_chat_ids()
        if not alert_ids:
            return
        msg = format_position_reduction_batch(reductions)
        for chat_id in alert_ids:
            message_id = await self._telegram.send_alert_tracked(chat_id, msg)
            if message_id is not None:
                _add_alert_message_id(chat_id, message_id)

    async def _send_pairs_alert(self, reductions: list[dict]) -> None:
        if not self._telegram:
            return
        pairs_ids = self._pairs_alert_chat_ids()
        if not pairs_ids:
            return
        msg = format_pair_imbalance_batch(reductions)
        for chat_id in pairs_ids:
            message_id = await self._telegram.send_alert_tracked(chat_id, msg)
            if message_id is not None:
                _add_alert_message_id(chat_id, message_id)

    def _load_pairs_from_file(self) -> tuple[list[Pair], list[dict]]:
        try:
            with open(_PAIRS_FILE, encoding="utf-8") as f:
                raw_list = json.load(f)
            if not isinstance(raw_list, list):
                return [], []
        except Exception:
            return [], []

        pairs: list[Pair] = []
        active_raw: list[dict] = []
        for entry in raw_list:
            if not entry.get("is_active", False):
                continue
            exchange_a = entry["exchange_a"]
            ticker_a = entry["ticker_a"]
            exchange_b = entry["exchange_b"]
            ticker_b = entry["ticker_b"]
            size_a = Decimal(str(entry["size_a"]))
            size_b = Decimal(str(entry["size_b"]))

            state_a = self.states.get(exchange_a)
            pos_a = state_a.positions.get(ticker_a) if state_a else None
            if pos_a is None:
                pos_a = Position(
                    ticker=ticker_a,
                    exchange_name=exchange_a,
                    direction="long",
                    amount=Decimal(0),
                    avg_price=Decimal(0),
                    current_price=Decimal(0),
                )

            state_b = self.states.get(exchange_b)
            pos_b = state_b.positions.get(ticker_b) if state_b else None
            if pos_b is None:
                pos_b = Position(
                    ticker=ticker_b,
                    exchange_name=exchange_b,
                    direction="short",
                    amount=Decimal(0),
                    avg_price=Decimal(0),
                    current_price=Decimal(0),
                )

            pairs.append(Pair(position_a=pos_a, position_b=pos_b, is_active=True))
            active_raw.append({
                "exchange_a": exchange_a,
                "ticker_a": ticker_a,
                "size_a": size_a,
                "exchange_b": exchange_b,
                "ticker_b": ticker_b,
                "size_b": size_b,
            })
        return pairs, active_raw

    def _check_pairs_reductions(self, active_raw: list[dict]) -> None:
        reductions: list[dict] = []
        # collect keys seen in this run to reset cleared conditions
        seen_keys: set[tuple[str, str]] = set()

        for entry in active_raw:
            exchange_a, ticker_a = entry["exchange_a"], entry["ticker_a"]
            exchange_b, ticker_b = entry["exchange_b"], entry["ticker_b"]
            size_a, size_b = entry["size_a"], entry["size_b"]

            key_a = (exchange_a, ticker_a)
            key_b = (exchange_b, ticker_b)
            seen_keys.add(key_a)
            seen_keys.add(key_b)

            # leg A
            state_a = self.states.get(exchange_a)
            real_pos_a = state_a.positions.get(ticker_a) if state_a else None
            real_amount_a = real_pos_a.amount if real_pos_a else Decimal(0)
            if real_amount_a < size_a * Decimal("0.995"):
                if not self._pairs_reduction_alerted.get(key_a):
                    state_b = self.states.get(exchange_b)
                    counterpart_b = state_b.positions.get(ticker_b) if state_b else None
                    reductions.append({
                        "exchange_name": exchange_a,
                        "ticker": ticker_a,
                        "old_amount": size_a,
                        "new_amount": real_amount_a,
                        "counterpart": counterpart_b,
                    })
                    self._pairs_reduction_alerted[key_a] = True
            else:
                self._pairs_reduction_alerted[key_a] = False

            # leg B
            state_b = self.states.get(exchange_b)
            real_pos_b = state_b.positions.get(ticker_b) if state_b else None
            real_amount_b = real_pos_b.amount if real_pos_b else Decimal(0)
            if real_amount_b < size_b * Decimal("0.995"):
                if not self._pairs_reduction_alerted.get(key_b):
                    state_a = self.states.get(exchange_a)
                    counterpart_a = state_a.positions.get(ticker_a) if state_a else None
                    reductions.append({
                        "exchange_name": exchange_b,
                        "ticker": ticker_b,
                        "old_amount": size_b,
                        "new_amount": real_amount_b,
                        "counterpart": counterpart_a,
                    })
                    self._pairs_reduction_alerted[key_b] = True
            else:
                self._pairs_reduction_alerted[key_b] = False

        # clear alerted state for legs no longer in active pairs
        stale_keys = set(self._pairs_reduction_alerted.keys()) - seen_keys
        for k in stale_keys:
            del self._pairs_reduction_alerted[k]

        if reductions:
            task = asyncio.create_task(self._send_pairs_alert(reductions))
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)

    async def _loop_pairs_reload(self) -> None:
        while True:
            await asyncio.sleep(15)
            pairs, active_raw = self._load_pairs_from_file()
            self._pairs = pairs
            self._check_pairs_reductions(active_raw)

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
                    "counterpart": None,
                })

        # обновляем снимок
        self._prev_amounts[name] = {
            ticker: pos.amount for ticker, pos in current_positions.items()
        }

        if reductions:
            task = asyncio.create_task(self._send_reduction_alert(reductions))
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)

        pairs_reductions: list[dict] = []
        for r in reductions:
            cp = self._get_counterpart(r["exchange_name"], r["ticker"])
            if cp is not None:
                pairs_reductions.append({
                    "exchange_name": r["exchange_name"],
                    "ticker": r["ticker"],
                    "old_amount": r["old_amount"],
                    "new_amount": r["new_amount"],
                    "counterpart": cp,
                })
        if pairs_reductions:
            task = asyncio.create_task(self._send_pairs_alert(pairs_reductions))
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
        for chat_id in alert_ids:
            message_id = await self._telegram.send_alert_tracked(chat_id, msg)
            if message_id is not None:
                _add_alert_message_id(chat_id, message_id)

    async def _delete_all_tracked_messages(self) -> None:
        """Delete all previously tracked alert messages from all chats."""
        if not self._telegram:
            return
        ids = _load_alert_message_ids()
        coros = []
        for chat_id, message_ids in ids.items():
            for message_id in message_ids:
                coros.append(self._telegram.delete_message(chat_id, message_id))
        if coros:
            await asyncio.gather(*coros, return_exceptions=True)
        _save_alert_message_ids({})

    async def start(self) -> None:
        await asyncio.gather(*(c.start() for c in self._connectors))
        pairs, active_raw = self._load_pairs_from_file()
        self._pairs = pairs
        self._check_pairs_reductions(active_raw)
        if self._telegram:
            await self._telegram.start()
            self._telegram.start_polling()
        await self._delete_all_tracked_messages()
        if self._broadcaster:
            await self._broadcaster.start()
        await self._send_session_start()
        self._engine_tasks = [
            asyncio.create_task(self._loop_stale_check()),
            asyncio.create_task(self._loop_pairs_reload()),
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
