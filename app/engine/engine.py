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
    READ_ONLY_MODE,
)
from app.connectors.model.state import ExchangeState
from app.engine.model.structure import Structure, StructureLeg
from app.telegram.formatters import (
    format_auto_close_failed,
    format_auto_close_success,
    format_high_margin_ratio_alert,
    format_leg_not_found,
    format_structure_imbalance,
    format_structures_state,
    format_position_reduction_batch,
    format_session_start_separator,
    format_stale_data_alert,
)
from app.telegram.service import TelegramAlertService
from app.telegram.state_broadcaster import StateBroadcaster

logger = logging.getLogger(__name__)

_MARGIN_RATIO_ALERT_THRESHOLD = Decimal("50")
_STRUCTURES_FILE = Path("data/structures.json")
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
        self._structures: list[Structure] = []
        self._telegram: TelegramAlertService | None = None
        self._broadcaster: StateBroadcaster | None = None
        # Предыдущие значения amounts позиций для детектирования сокращений
        self._prev_amounts: dict[str, dict[str, Decimal]] = {}
        # Флаги "алерт уже отправлен" для margin_ratio, чтобы не спамить
        self._margin_ratio_alerted: dict[str, bool] = {}
        # Ключи структур, для которых уже отправлен алерт о ненайденной ноге
        self._leg_not_found_alerted: set[str] = set()
        # Задачи фоновых циклов движка
        self._engine_tasks: list[asyncio.Task] = []
        self._pending_tasks: set[asyncio.Task] = set()
        self._queue = None
        self._discover_connectors()
        self._setup_telegram()
        if self._telegram:
            from app.telegram.queue import TelegramQueue
            self._queue = TelegramQueue()
            if self._broadcaster:
                self._broadcaster._queue = self._queue

    def _discover_connectors(self) -> None:
        """Автоматически обнаруживает и инстанциирует все неабстрактные подклассы BaseExchangeConnector."""
        pkg_path = connectors_pkg.__path__
        pkg_name = connectors_pkg.__name__
        for module_info in pkgutil.iter_modules(pkg_path):
            importlib.import_module(f"{pkg_name}.{module_info.name}")

        for subclass in BaseExchangeConnector.__subclasses__():
            try:
                if inspect.isabstract(subclass):
                    continue
                connector = subclass()
                self._connectors.append(connector)
                self.states[connector.name] = connector.state
                self._prev_amounts[connector.name] = {}
                self._margin_ratio_alerted[connector.name] = False
                connector.on_margin_updated = lambda c=connector: self._on_margin_updated(c)
                connector.on_positions_updated = lambda c=connector: self._on_positions_updated(c)
            except Exception as e:
                logger.warning(f"Failed to initialize connector {subclass.__name__}: {e}")
                continue

    def _setup_telegram(self) -> None:
        bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        state_chat_ids_raw = os.environ.get("STATE_CHAT_IDS", "")
        if not bot_token:
            logger.warning("TELEGRAM_BOT_TOKEN not set — Telegram disabled")
            return
        self._telegram = TelegramAlertService(bot_token)
        self._telegram.on_message = self._handle_bot_message
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
            pairs_state_fn=lambda: format_structures_state(self._structures, self.states),
            queue=None,
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

    def _get_structure(self, exchange_name: str, ticker: str) -> Structure | None:
        for s in self._structures:
            for leg in s.legs:
                if leg.exchange == exchange_name and leg.ticker == ticker:
                    return s
        return None

    def _get_connector(self, exchange_name: str) -> BaseExchangeConnector | None:
        for c in self._connectors:
            if c.name == exchange_name:
                return c
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
        from app.telegram.queue import TelegramQueue
        critical_ids = self._critical_chat_ids()
        if not critical_ids or not self._telegram or not self._queue:
            return
        for i in range(CRITICAL_ALERT_SEND_COUNT):
            for chat_id in critical_ids:
                async def _send(chat_id=chat_id, msg=msg):
                    message_id = await self._telegram.send_alert_tracked(chat_id, msg)
                    if message_id is not None:
                        _add_alert_message_id(chat_id, message_id)

                self._queue.enqueue(TelegramQueue.CRITICAL, _send)

    async def _send_reduction_alert(self, reductions: list[dict]) -> None:
        """Отправляет батч-алерт о сокращениях с повторами согласно конфигу."""
        from app.telegram.queue import TelegramQueue
        if not self._telegram or not self._queue:
            return
        alert_ids = self._alert_chat_ids()
        if not alert_ids:
            return
        msg = format_position_reduction_batch(reductions)
        for chat_id in alert_ids:
            async def _send(chat_id=chat_id, msg=msg):
                message_id = await self._telegram.send_alert_tracked(chat_id, msg)
                if message_id is not None:
                    _add_alert_message_id(chat_id, message_id)

            self._queue.enqueue(TelegramQueue.ALERT, _send)

    async def _send_structure_alert(self, msg: str) -> None:
        from app.telegram.queue import TelegramQueue
        if not self._telegram or not self._queue:
            return
        pairs_ids = self._pairs_alert_chat_ids()
        if not pairs_ids:
            return
        for chat_id in pairs_ids:
            async def _send(chat_id=chat_id, msg=msg):
                message_id = await self._telegram.send_alert_tracked(chat_id, msg)
                if message_id is not None:
                    _add_alert_message_id(chat_id, message_id)

            self._queue.enqueue(TelegramQueue.ALERT, _send)

    def _admin_user_id(self) -> str | None:
        return os.environ.get("TELEGRAM_ADMIN_USER_ID", "").strip() or None

    async def _handle_bot_message(self, chat_id: str, text: str, from_user: str, sender_id: int | None = None,
                                  message_id: int | None = None) -> None:
        cmd = "["
        if not text.startswith(cmd):
            return
        admin_id = self._admin_user_id()
        if admin_id is not None and str(sender_id) != admin_id:
            logger.warning(
                "Unauthorized set attempt from %s (id=%s)", from_user, sender_id
            )
            if self._telegram:
                await self._telegram.send_alert(chat_id, "Нет доступа.")
            return
        json_text = text
        try:
            data = json.loads(json_text)
            if not isinstance(data, list):
                raise ValueError("ожидается JSON-массив")
            for item in data:
                if not isinstance(item.get("legs"), list):
                    raise ValueError("каждый элемент должен содержать 'legs' (массив)")
                for leg in item["legs"]:
                    if "exchange" not in leg or "ticker" not in leg:
                        raise ValueError("каждая нога должна содержать 'exchange' и 'ticker'")
            with open(_STRUCTURES_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            reply = f"OK: structures.json обновлён ({len(data)} структур)"
            logger.info("structures.json updated via bot command by %s", from_user)
        except (json.JSONDecodeError, ValueError, KeyError) as e:
            reply = f"Ошибка: {e}"
            logger.warning("set_structures command failed from %s: %s", from_user, e)
        if self._telegram:
            await self._telegram.send_alert(chat_id, reply)

    def _load_structures_from_file(self) -> list[Structure]:
        try:
            with open(_STRUCTURES_FILE, encoding="utf-8") as f:
                raw_list = json.load(f)
            if not isinstance(raw_list, list):
                return []
        except Exception:
            return []

        structures: list[Structure] = []
        for entry in raw_list:
            is_active = entry.get("is_active", False)
            raw_legs = entry.get("legs", [])
            legs: list[StructureLeg] = []
            for raw_leg in raw_legs:
                exchange = raw_leg["exchange"]
                ticker = raw_leg["ticker"]
                multiplier = Decimal(str(raw_leg.get("multiplier", 1)))
                legs.append(StructureLeg(exchange=exchange, ticker=ticker, multiplier=multiplier))
            structures.append(Structure(legs=legs, is_active=is_active))
        return structures

    def _check_leg_not_found_alerts(self, structures: list[Structure]) -> None:
        """Отправляет алерт если нога Structure не найдена в позициях (один раз)."""
        for structure in structures:
            if not structure.is_active:
                continue
            for leg in structure.legs:
                alert_key = f"{leg.exchange}/{leg.ticker}"
                state = self.states.get(leg.exchange)
                if state is None:
                    continue
                if leg.ticker not in state.positions:
                    if alert_key not in self._leg_not_found_alerted:
                        msg = format_leg_not_found(leg.exchange, leg.ticker)
                        task = asyncio.create_task(
                            self._send_structure_alert(msg)
                        )
                        self._pending_tasks.add(task)
                        task.add_done_callback(self._pending_tasks.discard)
                        self._leg_not_found_alerted.add(alert_key)
                else:
                    self._leg_not_found_alerted.discard(alert_key)

    async def _loop_structures_reload(self) -> None:
        while True:
            await asyncio.sleep(15)
            structures = self._load_structures_from_file()
            self._structures = structures
            self._check_leg_not_found_alerts(structures)

    async def _on_positions_updated(self, connector: BaseExchangeConnector) -> None:
        """Вызывается немедленно после получения свежих позиций."""
        name = connector.name
        current_positions = connector.state.positions
        prev_snapshot = dict(self._prev_amounts)
        prev = prev_snapshot[name]

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

        for r in reductions:
            structure = self._get_structure(r["exchange_name"], r["ticker"])
            if structure is None:
                continue
            task = asyncio.create_task(
                self._handle_structure_reduction(structure, r, prev_snapshot)
            )
            self._pending_tasks.add(task)
            task.add_done_callback(self._pending_tasks.discard)

    async def _handle_structure_reduction(
            self,
            structure: Structure,
            reduction: dict,
            prev_amounts: dict[str, dict[str, Decimal]],
    ) -> None:
        trigger_exchange = reduction["exchange_name"]
        trigger_ticker = reduction["ticker"]
        old_amount = reduction["old_amount"]
        new_amount = reduction["new_amount"]

        # Найти ногу-триггер
        trigger_leg: StructureLeg | None = None
        for leg in structure.legs:
            if leg.exchange == trigger_exchange and leg.ticker == trigger_ticker:
                trigger_leg = leg
                break
        if trigger_leg is None:
            return

        prev_real_x = old_amount * trigger_leg.multiplier
        new_real_x = new_amount * trigger_leg.multiplier
        delta_real = prev_real_x - new_real_x
        if delta_real <= 0:
            return

        # Определяем сторону триггера — по предыдущему направлению позиции
        trigger_state = self.states.get(trigger_exchange)
        trigger_pos = trigger_state.positions.get(trigger_ticker) if trigger_state else None
        # direction берём из текущей позиции (может не быть если закрыта полностью)
        # используем prev_amounts: нет направления в prev_amounts — берём из текущей позиции
        trigger_direction: str | None = trigger_pos.direction if trigger_pos else None

        # Суммируем real_amount всех ног той же стороны (по prev_amounts)
        same_side_legs: list[StructureLeg] = []
        opposite_side_legs: list[StructureLeg] = []

        for leg in structure.legs:
            state = self.states.get(leg.exchange)
            pos = state.positions.get(leg.ticker) if state else None
            if pos is None:
                # если позиция закрыта — используем prev_amounts
                prev_amt = prev_amounts.get(leg.exchange, {}).get(leg.ticker, Decimal(0))
                # нет направления, пропускаем при сортировке по стороне
                # относим к same_side если это сам триггер
                if leg.exchange == trigger_exchange and leg.ticker == trigger_ticker:
                    same_side_legs.append(leg)
                continue
            if trigger_direction is None:
                # direction триггера неизвестно — ничего не делаем
                continue
            if pos.direction == trigger_direction:
                same_side_legs.append(leg)
            else:
                opposite_side_legs.append(leg)

        # Если trigger_direction неизвестен (позиция полностью закрыта),
        # и нога не попала в same_side_legs — добавляем вручную чтобы рассчитать долю
        if trigger_direction is None:
            # Не можем определить стороны — просто шлём алерт без автозакрытия
            msg = format_structure_imbalance(
                trigger_exchange, trigger_ticker, old_amount, new_amount,
                closed_legs=[],
            )
            await self._send_structure_alert(msg)
            return

        # total_real той же стороны (по prev_amounts)
        total_real_side_x = Decimal(0)
        for leg in same_side_legs:
            prev_amt = prev_amounts.get(leg.exchange, {}).get(leg.ticker, Decimal(0))
            total_real_side_x += prev_amt * leg.multiplier

        if total_real_side_x == 0:
            share = Decimal(1)
        else:
            share = delta_real / total_real_side_x

        # Рассчитываем close_amount для каждой ноги противоположной стороны
        close_legs: list[dict] = []
        for leg in opposite_side_legs:
            state = self.states.get(leg.exchange)
            pos = state.positions.get(leg.ticker) if state else None
            real_amount_y = pos.amount * leg.multiplier if pos else Decimal(0)
            if real_amount_y <= 0:
                continue
            close_real_y = real_amount_y * share
            close_exchange_y = close_real_y / leg.multiplier
            close_legs.append({
                "leg": leg,
                "close_exchange_units": close_exchange_y,
                "amount_before": pos.amount if pos else Decimal(0),
            })
        closed_leg_info = None
        # Отправляем алерт об имбалансе
        if not READ_ONLY_MODE:
            closed_leg_info = [
                {"exchange": cl["leg"].exchange, "ticker": cl["leg"].ticker, "amount": cl["close_exchange_units"]}
                for cl in close_legs
            ]

        msg = format_structure_imbalance(
            trigger_exchange, trigger_ticker, old_amount, new_amount,
            closed_legs=closed_leg_info,
        )
        await self._send_structure_alert(msg)

        if READ_ONLY_MODE or not close_legs:
            return

        # Параллельное автозакрытие всех ног противоположной стороны
        await asyncio.gather(
            *(
                self._auto_close_structure_leg(
                    trigger_exchange=trigger_exchange,
                    trigger_ticker=trigger_ticker,
                    leg=cl["leg"],
                    close_exchange_units=cl["close_exchange_units"],
                    amount_before=cl["amount_before"],
                )
                for cl in close_legs
            ),
            return_exceptions=True,
        )

    async def _auto_close_structure_leg(
            self,
            trigger_exchange: str,
            trigger_ticker: str,
            leg: StructureLeg,
            close_exchange_units: Decimal,
            amount_before: Decimal,
    ) -> None:
        connector = self._get_connector(leg.exchange)
        if connector is None:
            return

        success_threshold = (amount_before - close_exchange_units) / Decimal("0.995")

        async def _attempt() -> bool:
            try:
                ok = await connector.close_position(leg.ticker, close_exchange_units, order_type="market")
            except Exception:
                logger.exception(
                    "close_position failed: %s %s qty=%s",
                    connector.name, leg.ticker, close_exchange_units,
                )
                return False
            if not ok:
                msg = (
                    f"close_position вернул False (ордер не принят): "
                    f"{connector.name} {leg.ticker} qty={close_exchange_units}"
                )
                task = asyncio.create_task(self._send_critical_alert(msg))
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.discard)
                return False
            positions_after = await connector.fetch_positions()
            pos_after = next((p for p in positions_after if p.ticker == leg.ticker), None)
            amount_after = pos_after.amount if pos_after else Decimal(0)
            return amount_after <= success_threshold

        confirmed = await _attempt()
        if confirmed:
            msg = format_auto_close_success(
                trigger_exchange, trigger_ticker,
                connector.name, leg.ticker, close_exchange_units,
            )
            await self._send_reduction_alert_raw(msg)
            return

        confirmed = await _attempt()
        if confirmed:
            msg = format_auto_close_success(
                trigger_exchange, trigger_ticker,
                connector.name, leg.ticker, close_exchange_units,
            )
            await self._send_reduction_alert_raw(msg)
            return

        msg = format_auto_close_failed(
            trigger_exchange, trigger_ticker,
            connector.name, leg.ticker, close_exchange_units,
        )
        task = asyncio.create_task(self._send_critical_alert(msg))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _send_reduction_alert_raw(self, msg: str) -> None:
        """Отправляет готовое сообщение в ALERT_CHAT_IDS."""
        from app.telegram.queue import TelegramQueue
        if not self._telegram or not self._queue:
            return
        alert_ids = self._alert_chat_ids()
        for chat_id in alert_ids:
            async def _send(chat_id=chat_id, msg=msg):
                message_id = await self._telegram.send_alert_tracked(chat_id, msg)
                if message_id is not None:
                    _add_alert_message_id(chat_id, message_id)

            self._queue.enqueue(TelegramQueue.ALERT, _send)

    async def _send_structures_to_admin(self) -> None:
        """Отправляет сырое содержимое structures.json админу при старте."""
        admin_id = self._admin_user_id()
        if not self._telegram or not admin_id:
            return
        try:
            with open(_STRUCTURES_FILE, encoding="utf-8") as f:
                raw = f.read()
        except Exception:
            raw = "(не удалось прочитать structures.json)"
        await self._telegram.send_alert(admin_id, f"<code>{raw}</code>")

    async def _send_session_start(self) -> None:
        """Отправляет разделитель сессии в ALERT_CHAT_IDS при старте."""
        from app.telegram.queue import TelegramQueue
        if not self._telegram or not self._queue:
            return
        alert_ids = self._alert_chat_ids()
        if not alert_ids:
            return
        msg = format_session_start_separator()
        for chat_id in alert_ids:
            async def _send(chat_id=chat_id, msg=msg):
                message_id = await self._telegram.send_alert_tracked(chat_id, msg)
                if message_id is not None:
                    _add_alert_message_id(chat_id, message_id)

            self._queue.enqueue(TelegramQueue.ALERT, _send)

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
        pairs_init_delay = int(os.environ.get("STRUCTURES_INIT_DELAY", "10"))
        await asyncio.sleep(pairs_init_delay)
        self._structures = self._load_structures_from_file()
        self._check_leg_not_found_alerts(self._structures)
        if self._queue:
            await self._queue.start()
        if self._telegram:
            await self._telegram.start()
            self._telegram.start_polling()
        await self._delete_all_tracked_messages()
        if self._broadcaster:
            await self._broadcaster.start()
        await self._send_structures_to_admin()
        await self._send_session_start()
        self._engine_tasks = [
            asyncio.create_task(self._loop_stale_check()),
            asyncio.create_task(self._loop_structures_reload()),
        ]

    async def stop(self) -> None:
        for task in self._engine_tasks:
            task.cancel()
        await asyncio.gather(*self._engine_tasks, return_exceptions=True)
        self._engine_tasks.clear()
        if self._broadcaster:
            await self._broadcaster.stop()
        if self._queue:
            await self._queue.stop()
        if self._telegram:
            await self._telegram.stop_polling()
            await self._telegram.stop()
        await asyncio.gather(*(c.stop() for c in self._connectors))
