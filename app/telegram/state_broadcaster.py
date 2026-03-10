import asyncio
import json
import logging
from pathlib import Path

from app.connectors.model.state import ExchangeState
from app.telegram.formatters import format_exchange_state
from app.telegram.service import TelegramAlertService

logger = logging.getLogger(__name__)

# Путь к файлу с сохранёнными message_id (рядом с main.py, т.е. корень проекта)
_STATE_FILE = Path(__file__).parent.parent.parent / ".state_messages.json"


def _load_saved_ids() -> dict[str, dict[str, int]]:
    # Структура: {chat_id: {exchange_name: message_id}}
    if not _STATE_FILE.exists():
        return {}
    try:
        with open(_STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        logger.warning("Failed to load state messages file: %s", exc)
        return {}


def _save_ids(ids: dict[str, dict[str, int]]) -> None:
    try:
        with open(_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(ids, f)
    except Exception as exc:
        logger.warning("Failed to save state messages file: %s", exc)


class StateBroadcaster:
    def __init__(
            self,
            service: TelegramAlertService,
            states: dict[str, ExchangeState],
            chat_ids: list[str],
            update_interval: int = 30,
    ) -> None:
        self._service = service
        self._states = states
        self._chat_ids = chat_ids
        self._update_interval = update_interval
        # {chat_id: {exchange_name: message_id}}
        self._message_ids: dict[str, dict[str, int]] = {}
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        # Удалить старые сообщения из прошлой сессии
        saved = _load_saved_ids()
        if saved:
            delete_coros = []
            for chat_id, exchange_msgs in saved.items():
                for exchange_name, message_id in exchange_msgs.items():
                    delete_coros.append(self._service.delete_message(chat_id, message_id))
            if delete_coros:
                await asyncio.gather(*delete_coros, return_exceptions=True)

        # Отправить начальные сообщения для каждого коннектора в каждый чат
        for exchange_name, state in self._states.items():
            text = format_exchange_state(state)
            for chat_id in self._chat_ids:
                message_id = await self._service.send_message(chat_id, text)
                if message_id is not None:
                    if chat_id not in self._message_ids:
                        self._message_ids[chat_id] = {}
                    self._message_ids[chat_id][exchange_name] = message_id

        _save_ids(self._message_ids)

        self._task = asyncio.create_task(self._broadcast_loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _broadcast_loop(self) -> None:
        while True:
            await asyncio.sleep(self._update_interval)
            await self._update_all()

    async def _update_all(self) -> None:
        connector_names = list(self._states.keys())
        coros = []
        for exchange_name in connector_names:
            state = self._states.get(exchange_name)
            if state is None:
                continue
            text = format_exchange_state(state)
            for chat_id in self._chat_ids:
                coros.append(self._update_one(chat_id, exchange_name, text))
        await asyncio.gather(*coros, return_exceptions=True)

    async def _update_one(self, chat_id: str, exchange_name: str, text: str) -> None:
        existing_id = self._message_ids.get(chat_id, {}).get(exchange_name)
        if existing_id is not None:
            try:
                await self._service.edit_message(chat_id, existing_id, text)
                return
            except Exception as exc:
                # Сообщение удалено вручную или недоступно — создаём новое
                logger.warning(
                    "Failed to edit message %s in chat %s for %s, sending new",
                    existing_id,
                    chat_id,
                    exchange_name,
                )

        # Отправляем новое сообщение
        message_id = await self._service.send_message(chat_id, text)
        if message_id is not None:
            if chat_id not in self._message_ids:
                self._message_ids[chat_id] = {}
            self._message_ids[chat_id][exchange_name] = message_id
            _save_ids(self._message_ids)
