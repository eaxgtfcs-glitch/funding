import asyncio
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from app.connectors.model.state import ExchangeState
from app.telegram.formatters import format_all_states_brief
from app.telegram.service import TelegramAlertService

if TYPE_CHECKING:
    from app.telegram.queue import TelegramQueue

logger = logging.getLogger(__name__)

# Путь к файлу с сохранёнными message_id (рядом с main.py, т.е. корень проекта)
_STATE_FILE = Path(__file__).parent.parent.parent / "data" / ".state_messages.json"


def _load_saved_ids() -> dict[str, dict[str, int]]:
    # Структура: {chat_id: {key: message_id}}
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
            pairs_state_fn: Callable[[], str] | None = None,
            queue: "TelegramQueue | None" = None,
    ) -> None:
        self._service = service
        self._states = states
        self._chat_ids = chat_ids
        self._update_interval = update_interval
        self._pairs_state_fn = pairs_state_fn
        self._queue = queue
        # {chat_id: {"__state__": message_id, "__pairs__": message_id}}
        self._message_ids: dict[str, dict[str, int]] = {}
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        # Удалить старые сообщения из прошлой сессии
        saved = _load_saved_ids()
        if saved:
            delete_coros = []
            for chat_id, msgs in saved.items():
                for message_id in msgs.values():
                    delete_coros.append(self._service.delete_message(chat_id, message_id))
            if delete_coros:
                await asyncio.gather(*delete_coros, return_exceptions=True)

        # Отправить одно сводное сообщение по всем коннекторам в каждый чат
        text = format_all_states_brief(self._states)
        if self._queue:
            from app.telegram.queue import TelegramQueue
            for chat_id in self._chat_ids:
                async def _send_init(chat_id=chat_id, text=text):
                    message_id = await self._service.send_message(chat_id, text)
                    if message_id is not None:
                        if chat_id not in self._message_ids:
                            self._message_ids[chat_id] = {}
                        self._message_ids[chat_id]["__state__"] = message_id
                        _save_ids(self._message_ids)

                self._queue.enqueue(TelegramQueue.STATE, _send_init)

            if self._pairs_state_fn is not None:
                pairs_text = self._pairs_state_fn()
                for chat_id in self._chat_ids:
                    async def _send_pairs_init(chat_id=chat_id, pairs_text=pairs_text):
                        message_id = await self._service.send_message(chat_id, pairs_text)
                        if message_id is not None:
                            if chat_id not in self._message_ids:
                                self._message_ids[chat_id] = {}
                            self._message_ids[chat_id]["__pairs__"] = message_id
                            _save_ids(self._message_ids)

                    self._queue.enqueue(TelegramQueue.STATE, _send_pairs_init)
        else:
            for chat_id in self._chat_ids:
                message_id = await self._service.send_message(chat_id, text)
                if message_id is not None:
                    if chat_id not in self._message_ids:
                        self._message_ids[chat_id] = {}
                    self._message_ids[chat_id]["__state__"] = message_id

            if self._pairs_state_fn is not None:
                pairs_text = self._pairs_state_fn()
                for chat_id in self._chat_ids:
                    message_id = await self._service.send_message(chat_id, pairs_text)
                    if message_id is not None:
                        if chat_id not in self._message_ids:
                            self._message_ids[chat_id] = {}
                        self._message_ids[chat_id]["__pairs__"] = message_id

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
        text = format_all_states_brief(self._states)
        coros = [self._update_one(chat_id, "__state__", text) for chat_id in self._chat_ids]
        if self._pairs_state_fn is not None:
            pairs_text = self._pairs_state_fn()
            for chat_id in self._chat_ids:
                coros.append(self._update_one(chat_id, "__pairs__", pairs_text))
        await asyncio.gather(*coros, return_exceptions=True)

    async def _send_new(self, chat_id: str, key: str, text: str) -> None:
        message_id = await self._service.send_message(chat_id, text)
        if message_id is not None:
            if chat_id not in self._message_ids:
                self._message_ids[chat_id] = {}
            self._message_ids[chat_id][key] = message_id
            _save_ids(self._message_ids)

    async def _update_one(self, chat_id: str, key: str, text: str) -> None:
        existing_id = self._message_ids.get(chat_id, {}).get(key)
        if self._queue:
            from app.telegram.queue import TelegramQueue
            if existing_id is not None:
                async def _edit(chat_id=chat_id, existing_id=existing_id, key=key, text=text):
                    try:
                        await self._service.edit_message(chat_id, existing_id, text)
                    except Exception:
                        logger.warning(
                            "Failed to edit message %s in chat %s for %s, sending new",
                            existing_id,
                            chat_id,
                            key,
                        )
                        await self._send_new(chat_id, key, text)

                self._queue.enqueue(TelegramQueue.STATE, _edit)
            else:
                async def _new(chat_id=chat_id, key=key, text=text):
                    await self._send_new(chat_id, key, text)

                self._queue.enqueue(TelegramQueue.STATE, _new)
        else:
            if existing_id is not None:
                try:
                    await self._service.edit_message(chat_id, existing_id, text)
                    return
                except Exception:
                    logger.warning(
                        "Failed to edit message %s in chat %s for %s, sending new",
                        existing_id,
                        chat_id,
                        key,
                    )
            await self._send_new(chat_id, key, text)
