import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

_TELEGRAM_API_BASE = "https://api.telegram.org/bot{token}"


class TelegramAlertService:
    """
    Sends text alerts to Telegram chats/channels via the Bot API.

    Uses httpx for async HTTP without pulling in the full aiogram stack —
    keeps the service thin and dependency-free for the alert path.
    """

    def __init__(self, bot_token: str) -> None:
        if not bot_token:
            raise ValueError("bot_token must not be empty")
        self._base = _TELEGRAM_API_BASE.format(token=bot_token)
        self._client: httpx.AsyncClient | None = None

    async def start(self) -> None:
        self._client = httpx.AsyncClient(timeout=5)

    async def stop(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def send_alert(self, chat_id: str, message: str) -> None:
        """Send a plain-text message to a Telegram chat or channel."""
        if not self._client:
            logger.error("TelegramAlertService not started — cannot send alert")
            return

        # Telegram Bot API ожидает HTML-разметку для жирного текста в алертах
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
        }
        try:
            response = await self._client.post(f"{self._base}/sendMessage", json=payload)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.exception(
                "Telegram API error for chat %s: %s — %s",
                chat_id,
                exc.response.status_code,
                exc.response.text,
            )
        except httpx.RequestError as exc:
            logger.exception("Telegram request failed for chat %s: %s", chat_id, exc)

    async def broadcast(self, chat_ids: list[str], message: str) -> None:
        """Send the same message to multiple chats concurrently."""
        await asyncio.gather(
            *[self.send_alert(c, message) for c in chat_ids],
            return_exceptions=True,
        )

    async def send_message(self, chat_id: str, text: str) -> int | None:
        """Send a message and return the message_id, or None on failure."""
        if not self._client:
            logger.error("TelegramAlertService not started — cannot send message")
            return None
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
        }
        try:
            response = await self._client.post(f"{self._base}/sendMessage", json=payload)
            response.raise_for_status()
            return response.json()["result"]["message_id"]
        except httpx.HTTPStatusError as exc:
            logger.exception(
                "Telegram API error (sendMessage) for chat %s: %s — %s",
                chat_id,
                exc.response.status_code,
                exc.response.text,
            )
        except httpx.RequestError as exc:
            logger.exception("Telegram request failed (sendMessage) for chat %s: %s", chat_id, exc)
        except (KeyError, ValueError) as exc:
            logger.exception("Unexpected response format (sendMessage) for chat %s: %s", chat_id, exc)
        return None

    async def edit_message(self, chat_id: str, message_id: int, text: str) -> None:
        """Edit an existing message."""
        if not self._client:
            logger.error("TelegramAlertService not started — cannot edit message")
            return
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "HTML",
        }
        try:
            response = await self._client.post(f"{self._base}/editMessageText", json=payload)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 400 and "message is not modified" in exc.response.text:
                return
            logger.exception(
                "Telegram API error (editMessageText) for chat %s msg %s: %s — %s",
                chat_id,
                message_id,
                exc.response.status_code,
                exc.response.text,
            )
            raise
        except httpx.RequestError as exc:
            logger.exception(
                "Telegram request failed (editMessageText) for chat %s msg %s: %s",
                chat_id,
                message_id,
                exc,
            )
            raise

    async def delete_message(self, chat_id: str, message_id: int) -> None:
        """Delete a message."""
        if not self._client:
            logger.error("TelegramAlertService not started — cannot delete message")
            return
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
        }
        try:
            response = await self._client.post(f"{self._base}/deleteMessage", json=payload)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "Telegram API error (deleteMessage) for chat %s msg %s: %s — %s",
                chat_id,
                message_id,
                exc.response.status_code,
                exc.response.text,
            )
        except httpx.RequestError as exc:
            logger.exception(
                "Telegram request failed (deleteMessage) for chat %s msg %s: %s",
                chat_id,
                message_id,
                exc,
            )
