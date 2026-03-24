import asyncio
import logging
import os

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    level=logging.ERROR,
)
logger = logging.getLogger(__name__)

_PASSPHRASE_TIMEOUT = 120


def _parse_ids(raw: str) -> list[str]:
    return [c.strip() for c in raw.split(",") if c.strip()]


async def _wait_for_activation(
        telegram, admin_id: str, critical_ids: list[str], tracked_messages: list[tuple[str, int]]
) -> None:
    """
    Waits for admin to send correct passphrase and unlocks the vault.
    Sends a timeout notification after 120s if nothing is received.
    Loops on wrong passphrase until the correct one is entered.
    Collects all sent/received message_ids into tracked_messages.
    """
    from app.helper.key_vault import vault

    passphrase_event: asyncio.Event = asyncio.Event()
    passphrase_holder: list[str] = []
    incoming_msg_id_holder: list[int | None] = []
    timeout_notified = False

    async def _on_message(chat_id: str, text: str, from_user: str, sender_id: int | None = None,
                          message_id: int | None = None) -> None:
        if str(sender_id) != admin_id and chat_id != admin_id:
            return
        passphrase_holder.clear()
        passphrase_holder.append(text)
        incoming_msg_id_holder.clear()
        incoming_msg_id_holder.append(message_id)
        passphrase_event.set()

    telegram.on_message = _on_message
    mid = await telegram.send_message(admin_id, "Введите passphrase для активации системы")
    if mid:
        tracked_messages.append((admin_id, mid))

    # Send timeout notification once if no passphrase in 120s
    async def _timeout_notifier() -> None:
        nonlocal timeout_notified
        await asyncio.sleep(_PASSPHRASE_TIMEOUT)
        if not timeout_notified:
            timeout_notified = True
            timeout_msg = "Система не активирована: passphrase не введён в течение 2 минут"
            if critical_ids:
                await asyncio.gather(
                    *[telegram.send_alert(cid, timeout_msg) for cid in critical_ids],
                    return_exceptions=True,
                )

    timeout_task = asyncio.create_task(_timeout_notifier())

    while True:
        passphrase_event.clear()
        await passphrase_event.wait()

        user_msg_id = incoming_msg_id_holder[0] if incoming_msg_id_holder else None
        if user_msg_id is not None:
            tracked_messages.append((admin_id, user_msg_id))

        passphrase = passphrase_holder[0] if passphrase_holder else ""
        try:
            vault.unlock(passphrase)
        except Exception:
            mid = await telegram.send_message(admin_id, "Неверная passphrase")
            if mid:
                tracked_messages.append((admin_id, mid))
            continue
        timeout_task.cancel()
        break


async def main() -> None:
    logger.critical("Starting application")
    from dotenv import load_dotenv
    load_dotenv()

    from app.telegram.service import TelegramAlertService

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    admin_id = os.environ.get("TELEGRAM_ADMIN_USER_ID", "").strip()
    critical_ids = _parse_ids(os.environ.get("CRITICAL_ALERT_CHAT_IDS", ""))

    if not bot_token or not admin_id:
        logger.critical("TELEGRAM_BOT_TOKEN or TELEGRAM_ADMIN_USER_ID not set — cannot request passphrase")
        return

    telegram = TelegramAlertService(bot_token)
    await telegram.start()
    telegram.start_polling()

    tracked_messages: list[tuple[str, int]] = []
    await _wait_for_activation(telegram, admin_id, critical_ids, tracked_messages)

    act_mid = await telegram.send_message(admin_id, "Система активирована")
    if act_mid:
        tracked_messages.append((admin_id, act_mid))
    await asyncio.gather(
        *[telegram.delete_message(cid, mid) for cid, mid in tracked_messages],
        return_exceptions=True,
    )
    await telegram.stop_polling()
    await telegram.stop()

    from app.engine.engine import MonitoringEngine
    engine = MonitoringEngine()
    await engine.start()
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        await engine.stop()


if __name__ == "__main__":
    asyncio.run(main())
