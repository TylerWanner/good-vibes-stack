from __future__ import annotations

import logging
from typing import Any

import requests

from shared.secrets import load_telegram_credentials

logger = logging.getLogger(__name__)


def send_telegram_message(
    bot_token: str,
    chat_id: str,
    text: str,
    buttons: list[list[dict[str, str]]] | None = None,
    reply_to_message_id: str | int | None = None,
) -> None:
    """Send a message via Telegram Bot API. Fire-and-forget — logs on failure.

    Args:
        bot_token: Telegram bot token.
        chat_id: Target chat ID.
        text: Message text.
        buttons: Optional inline keyboard. Each inner list is a row of buttons.
                 Each button is a dict with "text" and "callback_data" keys.
                 e.g. [[{"text": "✅ Yes", "callback_data": "foo:yes"},
                         {"text": "❌ No",  "callback_data": "foo:no"}]]
    """
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    # Strip channel prefix if present (e.g. "telegram:123456789" → "123456789")
    if isinstance(chat_id, str) and ":" in chat_id:
        chat_id = chat_id.split(":", 1)[1]
    payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
    if reply_to_message_id is not None:
        payload["reply_parameters"] = {"message_id": int(reply_to_message_id)}
    if buttons:
        payload["reply_markup"] = {
            "inline_keyboard": [
                [{"text": btn["text"], "callback_data": btn["callback_data"]} for btn in row]
                for row in buttons
            ]
        }
    try:
        resp = requests.post(api_url, json=payload, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("telegram notification failed: %s", exc)


def notify_telegram(
    text: str,
    buttons: list[list[dict[str, str]]] | None = None,
    reply_to_message_id: str | int | None = None,
) -> bool:
    """Send a Telegram notification using credentials from Prefect block.
    
    Convenience wrapper — loads telegram-credentials block automatically.
    Returns True on success, False if credentials missing or send failed.
    """
    creds = load_telegram_credentials()
    if not creds:
        logger.debug("Telegram credentials not configured, skipping notification")
        return False
    
    try:
        send_telegram_message(
            creds.bot_token,
            creds.chat_id,
            text,
            buttons=buttons,
            reply_to_message_id=reply_to_message_id,
        )
        return True
    except Exception as exc:
        logger.warning("notify_telegram failed: %s", exc)
        return False
