"""Raw Telegram HTTP client. No Prefect dependency.

Callers are responsible for loading the bot token via shared.secrets.load_telegram_bot_token().
"""
from __future__ import annotations

import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)


def send_message(
    bot_token: str,
    chat_id: str,
    text: str,
    buttons: list[list[dict[str, str]]] | None = None,
    reply_to_message_id: str | int | None = None,
) -> bool:
    """Send a message via Telegram Bot API.

    Args:
        bot_token: Telegram bot token.
        chat_id: Target chat ID.
        text: Message text.
        buttons: Optional inline keyboard. Each inner list is a row of buttons.
                 Each button is a dict with "text" and "callback_data" keys.
                 e.g. [[{"text": "✅ Yes", "callback_data": "foo:yes"},
                        {"text": "❌ No",  "callback_data": "foo:no"}]]
        reply_to_message_id: Optional message ID to reply to.

    Returns:
        True on success, False on failure.
    """
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    # Strip channel prefix if present (e.g. "telegram:5851769790" → "5851769790")
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
        return True
    except Exception as exc:
        logger.warning("telegram send_message failed: %s", exc)
        return False


def send_with_buttons(
    bot_token: str,
    chat_id: str,
    text: str,
    buttons: list[list[dict[str, str]]],
    reply_to_message_id: str | int | None = None,
) -> bool:
    """Send a message with inline keyboard buttons.

    Convenience wrapper around send_message with required buttons param.
    """
    return send_message(
        bot_token=bot_token,
        chat_id=chat_id,
        text=text,
        buttons=buttons,
        reply_to_message_id=reply_to_message_id,
    )


# Backwards compatibility alias — matches old function name
send_telegram_message = send_message


def notify_telegram(
    text: str,
    buttons: list[list[dict[str, str]]] | None = None,
    reply_to_message_id: str | int | None = None,
) -> bool:
    """Send a Telegram notification using credentials from Prefect block.
    
    Convenience wrapper — loads telegram-bot-token-iggy block and TELEGRAM_CHAT_ID env var.
    Returns True on success, False if credentials missing or send failed.
    
    This is a compatibility shim for the old notify_telegram() function.
    New code should use resolve_telegram_target() + send_message() directly.
    """
    import os
    from shared.secrets import load_telegram_bot_token
    
    token = load_telegram_bot_token(bot="iggy")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    
    if not token or not chat_id:
        logger.debug("Telegram credentials not configured, skipping notification")
        return False
    
    return send_message(
        bot_token=token,
        chat_id=chat_id,
        text=text,
        buttons=buttons,
        reply_to_message_id=reply_to_message_id,
    )
