from __future__ import annotations

import json
import logging
from functools import lru_cache
from typing import Any

logger = logging.getLogger(__name__)


@lru_cache(maxsize=32)
def load_notify_context(context_name: str) -> dict[str, Any]:
    """Load a notification sender context from a Prefect Secret block.

    Expected block name: ``telegram-notify-<context_name>``

    Expected block value JSON shape:
      {
        "channel": "telegram",
        "account_id": "default",
        "telegram_bot_token": "<bot token>"
      }

    For convenience, a plain string secret is treated as a Telegram bot token.
    """
    try:
        from prefect.blocks.system import Secret
    except Exception as exc:  # pragma: no cover - runtime env dependent
        raise RuntimeError(f"Prefect blocks unavailable: {exc}") from exc

    block_name = f"telegram-notify-{context_name}"
    secret = Secret.load(block_name)
    value = secret.get()

    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("{"):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                logger.warning("notify context %s contained invalid JSON; treating as token", context_name)
        return {
            "channel": "telegram",
            "account_id": "default",
            "telegram_bot_token": text,
        }

    raise ValueError(f"Unsupported notify context payload for {context_name!r}: {type(value).__name__}")
