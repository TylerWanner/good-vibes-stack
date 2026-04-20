"""Secret loading utilities.

Loads secrets from Prefect blocks. No env var fallback — secrets belong in Prefect.
Use these anywhere credentials are needed (API server, flows, etc.).

To set up blocks:
    prefect block register -m prefect.blocks.system
    # Or run: python3 scripts/sync_blocks.py --env-file .env.blocks

Required blocks:
    - readwise-credentials: {api_token}
    - brave-credentials: {api_key}
    - telegram-bot-token-{bot}: plain string bot token (e.g. telegram-bot-token-default)
    - anthropic-credentials: {api_key}
    - safe-docker-credentials: {api_key}  # nervous-system caller credential for safe-docker

Deprecated blocks (will be removed):
    - telegram-credentials: {bot_token, chat_id} — use telegram-bot-token-{bot} instead
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request
from dataclasses import dataclass

logger = logging.getLogger(__name__)


def _fetch_block_value(block_name: str) -> str | dict | None:
    """Load a Prefect Secret block value.

    Returns the raw value from Prefect (string or dict), or None if block not found.

    Prefect 3.x made Block.load() a coroutine. We call the Prefect API directly
    via HTTP so this works from any context (FastAPI, flow tasks, scripts) without
    fighting asyncio event loop nesting.
    """
    prefect_api_url = os.environ.get("PREFECT_API_URL", "http://prefect-server:4200/api")

    try:
        payload = json.dumps({
            "block_documents": {"name": {"any_": [block_name]}},
            "include_secrets": True,
        }).encode()
        req = urllib.request.Request(
            f"{prefect_api_url}/block_documents/filter",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            docs = json.load(resp)

        if not docs:
            logger.debug("Prefect block %r not found", block_name)
            return None

        value = docs[0].get("data", {}).get("value")
        if value is None:
            logger.debug("Prefect block %r has no value", block_name)
            return None

        if isinstance(value, (str, dict)):
            return value

        logger.debug("Prefect block %r has unexpected value type: %s", block_name, type(value).__name__)
        return None
    except Exception as exc:
        logger.debug("Failed to load Prefect block %r: %s", block_name, exc)
        return None


def _load_string_block(block_name: str) -> str | None:
    """Load a Prefect Secret block and return its raw string value.

    Returns string on success, None if block not found.
    For JSON blocks, returns the unparsed JSON string.
    """
    value = _fetch_block_value(block_name)
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return json.dumps(value)
    return None


def _load_json_block(block_name: str) -> dict | None:
    """Load a Prefect Secret block and parse its JSON value.

    Returns parsed dict on success, None if block not found.
    """
    value = _fetch_block_value(block_name)
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            logger.debug("Prefect block %r did not contain valid JSON: %s", block_name, exc)
            return None
    return None


def _load_field_from_json_block(block_name: str, field: str) -> str | None:
    """Load a single string field from a JSON Prefect Secret block."""
    block = _load_json_block(block_name)
    return block.get(field) if block else None


@dataclass(frozen=True)
class S3BackupCredentials:
    endpoint: str | None
    access_key_id: str
    secret_access_key: str


def load_readwise_token() -> str | None:
    return _load_field_from_json_block("readwise-credentials", "api_token")


def load_brave_api_key() -> str | None:
    return _load_field_from_json_block("brave-credentials", "api_key")


def load_telegram_bot_token(bot: str | None = "default") -> str | None:
    """Load a Telegram bot token from Prefect Secret block.

    Block name: telegram-bot-token-{bot}
    Returns the token string, or None if block not found.
    Logs warning when using default bot.
    Logs error when block lookup fails.
    """
    if bot is None:
        bot = "default"
    if bot == "default":
        logger.warning("load_telegram_bot_token: bot not specified, using default 'default' — pass bot explicitly")
    token = _load_string_block(f"telegram-bot-token-{bot}")
    if token is None:
        logger.error("telegram-bot-token-%s block not found", bot)
    return token


# --- DEPRECATED: Use load_telegram_bot_token() instead ---

@dataclass(frozen=True)
class TelegramCredentials:
    bot_token: str
    chat_id: str


def load_telegram_credentials() -> TelegramCredentials | None:
    """DEPRECATED: Use load_telegram_bot_token() instead.
    
    Load Telegram credentials from Prefect block 'telegram-credentials'.
    Returns None if block not found or credentials incomplete.
    """
    logger.warning("load_telegram_credentials() is deprecated — use load_telegram_bot_token()")
    block = _load_json_block("telegram-credentials")
    if not block:
        return None
    
    bot_token = block.get("bot_token")
    chat_id = block.get("chat_id")
    
    if not bot_token or not chat_id:
        return None
    
    return TelegramCredentials(bot_token=bot_token, chat_id=chat_id)


def load_anthropic_api_key() -> str | None:
    return _load_field_from_json_block("anthropic-credentials", "api_key")


def load_safe_docker_api_key() -> str | None:
    return _load_field_from_json_block("safe-docker-credentials", "api_key")


def load_s3_backup_credentials() -> S3BackupCredentials | None:
    """Load S3/R2 backup credentials from Prefect block 's3-backup-credentials'.

    Returns None if block not found or incomplete.
    """
    block = _load_json_block("s3-backup-credentials")
    if not block:
        return None

    access_key_id = block.get("access_key_id")
    secret_access_key = block.get("secret_access_key")
    if not access_key_id or not secret_access_key:
        return None

    endpoint = block.get("endpoint") or None
    return S3BackupCredentials(
        endpoint=endpoint,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )


def resolve_telegram_target(notify: dict | None = None) -> tuple[str, str] | None:
    """Resolve bot token and chat_id from notify dict.

    Args:
        notify: Optional dict with 'bot' and 'chat_id' keys.
                'bot' maps to block name telegram-bot-token-{bot}.
                'chat_id' falls back to TELEGRAM_CHAT_ID env var.

    Returns:
        (token, chat_id) tuple on success, None if either is missing.
        Logs errors on failure.
    """
    notify = notify or {}
    bot = notify.get("bot")  # None → load_telegram_bot_token uses default (default)
    token = load_telegram_bot_token(bot=bot)
    chat_id = notify.get("chat_id") or os.getenv("TELEGRAM_CHAT_ID")
    if not token:
        logger.error("resolve_telegram_target: no token for bot=%s", bot)
        return None
    if not chat_id:
        logger.error("resolve_telegram_target: no chat_id provided and TELEGRAM_CHAT_ID not set")
        return None
    return token, chat_id
