"""Secret loading utilities.

Loads secrets from Prefect blocks. No env var fallback — secrets belong in Prefect.
Use these anywhere credentials are needed (API server, flows, etc.).

To set up blocks:
    prefect block register -m prefect.blocks.system
    # Then create via UI or:
    # prefect block create secret/twitter-credentials --value '{"api_key": "...", ...}'

Required blocks:
    - twitter-credentials: {api_key, api_secret, access_token, access_token_secret}
    - readwise-credentials: {api_token}
    - brave-credentials: {api_key}
    - telegram-credentials: {bot_token, chat_id}
    - anthropic-credentials: {api_key}
    - safe-docker-credentials: {api_key}
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


def _load_block(block_name: str) -> dict | None:
    """Load a Prefect Secret block and parse its JSON value.

    Returns parsed dict on success, None if block not found or not in a flow context.

    Prefect 3.x made Block.load() a coroutine. We call the Prefect API directly
    via HTTP so this works from any context (FastAPI, flow tasks, scripts) without
    fighting asyncio event loop nesting.
    """
    import os
    import urllib.request
    import urllib.parse

    prefect_api_url = os.environ.get("PREFECT_API_URL", "http://prefect-server:4200/api")

    try:
        # Fetch block document by name, including secrets
        url = f"{prefect_api_url}/block_documents/filter"
        payload = json.dumps({
            "block_documents": {"name": {"any_": [block_name]}},
            "include_secrets": True,
        }).encode()
        req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            docs = json.load(resp)

        if not docs:
            logger.debug("Prefect block %r not found", block_name)
            return None

        value = docs[0].get("data", {}).get("value")
        if value is None:
            logger.debug("Prefect block %r has no value", block_name)
            return None

        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            return json.loads(value)

        logger.debug("Prefect block %r has unexpected value type: %s", block_name, type(value).__name__)
        return None

    except Exception as exc:
        logger.debug("Failed to load Prefect block %r: %s", block_name, exc)
        return None


@dataclass(frozen=True)
class TwitterCredentials:
    api_key: str
    api_secret: str
    access_token: str
    access_token_secret: str


def load_twitter_credentials() -> TwitterCredentials | None:
    """Load Twitter credentials from Prefect block 'twitter-credentials'.
    
    Returns None if block not found or credentials incomplete.
    """
    block = _load_block("twitter-credentials")
    if not block:
        return None
    
    api_key = block.get("api_key")
    api_secret = block.get("api_secret")
    access_token = block.get("access_token")
    access_token_secret = block.get("access_token_secret")
    
    if not all([api_key, api_secret, access_token, access_token_secret]):
        return None
    
    return TwitterCredentials(
        api_key=api_key,
        api_secret=api_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
    )


def load_readwise_token() -> str | None:
    """Load Readwise API token from Prefect block 'readwise-credentials'.
    
    Returns None if block not found.
    """
    block = _load_block("readwise-credentials")
    if not block:
        return None
    return block.get("api_token")


def load_brave_api_key() -> str | None:
    """Load Brave Search API key from Prefect block 'brave-credentials'.
    
    Returns None if block not found.
    """
    block = _load_block("brave-credentials")
    if not block:
        return None
    return block.get("api_key")


@dataclass(frozen=True)
class TelegramCredentials:
    bot_token: str
    chat_id: str


def load_telegram_credentials() -> TelegramCredentials | None:
    """Load Telegram credentials from Prefect block 'telegram-credentials'.
    
    Returns None if block not found or credentials incomplete.
    """
    block = _load_block("telegram-credentials")
    if not block:
        return None
    
    bot_token = block.get("bot_token")
    chat_id = block.get("chat_id")
    
    if not bot_token or not chat_id:
        return None
    
    return TelegramCredentials(bot_token=bot_token, chat_id=chat_id)


def load_anthropic_api_key() -> str | None:
    """Load Anthropic API key from Prefect block 'anthropic-credentials'.
    
    Returns None if block not found.
    """
    block = _load_block("anthropic-credentials")
    if not block:
        return None
    return block.get("api_key")


def load_safe_docker_api_key() -> str | None:
    """Load safe-docker API key from Prefect block 'safe-docker-credentials'.
    
    Returns None if block not found.
    """
    block = _load_block("safe-docker-credentials")
    if not block:
        return None
    return block.get("api_key")
