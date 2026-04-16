#!/usr/bin/env python3
"""Sync credentials from .env.blocks to Prefect Secret blocks.

Usage:
    python3 scripts/sync_blocks.py
    python3 scripts/sync_blocks.py --env-file .env.blocks
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

BLOCKS: dict[str, dict[str, Any]] = {
    "readwise-credentials": {
        "env": {"api_token": "READWISE_API_TOKEN"},
        "string": False,
    },
    "brave-credentials": {
        "env": {"api_key": "BRAVE_API_KEY"},
        "string": False,
    },
    "s3-backup-credentials": {
        "env": {
            "endpoint": "R2_ENDPOINT",
            "access_key_id": "R2_ACCESS_KEY_ID",
            "secret_access_key": "R2_SECRET_ACCESS_KEY",
        },
        "string": False,
    },
    "anthropic-credentials": {
        "env": {"api_key": "WORKFLOW_ANTHROPIC_API_KEY"},
        "env_fallbacks": {"api_key": ["ANTHROPIC_API_KEY"]},
        "string": False,
    },
    "telegram-bot-token-default": {
        "env": {"_value": "TELEGRAM_BOT_TOKEN"},
        "string": True,
    },
    "safe-docker-credentials": {
        "env": {
            "api_key": "NERVOUS_SYSTEM_SAFE_DOCKER_API_KEY",
            "url": "SAFE_DOCKER_URL",
        },
        "string": False,
        "defaults": {"url": "http://safe-docker:8080"},
    },
}


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def load_env_file(path: Path) -> dict[str, str]:
    """Load key=value pairs from an env file."""
    result: dict[str, str] = {}
    if not path.exists():
        return result

    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip().strip('"').strip("'")
        if value:
            result[key.strip()] = value
    return result


def build_payload(config: dict[str, Any], env_vars: dict[str, str]) -> tuple[str | dict[str, str] | None, list[str]]:
    env_map: dict[str, str] = config["env"]
    env_fallbacks: dict[str, list[str]] = config.get("env_fallbacks", {})
    defaults: dict[str, str] = config.get("defaults", {})

    if config.get("string"):
        env_var = env_map["_value"]
        value = env_vars.get(env_var, "")
        return (value if value else None), ([env_var] if not value else [])

    payload: dict[str, str] = {}
    missing: list[str] = []

    for field, env_var in env_map.items():
        value = env_vars.get(env_var)
        if not value:
            for fallback in env_fallbacks.get(field, []):
                value = env_vars.get(fallback)
                if value:
                    break
        if not value:
            value = defaults.get(field)
        if value:
            payload[field] = value
        else:
            missing.append(env_var)

    return (payload if not missing else None), missing


def sync_blocks(env_vars: dict[str, str]) -> int:
    """Create/update Prefect Secret blocks from env vars."""
    try:
        from prefect.blocks.system import Secret
    except ImportError:
        logger.error("Prefect not installed")
        return 1

    synced = 0

    for block_name, config in BLOCKS.items():
        payload, missing = build_payload(config, env_vars)
        if payload is None:
            logger.warning("skip %s missing=%s", block_name, ", ".join(missing))
            continue

        try:
            value = payload if config.get("string") else json.dumps(payload)
            Secret(value=value).save(block_name, overwrite=True)
            logger.info("ok %s", block_name)
            synced += 1
        except Exception:
            logger.exception("failed to sync %s", block_name)

    logger.info("synced %s block(s)", synced)
    return 0


def main() -> int:
    configure_logging()

    parser = argparse.ArgumentParser(description="Sync env vars to Prefect Secret blocks")
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env.blocks"),
        help="Path to env file (default: .env.blocks)",
    )
    args = parser.parse_args()

    env_vars = dict(os.environ)
    if args.env_file.exists():
        env_vars.update(load_env_file(args.env_file))
    else:
        logger.info("%s not found, using environment variables only", args.env_file)

    return sync_blocks(env_vars)


if __name__ == "__main__":
    raise SystemExit(main())
