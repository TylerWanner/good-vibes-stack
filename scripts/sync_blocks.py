#!/usr/bin/env python3
"""Sync credentials from .env.blocks to Prefect Secret blocks.

Usage:
    python3 scripts/sync_blocks.py
    python3 scripts/sync_blocks.py --env-file .env.blocks
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def load_env_file(path: Path) -> dict[str, str]:
    """Load key=value pairs from an env file."""
    result = {}
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


def sync_blocks(env_vars: dict[str, str]) -> int:
    """Create/update Prefect Secret blocks from env vars."""
    try:
        from prefect.blocks.system import Secret
    except ImportError:
        print("Error: Prefect not installed", file=sys.stderr)
        return 1

    created = 0

    # Readwise (JSON blob)
    if env_vars.get("READWISE_API_TOKEN"):
        try:
            creds = {"api_token": env_vars["READWISE_API_TOKEN"]}
            Secret(value=json.dumps(creds)).save("readwise-credentials", overwrite=True)
            print("  ✓ readwise-credentials")
            created += 1
        except Exception as e:
            print(f"  ✗ readwise-credentials: {e}", file=sys.stderr)

    # Brave (JSON blob)
    if env_vars.get("BRAVE_API_KEY"):
        try:
            creds = {"api_key": env_vars["BRAVE_API_KEY"]}
            Secret(value=json.dumps(creds)).save("brave-credentials", overwrite=True)
            print("  ✓ brave-credentials")
            created += 1
        except Exception as e:
            print(f"  ✗ brave-credentials: {e}", file=sys.stderr)

    # R2 credentials (JSON blob)
    r2_keys = ["R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"]
    if all(env_vars.get(k) for k in r2_keys):
        try:
            creds = {
                "endpoint": env_vars["R2_ENDPOINT"],
                "access_key_id": env_vars["R2_ACCESS_KEY_ID"],
                "secret_access_key": env_vars["R2_SECRET_ACCESS_KEY"],
            }
            Secret(value=json.dumps(creds)).save("s3-backup-credentials", overwrite=True)
            print("  ✓ s3-backup-credentials")
            created += 1
        except Exception as e:
            print(f"  ✗ s3-backup-credentials: {e}", file=sys.stderr)

    # Anthropic (JSON blob)
    if env_vars.get("WORKFLOW_ANTHROPIC_API_KEY"):
        try:
            creds = {"api_key": env_vars["WORKFLOW_ANTHROPIC_API_KEY"]}
            Secret(value=json.dumps(creds)).save("anthropic-credentials", overwrite=True)
            print("  ✓ anthropic-credentials")
            created += 1
        except Exception as e:
            print(f"  ✗ anthropic-credentials: {e}", file=sys.stderr)

    # Telegram bot token (plain string — used by load_telegram_bot_token(bot="default"))
    if env_vars.get("TELEGRAM_BOT_TOKEN"):
        try:
            Secret(value=env_vars["TELEGRAM_BOT_TOKEN"]).save("telegram-bot-token-default", overwrite=True)
            print("  ✓ telegram-bot-token-default")
            created += 1
        except Exception as e:
            print(f"  ✗ telegram-bot-token-default: {e}", file=sys.stderr)

    # safe-docker compatibility block (older callers still expect api_key naming)
    safe_docker_key = env_vars.get("SAFE_DOCKER_API_KEY") or env_vars.get("SAFE_DOCKER_AUTH_SECRET")
    if safe_docker_key:
        try:
            creds = {
                "api_key": safe_docker_key,
                "url": env_vars.get("SAFE_DOCKER_URL", "http://safe-docker:8080"),
            }
            Secret(value=json.dumps(creds)).save("safe-docker-credentials", overwrite=True)
            print("  ✓ safe-docker-credentials")
            created += 1
        except Exception as e:
            print(f"  ✗ safe-docker-credentials: {e}", file=sys.stderr)

    print(f"\nSynced {created} blocks")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync env vars to Prefect Secret blocks")
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env.blocks"),
        help="Path to env file (default: .env.blocks)",
    )
    args = parser.parse_args()

    # Load from file + environment (file takes precedence)
    env_vars = dict(os.environ)
    if args.env_file.exists():
        env_vars.update(load_env_file(args.env_file))
    else:
        print(f"Note: {args.env_file} not found, using environment variables only")

    return sync_blocks(env_vars)


if __name__ == "__main__":
    raise SystemExit(main())
