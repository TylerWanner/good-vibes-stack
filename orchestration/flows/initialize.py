"""Initialize the second brain stack — run once after infra is up and flows are deployed.

Sequence:
  1. bootstrap-blocks       — delegates to sync_blocks.py (canonical block creation)
  2. set-concurrency-limits — ollama=1, scrapling=3
  3. run-migrations         — alembic upgrade head (subflow)
  4. validate               — verify everything is wired up correctly

Run via:
  prefect deployment run 'initialize/initialize'

Or directly (no worker needed):
  python -m orchestration.flows.initialize

Note: for full block bootstrap (twitter, readwise, brave, r2), populate .env.blocks
and run scripts/sync_blocks.py before or after initialize.
"""
from __future__ import annotations

import json
import os
import subprocess
import urllib.request
from typing import Any

from prefect import flow, get_run_logger, task

from orchestration.flows.run_migrations import run_migrations

PREFECT_API_URL = os.environ.get("PREFECT_API_URL", "http://prefect-server:4200/api")
CONCURRENCY_LIMITS = {"ollama": 1, "scrapling": 3}
BOOTSTRAP_BLOCKS_SCRIPT = os.path.join(
    os.path.dirname(__file__), "../../scripts/sync_blocks.py"
)


# ---------------------------------------------------------------------------
# Task: bootstrap-blocks
# ---------------------------------------------------------------------------

@task(name="bootstrap-blocks")
def sync_blocks() -> dict[str, Any]:
    """Delegate block creation to the canonical sync_blocks.py script.

    Requires .env.blocks to be present — skips gracefully if not found.
    """
    logger = get_run_logger()
    script = os.path.abspath(BOOTSTRAP_BLOCKS_SCRIPT)

    if not os.path.isfile(script):
        logger.warning("sync_blocks.py not found at %s — skipping", script)
        return {"status": "skipped", "reason": "script not found"}

    env_blocks = os.path.join(os.path.dirname(script), "../../../../.env.blocks")
    env_blocks = os.path.abspath(env_blocks)

    if not os.path.isfile(env_blocks):
        logger.info(".env.blocks not found — skipping block bootstrap (run scripts/sync_blocks.py manually)")
        return {"status": "skipped", "reason": ".env.blocks not found"}

    result = subprocess.run(
        ["python3", script, "--env-file", env_blocks],
        capture_output=True,
        text=True,
        env={**os.environ, "PREFECT_API_URL": PREFECT_API_URL},
    )
    for line in result.stdout.splitlines():
        logger.info(line)
    if result.returncode != 0:
        logger.warning("sync_blocks.py exited %d: %s", result.returncode, result.stderr)
        return {"status": "error", "stderr": result.stderr}

    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Task: set-concurrency-limits
# ---------------------------------------------------------------------------

@task(name="set-concurrency-limits")
def set_concurrency_limits() -> dict[str, Any]:
    logger = get_run_logger()
    import urllib.error

    results = {}
    for name, limit in CONCURRENCY_LIMITS.items():
        payload = json.dumps({"name": name, "limit": limit}).encode()
        req = urllib.request.Request(
            f"{PREFECT_API_URL}/v2/concurrency_limits/",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            urllib.request.urlopen(req, timeout=10)
            logger.info("set concurrency limit: %s=%d", name, limit)
            results[name] = limit
        except urllib.error.HTTPError as exc:
            if exc.code == 409:
                logger.info("concurrency limit already exists: %s (skipping)", name)
                results[name] = limit
            else:
                raise

    return results


# ---------------------------------------------------------------------------
# Task: validate
# ---------------------------------------------------------------------------

@task(name="validate")
def validate() -> dict[str, Any]:
    logger = get_run_logger()
    from prefect.blocks.system import Secret

    issues = []

    # Check Prefect API
    try:
        urllib.request.urlopen(f"{PREFECT_API_URL}/health", timeout=5)
        logger.info("✅ Prefect API reachable")
    except Exception as exc:
        issues.append(f"Prefect API unreachable: {exc}")

    # Check blocks
    for block_name in ["s3-backup-credentials", "twitter-credentials", "readwise-credentials", "brave-credentials"]:
        try:
            Secret.load(block_name)
            logger.info("✅ block: %s", block_name)
        except Exception:
            logger.warning("⚠️  block missing: %s (run scripts/sync_blocks.py)", block_name)

    # Check nervous-system-api
    try:
        urllib.request.urlopen("http://nervous-system-api:8001/health", timeout=5)
        logger.info("✅ nervous-system-api reachable")
    except Exception:
        logger.info("ℹ️  nervous-system-api not up yet — start it after initialize completes")

    if issues:
        raise RuntimeError(f"Validation failed: {'; '.join(issues)}")

    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Flow: initialize
# ---------------------------------------------------------------------------

@flow(name="initialize", log_prints=True)
def initialize() -> dict[str, Any]:
    """Initialize the second brain stack.

    Run once after:
      1. Infra is up (postgres, prefect-server, scrapling-fetcher, prefect-worker)
      2. Flows are deployed (bootstrap container has run)

    Idempotent — safe to re-run.
    """
    logger = get_run_logger()
    logger.info("Starting second-brain initialization sequence...")

    # Prefect infrastructure first
    blocks_result = sync_blocks()
    limits_result = set_concurrency_limits()

    # App layer
    migrations_result = run_migrations()

    # Validate everything
    validation_result = validate()

    logger.info("✅ Initialization complete.")
    return {
        "blocks": blocks_result,
        "concurrency_limits": limits_result,
        "migrations": migrations_result,
        "validation": validation_result,
    }


if __name__ == "__main__":
    initialize()
