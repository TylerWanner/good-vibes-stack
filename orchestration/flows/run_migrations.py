"""Alembic migration runner as a Prefect flow.

Run via:
  prefect deployment run run-migrations/run-migrations

Or directly:
  python -m orchestration.flows.run_migrations
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from prefect import flow, get_run_logger, task

# Alembic config lives at repo root (alembic.ini)
# orchestration/flows/run_migrations.py → 2 parents = repo root
_ALEMBIC_DIR = Path(__file__).resolve().parent.parent.parent  # = /app


@task
def run_alembic_upgrade(revision: str = "head") -> dict[str, str]:
    logger = get_run_logger()
    alembic_ini = _ALEMBIC_DIR / "alembic.ini"

    if not alembic_ini.exists():
        raise FileNotFoundError(f"alembic.ini not found at {alembic_ini}")

    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        raise RuntimeError("DATABASE_URL env var is required")

    logger.info("Running: alembic upgrade %s", revision)
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(alembic_ini), "upgrade", revision],
        capture_output=True,
        text=True,
        cwd=str(_ALEMBIC_DIR),
        env={**os.environ},
    )

    if result.stdout:
        logger.info("alembic stdout:\n%s", result.stdout.strip())
    if result.stderr:
        logger.info("alembic stderr:\n%s", result.stderr.strip())

    if result.returncode != 0:
        raise RuntimeError(f"alembic upgrade failed (exit {result.returncode})")

    logger.info("Migrations applied successfully")
    return {"status": "ok", "revision": revision}


@task
def get_current_revision() -> str:
    alembic_ini = _ALEMBIC_DIR / "alembic.ini"
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(alembic_ini), "current"],
        capture_output=True,
        text=True,
        cwd=str(_ALEMBIC_DIR),
        env={**os.environ},
    )
    current = (result.stdout + result.stderr).strip()
    return current


@flow(name="run-migrations", log_prints=True)
def run_migrations(revision: str = "head") -> dict[str, str]:
    """Apply pending Alembic migrations to the second brain database.

    Args:
        revision: Alembic revision target. Default 'head' applies all pending.
                  Pass a specific revision ID to migrate to a fixed point.
    """
    logger = get_run_logger()
    before = get_current_revision()
    logger.info("Current revision before migration: %s", before or "(none)")

    result = run_alembic_upgrade(revision=revision)

    after = get_current_revision()
    logger.info("Current revision after migration: %s", after)

    return {**result, "before": before, "after": after}


if __name__ == "__main__":
    run_migrations()
