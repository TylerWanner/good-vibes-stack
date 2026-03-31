#!/usr/bin/env python3
"""Startup cleanup — reconcile orphaned flow runs before worker starts.

Runs once at container start to:
1. Find RUNNING flow runs with stale heartbeats (no process executing them)
2. Mark them CRASHED (terminal state, won't block new work)
3. Reset leaked concurrency slots

This handles crash recovery — the graceful drain script (worker.py) handles
planned shutdowns.

Environment variables:
    PREFECT_API_URL: Prefect server URL (default: http://prefect-server:4200/api)
    PREFECT_WORK_POOL: Work pool to scope cleanup (default: default-pool, not currently used)
    CLEANUP_HEARTBEAT_THRESHOLD: Seconds since heartbeat before orphaned (default: 120)
    CLEANUP_STATE_AGE_THRESHOLD: Fallback seconds since state change (default: 300)

Exit codes:
    0: Success (cleanup ran, may or may not have found orphans)
    1: Error (couldn't connect to Prefect, etc.)
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import httpx

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("startup-cleanup")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://prefect-server:4200/api")
WORK_POOL = os.getenv("PREFECT_WORK_POOL", "default-pool")
HEARTBEAT_THRESHOLD = int(os.getenv("CLEANUP_HEARTBEAT_THRESHOLD", "120"))
STATE_AGE_THRESHOLD = int(os.getenv("CLEANUP_STATE_AGE_THRESHOLD", "300"))

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_iso(ts: str | None) -> datetime | None:
    """Parse ISO8601 timestamp, handling Z suffix."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


async def get_orphaned_runs(client: httpx.AsyncClient) -> list[dict]:
    """Find RUNNING flow runs with stale heartbeats.

    A run is considered orphaned if:
    - State is RUNNING
    - Heartbeat is older than HEARTBEAT_THRESHOLD seconds, OR
    - No heartbeat and state_timestamp is older than STATE_AGE_THRESHOLD seconds
    """
    resp = await client.post(
        f"{PREFECT_API_URL}/flow_runs/filter",
        json={
            "flow_runs": {"state": {"type": {"any_": ["RUNNING"]}}},
            "limit": 200,
        },
        timeout=15,
    )
    resp.raise_for_status()
    runs = resp.json()

    now = datetime.now(timezone.utc)
    heartbeat_cutoff = now - timedelta(seconds=HEARTBEAT_THRESHOLD)
    state_cutoff = now - timedelta(seconds=STATE_AGE_THRESHOLD)

    orphaned = []
    for run in runs:
        hb_dt = _parse_iso(run.get("heartbeat"))
        state_dt = _parse_iso(run.get("state_timestamp"))

        # Determine if orphaned and why
        is_orphaned = False
        reason = ""

        if hb_dt and hb_dt < heartbeat_cutoff:
            # Has heartbeat, but it's stale
            is_orphaned = True
            age_secs = int((now - hb_dt).total_seconds())
            reason = f"heartbeat stale ({age_secs}s ago)"
        elif not hb_dt and state_dt and state_dt < state_cutoff:
            # No heartbeat, state is old
            is_orphaned = True
            age_secs = int((now - state_dt).total_seconds())
            reason = f"no heartbeat, state entered {age_secs}s ago"

        if is_orphaned:
            orphaned.append({
                "id": run["id"],
                "name": run.get("name", "unknown"),
                "flow_id": run.get("flow_id"),
                "heartbeat": run.get("heartbeat"),
                "state_timestamp": run.get("state_timestamp"),
                "reason": reason,
            })

    return orphaned


async def mark_crashed(client: httpx.AsyncClient, run_id: str, reason: str) -> bool:
    """Mark a flow run as CRASHED."""
    try:
        resp = await client.post(
            f"{PREFECT_API_URL}/flow_runs/{run_id}/set_state",
            json={
                "state": {
                    "type": "CRASHED",
                    "name": "Crashed",
                    "message": f"Marked crashed by startup cleanup: {reason}",
                }
            },
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except httpx.HTTPStatusError as exc:
        logger.error("Failed to mark run %s crashed: HTTP %d", run_id, exc.response.status_code)
        return False
    except Exception as exc:
        logger.error("Failed to mark run %s crashed: %s", run_id, exc)
        return False


async def reset_leaked_slots(client: httpx.AsyncClient) -> list[dict]:
    """Reset concurrency limits with leaked active slots.

    Leaked slots occur when a flow run dies without releasing its slot.
    We detect this by checking for active_slots > 0 and delete/recreate the limit.
    """
    reset = []
    try:
        resp = await client.post(
            f"{PREFECT_API_URL}/v2/concurrency_limits/filter",
            json={},
            timeout=10,
        )
        resp.raise_for_status()
        limits = resp.json()

        for limit in limits:
            active = limit.get("active_slots", 0)
            if active > 0:
                lid = limit["id"]
                name = limit["name"]
                lim = limit["limit"]

                # Delete and recreate to reset slots
                await client.delete(
                    f"{PREFECT_API_URL}/v2/concurrency_limits/{lid}",
                    timeout=10,
                )
                await client.post(
                    f"{PREFECT_API_URL}/v2/concurrency_limits/",
                    json={"name": name, "limit": lim},
                    timeout=10,
                )
                reset.append({"name": name, "limit": lim, "was_active": active})
                logger.info("Reset leaked slot: %s (had %d active, limit=%d)", name, active, lim)

    except httpx.HTTPStatusError as exc:
        logger.error("Failed to query concurrency limits: HTTP %d", exc.response.status_code)
    except Exception as exc:
        logger.error("Failed to reset concurrency slots: %s", exc)

    return reset


async def wait_for_prefect(client: httpx.AsyncClient, max_wait: int = 60) -> bool:
    """Wait for Prefect server to be healthy before proceeding."""
    logger.info("Waiting for Prefect server at %s...", PREFECT_API_URL)
    deadline = asyncio.get_event_loop().time() + max_wait

    while asyncio.get_event_loop().time() < deadline:
        try:
            resp = await client.get(f"{PREFECT_API_URL}/health", timeout=5)
            if resp.status_code == 200:
                logger.info("Prefect server is healthy")
                return True
        except Exception:
            pass
        await asyncio.sleep(2)

    logger.error("Prefect server not healthy after %ds", max_wait)
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> int:
    """Run startup cleanup."""
    logger.info("=" * 60)
    logger.info("STARTUP CLEANUP")
    logger.info("=" * 60)
    logger.info("Thresholds: heartbeat=%ds, state_age=%ds", HEARTBEAT_THRESHOLD, STATE_AGE_THRESHOLD)

    async with httpx.AsyncClient() as client:
        # Wait for Prefect to be ready
        if not await wait_for_prefect(client):
            return 1

        # Find orphaned runs
        logger.info("Checking for orphaned RUNNING flow runs...")
        try:
            orphaned = await get_orphaned_runs(client)
        except httpx.HTTPStatusError as exc:
            logger.error("Failed to query flow runs: HTTP %d", exc.response.status_code)
            return 1
        except Exception as exc:
            logger.error("Failed to query flow runs: %s", exc)
            return 1

        if not orphaned:
            logger.info("No orphaned runs found")
        else:
            logger.warning("Found %d orphaned run(s):", len(orphaned))
            for run in orphaned:
                logger.warning("  - %s (%s...): %s", run["name"], run["id"][:8], run["reason"])

            # Mark them crashed
            marked = 0
            for run in orphaned:
                if await mark_crashed(client, run["id"], run["reason"]):
                    marked += 1
                    logger.info("Marked CRASHED: %s (%s...)", run["name"], run["id"][:8])

            logger.info("Marked %d/%d runs as CRASHED", marked, len(orphaned))

        # Reset leaked concurrency slots
        logger.info("Checking for leaked concurrency slots...")
        reset = await reset_leaked_slots(client)
        if reset:
            logger.info("Reset %d leaked slot(s)", len(reset))
        else:
            logger.info("No leaked concurrency slots")

    logger.info("=" * 60)
    logger.info("STARTUP CLEANUP COMPLETE")
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        logger.info("Interrupted")
        sys.exit(1)
