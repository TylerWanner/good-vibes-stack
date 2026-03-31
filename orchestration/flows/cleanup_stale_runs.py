"""
cleanup_stale_runs.py — Cancel Prefect flow runs stuck in RUNNING state older than N hours.

Runs on a cron schedule. Protects active runs by only touching runs older than
the configured threshold. Notifies via Telegram on completion.
"""

import os
from datetime import datetime, timezone, timedelta

import httpx
from prefect import flow, task, get_run_logger

PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://prefect-server:4200/api")
NERVOUS_SYSTEM_URL = os.getenv("NERVOUS_SYSTEM_URL", "http://nervous-system-api:8001")
STALE_THRESHOLD_HOURS = int(os.getenv("STALE_RUN_THRESHOLD_HOURS", "2"))


@task
def find_stale_runs(threshold_hours: int) -> list[dict]:
    logger = get_run_logger()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=threshold_hours)
    cutoff_str = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")

    with httpx.Client(timeout=30) as client:
        resp = client.post(
            f"{PREFECT_API_URL}/flow_runs/filter",
            json={
                "limit": 200,
                "flow_runs": {
                    "state": {"type": {"any_": ["RUNNING"]}},
                    "start_time": {"before_": cutoff_str},
                },
            },
        )
        resp.raise_for_status()
        runs = resp.json()

    logger.info(f"Found {len(runs)} stale runs (started before {cutoff_str})")
    return runs


@task
def cancel_runs(runs: list[dict]) -> tuple[int, int]:
    logger = get_run_logger()
    success = 0
    failed = 0

    with httpx.Client(timeout=30) as client:
        for run in runs:
            rid = run["id"]
            name = run.get("name", rid)
            try:
                resp = client.post(
                    f"{PREFECT_API_URL}/flow_runs/{rid}/set_state",
                    json={
                        "state": {
                            "type": "CANCELLED",
                            "message": f"Cancelled by cleanup_stale_runs: stuck in RUNNING >{STALE_THRESHOLD_HOURS}h",
                        }
                    },
                )
                resp.raise_for_status()
                logger.info(f"  ✅ Cancelled {name} ({rid[:8]})")
                success += 1
            except Exception as e:
                logger.warning(f"  ❌ Failed to cancel {name} ({rid[:8]}): {e}")
                failed += 1

    return success, failed


@task
def reset_concurrency_limits() -> int:
    """Delete and recreate any concurrency limits with leaked active slots.

    After cancelling stale runs, slots may remain occupied because the flow
    never got a chance to release them cleanly. Resetting the limit clears
    all active slots so new runs can acquire them immediately.

    Returns the number of limits reset.
    """
    logger = get_run_logger()
    reset = 0

    # Use v2 concurrency limits API
    base = PREFECT_API_URL.rstrip("/api").rstrip("/")
    v2_url = f"{base}/api/v2/concurrency_limits"

    with httpx.Client(timeout=15) as client:
        resp = client.post(f"{v2_url}/filter", json={})
        resp.raise_for_status()
        limits = resp.json()

        for limit in limits:
            if limit.get("active_slots", 0) > 0:
                name = limit["name"]
                limit_val = limit["limit"]
                lid = limit["id"]
                try:
                    client.delete(f"{v2_url}/{lid}").raise_for_status()
                    client.post(v2_url + "/", json={
                        "name": name,
                        "limit": limit_val,
                        "active": True,
                        "slot_decay_per_second": limit.get("slot_decay_per_second", 0.0),
                    }).raise_for_status()
                    logger.info(f"  🔄 Reset concurrency limit '{name}' (was {limit['active_slots']} active slots)")
                    reset += 1
                except Exception as e:
                    logger.warning(f"  ❌ Failed to reset limit '{name}': {e}")

    return reset


@task
def notify(cancelled: int, failed: int, threshold_hours: int, limits_reset: int):
    logger = get_run_logger()
    if cancelled == 0:
        logger.info("No stale runs found — nothing to cancel.")
        return

    msg = f"🧹 Stale run cleanup: cancelled {cancelled} run(s) stuck >{threshold_hours}h"
    if failed:
        msg += f" ({failed} failed to cancel)"
    if limits_reset:
        msg += f", reset {limits_reset} concurrency limit(s)"

    # Send via Telegram
    try:
        from integrations.telegram import notify_telegram
        if notify_telegram(msg):
            logger.info(f"Notified: {msg}")
        else:
            logger.info(f"Telegram not configured — skipping notify. Message: {msg}")
    except Exception as e:
        logger.warning(f"Notification failed: {e}")


@flow(name="cleanup-stale-runs", log_prints=True)
def cleanup_stale_runs(threshold_hours: int = STALE_THRESHOLD_HOURS):
    """Cancel flow runs stuck in RUNNING state older than threshold_hours.

    After cancelling, resets any concurrency limits with leaked active slots
    so new runs can proceed immediately rather than waiting for the next cleanup.
    """
    runs = find_stale_runs(threshold_hours)
    cancelled, failed = 0, 0
    if runs:
        cancelled, failed = cancel_runs(runs)

    # Always check for leaked slots — even if no stale runs were found.
    # Slots can leak when flows are cancelled externally (drain, SIGTERM, timeout)
    # without going through cleanup-stale-runs.
    limits_reset = reset_concurrency_limits()

    if cancelled or limits_reset:
        notify(cancelled, failed, threshold_hours, limits_reset)
