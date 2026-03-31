#!/usr/bin/env python3
"""Prefect worker with graceful drain semantics.

Replaces the CLI `prefect worker start` with explicit signal handling and
configurable shutdown policies. Based on guidance from Prefect maintainers.

On SIGTERM/SIGINT:
  1. Stop accepting new work (signal handled, stop event set)
  2. Send SIGTERM to all "prefect flow-run execute" subprocesses
  3. Wait grace period for children to reschedule/exit
  4. Apply shutdown policy (reschedule, cancel, or crash_via_kill)
  5. Clean up worker

Environment variables:
  PREFECT_WORK_POOL: Work pool name (default: default-pool)
  PREFECT_WORKER_NAME: Worker name (default: good-vibes-worker)
  PREFECT_SHUTDOWN_POLICY: reschedule|cancel|crash_via_kill (default: reschedule)
  WORKER_DRAIN_GRACE: Seconds to wait for children (default: 30)
  OTEL_EXPORTER_OTLP_ENDPOINT: If set, enables OTEL tracing

Shutdown policies:
  - reschedule: Mark running runs AwaitingRetry via SIGTERM (recommended)
  - cancel: Cancel running runs (terminal state)
  - crash_via_kill: SIGKILL children, rely on heartbeat automation to mark Crashed
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import sys
import time
from typing import Literal

# ---------------------------------------------------------------------------
# Environment setup — must happen before Prefect imports
# ---------------------------------------------------------------------------

# Ensure flow-run subprocesses will reschedule on SIGTERM
os.environ.setdefault("PREFECT_FLOW_RUN_EXECUTE_SIGTERM_BEHAVIOR", "reschedule")

# ---------------------------------------------------------------------------
# OTEL setup — initialize before worker starts
# ---------------------------------------------------------------------------

try:
    from shared.telemetry import setup_tracing
    setup_tracing(service_name="prefect-worker")
except ImportError:
    pass  # telemetry module not available
except Exception as exc:
    print(f"[worker] OTEL setup failed: {exc}", file=sys.stderr)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("worker")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ShutdownPolicy = Literal["reschedule", "cancel", "crash_via_kill"]

WORK_POOL = os.getenv("PREFECT_WORK_POOL", "default-pool")
WORKER_NAME = os.getenv("PREFECT_WORKER_NAME", "good-vibes-worker")
SHUTDOWN_POLICY: ShutdownPolicy = os.getenv("PREFECT_SHUTDOWN_POLICY", "reschedule")  # type: ignore
GRACE_SECONDS = int(os.getenv("WORKER_DRAIN_GRACE", "30"))

# ---------------------------------------------------------------------------
# Process discovery (portable /proc scan, no psutil dependency)
# ---------------------------------------------------------------------------


def _pids_matching_cmdline(substr: str) -> list[int]:
    """Find PIDs whose cmdline contains substr.

    Uses /proc filesystem — works on Linux containers (K8s, Docker).
    """
    pids: list[int] = []
    this_pid = os.getpid()

    try:
        entries = os.listdir("/proc")
    except FileNotFoundError:
        # Not on Linux — fall back to empty list
        logger.warning("/proc not available — cannot discover child processes")
        return pids

    for entry in entries:
        if not entry.isdigit():
            continue
        pid = int(entry)
        if pid == this_pid:
            continue

        cmdline_path = f"/proc/{pid}/cmdline"
        try:
            with open(cmdline_path, "rb") as f:
                raw = f.read()
            # cmdline is null-separated
            parts = [p.decode("utf-8", "replace") for p in raw.split(b"\x00") if p]
            cmd = " ".join(parts)
            if substr in cmd:
                pids.append(pid)
        except (FileNotFoundError, PermissionError):
            # Process exited or we can't read it
            continue
        except Exception:
            continue

    return pids


def _signal_pids(pids: list[int], sig: int) -> int:
    """Send signal to PIDs. Returns count of successful sends."""
    sent = 0
    for pid in pids:
        try:
            os.kill(pid, sig)
            sent += 1
        except (ProcessLookupError, PermissionError):
            pass
    return sent


def _wait_for_exit(pids: list[int], timeout: float) -> list[int]:
    """Wait up to timeout seconds for PIDs to exit. Returns PIDs still alive."""
    deadline = time.time() + timeout
    alive = set(pids)

    while alive and time.time() < deadline:
        for pid in list(alive):
            try:
                os.kill(pid, 0)  # signal 0 = existence check
            except ProcessLookupError:
                alive.discard(pid)
        if alive:
            time.sleep(0.2)

    return list(alive)


# ---------------------------------------------------------------------------
# Shutdown logic
# ---------------------------------------------------------------------------


def _drain_flow_subprocesses(policy: ShutdownPolicy) -> None:
    """Drain in-flight flow-run execute processes according to policy."""
    pids = _pids_matching_cmdline("prefect flow-run execute")

    if not pids:
        logger.info("No flow subprocesses to drain")
        return

    logger.info("Found %d flow subprocess(es): %s", len(pids), pids)

    if policy == "crash_via_kill":
        # Immediate SIGKILL — rely on heartbeat automation to mark Crashed
        logger.info("Policy=crash_via_kill — sending SIGKILL")
        _signal_pids(pids, signal.SIGKILL)
        return

    # Send SIGTERM to trigger reschedule behavior
    logger.info("Sending SIGTERM to flow subprocesses (policy=%s)", policy)
    sent = _signal_pids(pids, signal.SIGTERM)
    logger.info("Sent SIGTERM to %d process(es)", sent)

    # Wait for graceful exit
    logger.info("Waiting up to %ds for subprocesses to exit...", GRACE_SECONDS)
    still_alive = _wait_for_exit(pids, GRACE_SECONDS)

    if still_alive:
        logger.warning(
            "Grace period expired — %d process(es) still alive: %s",
            len(still_alive),
            still_alive,
        )
        if policy == "cancel":
            # Force kill stragglers
            logger.info("Policy=cancel — sending SIGKILL to stragglers")
            _signal_pids(still_alive, signal.SIGKILL)
        else:
            # reschedule policy — let them be, they'll get picked up by heartbeat
            logger.info("Policy=reschedule — leaving stragglers for heartbeat automation")
    else:
        logger.info("All flow subprocesses exited cleanly")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    """Run the Prefect worker with graceful shutdown handling."""
    from prefect.workers.process import ProcessWorker

    logger.info(
        "Starting worker: pool=%s, name=%s, policy=%s, grace=%ds",
        WORK_POOL,
        WORKER_NAME,
        SHUTDOWN_POLICY,
        GRACE_SECONDS,
    )

    worker = ProcessWorker(
        work_pool_name=WORK_POOL,
        name=WORKER_NAME,
    )

    stop = asyncio.Event()
    shutdown_initiated = False

    def handle_signal(signum: int, frame) -> None:
        nonlocal shutdown_initiated
        sig_name = signal.Signals(signum).name
        if shutdown_initiated:
            logger.warning("Received %s again — ignoring (shutdown already in progress)", sig_name)
            return
        shutdown_initiated = True
        logger.info("Received %s — initiating graceful shutdown", sig_name)
        stop.set()

    # Install signal handlers
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    async with worker:
        worker_task = asyncio.create_task(worker.start())
        logger.info("Worker started — waiting for shutdown signal")

        try:
            await stop.wait()
        finally:
            logger.info("Shutdown initiated — draining flow subprocesses")

            # Drain children first (blocking, but that's intentional)
            _drain_flow_subprocesses(SHUTDOWN_POLICY)

            # Cancel the worker loop
            logger.info("Cancelling worker loop")
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task

    logger.info("Worker exited cleanly")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted — exiting")
        sys.exit(0)
