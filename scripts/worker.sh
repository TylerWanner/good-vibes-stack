#!/usr/bin/env bash
# Prefect worker entrypoint with graceful drain on SIGTERM
# Intercepts SIGTERM, pkills flow-run execute subprocesses so they self-reschedule,
# then forwards to the worker for clean teardown.
set -euo pipefail

drain_and_exit() {
    echo "[worker] Drain signal received — rescheduling in-flight runs..."
    pkill -TERM -f "prefect flow-run execute" || true
    sleep 5
    echo "[worker] Forwarding SIGTERM to worker (PID $WORKER_PID)"
    kill -TERM "$WORKER_PID" || true
    wait "$WORKER_PID" || true
    echo "[worker] Worker exited cleanly"
}

trap drain_and_exit TERM INT

opentelemetry-instrument prefect worker start --pool default-pool --type process &
WORKER_PID=$!

wait "$WORKER_PID"
