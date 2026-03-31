"""Run the test suite via pytest inside the worker container.

Triggered via POST /ops/run-tests or directly from the Prefect UI.
Results are logged and optionally sent via Telegram notification.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from prefect import flow, get_run_logger
from shared.config import load_settings


@flow(name="run-tests", log_prints=True)
def run_tests(
    path: str = "tests/",
    extra_args: list[str] | None = None,
    notify: bool = True,
) -> dict:
    """Run pytest in the worker container and return results.

    Args:
        path: Path to tests directory or specific test file (relative to app root).
        extra_args: Additional pytest arguments e.g. ["-k", "test_health", "-x"].
        notify: Send Telegram notification with summary when done.
    """
    logger = get_run_logger()
    settings = load_settings()

    # Resolve app root — the worker mounts the codebase at /app
    app_root = Path("/app")
    test_path = app_root / path

    if not test_path.exists():
        logger.error("Test path does not exist: %s", test_path)
        return {"status": "error", "message": f"Path not found: {test_path}"}

    cmd = [sys.executable, "-m", "pytest", str(test_path), "-v", "--tb=short", "--no-header"]
    if extra_args:
        cmd.extend(extra_args)

    logger.info("Running: %s", " ".join(cmd))

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(app_root),
    )

    # Log full output
    if result.stdout:
        for line in result.stdout.splitlines():
            logger.info(line)
    if result.stderr:
        for line in result.stderr.splitlines():
            logger.warning(line)

    passed = result.returncode == 0
    status = "passed" if passed else "failed"

    # Extract summary line from pytest output (last non-empty line)
    summary = ""
    for line in reversed(result.stdout.splitlines()):
        if line.strip() and ("passed" in line or "failed" in line or "error" in line):
            summary = line.strip()
            break

    logger.info("Test suite %s — %s", status, summary or f"exit code {result.returncode}")

    if notify:
        emoji = "✅" if passed else "❌"
        msg = f"{emoji} Test suite {status}\n`{summary or f'exit {result.returncode}'}`"
        try:
            import os
            from nervous_system.notifications.telegram import send_telegram_message
            from shared.secrets import load_telegram_bot_token
            token = load_telegram_bot_token(bot="default")
            chat_id = os.getenv("TELEGRAM_CHAT_ID")
            if token and chat_id:
                send_telegram_message(token, chat_id, msg)
        except Exception as exc:
            logger.warning("Notification failed: %s", exc)

    return {
        "status": status,
        "returncode": result.returncode,
        "summary": summary,
        "path": str(test_path),
    }
