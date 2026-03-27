"""Deploy all Prefect flows — runs `prefect deploy --all` inside the worker.

Use this flow to redeploy flows after code changes without operator intervention.

Run via:
    prefect deployment run 'deploy-flows/deploy-flows'

Or trigger via second-brain API:
    POST /flows/trigger  {"deployment_name": "deploy-flows/deploy-flows"}
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from prefect import flow, get_run_logger


@flow(name="deploy-flows", log_prints=True)
def deploy_flows() -> dict:
    """Run `prefect deploy --all` to register all flow deployments."""
    logger = get_run_logger()

    prefect_yaml = Path("/app/orchestration/prefect/prefect.yaml")
    if not prefect_yaml.exists():
        raise RuntimeError(f"prefect.yaml not found at {prefect_yaml}")

    # Must run from /app (repo root) — prefect.yaml pull step sets working_dir to /app
    # and entrypoints are relative to /app
    working_dir = Path("/app")
    logger.info("Running prefect deploy --all --prefect-file %s from %s", prefect_yaml, working_dir)

    result = subprocess.run(
        ["prefect", "deploy", "--all", "--prefect-file", str(prefect_yaml)],
        cwd=str(working_dir),
        capture_output=True,
        text=True,
    )

    if result.stdout:
        logger.info("stdout:\n%s", result.stdout)
    if result.stderr:
        logger.info("stderr:\n%s", result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"prefect deploy --all failed (exit {result.returncode})")

    logger.info("✅ Flows deployed successfully")
    return {"returncode": result.returncode, "status": "ok"}


if __name__ == "__main__":
    deploy_flows()
