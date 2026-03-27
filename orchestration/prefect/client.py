"""Prefect API client utilities.

Helpers for interacting with the Prefect REST API — specifically deployment
lookup by name, which avoids hardcoding or env-var'ing deployment UUIDs that
change on every redeploy.

Distributed tracing: when an active OTEL trace context exists in the caller
(e.g. nervous-system-api handling an ingest request), ``trigger_deployment``
injects the W3C ``traceparent`` / ``tracestate`` headers into the flow
parameters as ``_trace_context``. The flow extracts these and re-parents its
spans, giving a single connected trace across the API → Prefect boundary.
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading

import httpx

logger = logging.getLogger(__name__)

_deployment_cache: dict[str, str] = {}
_sync_cache_lock = threading.Lock()
_async_cache_lock_init = threading.Lock()  # Guards creation of the asyncio lock
_async_cache_lock: asyncio.Lock | None = None


def _normalize_prefect_api_url(url: str) -> str:
    """Normalize Prefect URL to always end with /api.
    
    Accepts any of:
      - http://prefect-server:4200
      - http://prefect-server:4200/
      - http://prefect-server:4200/api
      - http://prefect-server:4200/api/
    
    Returns URL ending with /api (no trailing slash).
    """
    url = url.rstrip("/")
    if not url.endswith("/api"):
        url = url + "/api"
    return url


PREFECT_API_URL = _normalize_prefect_api_url(
    os.getenv("PREFECT_API_URL", "http://prefect-server:4200/api")
)


def _get_async_lock() -> asyncio.Lock:
    """Get or create the async cache lock (lazy init for event loop safety).
    
    Uses double-check locking with a threading.Lock to prevent race condition
    where two coroutines could both see None and create different locks.
    """
    global _async_cache_lock
    if _async_cache_lock is None:
        with _async_cache_lock_init:
            if _async_cache_lock is None:
                _async_cache_lock = asyncio.Lock()
    return _async_cache_lock


def get_deployment_id(flow_name: str, deployment_name: str) -> str:
    """Return the UUID for a Prefect deployment, looked up by name (sync).

    Uses an in-memory cache — the lookup only hits the Prefect API once per
    process lifetime. Use for Prefect flows and sync contexts.

    Args:
        flow_name: The flow name as registered in Prefect (e.g. "ingest-url").
        deployment_name: The deployment name (e.g. "ingest-url").

    Returns:
        Deployment UUID string.

    Raises:
        httpx.HTTPStatusError: If the deployment is not found or Prefect is unreachable.
    """
    cache_key = f"{flow_name}/{deployment_name}"
    # Fast path: cache hit
    if cache_key in _deployment_cache:
        return _deployment_cache[cache_key]
    # Slow path: acquire lock, double-check, fetch
    with _sync_cache_lock:
        if cache_key not in _deployment_cache:
            url = f"{PREFECT_API_URL}/deployments/name/{flow_name}/{deployment_name}"
            resp = httpx.get(url, timeout=10)
            resp.raise_for_status()
            _deployment_cache[cache_key] = resp.json()["id"]
            logger.debug("Resolved deployment %s → %s", cache_key, _deployment_cache[cache_key])
    return _deployment_cache[cache_key]


async def get_deployment_id_async(flow_name: str, deployment_name: str) -> str:
    """Return the UUID for a Prefect deployment, looked up by name (async).

    Uses an in-memory cache — the lookup only hits the Prefect API once per
    process lifetime. Use from FastAPI async handlers.
    """
    cache_key = f"{flow_name}/{deployment_name}"
    # Fast path: cache hit
    if cache_key in _deployment_cache:
        return _deployment_cache[cache_key]
    # Slow path: acquire lock, double-check, fetch
    async with _get_async_lock():
        if cache_key not in _deployment_cache:
            url = f"{PREFECT_API_URL}/deployments/name/{flow_name}/{deployment_name}"
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                _deployment_cache[cache_key] = resp.json()["id"]
            logger.debug("Resolved deployment %s → %s", cache_key, _deployment_cache[cache_key])
    return _deployment_cache[cache_key]


def _get_traceparent() -> str | None:
    """Extract the current W3C traceparent header value from the active span.

    Returns None if opentelemetry is not installed or no valid span exists.
    """
    try:
        from opentelemetry import trace, propagate

        span = trace.get_current_span()
        if not span.get_span_context().is_valid:
            return None

        carrier: dict[str, str] = {}
        propagate.inject(carrier)
        return carrier.get("traceparent")
    except ImportError:
        return None
    except Exception:
        return None


def trigger_deployment(
    flow_name: str,
    deployment_name: str,
    parameters: dict | None = None,
) -> str:
    """Dispatch a Prefect deployment run (sync). Returns the flow run ID.

    Fire-and-forget — does not wait for the run to complete.
    Injects W3C traceparent into flow run labels using Prefect's native
    ``__OTEL_TRACEPARENT`` mechanism for distributed tracing.
    
    For FastAPI async handlers, prefer trigger_deployment_async.
    """
    deployment_id = get_deployment_id(flow_name, deployment_name)
    url = f"{PREFECT_API_URL}/deployments/{deployment_id}/create_flow_run"

    body: dict = {"parameters": parameters or {}}

    traceparent = _get_traceparent()
    if traceparent:
        body["labels"] = {"__OTEL_TRACEPARENT": traceparent}
        logger.debug(f"Injected traceparent into flow run labels: {traceparent}")

    resp = httpx.post(url, json=body, timeout=10)
    resp.raise_for_status()
    flow_run_id = resp.json()["id"]
    logger.info(f"Triggered {flow_name}/{deployment_name} → flow_run_id={flow_run_id}")
    return flow_run_id


async def trigger_deployment_async(
    flow_name: str,
    deployment_name: str,
    parameters: dict | None = None,
) -> str:
    """Dispatch a Prefect deployment run (async). Returns the flow run ID.

    Fire-and-forget — does not wait for the run to complete.
    Use this from FastAPI async handlers to avoid blocking the event loop.
    """
    deployment_id = await get_deployment_id_async(flow_name, deployment_name)
    url = f"{PREFECT_API_URL}/deployments/{deployment_id}/create_flow_run"

    body: dict = {"parameters": parameters or {}}

    traceparent = _get_traceparent()
    if traceparent:
        body["labels"] = {"__OTEL_TRACEPARENT": traceparent}
        logger.debug(f"Injected traceparent into flow run labels: {traceparent}")

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(url, json=body)
        resp.raise_for_status()
        flow_run_id = resp.json()["id"]
    logger.info(f"Triggered {flow_name}/{deployment_name} → flow_run_id={flow_run_id}")
    return flow_run_id
