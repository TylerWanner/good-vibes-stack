"""OpenTelemetry setup for Prefect flow runs.

Call ``setup_tracing()`` at the top of any flow that should emit spans.
It's a no-op when OTEL_EXPORTER_OTLP_ENDPOINT is not set.

Usage::

    from shared.telemetry import setup_tracing
    setup_tracing()

    @flow
    def my_flow():
        ...
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

_initialized = False
_tracer = None


def setup_tracing(service_name: str | None = None) -> None:
    """Configure OTLP tracing for a Prefect flow process.

    Safe to call multiple times — only initializes once per process.
    No-op when ``OTEL_EXPORTER_OTLP_ENDPOINT`` is not set.
    """
    global _initialized
    if _initialized:
        return

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
    if not endpoint:
        return

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource, SERVICE_NAME
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        svc = service_name or os.getenv("OTEL_SERVICE_NAME", "prefect-worker")
        resource = Resource.create({
            SERVICE_NAME: svc,
            "deployment.environment": os.getenv("DEPLOYMENT_ENV", "local"),
            "service.namespace": "provision",
        })
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
        # SimpleSpanProcessor for short-lived flow subprocesses — ensures spans
        # export immediately rather than waiting for batch flush (which may never
        # fire if the process exits before the interval).
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        # Only set if no real provider exists yet — Prefect may have already set one
        # via its own OTEL auto-config. Prefer ours (has correct resource attributes)
        # but don't clobber if already initialized with the same config.
        existing = trace.get_tracer_provider()
        existing_type = type(existing).__name__
        if existing_type == "ProxyTracerProvider":
            # Default no-op proxy — safe to replace
            trace.set_tracer_provider(provider)
        elif existing_type == "TracerProvider":
            # Already configured (likely by Prefect) — add our exporter to it instead
            existing.add_span_processor(SimpleSpanProcessor(exporter))
            provider = existing
            logger.debug("OTEL TracerProvider already set — added exporter to existing provider")
        else:
            trace.set_tracer_provider(provider)

        _initialized = True
        global _tracer
        _tracer = trace.get_tracer("provision")
        logger.info("OTEL tracing enabled → %s (service=%s)", endpoint, svc)
    except ImportError:
        logger.debug("opentelemetry packages not installed — tracing disabled")
    except Exception as exc:
        logger.warning("OTEL setup failed: %s", exc)


def extract_trace_context(carrier: dict[str, str] | None):
    """Extract W3C trace context from a carrier dict and return a context object.

    Used in flow entrypoints to re-parent spans under the API's trace.
    Returns None if opentelemetry is not available or carrier is empty.

    Usage::

        ctx = extract_trace_context(params.get("_trace_context"))
        with use_trace_context(ctx):
            # spans created here are children of the API span
    """
    if not carrier:
        return None
    try:
        from opentelemetry import propagate
        return propagate.extract(carrier)
    except ImportError:
        return None
    except Exception as exc:
        logger.debug("trace context extraction failed: %s", exc)
        return None


def span_in_context(name: str, carrier: dict[str, str] | None = None):
    """Context manager: start a span optionally re-parented from a carrier.

    Usage::

        with span_in_context("ingest-url-flow", params.get("_trace_context")):
            ...
    """
    from contextlib import contextmanager

    @contextmanager
    def _noop():
        yield

    if not _initialized or _tracer is None:
        return _noop()

    try:
        from opentelemetry import propagate, context as otel_context

        ctx = propagate.extract(carrier or {}) if carrier else otel_context.get_current()
        return _tracer.start_as_current_span(name, context=ctx)
    except Exception as exc:
        logger.debug("span_in_context failed: %s", exc)
        return _noop()
