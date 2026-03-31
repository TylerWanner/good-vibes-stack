# sitecustomize.py — auto-loaded by Python on every interpreter startup.
# Initializes OTEL tracing for all Prefect flow subprocesses without requiring
# per-flow imports. No-op when OTEL_EXPORTER_OTLP_ENDPOINT is not set.
try:
    from shared.telemetry import setup_tracing
    setup_tracing()
except Exception:
    pass  # never crash a flow over telemetry
