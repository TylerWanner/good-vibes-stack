# Telemetry State of the World

## Purpose

This document captures the intended and current state of tracing/telemetry in the Good Vibes Stack.

It exists because traces are present in some parts of the stack (OpenClaw, nervous-system) but may be missing or unreliable in others (notably Prefect), and the intended design is otherwise scattered across code and compose env.

---

## Current Goal

The stack should emit useful traces for:
- OpenClaw agent activity
- nervous-system-api requests and operations
- Prefect flow execution
- second-brain capability pipeline steps (ingest, summarize, index, notify)

Traces should land in Tempo and be queryable in Grafana/Tempo.

---

## What Currently Works

### OpenClaw traces
Observed working.

### nervous-system traces
Observed working.

### Tempo/Grafana path
Observed working enough to receive at least some traces.

This strongly suggests the OTLP pipeline itself is alive.

---

## What Appears Broken or Unclear

### Prefect traces
Suspected broken / missing.

Current working theory:
- not a Tempo outage
- not a global OTEL outage
- likely Prefect-specific tracing regression, drift, or instrumentation gap

### Second-brain capability observability
Not yet strong enough for debugging product issues like:
- completed ingest notification but no searchability
- indexing/document-generation failures
- canonical URL mismatches
- thin/empty records

---

## Intended Prefect Tracing Design

### Bootstrap code
File:
- `shared/telemetry.py`

This helper:
- defines `setup_tracing()`
- configures OTLP export when `OTEL_EXPORTER_OTLP_ENDPOINT` is set
- defaults `service.name` to `prefect-worker`
- uses `SimpleSpanProcessor` explicitly for short-lived flow subprocesses

### Flow usage
Example:
- `orchestration/flows/ingest_url.py`

This imports and calls:

```python
from shared.telemetry import setup_tracing
setup_tracing()
```

The intended model is explicit tracing bootstrap at flow import/startup time.

### Worker env
`docker-compose.yml` sets Prefect worker OTEL env including:
- `OTEL_EXPORTER_OTLP_ENDPOINT=http://tempo:4317` (default)
- `OTEL_EXPORTER_OTLP_PROTOCOL=grpc`
- `OTEL_SERVICE_NAME=prefect-worker`
- `OTEL_RESOURCE_ATTRIBUTES=deployment.environment=local,service.namespace=good-vibes`
- `PREFECT_CLOUD_ENABLE_ORCHESTRATION_TELEMETRY=true`

Prefect tracing was not accidental — it was deliberately designed.

---

## Most Likely Failure Modes for Missing Prefect Traces

1. **Flow coverage gap**
   - only some flows call `setup_tracing()`
   - important flows/subflows may not be instrumented

2. **Prefect version/config drift**
   - Prefect native OTEL behavior changed
   - orchestration telemetry env no longer does what we think

3. **Exporter/runtime mismatch**
   - worker env present but subprocesses still fail to export consistently

4. **Service name/resource mismatch**
   - traces exist under unexpected labels, making them appear missing

---

## Practical Debugging Order

If Prefect traces appear missing:

1. Verify worker still has OTEL env in rendered compose
2. Verify target flow imports/calls `setup_tracing()`
3. Check worker logs for OTEL/exporter errors
4. Search Tempo for unexpected Prefect service names
5. Compare nervous-system env vs prefect-worker env for OTEL drift

---

## What Would Make Telemetry More Useful

For second-brain capability and workflow debugging, traces need business identifiers attached.

Recommended span attributes:
- `ingest.id`
- `flow_run.id`
- `article.id`
- `article.url`
- `article.source_type`
- `search.indexed`
- `title.length`
- `summary.length`

Without these, Tempo is infrastructure plumbing but not yet a high-signal debugging surface.

---

## Current Product Gaps Telemetry Should Help Explain

### Ingest/search mismatch
Cases where:
- completion notification fires
- item is not searchable/retrievable

Telemetry should help answer:
- did indexing run?
- did canonicalization change the URL?
- was the stored record thin/empty?
- did notify happen before index write?

---

## Current Recommendation

Treat telemetry as partially working infrastructure, not yet a fully trustworthy debugging product.

Priority order:
1. add simple internal read/debug endpoints for recent ingests/articles
2. restore/verify Prefect traces
3. improve span attributes around second-brain ingest/index/notify
4. use Tempo as a causal debugging surface once the above are in place

---

## Summary

The stack already has a real tracing design:
- OTEL env in compose
- explicit Prefect tracing bootstrap in code
- Tempo as the sink

What is missing is confidence that Prefect traces still land reliably and enough business-level attributes to make telemetry genuinely useful for debugging second-brain capability issues.
