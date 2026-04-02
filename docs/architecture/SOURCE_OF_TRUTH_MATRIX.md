# Source of Truth Matrix

The fastest way for a system to become unreliable is to let multiple layers pretend to own the same truth.

This document defines which system is authoritative for which kind of state.
If two layers disagree, the source of truth wins.

---

## Principles

1. **Interactive systems are not durable truth by default.**
2. **Execution telemetry is not domain state.**
3. **Config should be explicit and reviewable.**
4. **Runtime state should be isolated, named, and understood.**
5. **If recovery depends on memory or chat archaeology, the source of truth is wrong.**

---

## Matrix

| Domain | Source of truth | Not source of truth | Notes |
|---|---|---|---|
| Article ingest lifecycle (`pending` / `completed` / `failed`) | Postgres / second-brain domain state | Prefect run history | Prefect may execute ingest, but the DB determines what actually exists and what needs retrying. |
| Retry eligibility for second-brain work | Postgres / explicit domain status | Memory of prior runs, chat history | Retry decisions should be queryable and derivable from durable state. |
| Agent runtime behavior semantics | OpenClaw runtime | Persona files alone | Persona/config artifacts shape behavior, but OpenClaw is the engine executing that behavior. |
| Agent personality / operator-editable behavior layer | Repo-owned persona/config artifacts | Runtime-mutated copies | `AGENTS.md`, `SOUL.md`, `IDENTITY.md`, `HEARTBEAT.md`, `TOOLS.md`, `USER.md` are the intended editable surface. |
| OpenClaw runtime config defaults | Repo-owned config (`agents/*/config/openclaw.json`) | Drifted copies under runtime state dirs | Runtime copies may exist operationally, but repo-owned config is the reviewed definition. |
| Agent mutable runtime state | Agent-local state directories | Git-tracked source files | Delivery queues, memory stores, sessions, logs, and caches are operational state, not source. |
| Postgres durable storage | Postgres data directory / database contents | Prefect state, API responses, logs | Backups and restore procedures exist to preserve and recover this truth. |
| Prefect deployment identity | Deployment names / declarative deployment config | Transient Prefect IDs | IDs churn across redeploys; names are the stable reference. |
| Prefect workflow execution history | Prefect | Chat updates, ad hoc notes | Prefect is authoritative for run telemetry and workflow execution records. |
| Infrastructure mutation policy | Bounded control plane config/policy | Shell habits, tribal knowledge | Restart/stop/start permissions should be explicit, not socially implied. |
| Approval decisions that matter after the moment | Durable app/DB/block state | Telegram messages alone | Chat can be the interface, but not the only durable record. |
| Shared workspace contents | Explicitly mounted/shared workspace paths | Assumed local filesystem context | Shared collaboration state must be visible and intentionally mounted. |
| Secrets and credentials | Explicit secret/config delivery mechanism for that scope | Hardcoded defaults, copied chat text | Current rule: workflow secrets live in Prefect blocks (no env fallback); runtime/service auth and agent/tool creds live in runtime env or agent-local env. |
| Bootstrap/init completeness | Declared init contract + created artifacts | Human memory of setup steps | A healthy stack should be reconstructable from code and documented contracts. |

---

## What each layer knows

### OpenClaw knows
- the current interaction
- routing/judgment context
- tool availability
- conversational intent

OpenClaw does **not** automatically know durable product truth just because it discussed it.

### Prefect knows
- what runs were created
- what tasks ran
- which retries happened
- execution-level telemetry

Prefect does **not** automatically know what the product/domain state should now be.

### The database knows
- what domain objects exist
- what their durable lifecycle state is
- what should be retried or considered complete

The database should not be replaced by scheduler history or chat memory.

### Repo-owned config knows
- intended definitions
- reviewed defaults
- committed architecture choices

Runtime copies may exist, but they are not automatically authoritative.

---

## Conflict resolution rules

When two layers appear to disagree, use these defaults:

### Domain state conflict
If Prefect says a run succeeded but the DB says the item is still pending/failed, the DB wins.

### Config conflict
If runtime-mutated config drifts from reviewed repo-owned config, the repo-owned definition wins unless the system explicitly declares runtime ownership for that field.

### Approval conflict
If a chat message implies approval but there is no durable approval record where one is required, approval should be treated as non-authoritative.

### Deployment identity conflict
If a deployment ID changes but the named deployment contract remains the same, names win.

---

## Failure smell test

You likely have the wrong source of truth if any of the following are true:

- recovery requires reading old chat logs
- operators must remember whether a flow "already kind of ran"
- retries depend on scheduler history rather than domain state
- approvals exist only as message artifacts
- runtime copies silently override reviewed config
- multiple systems each claim to know whether work is complete

That is not resilience. That is ambiguity waiting for stress.

---

## Desired property

At any given moment, a new maintainer should be able to answer:
- what is true
- where that truth lives
- how to recover it
- how to reconcile disagreement

without asking a human historian.

That is the standard.
