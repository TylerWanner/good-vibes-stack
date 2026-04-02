# Naming Doctrine

This file defines the canonical naming used by active docs in this repo.

The goal is simple: one architectural thing should have one primary name.
If reality changes, update this file early.

---

## Core layers

### Second brain
**Meaning:** the memory and intelligence capability layer.

Includes:
- ingestion
- retrieval
- enrichment
- ranking/scoring
- digest/research substrate
- structured knowledge stored in Postgres

**Do not use it to mean:**
- the API server
- the notification surface
- the whole stack

**Correct usage:**
- "the nervous-system API exposes second-brain operations"
- "second-brain flows"
- "second-brain retrieval"

---

### Nervous system
**Meaning:** the API and signal-routing surface for the stack.

Includes:
- inbound API requests
- notification routing
- approval relay/webhook handling
- workflow dispatch entrypoints
- operational endpoints that coordinate stack state

**Do not use it to mean:**
- the second brain itself
- the container control backend
- the universal interface for every capability

**Canonical runtime/service name:** `nervous-system-api`

---

### Control plane
**Meaning:** the architectural category of bounded mutation surfaces.

This is a category, not a single service.

Current control-plane surfaces include:
- nervous-system operational endpoints for workflow/orchestration actions
- safe-docker for container lifecycle control

**Do not use it to imply:**
- one central API that must front every action

---

### safe-docker
**Meaning:** the bounded container control service.

Includes:
- service status/logs
- restart/start/stop
- compose up/down
- guarded dangerous actions like build/recreate

**Do not use it to mean:**
- generic ops
- workflow orchestration
- a shell replacement

---

## Ops taxonomy

The word **ops** is allowed only as an umbrella term.
By itself, it is too vague for boundary decisions.

Use one of these instead when precision matters:

### Workflow ops
Operational actions that coordinate workflows, Prefect state, or stack-level runtime behavior.

Examples:
- drain runs
- reset concurrency
- sync blocks
- trigger a test flow

**Primary surface:** nervous-system API

### Container control
Operational actions that mutate Docker/Compose service state.

Examples:
- restart/start/stop
- logs
- build
- up/down

**Primary surface:** safe-docker

### Operational endpoints
This phrase refers specifically to `/ops/*` endpoints on `nervous-system-api`.
It should not be used as a synonym for safe-docker.

---

## Agents and runtimes

### OpenClaw
**Meaning:** the harness/platform.

### Agent / OpenClaw agent
**Meaning:** a configured OpenClaw runtime instance connected to this stack.

### Agent service
**Meaning:** the runtime container/service for a specific agent.

---

## Practical rules

1. Do not call `nervous-system-api` the second-brain API.
2. Do not use bare **ops** when the distinction between workflow ops and container control matters.
3. Do not route actions through nervous-system just to make the architecture look uniform.
4. Prefer the backend that already honestly owns the capability.
5. If a term hides ownership, it is the wrong term.

---

## Canonical summary

- **Second brain** = memory/intelligence capability layer
- **Nervous system** = API and signal-routing surface
- **safe-docker** = bounded container control surface
- **Workflow ops** = nervous-system operational actions
- **Container control** = safe-docker actions
- **Control plane** = the set of bounded mutation surfaces across the stack
