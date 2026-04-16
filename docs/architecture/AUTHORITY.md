# Authority

This stack contains multiple systems that can observe, decide, or cause side effects.
That is fine. What is not fine is letting their authority blur together.

This document covers three things in one place:
1. **What planes exist** — and which kind of power each one owns
2. **Where credentials belong** — mapped to authority domains
3. **Where authority actually crosses a boundary** — in the running stack

---

## The planes

### 1. OpenClaw — the interactive judgment plane

Faces intent. Interprets ambiguity. Routes to the right plane.

Responsible for: interpreting intent, deciding which subsystem acts, deciding whether
work is immediate/deferred/scheduled/approval-gated/refused, coordinating agents and tools.

**Not** the durable execution substrate. If work must survive interruption, be retried,
be scheduled, or be observable as a durable job, OpenClaw hands it off.

---

### 2. Prefect — the durable workflow execution plane

Exists to make multi-step work reliable.

Responsible for: retries, scheduling, fan-out/fan-in, concurrency limits, durable run
visibility, structured execution of jobs that should survive beyond one interactive turn.

**Not** the judgment layer. Prefect runs the play. It does not decide the strategy.

---

### 3. Application services — the domain planes

Own their domain semantics.

Examples: `nervous-system-api` (second-brain ops, routing), Postgres (durable domain state).

Responsible for: domain rules, state transitions, validation, stable domain APIs.

**Not** generic orchestration layers.

---

### 4. Bounded infrastructure control planes — mutation with guardrails

Exist because infrastructure mutation is too dangerous to expose as ambient shell or raw
Docker access.

Example: `safe-docker`.

Responsible for: narrow operational actions, policy-bounded mutation, auditable control,
reduced blast radius.

**Not** general remote execution systems. If a tool can do everything, it is not a bounded
control plane. It is just power with nicer branding.

---

### 5. Scripts — bootstrap and repair tools

Allowed because systems need setup and recovery paths. Not the steady-state operational interface.

If normal system behavior depends on a script, the architecture is not done consolidating.

---

## Authority boundaries

### A: OpenClaw → Prefect
When work needs retries, scheduling, fan-out, or durable observability: route to Prefect.
Do not let the agent session become a shadow workflow engine.

### B: Prefect → application/domain state
Prefect run state is execution telemetry. Domain state lives in the domain's durable store.

### C: OpenClaw / Prefect → infrastructure mutation
Mutating infrastructure actions should transit explicit, constrained APIs rather than
ambient shell/socket power.

### D: scripts → everything else
If a script becomes a routine operational dependency, promote it into a control plane
or make its capability declarative.

---

## Escalation rule

Choose the narrowest plane that can own a behavior correctly:

1. domain service/API — if behavior is domain-native
2. bounded control plane — if behavior is operational mutation
3. Prefect — if behavior is durable workflow execution
4. OpenClaw — if behavior is judgment/routing/interactive control
5. script — if behavior is bootstrap/recovery and not yet consolidated

**Narrow authority beats broad convenience.**

---

## Credential ownership

Credentials follow authority domains. The goal is lower blast radius — if a flow is
compromised, it should not inherit agent credentials. If an agent is compromised,
it should not inherit broad workflow secrets.

### Three domains

**Service auth** — protects a service boundary. Lives in `.env`.
- `NERVOUS_SYSTEM_API_KEY` (nervous-system-api)
- `SAFE_DOCKER_AUTH_SECRET` (safe-docker signing secret)

**Agent/tool credentials** — used directly by an OpenClaw agent as part of its tool surface.
Lives in `.env` or agent-local config.
- agent Telegram bot token
- `BRAVE_API_KEY` (agent-direct search)
- `ANTHROPIC_API_KEY` (optional agent runtime auth)
- `NERVOUS_SYSTEM_SAFE_DOCKER_API_KEY` (consumer credential used by nervous-system/plugin callers)

**Workflow secrets** — used by Prefect flows and workers. Canonical location: Prefect
Secret blocks. Source values in `.env.blocks`. No env fallback.
- `anthropic-credentials`, `brave-credentials`, `readwise-credentials`
- `s3-backup-credentials`
- agent notification tokens (if using a dedicated bot for flow notifications)

### Credential inventory

| Credential | Domain | Where it lives | Consumers |
|---|---|---|---|
| `NERVOUS_SYSTEM_API_KEY` | service auth | `.env` | nervous-system-api |
| `SAFE_DOCKER_AUTH_SECRET` | service auth | `.env` | safe-docker token verification |
| `NERVOUS_SYSTEM_SAFE_DOCKER_API_KEY` | caller credential | `.env` | nervous-system/plugin calls into safe-docker |
| `TELEGRAM_CHAT_ID` | routing config | `.env` | API + flows (not secret, but sensitive) |
| `TELEGRAM_BOT_TOKEN` | agent/tool | agent config or `.env` | OpenClaw runtime |
| `anthropic-credentials` | workflow secret | Prefect block | flows via `load_anthropic_api_key()` |
| `ANTHROPIC_API_KEY` | agent/tool | `.env` or agent-local | agent runtime |
| `brave-credentials` | workflow secret | Prefect block | flows via `load_brave_api_key()` |
| `BRAVE_API_KEY` | agent/tool | agent-local or `.env` | agents using Brave directly |
| `readwise-credentials` | workflow secret | Prefect block | `sync_readwise` and related flows |
| `s3-backup-credentials` | workflow secret | Prefect block | `backup_postgres` flow |
| `POSTGRES_PASSWORD` / DB URL | runtime secret | `.env` | Postgres, API, worker |

### Split guidance for shared providers

Some providers span agent tools and workflows. That is normal. The mistake is treating
one credential as sufficient for both.

- **Anthropic:** optional. Workflow key → block; agent auth → agent-local or `.env`.
- **Brave:** workflow key → block; agent key → agent-local
- **Telegram:** agent bot token → agent-local; workflow notification token → block
- **safe-docker:** two-layer model. `SAFE_DOCKER_AUTH_SECRET` is the canonical service
  signing secret. `NERVOUS_SYSTEM_SAFE_DOCKER_API_KEY` is the consumer credential used by
  nervous-system/plugin callers when invoking safe-docker.

---

## Trust seams: where authority crosses a boundary in the running stack

### Seam 1: Human → Agent

**What crosses:** intent, approval keys, task direction  
**Gate:** Telegram channel (owner-controlled), OpenClaw gateway token  
**Weakness:** approval keys are plaintext in chat. Replayable within TTL.  
**Verdict:** acceptable for a self-hosted personal stack.

---

### Seam 2: Agent → nervous-system-api

**What crosses:** ingest requests, ops calls, flow triggers  
**Gate:** `NERVOUS_SYSTEM_API_KEY`  
**Weakness:** same key across all callers; no per-caller scoping.  
**Verdict:** acceptable. Closed Docker network; internal boundary credential.

---

### Seam 3: nervous-system-api → Prefect

**What crosses:** flow dispatch, run state queries  
**Gate:** none — Prefect API is unauthenticated internally (standard self-hosted posture).
Port 4200 bound to `127.0.0.1`.  
**Weakness:** anything on the Docker network can reach Prefect directly. The "agents only
talk to the API" rule is intent, not network enforcement.  
**Verdict:** typical. Mitigated once network segmentation is in place.

---

### Seam 4: nervous-system-api / worker → safe-docker (build approval path)

**What crosses:** dangerous container ops (builds) via approval webhook  
**Gate:** caller token signed by `SAFE_DOCKER_AUTH_SECRET` + policy.yaml allowlist + human approval  
**Weakness:** approval tokens are key-only, not bound to action/service/requester triplet.  
**Verdict:** best-defended seam in the stack.

---

### Seam 5: Agent → safe-docker (direct, by design)

**What crosses:** container lifecycle commands (status, logs, restart, up)  
**Gate:** caller token signed by `SAFE_DOCKER_AUTH_SECRET` + policy.yaml allowlist  
**Design intent:** agents call safe-docker directly. nervous-system-api is a domain service,
not a security proxy. Routing all lifecycle calls through it adds a useless hop — same
shared key, same policy. safe-docker is the real gate. The API is correctly absent from
this path.  
**Verdict:** clean. policy.yaml is the audit layer; it applies regardless of caller.

---

### Seam 6: Prefect worker → external services

**What crosses:** LLM calls, backups, web search  
**Gate:** Prefect Secret blocks (workflow secrets, no env fallback)  
**Weakness:** `TELEGRAM_CHAT_ID` is in compose env rather than blocks. Deliberate pragmatic
choice — routing config, not key material.  
**Verdict:** well-structured.

---

### Seam 7: Agent → host filesystem (rw bind mounts)

**What crosses:** read/write to the repo and any mounted workspace  
**Gate:** nothing at the filesystem level. Agent reasoning + human-gated rebuild is the only check.  
**Design intent:** intentional during active development. When the system stabilizes and
the agent shifts from building to operating, this narrows.  
**Weakness:** the gate is soft. A confused agent can stage bad code silently.  
**Verdict:** known architectural trade-off. Highest-trust grant in the system.

---

## Summary: three tiers of authority

| Tier | Path | Gate |
|---|---|---|
| **Mediated** | Agent → API → Prefect → worker | Logged, auditable, approval-gated for dangerous ops |
| **Direct** | Agent → safe-docker | Policy-gated at safe-docker; by design, bypasses API |
| **Ambient** | Agent → host filesystem | Rebuild gate only; widest exposure in the system |

The widest exposure is not the network boundary.
It is the `rw` mount — "agent proposes, human rebuilds" is the only check.
That is accepted posture, not an oversight.

---

## Anti-patterns

Signs that authority boundaries are rotting:

- OpenClaw sessions carrying durable workflow state
- Prefect flows encoding conversational judgment
- application services acting as generic control planes
- shell scripts becoming required daily operations
- raw Docker or host power where a bounded control plane should exist
- approvals represented only in chat messages with no durable state
- multiple ways to perform the same mutating action with different audit properties
