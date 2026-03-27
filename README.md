# Good Vibes Stack

> A reference stack for agentic systems that are reliable, observable, and governable — running in production, and helping write this project.

OpenClaw agents • Prefect orchestration • pgvector memory • safe-docker governance — working implementation on Docker Compose. Runs anywhere Docker does. Recommended minimum: 2 vCPU, 4GB RAM. Benefits from extra memory and local Ollama, but neither is required.

---

## The Problem

Most agent setups work until they don't — and when they break, you don't know why. Silent failures. Missed schedules. Workflows that need babysitting. The model gets blamed, but the model isn't the problem.

The problem is that the agent is doing jobs it shouldn't be: managing execution, holding state, scheduling work, retrying failures. When the agent is both the reasoning layer and the execution engine, everything becomes load-bearing and nothing is observable.

**The fix isn't better prompting. It's proper infrastructure.**

---

## Architecture

```
agent → constrained tool surface → Prefect workflow → execution workers
```

Reasoning, orchestration, and execution stay cleanly separated. The agent requests capabilities through a constrained interface — it doesn't own execution directly. Prefect makes every run observable, retryable, and auditable. The database is the source of truth, not scheduler history or chat logs.

| Layer | Tool | Responsibility |
|---|---|---|
| **Intent** | You | Goals, direction, values |
| **Agency** | OpenClaw | Translates intent into action, routes to orchestration |
| **Orchestration** | Prefect 3 | Reliable execution, retries, scheduling, audit trail |
| **Second Brain** | Postgres + pgvector + Ollama/Claude | Ingest, analyze, score, and retrieve knowledge |
| **Control** | safe-docker | Service lifecycle, ops control surface |
| **Nervous System** | FastAPI | Inbound/outbound signals, notifications, API surface |
| **Observability** | Grafana + Tempo | Traces, metrics, dashboards — what the nervous system sees |

Each layer has a job. None of them do each other's job. The result is a system that's **reliable** (Prefect handles execution guarantees), **observable** (every run is traceable end-to-end), and **governable** (the agent operates through constrained surfaces, not raw host access).

---

## What's Included

### Second Brain
An AI-powered knowledge base. Ingest URLs, tweets, YouTube videos, GitHub repos — anything gets fetched, analyzed, scored, and stored for agent retrieval.

**Pipeline:** `POST /articles` → Prefect → Scrapling → Ollama/Claude → Postgres (pgvector)

**Source-aware fetchers:** YouTube (yt-dlp), Reddit, GitHub, X/Twitter (Scrapling), generic URLs

**Key flows:**
- `ingest-url` — fetch → analyze → store
- `retry-failed` — reruns failed articles from DB state, not scheduler history
- `weekly-digest` — LLM-generated digest from recent content
- `backup-postgres` — pg_dump to S3/R2

### Control Plane (safe-docker)
Minimal Docker Compose lifecycle API. API key auth, explicitly allowlisted operations only.

- **Safe:** `status`, `logs`, `restart`, `start`, `stop`, `up`, `down`
- **Dangerous (require opt-in):** `recreate`, `build`
- **Blocked by design:** `exec`, `run`

The agent can operate the stack without raw host access. See: [github.com/ReptilianHQ/safe-docker](https://github.com/ReptilianHQ/safe-docker)

### Orchestration (Prefect 3)
Self-hosted Prefect server. All deployments defined in `orchestration/prefect.yaml`. Observable, retryable, scheduled.

### Observability
Tempo for distributed tracing, Grafana for dashboards. The system is meant to be inspected — not just used.

---

## Design Principles

**The agent doesn't own execution.**  
It requests work through a constrained tool surface. Prefect runs it. The agent never has ambient access to infrastructure. It can't exec arbitrary commands, restart arbitrary services, or reach the host directly — only what the control plane explicitly allows.

**The agent proposes flow code. The build deploys it.**  
Flow code is baked into the worker image, not mounted from a live volume. The agent can write and modify flows, but nothing runs until a human-initiated rebuild. The build step is the human-in-the-loop gate between agent-authored code and running code.

**The database is the source of truth.**  
Not Prefect run history. Not chat logs. Articles have a `status` column. Retry decisions come from DB state — if the DB says failed, it gets retried, regardless of what the scheduler thinks.

**Concurrency is a system primitive, not an app concern.**  
Prefect's global concurrency limits work across any Python code — flows, workers, scripts. One Ollama call at a time, enforced at the infrastructure level.

**Recovery should be boring.**  
`init.sh` handles bootstrap explicitly. No migrations on boot. No magic. A new maintainer should be able to reconstruct the system from code and docs without asking anyone.

For the full architecture doctrine, see [`docs/architecture/INVARIANTS.md`](docs/architecture/INVARIANTS.md).

---

## Getting Started

```bash
git clone https://github.com/TylerWanner/good-vibes-stack
cd good-vibes-stack
./setup.sh
```

`setup.sh` is an interactive walkthrough — it prompts for credentials, configures your environment, and gets the stack running. No manual `.env` editing required.

For a full step-by-step deploy on a fresh Ubuntu VPS, see [`docs/DEPLOY_HETZNER.md`](docs/DEPLOY_HETZNER.md).

---

## Repo Structure

```
/orchestration      Prefect flows + deployment definitions
/second_brain       LLM client, content classification, analysis
/nervous_system     FastAPI server, Telegram notifications
/data               Postgres client, Alembic migrations
/integrations       Source-specific fetchers (YouTube, Reddit, GitHub, Twitter)
/shared             Config, secrets loaders
/control            safe-docker client + policy
/infra              Grafana, Tempo configs
/scripts            Operational scripts
/docs               Architecture specs and design decisions
/agents/example     Example OpenClaw agent config (AGENTS.md, SOUL.md, etc.)
```

---

## Stack

- **Agent harness:** [OpenClaw](https://github.com/openclaw/openclaw)
- **Orchestration:** Prefect 3 (self-hosted)
- **Web fetching:** Scrapling sidecar (Playwright-backed)
- **LLM:** Ollama for flows; Claude via OpenClaw for agent reasoning
- **DB:** Postgres + pgvector
- **Control:** [safe-docker](https://github.com/ReptilianHQ/safe-docker)
- **Observability:** Grafana + Tempo

---

## License

AGPLv3
