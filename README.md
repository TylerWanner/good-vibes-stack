# Good Vibes Stack

> A portable agentic stack that won't harsh your vibes.

OpenClaw agents • Prefect workflows • pgvector search • safe-docker governance — all on Docker Compose.

---

## What Is This?

Production-grade infrastructure for AI agents. The insight: **the model is table stakes. The orchestration layer is the product.**

Most people building with AI agents hit the same wall: things work until they don't, and when they break you don't know why. Silent failures. Unreliable schedules. Workflows that need babysitting.

This stack is the answer. Not more prompting — actual infrastructure.

---

## Architecture

```
agent → constrained tool surface → Prefect workflow → execution workers
```

Reasoning, orchestration, and execution stay cleanly separated. The agent requests capabilities without being trusted with raw system control. Prefect makes every run observable, retryable, and auditable.

| Layer | Tool | Responsibility |
|---|---|---|
| **Intent** | You | Goals, direction, values |
| **Agency** | OpenClaw | Translates intent into action, routes to orchestration |
| **Orchestration** | Prefect 3 | Reliable execution, retries, scheduling, audit trail |
| **Intelligence** | Ollama / Claude | LLM analysis, summarization, scoring |
| **Memory** | Second Brain (Postgres) | Persistent, agent-accessible knowledge |
| **Control** | safe-docker | Service lifecycle, ops control surface |
| **Nervous System** | FastAPI | API surface, inbound hooks, notification routing |

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
/docs               Architecture specs
```

---

## What It Does

### Second Brain

An AI-powered knowledge base. Ingest anything — URLs, tweets, YouTube videos, GitHub repos — and make it searchable and queryable by agents.

**Pipeline:** `POST /articles` → Prefect → Scrapling → LLM → Postgres (pgvector)

**Source-aware fetchers:** YouTube (yt-dlp), Reddit (JSON), GitHub (API), X/Twitter (Scrapling), generic URLs

**Key flows:**
- `ingest-url` — fetch → analyze → store
- `retry-failed` — reruns failed articles
- `weekly-digest` — LLM-generated digest from recent content
- `backup-postgres` — pg_dump to S3/R2

### Control Plane (safe-docker)

Minimal Docker Compose lifecycle API. API key auth, allowlisted operations only.

- **Safe:** `status`, `logs`, `restart`, `start`, `stop`, `up`, `down`
- **Dangerous (require opt-in):** `recreate`, `build`
- **Blocked by design:** `exec`, `run`

See: [github.com/ReptilianHQ/safe-docker](https://github.com/ReptilianHQ/safe-docker)

### Orchestration (Prefect 3)

Self-hosted Prefect server. All deployments defined in `orchestration/prefect.yaml`. Observable, retryable, scheduled workflows.

### Observability

Tempo for distributed tracing, Grafana for dashboards. Every flow run is traceable end-to-end.

---

## Design Decisions

**Postgres uses a bind mount, not a named volume.**
`docker compose down -v` won't wipe your data.

**DB is source of truth, not Prefect flow state.**
Articles have a `status` column. The retry flow queries the DB — not Prefect history.

**Prefect concurrency limits as a coordination primitive.**
Tasks use named concurrency slots. Observable, automatic, works across any Python code.

**Block-first credential loading.**
`shared/secrets.py` loads credentials from Prefect Secret blocks. Secrets belong in Prefect, not scattered across environment files.

**Worker startup is boring.**
No migrations on boot. `init.sh` handles bootstrap explicitly.

---

## Deploy

### Quick start

```bash
cp .env.example .env
# Fill in required values

docker compose up -d
./init.sh
```

`init.sh` handles: deploy flows, run migrations, set concurrency limits, sync Prefect blocks.

### Full deploy guide

See [docs/DEPLOY_HETZNER.md](docs/DEPLOY_HETZNER.md) for step-by-step from a fresh Ubuntu VPS.

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
