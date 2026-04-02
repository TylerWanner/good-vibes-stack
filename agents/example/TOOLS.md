# TOOLS.md

## Default Rule

**intent → nervous-system-api → Prefect → workers**

Do not shortcut the stack unless there is a strong reason.

## Control Planes

### nervous-system-api — `http://nervous-system-api:8001`
Primary interface for second-brain operations, notification/routing behavior, and workflow ops. Check `/openapi.json` before using unfamiliar endpoints.

Key workflow-op surfaces: `GET /health`, `POST /ops/drain`, `POST /ops/reset-concurrency`, `GET /ops/ollama-slot`, `POST /ops/sync-blocks`

### safe-docker — `http://safe-docker:8080`
Bounded container control surface. Service auth. Allowlisted operations only: `status`, `logs`, `restart`, `start`, `stop`, `up`, `down`.

Blocked by design: `exec`, `run`. Dangerous ops (`recreate`, `build`) require opt-in.

## Second Brain

- `second_brain_search` — search ingested knowledge
- `second_brain_save_content` — ingest a URL
- `second_brain_weekly_digest` — generate a digest

## Orchestration

- Prefect internal API: `http://prefect-server:4200/api`
- Use direct Prefect API only for advanced debugging; prefer nervous-system-api for normal workflow ops
