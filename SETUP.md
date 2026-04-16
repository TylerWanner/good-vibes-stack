# SETUP.md

Step-by-step guide to getting the Good Vibes Stack running from scratch.

---

## Prerequisites

- Linux host (tested on Ubuntu 22.04+)
- Docker + Docker Compose v2.24+ (v5+ recommended)
- [Ollama](https://ollama.ai) running locally with models pulled:
  ```bash
  ollama pull qwen2.5:7b
  ollama pull nomic-embed-text
  ```
- Telegram chat ID for notifications
- (Optional) [OpenClaw](https://docs.openclaw.ai) for agent capabilities
- (Optional) Cloudflare R2 bucket for Postgres backups
- (Optional) Anthropic API key if you want Anthropic for workflows or a specific agent

---

## Step 1 — Environment

```bash
cp .env.example .env
```

Edit `.env` and fill in:

> `HOST_PROJECT_PATH` should be the absolute path to your local `good-vibes-stack` checkout. `init.sh` will backfill it if missing, but making it explicit up front is better.

| Variable | Required | Notes |
|---|---|---|
| `POSTGRES_USER` | ✅ | e.g. `second_brain` |
| `POSTGRES_PASSWORD` | ✅ | choose something strong |
| `POSTGRES_DB` | ✅ | e.g. `second_brain` |
| `TELEGRAM_CHAT_ID` | ✅ | your Telegram user/chat ID |
| `NERVOUS_SYSTEM_API_KEY` | ✅ | auth for nervous-system-api; fail closed by default |
| `SECOND_BRAIN_LLM_PROVIDER` | ❌ | `ollama` (default) or `anthropic` |
| `SECOND_BRAIN_LLM_MODEL` | ❌ | `qwen2.5:7b` (default) |
| `OPENCLAW_MODEL` | ❌ | `openai-codex/gpt-5.4` (default agent model) |
| `ANTHROPIC_API_KEY` | ❌ | optional root-level Anthropic key; agent-local config can override it |
| `SAFE_DOCKER_AUTH_SECRET` | ✅ | generate with `openssl rand -hex 32` |
| `NERVOUS_SYSTEM_SAFE_DOCKER_API_KEY` | ✅ | caller credential used by nervous-system/plugin surfaces when calling safe-docker |
| `HOST_PROJECT_PATH` | ✅ | absolute path to this `good-vibes-stack` checkout |
| `BACKUP_S3_BUCKET` | ❌ | backup destination bucket |
| `BACKUP_S3_PREFIX` | ❌ | default `postgres/` |

---

## Step 2 — Start the stack

```bash
docker compose up -d
```

This starts: Postgres, Prefect server, Prefect worker, nervous-system API, Scrapling fetcher, safe-docker.

Wait ~30 seconds for Prefect server to initialize.

---

## Step 3 — Bootstrap

```bash
./init.sh
```

This runs in sequence:
1. **Deploy flows** — registers all Prefect deployments
2. **Initialize** — runs DB migrations, sets concurrency limits
3. **Start services** — brings up API and observability stack

---

## Step 4 — Sync Prefect blocks (optional)

If you have Readwise, Brave, backup, or other workflow credentials, populate `.env.blocks`:

```bash
cp .env.blocks.example .env.blocks
# Fill in values
python3 scripts/sync_blocks.py
```

This creates/updates Prefect Secret blocks for workflow credentials. Policy: no env var fallback.

Notes:
- Workflows use Prefect blocks for provider credentials.
- The example agent reads root `.env` first, then `agents/example/config/.env`.
- Anthropic is optional and API-key-based.

---

## Step 5 — Verify

```bash
# Check API health
curl http://localhost:8001/health

# Check stats
curl http://localhost:8001/articles/stats

# Trigger a test ingest
curl -X POST http://localhost:8001/ingest \
  -H "Content-Type: application/json" \
  -d '{"url": "https://example.com"}'
```

Check Prefect UI at `http://localhost:4200` to watch the flow run.

---

## Adding an OpenClaw Agent (optional)

To add an agent that can interact with your stack:

The repo currently ships one concrete example agent, but the compose and filesystem contract are intended to extend to multiple agents with the same shape.

1. The `example-agent` service is already defined behind the `agent` profile in `docker-compose.yml`
2. Configure `agents/example/config/openclaw.json` with your agent settings if needed
3. Set `OPENCLAW_GATEWAY_TOKEN` in `.env`
4. Put your agent bot token in `agents/example/config/.env`
5. Run `docker compose --profile agent up -d example-agent`

Notes:
- Default agent model is `openai-codex/gpt-5.4`.
- Anthropic is optional. If you want it for this agent only, set `ANTHROPIC_API_KEY` in `agents/example/config/.env`.
- Root `.env` loads first; `agents/example/config/.env` loads second and wins on overlapping agent vars.
- The agent uses a named volume for broad `.openclaw` runtime state, plus explicit durable submounts for `workspace`, `media`, `agents`, `memory`, and `tmp`.
- Filesystem contract: role-defining files are mounted read-only into the startup workspace, `shared-workspace` is the handoff area, and `/workspace` is the larger project tree.

See [OpenClaw docs](https://docs.openclaw.ai) for agent configuration.

---

## Restore from backup

If you need to rebuild a fresh install with an existing Postgres dump:

```bash
./scripts/restore.sh
```

---

## Troubleshooting

**Worker not picking up flows:**
```bash
docker compose logs prefect-worker --tail 50
```

**Orphaned flows after crash:**
The worker runs a startup cleanup that marks orphaned RUNNING flows as CRASHED. Check the logs:
```bash
docker compose logs prefect-worker | grep "startup-cleanup"
```

**Worker shutdown tuning:**
The worker uses graceful drain semantics on SIGTERM. Two knobs control this:

| Variable | Default | Notes |
|---|---|---|
| `PREFECT_SHUTDOWN_POLICY` | `reschedule` | `reschedule` (recommended), `cancel`, or `crash_via_kill` |
| `WORKER_DRAIN_GRACE` | `30` | Seconds to wait for in-flight flows to exit before escalating |

On slow hardware with long-running flows, increase `WORKER_DRAIN_GRACE`. Set in `.env`.

**Ollama timeout:**
Default timeout is 600s. For large models on slow hardware, increase `OLLAMA_TIMEOUT` or switch to a smaller model.

**Postgres connection refused:**
Wait for Postgres to finish initializing. Check `docker compose logs postgres`.

**Block not found:**
Run `python3 scripts/sync_blocks.py` to re-sync blocks from `.env.blocks`.
