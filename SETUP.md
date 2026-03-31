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
- Telegram bot token — create via [@BotFather](https://t.me/BotFather)
- (Optional) [OpenClaw](https://docs.openclaw.ai) for agent capabilities
- (Optional) Cloudflare R2 bucket for Postgres backups
- (Optional) Anthropic API key for Claude-backed flows

---

## Step 1 — Environment

```bash
cp .env.example .env
```

Edit `.env` and fill in:

| Variable | Required | Notes |
|---|---|---|
| `POSTGRES_USER` | ✅ | e.g. `second_brain` |
| `POSTGRES_PASSWORD` | ✅ | choose something strong |
| `POSTGRES_DB` | ✅ | e.g. `second_brain` |
| `TELEGRAM_BOT_TOKEN` | ✅ | from @BotFather |
| `TELEGRAM_CHAT_ID` | ✅ | your Telegram user/chat ID |
| `ANTHROPIC_API_KEY` | ❌ | only if using Claude for flows |
| `SECOND_BRAIN_LLM_PROVIDER` | ❌ | `ollama` (default) or `anthropic` |
| `SECOND_BRAIN_LLM_MODEL` | ❌ | `qwen2.5:7b` (default) |
| `SAFE_DOCKER_API_KEY` | ✅ | generate with `openssl rand -hex 32` |
| `R2_ENDPOINT` | ❌ | Cloudflare R2 endpoint for backups |
| `R2_ACCESS_KEY_ID` | ❌ | |
| `R2_SECRET_ACCESS_KEY` | ❌ | |

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

If you have Readwise, Twitter, or Brave API keys, populate `.env.blocks`:

```bash
cp .env.blocks.example .env.blocks
# Fill in values
python3 scripts/sync_blocks.py
```

This creates/updates Prefect Secret blocks for all credentials.

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

1. Uncomment the `openclaw-agent` service in `docker-compose.yml`
2. Configure `agent-config/openclaw.json` with your agent settings
3. Set `OPENCLAW_GATEWAY_TOKEN` in `.env`
4. Run `docker compose up -d openclaw-agent`

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

**Ollama timeout:**
Default timeout is 600s. For large models on slow hardware, increase `OLLAMA_TIMEOUT` or switch to a smaller model.

**Postgres connection refused:**
Wait for Postgres to finish initializing. Check `docker compose logs postgres`.

**Block not found:**
Run `python3 scripts/sync_blocks.py` to re-sync blocks from `.env.blocks`.
