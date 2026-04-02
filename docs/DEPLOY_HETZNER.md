# Deploying to Hetzner

Step-by-step guide to running the good-vibes stack on a fresh Hetzner VPS.

Tested on: **Ubuntu 24.04**, Hetzner CX22 (2 vCPU, 4GB RAM) — minimum viable. CX32 (4 vCPU, 8GB RAM) recommended for running Ollama + Prefect comfortably.

---

## What You Need Before Starting

- A Hetzner account and a new VPS (Ubuntu 24.04)
- A domain or subdomain pointed at the server IP (optional but useful)
- A Telegram bot token — create one via [@BotFather](https://t.me/BotFather)
- Your Telegram user ID — send `/start` to [@userinfobot](https://t.me/userinfobot) to find it
- (Optional) Cloudflare R2 bucket for Postgres backups
- (Optional) Anthropic API key if you want Claude-backed flows instead of Ollama

---

## 1. Provision the Server

Create a new VPS in the Hetzner Cloud Console:
- **Image:** Ubuntu 24.04
- **Type:** CX22 minimum, CX32 recommended
- **Location:** your preference
- **SSH key:** add yours during creation

SSH in:
```bash
ssh root@<your-server-ip>
```

---

## 2. Install Dependencies

```bash
# System updates
apt update && apt upgrade -y

# Docker
curl -fsSL https://get.docker.com | sh
systemctl enable docker
systemctl start docker

# Docker Compose v2 (included with Docker Engine — verify)
docker compose version

# Git
apt install -y git
```

---

## 3. Install Ollama

Ollama runs on the host, not in Docker. The containers reach it via `host.docker.internal`.

```bash
curl -fsSL https://ollama.ai/install.sh | sh

# Pull the models used by the stack
ollama pull qwen2.5:14b       # LLM for analysis (large — ~9GB)
ollama pull nomic-embed-text  # Embeddings model (~270MB)
```

Ollama runs as a systemd service and starts automatically on boot.

> **On a CX22 (4GB RAM):** qwen2.5:14b will run but slowly. Consider `qwen2.5:7b` as an alternative — set `SECOND_BRAIN_LLM_MODEL=qwen2.5:7b` in `.env`.

---

## 4. Install OpenClaw

OpenClaw is the agent harness that talks to Telegram and runs the main agent.

```bash
# Follow the official install instructions
# https://docs.openclaw.ai/getting-started

# Quick install (check docs for latest)
curl -fsSL https://install.openclaw.ai | sh
```

After installing, run `openclaw setup` and connect your Telegram bot.

---

## 5. Clone the Repo

```bash
cd /opt
git clone https://github.com/reptilianhq/good-vibes-stack.git
cd good-vibes-stack
```

---

## 6. Configure Environment

```bash
cp .env.example .env
```

Open `.env` and fill in the required values:

```bash
nano .env
```

**Required:**
```env
POSTGRES_USER=second_brain
POSTGRES_PASSWORD=<choose-something-strong>
POSTGRES_DB=second_brain

TELEGRAM_BOT_TOKEN=<from-BotFather>
TELEGRAM_CHAT_ID=<your-telegram-user-id>

SAFE_DOCKER_API_KEY=<generate-a-random-key>
```

Generate a random API key:
```bash
openssl rand -hex 32
```

**Optional — Cloudflare R2 backups:**
```env
R2_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=<your-key-id>
R2_SECRET_ACCESS_KEY=<your-secret>
BACKUP_S3_BUCKET=<your-bucket-name>
```

**Optional — Claude instead of Ollama:**
```env
SECOND_BRAIN_LLM_PROVIDER=anthropic
SECOND_BRAIN_ANTHROPIC_API_KEY=sk-ant-...
SECOND_BRAIN_LLM_MODEL=claude-3-5-haiku-20241022
```

---

## 7. Configure the Agent

Each OpenClaw agent needs its Telegram bot token:

```bash
mkdir -p state/agent
echo "TELEGRAM_BOT_TOKEN=<your-bot-token>" > state/agent/.env
```

---

## 8. Configure safe-docker

Copy the example policy and edit it to match your service names:

```bash
cp control/safe_docker/policy.example.yaml control/safe_docker/policy.yaml
```

The default policy allows `restart`, `stop`, `start`, and `logs` on the main services. Edit as needed.

---

## 9. Start the Stack

```bash
docker compose up -d
```

Wait ~30 seconds for Prefect server to initialize, then check everything is up:

```bash
docker compose ps
```

All services should show `running` or `healthy`.

---

## 10. Bootstrap

```bash
./init.sh
```

This runs in sequence:
1. **DB migrations** — creates tables, indexes, pgvector extension
2. **Deploy flows** — registers all Prefect deployments
3. **Concurrency limits** — sets `ollama=1`, `scrapling=3`
4. **Telegram blocks** — creates notification blocks for each agent

Output should end with `✅ Bootstrap complete`.

---

## 11. Verify

```bash
# API health
curl http://localhost:8001/health

# Stats
curl http://localhost:8001/articles/stats

# Test ingest
curl -X POST http://localhost:8001/ingest \
  -H "Content-Type: application/json" \
  -d '{"url": "https://github.com/PrefectHQ/prefect"}'
```

Open the Prefect UI: `http://<your-server-ip>:4200`

You should see an `ingest-url` flow run appear within a few seconds.

---

## 12. (Optional) Firewall

Lock down the server. Only expose what you need:

```bash
# Allow SSH, Telegram webhooks (if using), block everything else
ufw allow OpenSSH
ufw allow 8001/tcp   # nervous-system-api (internal or behind proxy)
ufw allow 4200/tcp   # Prefect UI (consider restricting to your IP)
ufw enable
```

If you're exposing the API publicly, put it behind nginx with HTTPS.

---

## Keeping It Running

**Auto-start on reboot:**
Docker services restart automatically (`restart: unless-stopped` in compose). Ollama is a systemd service and also restarts automatically.

**Logs:**
```bash
docker compose logs -f prefect-worker    # flow execution
docker compose logs -f nervous-system-api  # nervous-system API + notifications
```

**Drain and restart worker safely:**
```bash
curl -X POST http://localhost:8001/ops/drain \
  -H "Content-Type: application/json" \
  -d '{"cancel_running": true}'

docker compose restart prefect-worker
```

**Backup Postgres manually:**
```bash
# Trigger via API (requires R2 configured)
curl -X POST http://localhost:8001/ops/backup
```

Or dump directly:
```bash
docker exec good-vibes-stack-postgres-1 pg_dump -U second_brain second_brain | gzip > backup.sql.gz
```

---

## Sizing Guide

| Workload | Recommended |
|---|---|
| Light (few ingests/day, small model) | CX22 — 2 vCPU, 4GB RAM |
| Medium (active ingest, qwen2.5:14b) | CX32 — 4 vCPU, 8GB RAM |
| Heavy (high volume, embeddings running) | CX42 — 8 vCPU, 16GB RAM |

The main bottleneck is Ollama. qwen2.5:14b needs ~10GB of memory to run comfortably. On a CX22, it'll work but you'll see queuing.

---

## Troubleshooting

**Worker not picking up flows:**
```bash
docker compose logs prefect-worker --tail 50
# Look for "heartbeat" lines — if absent, worker lost connection to Prefect server
docker compose restart prefect-worker
```

**Ollama timeouts:**
```bash
# Check if Ollama is running
systemctl status ollama

# Check if reachable from inside Docker
docker exec good-vibes-stack-prefect-worker-1 curl -s http://host.docker.internal:11434/api/tags
```

**Postgres connection refused:**
```bash
docker compose logs postgres --tail 20
# Usually just needs more time to initialize
```

**Flows stuck in RUNNING:**
```bash
curl -X POST http://localhost:8001/ops/drain \
  -H "Content-Type: application/json" \
  -d '{"cancel_running": true}'
```

**Block not found errors:**
```bash
./init.sh  # Re-run bootstrap — idempotent, safe to run again
```
