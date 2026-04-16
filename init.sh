#!/usr/bin/env bash
# init.sh — bring up the Good Vibes Stack (idempotent)
# Run from the repo root: ./init.sh
# Safe to re-run — checks state and skips completed steps
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

ok()   { echo -e "${GREEN}  ✓ $*${NC}"; }
skip() { echo -e "${CYAN}  ○ $* (already done)${NC}"; }
warn() { echo -e "${YELLOW}  ⚠ $*${NC}"; }
fail() { echo -e "${RED}  ✗ $*${NC}"; exit 1; }
step() { echo -e "\n${YELLOW}▶ $*${NC}"; }

echo ""
echo "🎸 Good Vibes Stack init"
echo "========================"

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

# Check if a container is running and healthy
container_healthy() {
  local service="$1"
  local status
  status=$(docker compose ps "$service" --format '{{.Status}}' 2>/dev/null | head -1)
  [[ "$status" == *"Up"* ]] && [[ "$status" != *"unhealthy"* ]]
}

# Check if postgres is connectable with current credentials
postgres_connectable() {
  docker compose exec -T postgres pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" > /dev/null 2>&1
}

# Check if a database exists
db_exists() {
  local dbname="$1"
  docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -tAc \
    "SELECT 1 FROM pg_database WHERE datname='$dbname'" 2>/dev/null | grep -q 1
}

# Check if prefect server is healthy
prefect_healthy() {
  curl -sf http://localhost:4200/api/health > /dev/null 2>&1
}

# Check if work pool exists
workpool_exists() {
  curl -sf "http://localhost:4200/api/work_pools/default-pool" > /dev/null 2>&1
}

# Check if any deployments exist
deployments_exist() {
  curl -sf "http://localhost:4200/api/deployments/filter" -X POST \
    -H "Content-Type: application/json" -d '{"limit":1}' 2>/dev/null | grep -q '"id"'
}

# Check if nervous-system-api is healthy
api_healthy() {
  curl -sf http://localhost:8001/health > /dev/null 2>&1
}

# Wait for a service with restart detection
wait_for_service() {
  local service="$1"
  local check_cmd="$2"
  local max_attempts="${3:-60}"
  local attempt=0
  local last_status=""
  
  while ! eval "$check_cmd" > /dev/null 2>&1; do
    attempt=$((attempt + 1))
    if [ $attempt -ge $max_attempts ]; then
      fail "$service failed to start after $max_attempts attempts"
    fi
    
    # Check container status every 5 attempts
    if [ $((attempt % 5)) -eq 0 ]; then
      status=$(docker compose ps "$service" --format '{{.Status}}' 2>/dev/null | head -1)
      if echo "$status" | grep -qi "restarting"; then
        if [ "$last_status" != "restarting" ]; then
          warn "$service is restarting — checking logs..."
          docker compose logs "$service" --tail 10 2>/dev/null | tail -5
          last_status="restarting"
        fi
      elif echo "$status" | grep -qi "exit"; then
        fail "$service exited unexpectedly. Check: docker compose logs $service"
      else
        echo "  waiting... (status: ${status:-starting})"
      fi
    else
      echo "  waiting..."
    fi
    sleep 2
  done
}

# ---------------------------------------------------------------------------
# Pre-flight: .env validation
# ---------------------------------------------------------------------------

step "Checking .env..."
if [ ! -f .env ]; then
  if [ -f .env.example ]; then
    warn ".env not found — run ./setup.sh first"
    exit 1
  else
    fail ".env not found and no .env.example available"
  fi
fi

# Load env vars
set -a
source .env
set +a

# Normalize explicit host bind roots.
if [ -z "${HOST_PROJECT_PATH:-}" ] || [ "${HOST_PROJECT_PATH}" = "/absolute/path/to/good-vibes-stack" ]; then
  warn "HOST_PROJECT_PATH not set (or still placeholder) — defaulting to ${SCRIPT_DIR}"
  if grep -q '^HOST_PROJECT_PATH=' .env; then
    sed -i "s|^HOST_PROJECT_PATH=.*|HOST_PROJECT_PATH=${SCRIPT_DIR}|" .env
  else
    printf '\nHOST_PROJECT_PATH=%s\n' "$SCRIPT_DIR" >> .env
  fi
  export HOST_PROJECT_PATH="$SCRIPT_DIR"
  ok "Saved HOST_PROJECT_PATH to .env"
fi

# Validate required vars
missing_vars=""
[ -z "${POSTGRES_PASSWORD:-}" ] && missing_vars+=" POSTGRES_PASSWORD"
[ -z "${POSTGRES_USER:-}" ] && missing_vars+=" POSTGRES_USER"
[ -z "${POSTGRES_DB:-}" ] && missing_vars+=" POSTGRES_DB"
[ -z "${SAFE_DOCKER_AUTH_SECRET:-}" ] && missing_vars+=" SAFE_DOCKER_AUTH_SECRET"
[ -z "${NERVOUS_SYSTEM_SAFE_DOCKER_API_KEY:-}" ] && missing_vars+=" NERVOUS_SYSTEM_SAFE_DOCKER_API_KEY"
[ -z "${NERVOUS_SYSTEM_API_KEY:-}" ] && [ "${ALLOW_UNAUTHENTICATED_API:-false}" != "true" ] && missing_vars+=" NERVOUS_SYSTEM_API_KEY"
[ -z "${HOST_PROJECT_PATH:-}" ] && missing_vars+=" HOST_PROJECT_PATH"

if [ -n "$missing_vars" ]; then
  fail "Missing required env vars:$missing_vars — run ./setup.sh"
fi
ok ".env valid"

# ---------------------------------------------------------------------------
# Step 1: Postgres
# ---------------------------------------------------------------------------

# Start observability first so traces work from the start
step "Observability stack..."
if container_healthy tempo && container_healthy grafana; then
  skip "Tempo + Grafana running"
else
  echo "  Starting tempo + grafana..."
  docker compose up -d tempo grafana
  ok "Observability started"
fi

step "Postgres..."
if container_healthy postgres && postgres_connectable; then
  skip "Postgres running and connectable"
else
  # Check if data exists but password might be wrong
  if [ -d "state/postgres" ] && container_healthy postgres && ! postgres_connectable; then
    fail "Postgres running but can't connect — password mismatch?\n  Fix: rm -rf state/postgres && ./init.sh"
  fi
  
  echo "  Starting postgres..."
  docker compose up -d postgres
  wait_for_service "postgres" "postgres_connectable" 30
  ok "Postgres ready"
fi

# ---------------------------------------------------------------------------
# Step 2: Prefect database
# ---------------------------------------------------------------------------

step "Prefect database..."
if db_exists "prefect"; then
  skip "prefect database exists"
else
  echo "  Creating prefect database..."
  docker compose exec -T postgres psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
    -c "CREATE DATABASE prefect;" > /dev/null 2>&1
  ok "prefect database created"
fi

# ---------------------------------------------------------------------------
# Step 3: Prefect server
# ---------------------------------------------------------------------------

step "Prefect server..."
if prefect_healthy; then
  skip "Prefect server healthy"
else
  echo "  Starting prefect-server..."
  docker compose up -d prefect-server
  wait_for_service "prefect-server" "prefect_healthy" 60
  ok "Prefect server ready"
fi

# ---------------------------------------------------------------------------
# Step 4: Work pool
# ---------------------------------------------------------------------------

step "Work pool..."
if workpool_exists; then
  skip "default-pool exists"
else
  echo "  Creating work pool..."
  docker compose exec -T prefect-server prefect work-pool create default-pool --type process > /dev/null 2>&1
  ok "default-pool created"
fi

# Set concurrency (idempotent)
curl -s -X PATCH "http://localhost:4200/api/work_pools/default-pool" \
  -H "Content-Type: application/json" \
  -d '{"concurrency_limit": 10}' > /dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# Step 5: Prefect worker
# ---------------------------------------------------------------------------

step "Prefect worker..."
if container_healthy prefect-worker; then
  skip "Prefect worker running"
else
  echo "  Building prefect-worker..."
  docker compose build prefect-worker > /dev/null 2>&1
  echo "  Starting prefect-worker..."
  docker compose up -d prefect-worker
  ok "Prefect worker started"
fi

# Wait for worker to be ready
step "Waiting for worker to be ready..."
until docker compose exec -T prefect-worker sh -c "prefect version" > /dev/null 2>&1; do
  echo "  waiting..."
  sleep 3
done
ok "Worker ready"

# ---------------------------------------------------------------------------
# Step 6: Deploy flows
# ---------------------------------------------------------------------------

step "Deploying flows..."
if deployments_exist; then
  skip "Flows already deployed"
else
  echo "  Deploying all flows..."
  docker compose exec -T prefect-worker sh -lc \
    "cd /app && prefect deploy --all --prefect-file orchestration/prefect/prefect.yaml"
  ok "Flows deployed"
fi

# ---------------------------------------------------------------------------
# Step 7: Initialize (migrations, concurrency limits, validation)
# ---------------------------------------------------------------------------

step "Running initialization..."
echo "  (migrations, concurrency limits, validation)"
docker compose exec -T prefect-worker sh -c \
  "cd /app && python -m orchestration.flows.initialize"
ok "Initialization complete"

# ---------------------------------------------------------------------------
# Step 8: Sync Prefect blocks (if .env.blocks exists)
# ---------------------------------------------------------------------------

step "Prefect blocks..."
if [ -f ".env.blocks" ]; then
  echo "  Syncing blocks from .env.blocks..."
  docker compose exec -T prefect-worker sh -c "cd /app && python scripts/sync_blocks.py --env-file .env.blocks" || warn "Block sync failed"
  ok "Blocks synced"
else
  skip "No .env.blocks — skipping block sync"
fi

# ---------------------------------------------------------------------------
# Step 9: Scrapling fetcher
# ---------------------------------------------------------------------------

step "Scrapling fetcher..."
if container_healthy scrapling-fetcher; then
  skip "Scrapling fetcher running"
else
  echo "  Starting scrapling-fetcher..."
  docker compose up -d scrapling-fetcher
  ok "Scrapling fetcher started"
fi

# ---------------------------------------------------------------------------
# Step 10: Nervous system API
# ---------------------------------------------------------------------------

step "Nervous system API..."
if api_healthy; then
  skip "API healthy"
else
  echo "  Starting nervous-system-api..."
  docker compose up -d nervous-system-api
  wait_for_service "nervous-system-api" "api_healthy" 60
  ok "API ready"
fi

# ---------------------------------------------------------------------------
# Step 11: safe-docker
# ---------------------------------------------------------------------------

step "safe-docker..."
if container_healthy safe-docker; then
  skip "safe-docker running"
else
  echo "  Starting safe-docker..."
  docker compose up -d safe-docker
  ok "safe-docker started"
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------

echo ""
echo -e "${GREEN}✅ Stack is up!${NC}"
echo ""
echo "  Prefect UI:     http://localhost:4200"
echo "  API:            http://localhost:8001"
echo "  Grafana:        http://localhost:3000"
echo ""
echo "Test it:"
echo "  curl -X POST http://localhost:8001/ingest -H 'Content-Type: application/json' -d '{\"url\":\"https://example.com\"}'"
echo ""
echo "To start the OpenClaw agent:"
echo "  docker compose --profile agent up -d"
echo ""
