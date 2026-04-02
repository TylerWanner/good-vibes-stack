#!/bin/bash
# restore.sh — rebuild the second brain stack from scratch
# Run from the repo root: ./scripts/restore.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$ROOT_DIR"

echo "🦎 Provision restore script"
echo "==========================="

# Load env
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
else
  echo "❌ .env not found — run from the repo root"
  exit 1
fi

echo ""
echo "Step 1: Bringing stack up..."
docker compose up -d

echo ""
echo "Step 2: Waiting for postgres to be healthy..."
until docker compose exec -T postgres pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" > /dev/null 2>&1; do
  echo "  waiting..."
  sleep 2
done
echo "  ✅ postgres ready"

echo ""
echo "Step 3: Creating prefect database..."
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE DATABASE prefect;" 2>/dev/null || echo "  (prefect DB already exists)"

echo ""
echo "Step 4: Creating work pool..."
docker compose exec -T prefect-server prefect work-pool create default-pool --type process 2>/dev/null || echo "  (work pool already exists)"

echo ""
echo "Step 5: Applying schema + migrations..."
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -f /dev/stdin < apps/second_brain/db/schema.sql
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -f /dev/stdin < apps/second_brain/db/migrations/001_add_pgvector.sql
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -f /dev/stdin < apps/second_brain/db/migrations/002_add_privacy_and_storage.sql
echo "  ✅ schema applied"

echo ""
echo "Step 5b: Restoring Prefect concurrency limits..."
# Wait for Prefect server to be ready
until curl -s http://localhost:4200/api/health > /dev/null 2>&1; do
  echo "  waiting for Prefect server..."
  sleep 2
done
curl -s -X POST http://localhost:4200/api/v2/concurrency_limits/ \
  -H "Content-Type: application/json" \
  -d '{"name": "ollama", "limit": 1}' > /dev/null 2>&1 || echo "  (ollama limit already exists)"
echo "  ✅ ollama v2 global concurrency limit set (max 1)"
curl -s -X POST http://localhost:4200/api/v2/concurrency_limits/ \
  -H "Content-Type: application/json" \
  -d '{"name": "scrapling", "limit": 3}' > /dev/null 2>&1 || echo "  (scrapling limit already exists)"
echo "  ✅ scrapling v2 global concurrency limit set (max 3)"

echo ""
echo "Step 6: Deploying Prefect flows..."
docker compose exec -T prefect-worker sh -c "cd /app && prefect deploy --all --prefect-file orchestration/prefect/prefect.yaml"
echo "  ✅ flows deployed"

echo ""
echo "Step 7: Restarting API..."
docker compose restart nervous-system-api
echo "  ✅ API restarted"

echo ""
echo "✅ Restore complete. Stack is back up."
echo ""
echo "Next steps:"
echo "  - Trigger Readwise sync to restore saved articles"
echo "  - Re-ingest any URLs from memory/recovery-urls-*.md"
