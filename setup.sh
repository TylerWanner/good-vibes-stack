#!/usr/bin/env bash
# setup.sh — interactive onboarding for Good Vibes Stack
# Creates .env with sensible defaults, then runs init.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

echo ""
echo -e "${GREEN}🎸 Good Vibes Stack Setup${NC}"
echo "========================="
echo ""

# Check if .env already exists
if [ -f .env ]; then
  echo -e "${YELLOW}⚠️  .env already exists${NC}"
  read -rp "Overwrite? (y/N): " overwrite
  if [[ ! "$overwrite" =~ ^[Yy]$ ]]; then
    echo ""
    echo "Keeping existing .env. Running init.sh..."
    exec ./init.sh
  fi
  echo ""
fi

# Start with example
cp .env.example .env

# Helper to prompt with auto-generate option (masked input for secrets)
prompt_secret() {
  local var_name="$1"
  local description="$2"
  local generator="$3"
  local default="${4:-}"
  
  echo -e "${CYAN}${description}${NC}"
  if [ -n "$generator" ]; then
    echo -e "  Press enter to auto-generate, or paste your own value"
  fi
  echo -n "  ${var_name}: "
  read -rs value
  echo ""  # newline after silent input
  
  if [ -z "$value" ]; then
    if [ -n "$generator" ]; then
      value=$(eval "$generator")
      echo -e "  ${GREEN}→ Generated${NC}"
    elif [ -n "$default" ]; then
      value="$default"
      echo -e "  ${GREEN}→ Using default${NC}"
    fi
  else
    echo -e "  ${GREEN}→ Saved [hidden]${NC}"
  fi
  
  if [ -n "$value" ]; then
    # Escape special chars and update .env
    escaped_value=$(printf '%s\n' "$value" | sed 's/[&/\]/\\&/g')
    sed -i "s|^${var_name}=.*|${var_name}=${escaped_value}|" .env
    sed -i "s|^# *${var_name}=.*|${var_name}=${escaped_value}|" .env
  fi
  echo ""
}

prompt_optional() {
  local var_name="$1"
  local description="$2"
  
  echo -e "${CYAN}${description}${NC}"
  echo "  Press enter to skip"
  read -rp "  ${var_name}: " value
  
  if [ -n "$value" ]; then
    escaped_value=$(printf '%s\n' "$value" | sed 's/[&/\]/\\&/g')
    sed -i "s|^${var_name}=.*|${var_name}=${escaped_value}|" .env
    sed -i "s|^# *${var_name}=.*|${var_name}=${escaped_value}|" .env
    echo -e "  ${GREEN}→ Saved${NC}"
  else
    echo -e "  ${YELLOW}→ Skipped${NC}"
  fi
  echo ""
}

echo -e "${YELLOW}▶ Required secrets${NC}"
echo ""

prompt_secret "POSTGRES_PASSWORD" \
  "Postgres password (for the second_brain database)" \
  "openssl rand -hex 16"

prompt_secret "SAFE_DOCKER_AUTH_SECRET" \
  "safe-docker auth secret (signs/verifies container control caller tokens)" \
  "openssl rand -hex 32"

prompt_secret "OPENCLAW_GATEWAY_TOKEN" \
  "OpenClaw gateway token (for agent API auth)" \
  "openssl rand -hex 32"

echo -e "${YELLOW}▶ LLM Provider${NC}"
echo ""
echo -e "${CYAN}Which LLM provider do you want to use for workflows?${NC}"
echo "  1) ollama (local, free, recommended default)"
echo "  2) anthropic (cloud, optional, requires API key)"
read -rp "  Choice [1]: " llm_choice

if [ "$llm_choice" = "2" ]; then
  sed -i "s|^SECOND_BRAIN_LLM_PROVIDER=.*|SECOND_BRAIN_LLM_PROVIDER=anthropic|" .env
  sed -i "s|^SECOND_BRAIN_LLM_MODEL=.*|SECOND_BRAIN_LLM_MODEL=claude-sonnet-4-6|" .env
  echo ""
  echo -e "${CYAN}Anthropic API key${NC}"
  echo "  Used for workflow credentials when Anthropic is selected."
  echo -n "  ANTHROPIC_API_KEY: "
  read -rs anthropic_key
  echo ""
  if [ -n "$anthropic_key" ]; then
    sed -i "s|^ANTHROPIC_API_KEY=.*|ANTHROPIC_API_KEY=${anthropic_key}|" .env
    sed -i "s|^# *ANTHROPIC_API_KEY=.*|ANTHROPIC_API_KEY=${anthropic_key}|" .env
    # Will be added to .env.blocks later for Prefect block
    echo -e "  ${GREEN}→ Saved [hidden]${NC}"
  fi
else
  echo -e "  ${GREEN}→ Using ollama with qwen2.5:7b${NC}"
  echo ""
  echo -e "${YELLOW}Note:${NC} Make sure Ollama is running with the model pulled:"
  echo "  ollama pull qwen2.5:7b"
  echo "  ollama pull nomic-embed-text"
  echo ""
fi

# Default agent model is Codex unless the operator overrides it.
sed -i "s|^OPENCLAW_MODEL=.*|OPENCLAW_MODEL=openai-codex/gpt-5.4|" .env

echo -e "${YELLOW}▶ Telegram (for notifications + agent)${NC}"
echo ""

echo -e "${CYAN}Telegram bot token (from @BotFather)${NC}"
echo "  Required if you want flow notifications or to run the OpenClaw agent"
echo -n "  TELEGRAM_BOT_TOKEN: "
read -rs telegram_bot_token
echo ""
if [ -n "$telegram_bot_token" ]; then
  # Save to root .env for flows
  sed -i "s|^TELEGRAM_BOT_TOKEN=.*|TELEGRAM_BOT_TOKEN=${telegram_bot_token}|" .env
  sed -i "s|^# *TELEGRAM_BOT_TOKEN=.*|TELEGRAM_BOT_TOKEN=${telegram_bot_token}|" .env
  
  # Save to agent config
  mkdir -p agents/example/config
  echo "TELEGRAM_BOT_TOKEN=${telegram_bot_token}" > agents/example/config/.env
  
  echo -e "  ${GREEN}→ Saved to .env and agents/example/config/.env${NC}"
else
  echo -e "  ${YELLOW}→ Skipped (no Telegram notifications or agent)${NC}"
fi
echo ""

prompt_optional "TELEGRAM_CHAT_ID" \
  "Telegram chat ID (for flow notifications — use your user ID from @userinfobot, or a group chat ID)"

echo -e "${YELLOW}▶ Optional configuration${NC}"
echo ""

prompt_secret "BRAVE_API_KEY" \
  "Brave Search API key (for web search — used by both flows and OpenClaw agent)" \
  ""

# ---------------------------------------------------------------------------
# Integrations (.env.blocks)
# ---------------------------------------------------------------------------

echo -e "${YELLOW}▶ Integrations (optional — for Prefect blocks)${NC}"
echo ""
echo "These create Prefect Secret blocks for flows that need external APIs."
echo "Skip any you don't need."
echo ""

blocks_content=""

# Readwise
echo -e "${CYAN}Readwise (sync highlights to second brain)${NC}"
echo -n "  READWISE_API_TOKEN: "
read -rs readwise_token
echo ""
if [ -n "$readwise_token" ]; then
  blocks_content+="READWISE_API_TOKEN=${readwise_token}\n"
  echo -e "  ${GREEN}→ Saved [hidden]${NC}"
else
  echo -e "  ${YELLOW}→ Skipped${NC}"
fi
echo ""

# S3 Backups
echo -e "${CYAN}S3-compatible backups (AWS S3, Cloudflare R2, Backblaze B2, etc.)${NC}"
echo "  Leave blank to skip"
echo -n "  BACKUP_S3_ENDPOINT (e.g. https://s3.us-east-1.amazonaws.com): "
read -r s3_endpoint
if [ -n "$s3_endpoint" ]; then
  blocks_content+="BACKUP_S3_ENDPOINT=${s3_endpoint}\n"
  
  echo -n "  BACKUP_S3_ACCESS_KEY_ID: "
  read -rs s3_access_key
  echo ""
  blocks_content+="BACKUP_S3_ACCESS_KEY_ID=${s3_access_key}\n"
  
  echo -n "  BACKUP_S3_SECRET_ACCESS_KEY: "
  read -rs s3_secret_key
  echo ""
  blocks_content+="BACKUP_S3_SECRET_ACCESS_KEY=${s3_secret_key}\n"
  
  read -rp "  BACKUP_S3_BUCKET [backups]: " s3_bucket
  blocks_content+="BACKUP_S3_BUCKET=${s3_bucket:-backups}\n"
  
  echo -e "  ${GREEN}→ S3 backup credentials saved${NC}"
else
  echo -e "  ${YELLOW}→ Skipped${NC}"
fi
echo ""

# Add telegram to blocks if set
if [ -n "$telegram_bot_token" ]; then
  blocks_content+="TELEGRAM_BOT_TOKEN=${telegram_bot_token}\n"
  if [ -n "$(grep '^TELEGRAM_CHAT_ID=' .env | cut -d= -f2)" ]; then
    chat_id=$(grep '^TELEGRAM_CHAT_ID=' .env | cut -d= -f2)
    blocks_content+="TELEGRAM_CHAT_ID=${chat_id}\n"
  fi
fi

# Add anthropic to blocks if set (check variable or fall back to .env)
if [ -n "${anthropic_key:-}" ]; then
  blocks_content+="ANTHROPIC_API_KEY=${anthropic_key}\n"
elif grep -q "^ANTHROPIC_API_KEY=." .env 2>/dev/null; then
  blocks_content+="ANTHROPIC_API_KEY=$(grep '^ANTHROPIC_API_KEY=' .env | cut -d= -f2)\n"
fi

# Write .env.blocks if any integrations configured
if [ -n "$blocks_content" ]; then
  echo -e "$blocks_content" > .env.blocks
  echo -e "${GREEN}✅ .env.blocks created with integration credentials${NC}"
  echo ""
fi

# Done
echo ""
echo -e "${GREEN}✅ .env configured!${NC}"
echo ""
echo "Review your config:"
echo "  cat .env"
echo ""
read -rp "Run init.sh now? (Y/n): " run_init

if [[ ! "$run_init" =~ ^[Nn]$ ]]; then
  echo ""
  exec ./init.sh
else
  echo ""
  echo "Run ./init.sh when ready to start the stack."
fi
