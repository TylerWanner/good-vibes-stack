#!/bin/bash
set -euo pipefail

LOG=/var/log/cloud-init-reptilian.log
exec > >(tee -a "$LOG") 2>&1

echo "=== Reptilian bootstrap starting ==="

# ---------------------------------------------------------------------------
# System
# ---------------------------------------------------------------------------
apt-get update -y
apt-get install -y curl git make unzip jq ca-certificates gnupg

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------
curl -fsSL https://get.docker.com | sh
systemctl enable docker
systemctl start docker

# ---------------------------------------------------------------------------
# Tailscale
# ---------------------------------------------------------------------------
curl -fsSL https://tailscale.com/install.sh | sh
systemctl enable tailscaled

# ---------------------------------------------------------------------------
# Ollama
# ---------------------------------------------------------------------------
curl -fsSL https://ollama.ai/install.sh | sh
systemctl enable ollama
systemctl start ollama

# Pull models (background — takes a while)
ollama pull nomic-embed-text &
ollama pull qwen2.5:7b &

# ---------------------------------------------------------------------------
# Clone repo
# ---------------------------------------------------------------------------
mkdir -p /opt/reptilian
git clone https://github.com/TylerWanner/good-vibes-stack.git /opt/reptilian/good-vibes-stack
ln -sf /opt/reptilian/good-vibes-stack /root/repos

echo "=== Bootstrap complete ==="
echo "Next steps:"
echo "  1. tailscale up   (authenticate)"
echo "  2. cd /opt/reptilian/good-vibes-stack"
echo "  3. cp .env.example .env && nano .env"
echo "  4. ./init.sh"
