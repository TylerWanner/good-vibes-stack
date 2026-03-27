#!/bin/bash
set -e

# Update and install essentials
apt-get update -y
apt-get install -y curl git make unzip

# Install Docker
curl -fsSL https://get.docker.com | sh
usermod -aG docker root

# Install Docker Compose plugin
mkdir -p /usr/local/lib/docker/cli-plugins
curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-aarch64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

echo "Bootstrap complete. Docker ready."
