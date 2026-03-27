FROM ghcr.io/openclaw/openclaw:main

USER root

# Preinstall ACP harness CLIs so containerized gateways have a deterministic
# local runtime for acpx-backed Codex / Claude Code sessions.
RUN npm install -g @anthropic-ai/claude-code @openai/codex \
    && npm cache clean --force

USER node
