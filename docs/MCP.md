# MCP Server

The Good Vibes Stack ships a native MCP server that exposes your second brain to any MCP-compatible client — Claude Desktop, Cursor, or any agent that speaks MCP.

Your data stays on your infra. The MCP server talks to your local `nervous-system-api`.

---

## What's exposed

| Tool | What it does |
|---|---|
| `second_brain_save_content` | Ingest a URL into the second brain |
| `second_brain_search` | Semantic search over ingested knowledge |
| `second_brain_weekly_digest` | Trigger a digest of recent content |
| `second_brain_reingest` | Re-fetch and re-analyze existing articles |
| `second_brain_get_stats` | Article counts, status breakdown, embedding coverage |
| `second_brain_get_article` | Fetch a single article by URL |

---

## Prerequisites

- Stack running (`docker compose up -d`)
- Python 3.10+ with dependencies installed (`pip install -e .`)
- `NERVOUS_SYSTEM_API_URL` pointing at your `nervous-system-api` (default: `http://host.docker.internal:8001`)

---

## Claude Desktop

Add to your `claude_desktop_config.json` (`~/Library/Application Support/Claude/` on macOS):

```json
{
  "mcpServers": {
    "second-brain": {
      "command": "python",
      "args": ["-m", "mcp.server", "--transport", "stdio"],
      "cwd": "/path/to/good-vibes-stack",
      "env": {
        "NERVOUS_SYSTEM_API_URL": "http://localhost:8001"
      }
    }
  }
}
```

Restart Claude Desktop. The second brain tools will appear in the tool picker.

---

## Cursor / other stdio clients

Same pattern — run `python -m mcp.server --transport stdio` from the repo root with `NERVOUS_SYSTEM_API_URL` set.

---

## HTTP transport (remote clients)

```bash
NERVOUS_SYSTEM_API_URL=http://localhost:8001 python -m mcp.server \
  --transport streamable-http \
  --host 0.0.0.0 \
  --port 8000 \
  --path /mcp
```

Connect any MCP HTTP client to `http://your-host:8000/mcp`.

---

## Quick test

```bash
# Verify the API is reachable
curl http://localhost:8001/health

# Run the MCP server
NERVOUS_SYSTEM_API_URL=http://localhost:8001 python -m mcp.server --transport streamable-http

# In another terminal — list tools
curl http://localhost:8000/mcp
```
