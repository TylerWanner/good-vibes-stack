"""Second Brain MCP server.

Exposes the second brain capabilities over the MCP protocol so any
MCP-compatible client (Claude Desktop, Cursor, external agents) can
interact with the second brain without going through OpenClaw.

Mirrors the tool surface of agents/openclaw-extensions/second-brain-tools/index.ts.
Talks to the nervous-system HTTP API — works inside or outside the container.

Usage:
    python -m mcp.server
    python -m mcp.server --transport stdio          # Claude Desktop
    python -m mcp.server --host 0.0.0.0 --port 8000  # HTTP
"""
from __future__ import annotations

import os
from typing import Any

import requests
from fastmcp import FastMCP

mcp = FastMCP("Second Brain MCP")

# Base URL for the nervous-system API
# Defaults to host.docker.internal:8001 for running outside the container
_BASE_URL = os.environ.get("NERVOUS_SYSTEM_API_URL", "http://host.docker.internal:8001").rstrip("/")
_TIMEOUT = int(os.environ.get("SECOND_BRAIN_TIMEOUT_MS", "120000")) // 1000


def _get(path: str, **params) -> Any:
    resp = requests.get(f"{_BASE_URL}{path}", params=params, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _post(path: str, body: dict) -> Any:
    resp = requests.post(f"{_BASE_URL}{path}", json=body, timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


@mcp.tool
def second_brain_save_content(
    url: str,
    force: bool = False,
    notify: dict | None = None,
) -> dict[str, Any]:
    """Save and analyze a URL using the second-brain workflow.

    Use this whenever a user sends a URL, asks to save/bookmark/ingest content,
    or asks to add content to second brain.
    """
    body: dict[str, Any] = {"url": url, "force": force}
    if notify:
        body["notify"] = notify
    return _post("/ingest", body)


@mcp.tool
def second_brain_search(
    query: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """Search ingested second-brain knowledge.

    Use this for questions about saved content, recent items, tags,
    sources, or lookups in second brain.
    """
    return _get("/articles", q=query, limit=limit)


@mcp.tool
def second_brain_weekly_digest(
    days: int = 7,
) -> dict[str, Any]:
    """Generate a weekly digest from processed second-brain articles.

    Use this when users ask for a digest/summary over recent saved content.
    """
    return _post("/digest", {"days": days})


@mcp.tool
def second_brain_reingest(
    query: str,
    limit: int = 5,
) -> dict[str, Any]:
    """Search for articles in the second brain by query and re-ingest them.

    Use when summaries are thin or content needs refreshing.
    """
    return _post("/ingest/reprocess", {"query": query, "limit": limit})


@mcp.tool
def second_brain_get_stats() -> dict[str, Any]:
    """Get stats on the second brain — article counts by status, recent failures."""
    return _get("/articles/stats")


@mcp.tool
def second_brain_get_article(url: str) -> dict[str, Any]:
    """Get a single article record by exact URL."""
    return _get("/articles", url=url)


_SECRET_KEY = os.environ.get("MCP_SECRET_KEY", "")


@mcp.tool
def meaning_of_life(secret: str = "") -> str:
    """What is the meaning of life?

    Provide your secret key to get the real answer.
    """
    if _SECRET_KEY and secret == _SECRET_KEY:
        return "42"
    return "Unknown. Only those with access know the answer."


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run Second Brain MCP server")
    parser.add_argument(
        "--transport",
        default="streamable-http",
        choices=["streamable-http", "stdio", "sse"],
        help="MCP transport (default: streamable-http)",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (HTTP transport)")
    parser.add_argument("--port", type=int, default=8000, help="Bind port (HTTP transport)")
    parser.add_argument("--path", default="/mcp", help="HTTP path (HTTP transport)")
    args = parser.parse_args()

    if args.transport == "stdio":
        mcp.run(transport="stdio")
    else:
        try:
            mcp.run(
                transport=args.transport,
                host=args.host,
                port=args.port,
                path=args.path,
            )
        except TypeError:
            # Fallback for older FastMCP versions
            mcp.run()


if __name__ == "__main__":
    main()
