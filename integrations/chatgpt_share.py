from __future__ import annotations

from typing import Any

from second_brain.acquisition.scrapling import ScraplingClient


def is_chatgpt_share_url(url: str) -> bool:
    normalized = url.lower()
    return "chatgpt.com/share/" in normalized or "chat.openai.com/share/" in normalized


def fetch_chatgpt_share_document(url: str, scrapling_fetcher_url: str) -> dict[str, Any]:
    """Fetch a ChatGPT share link via the Scrapling sidecar.

    ChatGPT share pages are JS-rendered SPAs — plain HTTP requests return an
    empty shell. The Scrapling sidecar runs a real browser (Playwright) and
    waits for network idle before extracting content.
    """
    client = ScraplingClient(scrapling_fetcher_url)
    result = client.fetch(url, fetcher="dynamic")

    title = result.get("title") or "ChatGPT Share Conversation"
    text = result.get("text", "").strip()

    if not text:
        raise RuntimeError("Unable to extract readable content from ChatGPT share page")

    return {
        "title": title,
        "text": text,
        "source_type": "chatgpt_share",
        "url": url,
    }
