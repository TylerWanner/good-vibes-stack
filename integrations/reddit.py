from __future__ import annotations

import re
from typing import Any

import requests


def is_reddit_url(url: str) -> bool:
    return "reddit.com/" in url or "redd.it/" in url


def _to_json_url(url: str) -> str:
    """Convert a Reddit URL to its JSON endpoint."""
    # Strip query params and fragments
    url = url.split("?")[0].split("#")[0].rstrip("/")
    # Use old.reddit.com for more reliable JSON responses
    import re as _re
    url = _re.sub(r"(?:www\.)?reddit\.com", "old.reddit.com", url)
    if not url.endswith(".json"):
        url += ".json"
    return url


def fetch_reddit_document(url: str) -> dict[str, Any]:
    """Fetch a Reddit post or subreddit listing via the public JSON endpoint.

    No API key required — Reddit exposes a public JSON endpoint by appending
    .json to any URL.
    """
    json_url = _to_json_url(url)

    response = requests.get(
        json_url,
        headers={"User-Agent": "second-brain-ingest/0.1 (personal research tool)"},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    # Single post view: [post_listing, comments_listing]
    if isinstance(data, list) and len(data) >= 1:
        return _parse_post(data, url)

    # Subreddit listing
    if isinstance(data, dict) and data.get("kind") == "Listing":
        return _parse_listing(data, url)

    raise RuntimeError(f"Unexpected Reddit JSON structure for {url}")


def _parse_post(data: list, url: str) -> dict[str, Any]:
    post_listing = data[0]
    post = post_listing["data"]["children"][0]["data"]

    title = post.get("title", "")
    selftext = post.get("selftext", "")
    subreddit = post.get("subreddit_name_prefixed", "")
    author = post.get("author", "")
    score = post.get("score", 0)
    num_comments = post.get("num_comments", 0)

    # Grab top comments if available
    top_comments = []
    if len(data) >= 2:
        comments = data[1]["data"]["children"]
        for c in comments[:10]:
            if c.get("kind") == "t1":
                body = c["data"].get("body", "").strip()
                if body and body != "[deleted]":
                    top_comments.append(body)

    content_parts = [f"Title: {title}", f"Subreddit: {subreddit}", f"Author: u/{author}",
                     f"Score: {score} | Comments: {num_comments}"]
    if selftext:
        content_parts.append(f"\nPost:\n{selftext}")
    if top_comments:
        content_parts.append("\nTop comments:\n" + "\n\n".join(f"- {c}" for c in top_comments))

    return {
        "title": title,
        "text": "\n".join(content_parts),
        "source_type": "reddit",
        "url": url,
    }


def _parse_listing(data: dict, url: str) -> dict[str, Any]:
    posts = data["data"]["children"]
    subreddit = url.split("/r/")[-1].split("/")[0] if "/r/" in url else "reddit"

    lines = [f"Subreddit: r/{subreddit}", ""]
    for child in posts[:20]:
        if child.get("kind") == "t3":
            p = child["data"]
            lines.append(f"• {p.get('title', '')} (score: {p.get('score', 0)}, comments: {p.get('num_comments', 0)})")

    return {
        "title": f"r/{subreddit} — recent posts",
        "text": "\n".join(lines),
        "source_type": "reddit",
        "url": url,
    }
