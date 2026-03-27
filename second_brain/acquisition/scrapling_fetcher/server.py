from __future__ import annotations

import asyncio
from functools import partial
from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from scrapling.fetchers import DynamicFetcher, StealthyFetcher

app = FastAPI(title="Scrapling Fetcher", version="0.1.0")

IGNORED_TAGS = ("script", "style", "nav", "footer", "head", "noscript", "meta")

# Hard cap on how long a single browser fetch is allowed to take
# Stealthy mode needs more time — Playwright + anti-bot evasion is slow
FETCH_TIMEOUT_DYNAMIC = 60
FETCH_TIMEOUT_STEALTHY = 90


class FetchRequest(BaseModel):
    url: str
    fetcher: Literal["dynamic", "stealthy"] = "dynamic"
    network_idle: bool = True


class FetchResponse(BaseModel):
    url: str
    title: str
    text: str
    publish_date: str | None = None  # ISO8601 if found, else None


def _sync_fetch(url: str, fetcher: str, network_idle: bool) -> object:
    """Run the blocking Scrapling fetch. Intended to be called via run_in_executor."""
    if fetcher == "stealthy":
        # network_idle=True causes hangs on sites that continuously fire requests (Twitter).
        # Force network_idle=False for stealthy to avoid timeout races.
        return StealthyFetcher.fetch(url, headless=True, network_idle=False)
    return DynamicFetcher.fetch(url, headless=True, network_idle=network_idle)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/fetch", response_model=FetchResponse)
async def fetch_url(req: FetchRequest) -> FetchResponse:
    loop = asyncio.get_running_loop()
    fetch_timeout = FETCH_TIMEOUT_STEALTHY if req.fetcher == "stealthy" else FETCH_TIMEOUT_DYNAMIC
    try:
        page = await asyncio.wait_for(
            loop.run_in_executor(None, partial(_sync_fetch, req.url, req.fetcher, req.network_idle)),
            timeout=fetch_timeout,
        )
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail=f"Fetch timed out after {fetch_timeout}s")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Fetch failed: {exc}") from exc

    title_el = page.css("title")
    title = title_el[0].text if title_el else ""

    text = page.get_all_text(ignore_tags=IGNORED_TAGS)

    if not text.strip():
        raise HTTPException(status_code=422, detail="No readable content extracted from page")

    publish_date = _extract_publish_date(page) or _extract_date_from_text(text)

    return FetchResponse(url=req.url, title=title.strip(), text=text.strip(), publish_date=publish_date)


def _extract_publish_date(page: object) -> str | None:
    """Extract article publish date from HTML metadata. Returns ISO8601 string or None.

    Tries in order:
    1. <meta property="article:published_time"> (Open Graph)
    2. <meta property="og:article:published_time">
    3. <meta name="date"> / <meta name="DC.date">
    4. <time datetime="..."> elements
    5. JSON-LD schema.org datePublished
    """
    import json as _json
    import re

    # 1-3: Meta tags
    for selector, attr in [
        ('meta[property="article:published_time"]', "content"),
        ('meta[property="og:article:published_time"]', "content"),
        ('meta[name="date"]', "content"),
        ('meta[name="DC.date"]', "content"),
        ('meta[name="pubdate"]', "content"),
        ('meta[itemprop="datePublished"]', "content"),
    ]:
        try:
            els = page.css(selector)
            if els:
                val = els[0].attrib.get(attr, "").strip()
                if val:
                    return val
        except Exception:
            pass

    # 4: <time datetime="...">
    try:
        time_els = page.css("time[datetime]")
        if time_els:
            val = time_els[0].attrib.get("datetime", "").strip()
            if val:
                return val
    except Exception:
        pass

    # 5: JSON-LD
    try:
        for script_el in page.css('script[type="application/ld+json"]'):
            try:
                data = _json.loads(script_el.text or "")
                if isinstance(data, dict):
                    date = data.get("datePublished") or data.get("dateCreated")
                    if date:
                        return str(date)
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            date = item.get("datePublished") or item.get("dateCreated")
                            if date:
                                return str(date)
            except Exception:
                continue
    except Exception:
        pass

    return None


def _extract_date_from_text(text: str) -> str | None:
    """Last-resort: scan the first 500 chars of extracted text for a date string.

    Only looks near the top where bylines/publish dates typically appear.
    Returns ISO8601 string or None.
    """
    import re
    from datetime import datetime

    snippet = text[:500]

    # Common date patterns
    patterns = [
        # ISO: 2026-03-23 or 2026-03-23T...
        r"\b(20\d\d-\d{2}-\d{2}(?:T[\d:\.Z+-]+)?)\b",
        # Written: March 23, 2026 / Mar 23, 2026
        r"\b((?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        r"\s+\d{1,2},?\s+20\d\d)\b",
        # Written reversed: 23 March 2026
        r"\b(\d{1,2}\s+(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        r"\s+20\d\d)\b",
    ]

    for pattern in patterns:
        match = re.search(pattern, snippet, re.IGNORECASE)
        if match:
            try:
                # Use stdlib only — no dateutil dependency
                from datetime import datetime
                raw = match.group(1)
                # Try ISO first
                try:
                    dt = datetime.fromisoformat(raw.replace('Z', '+00:00'))
                except ValueError:
                    # Try common written formats
                    for fmt in ("%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"):
                        try:
                            dt = datetime.strptime(raw, fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        continue
                # Sanity check: year between 2000 and 2035
                if 2000 <= dt.year <= 2035:
                    return dt.isoformat()
            except Exception:
                continue

    return None
