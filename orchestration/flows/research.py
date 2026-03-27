"""Research agent flow — search, fetch, synthesize, report.

CLI usage:
    second-brain-research --query "what is context engineering" --num-sources 5
"""
from __future__ import annotations

import argparse
import sys

import time
from typing import Any

import requests
from prefect import flow, get_run_logger, task

from second_brain.llm import LLMClient
from second_brain.acquisition.scrapling import ScraplingClient
from nervous_system.notifications.telegram import send_telegram_message
from shared.config import load_settings


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(retries=0)
def search_brave(query: str, num_results: int, api_key: str) -> list[dict[str, Any]]:
    """Search Brave API and return top results."""
    logger = get_run_logger()
    resp = requests.get(
        "https://api.search.brave.com/res/v1/web/search",
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": api_key,
        },
        params={"q": query, "count": num_results},
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    results = data.get("web", {}).get("results", [])
    logger.info("Brave returned %d results for query: %s", len(results), query)
    return [{"url": r.get("url"), "title": r.get("title", ""), "snippet": r.get("description", "")} for r in results]


@task(retries=0)
def fetch_source(url: str, title: str, scrapling_url: str) -> dict[str, Any]:
    """Fetch a single source via Scrapling dynamic mode."""
    logger = get_run_logger()
    try:
        client = ScraplingClient(base_url=scrapling_url)
        doc = client.fetch(url, fetcher="dynamic")
        content = doc.get("text") or doc.get("content") or ""
        logger.info("Fetched %d chars from %s", len(content), url)
        return {"url": url, "title": title, "content": content}
    except Exception as exc:
        logger.warning("Failed to fetch %s: %s", url, exc)
        return {"url": url, "title": title, "content": ""}


@task(retries=0)
def synthesize_report(query: str, sources: list[dict[str, Any]], settings: Any) -> str:
    """Use LLM to synthesize sources into a research report."""
    logger = get_run_logger()
    # Filter out empty sources
    valid = [s for s in sources if len(s.get("content", "")) > 100]
    if not valid:
        logger.warning("No valid source content to synthesize")
        return f"⚠️ Research failed: could not fetch content from any sources for query: {query}"
    logger.info("Synthesizing %d sources", len(valid))
    from shared.secrets import load_anthropic_api_key
    llm = LLMClient(
        provider=settings.llm_provider,
        model=settings.llm_model,
        anthropic_api_key=load_anthropic_api_key(),
        ollama_base_url=settings.ollama_base_url,
    )
    return llm.research_synthesis(query=query, sources=valid)


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="research-topic", log_prints=True)
def research_topic(query: str, num_sources: int = 5) -> dict[str, Any]:
    """Search the web, fetch top sources, synthesize a report, send via Telegram."""
    from shared.secrets import load_brave_api_key
    
    logger = get_run_logger()
    settings = load_settings()
    start = time.time()

    brave_key = load_brave_api_key()
    if not brave_key:
        raise RuntimeError(
            "Brave API key missing. Set up 'brave-credentials' Prefect block "
            "or set BRAVE_API_KEY env var."
        )

    logger.info("Starting research for: %s", query)

    # 1. Search
    search_results = search_brave(query=query, num_results=num_sources, api_key=brave_key)
    if not search_results:
        msg = f"⚠️ Research: no results found for '{query}'"
        from nervous_system.notifications.telegram import notify_telegram
        notify_telegram(msg)
        return {"query": query, "status": "no_results"}

    # 2. Fetch sources (sequential to avoid hammering scrapling)
    fetched = []
    for r in search_results:
        doc = fetch_source(url=r["url"], title=r["title"], scrapling_url=settings.scrapling_fetcher_url)
        fetched.append(doc)

    # 3. Synthesize
    report = synthesize_report(query=query, sources=fetched, settings=settings)

    elapsed = round(time.time() - start, 1)

    # 4. Build source list footer
    source_lines = "\n".join(
        f"[{i}] {r['title'] or r['url']}: {r['url']}"
        for i, r in enumerate(search_results, 1)
    )
    full_report = f"🔍 Research: {query}\n\n{report}\n\n**Sources:**\n{source_lines}\n\n⏱ {elapsed}s"

    # 5. Send via Telegram (chunk if needed — 4096 char limit)
    from nervous_system.notifications.telegram import notify_telegram
    chunk_size = 4000
    chunks = [full_report[i:i + chunk_size] for i in range(0, len(full_report), chunk_size)]
    for chunk in chunks:
        notify_telegram(chunk)

    logger.info("Research complete in %.1fs — %d chunks sent", elapsed, len(chunks))
    return {"query": query, "status": "completed", "elapsed_seconds": elapsed, "report": report}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run research agent")
    parser.add_argument("--query", required=True, help="Research query")
    parser.add_argument("--num-sources", type=int, default=5, help="Number of sources to fetch")
    args = parser.parse_args()
    result = research_topic(query=args.query, num_sources=args.num_sources)
    print(f"Status: {result.get('status')}")
    print(f"Elapsed: {result.get('elapsed_seconds')}s")
    if result.get("report"):
        print("\n" + result["report"])
    sys.exit(0 if result.get("status") == "completed" else 1)
