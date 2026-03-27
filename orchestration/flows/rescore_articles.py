"""Rescore articles that are missing scores or tags.

Finds processed articles with NULL scores or empty tags, re-runs LLM analysis
on their stored raw_text, and updates the DB. Safe to run multiple times —
only updates articles that are still missing data.
"""
from __future__ import annotations

import logging
from typing import Any

from prefect import flow, task, get_run_logger

from data.postgres.client import PostgresClient
from second_brain.llm import LLMClient
from shared.config import load_settings

logger = logging.getLogger(__name__)


@task(retries=1, retry_delay_seconds=10)
def rescore_one(url: str, title: str, raw_text: str) -> dict[str, Any]:
    logger = get_run_logger()
    from shared.secrets import load_anthropic_api_key
    settings = load_settings()
    llm = LLMClient(
        provider=settings.llm_provider,
        model=settings.llm_model,
        anthropic_api_key=load_anthropic_api_key(),
        ollama_base_url=settings.ollama_base_url,
    )
    content = raw_text[:16000]
    from shared.config import llm_concurrency
    with llm_concurrency():
        result = llm.summarize_and_tag(url=url, title=title, content=content)
    logger.info(
        "rescore result for %s: tags=%s scores=%s/%s/%s",
        url[:60],
        result.get("tags"),
        result.get("score_usefulness"),
        result.get("score_interest"),
        result.get("score_pov"),
    )
    return result


@flow(name="rescore-articles")
def rescore_articles(limit: int = 100) -> dict[str, Any]:
    """Re-run LLM scoring on processed articles missing scores or tags."""
    run_logger = get_run_logger()
    settings = load_settings()
    db = PostgresClient(settings.database_url)

    candidates = db.get_articles_missing_scores(limit=limit)
    run_logger.info("Found %d articles to rescore", len(candidates))

    updated = 0
    failed = 0

    for article in candidates:
        url = article["url"]
        title = article["title"] or ""
        raw_text = article["raw_text"] or article["summary"] or ""

        if not raw_text.strip():
            run_logger.warning("Skipping %s — no raw_text or summary to rescore", url[:60])
            continue

        try:
            result = rescore_one(url=url, title=title, raw_text=raw_text)
            tags = result.get("tags") or []
            if isinstance(tags, str):
                tags = [tags] if tags else []

            db.update_article_scores(
                url=url,
                score_usefulness=result.get("score_usefulness"),
                score_interest=result.get("score_interest"),
                score_pov=result.get("score_pov"),
                score_uniqueness=result.get("score_uniqueness"),
                tags=tags or None,
            )
            updated += 1
        except Exception as exc:
            run_logger.warning("Failed to rescore %s: %s", url[:60], exc)
            failed += 1

    run_logger.info("Rescore complete: %d updated, %d failed", updated, failed)
    return {"updated": updated, "failed": failed, "total": len(candidates)}


if __name__ == "__main__":
    rescore_articles()
