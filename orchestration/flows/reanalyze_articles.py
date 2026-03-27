"""Re-analyze articles with the current prompt, overwriting old summaries.

Run once after significant prompt improvements to refresh stale summaries.
After this completes, run backfill-embeddings to update embeddings.

Usage:
  From Prefect UI: trigger 'reanalyze-articles' deployment
  With cutoff: trigger with {"cutoff_date": "2026-03-05T23:40:00"}
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from prefect import flow, task, get_run_logger

from second_brain.llm import LLMClient
from data.postgres.client import PostgresClient
from shared.config import load_settings

logger = logging.getLogger(__name__)

# Default cutoff: March 5, 2026 6:40 PM ET = 23:40 UTC
# Articles before this date used an earlier, weaker prompt.
DEFAULT_CUTOFF = "2026-03-05T22:40:00"


@task(retries=1, retry_delay_seconds=5)
def reanalyze_article(article: dict, llm: LLMClient, db: PostgresClient) -> bool:
    """Re-run analyze_document on a single article and overwrite its summary/tags."""
    task_logger = get_run_logger()
    url = article.get("url", "")
    content = article.get("raw_text") or ""
    title = article.get("title") or ""

    if not content:
        task_logger.warning(f"No content for {url} — skipping")
        return False

    try:
        from shared.config import llm_concurrency
        with llm_concurrency():
            result = llm.summarize_and_tag(url=url, title=title, content=content)
        summary = result.get("summary", "")
        tags = result.get("tags", [])

        if not summary:
            task_logger.warning(f"Empty summary returned for {url} — skipping")
            return False

        import sqlalchemy as sa
        from data.postgres.models import Article
        with db._session_factory() as session:
            session.execute(
                sa.update(Article)
                .where(Article.__table__.c.id == article["id"])
                .values(
                    summary=summary,
                    tags=tags,
                    score_usefulness=result.get("score_usefulness"),
                    score_interest=result.get("score_interest"),
                    score_pov=result.get("score_pov"),
                    embedding=None,
                )
            )
            session.commit()

        task_logger.info(f"Re-analyzed: {url[:60]}")
        return True

    except Exception as e:
        task_logger.warning(f"Failed to re-analyze {url}: {e}")
        return False


@flow(name="reanalyze-articles")
def reanalyze_articles(
    cutoff_date: str = DEFAULT_CUTOFF,
    limit: int | None = None,
    batch_size: int = 5,
    urls: list[str] | None = None,
) -> dict[str, Any]:
    """Re-analyze articles with the current prompt.

    Args:
        cutoff_date: ISO datetime string. Articles processed before this are re-analyzed.
                     Ignored if urls is provided.
        limit: Max articles to process (None = all). Use small number to test.
        batch_size: Articles per task batch (controls Ollama concurrency).
        urls: Optional list of specific URLs to re-analyze (overrides cutoff_date).
    """
    flow_logger = get_run_logger()
    from shared.secrets import load_anthropic_api_key
    settings = load_settings()
    db = PostgresClient(settings.database_url)
    llm = LLMClient(
        provider=settings.llm_provider,
        model=settings.llm_model,
        anthropic_api_key=load_anthropic_api_key(),
        ollama_base_url=settings.ollama_base_url,
    )

    import sqlalchemy as sa
    from data.postgres.engine import get_session_factory
    from data.postgres.models import Article

    t = Article.__table__
    if urls:
        stmt = (
            sa.select(t.c.id, t.c.url, t.c.title, t.c.raw_text)
            .where(
                t.c.url.in_(urls),
                t.c.raw_text.isnot(None),
                t.c.raw_text != "",
            )
        )
    else:
        stmt = (
            sa.select(t.c.id, t.c.url, t.c.title, t.c.raw_text)
            .where(
                t.c.status == "processed",
                t.c.processed_at < cutoff_date,
                t.c.raw_text.isnot(None),
                t.c.raw_text != "",
            )
            .order_by(t.c.processed_at.asc())
        )
        if limit:
            stmt = stmt.limit(int(limit))

    session_factory = get_session_factory(settings.database_url)
    with session_factory() as session:
        articles = [dict(r) for r in session.execute(stmt).mappings().all()]

    flow_logger.info(

        f"Found {len(articles)} articles to re-analyze "
        f"(processed before {cutoff_date})"
    )

    if not articles:
        flow_logger.info("Nothing to re-analyze.")
        return {"reanalyzed": 0, "skipped": 0, "total": 0}

    success = 0
    skipped = 0

    # Process in small batches to avoid overwhelming Ollama
    for i in range(0, len(articles), batch_size):
        batch = articles[i : i + batch_size]
        results = [reanalyze_article(a, llm, db) for a in batch]
        batch_success = sum(1 for r in results if r)
        success += batch_success
        skipped += len(batch) - batch_success
        flow_logger.info(
            f"Progress: {success + skipped}/{len(articles)} "
            f"({success} updated, {skipped} skipped)"
        )

    flow_logger.info(f"Re-analysis complete: {success} updated, {skipped} skipped")
    return {"reanalyzed": success, "skipped": skipped, "total": len(articles)}
