"""Backfill embeddings for existing processed articles.

Run once manually after deploying pgvector:
  From Prefect UI: trigger 'backfill-embeddings' deployment
  Or: prefect deployment run backfill-embeddings/backfill-embeddings

Requires: ollama pull nomic-embed-text (run on host before executing)
"""
from __future__ import annotations

import logging
from typing import Any

from prefect import flow, task, get_run_logger

from integrations.ollama import OllamaClient
from data.postgres.client import PostgresClient
from shared.config import load_settings

logger = logging.getLogger(__name__)


@task(retries=0)
def embed_batch(articles: list[dict], db: PostgresClient, ollama: OllamaClient) -> int:
    task_logger = get_run_logger()
    count = 0
    for article in articles:
        try:
            title = article.get("title") or ""
            summary = article.get("summary") or ""
            text = f"{title} {summary}".strip()
            if not text:
                continue
            embedding = ollama.embed(text)
            if embedding:
                db.store_embedding(str(article["id"]), embedding)
                count += 1
        except Exception as e:
            task_logger.warning(f"Failed to embed {article.get('url')}: {e}")
    return count


@flow(name="backfill-embeddings")
def backfill_embeddings(batch_size: int = 50, limit: int | None = None) -> int:
    """Backfill embeddings for articles missing them.

    Args:
        batch_size: Articles per embed_batch task call.
        limit: Max articles to process (None = all). Use a small number to test.
    """
    flow_logger = get_run_logger()
    settings = load_settings()
    db = PostgresClient(settings.database_url)
    ollama = OllamaClient(settings.ollama_base_url)

    import sqlalchemy as sa
    from data.postgres.engine import get_session_factory
    from data.postgres.models import Article

    t = Article.__table__
    stmt = (
        sa.select(t.c.id, t.c.url, t.c.title, t.c.summary)
        .where(
            t.c.embedding.is_(None),
            t.c.status == "processed",
            t.c.summary.isnot(None),
        )
        .order_by(t.c.processed_at.desc())
    )
    if limit:
        stmt = stmt.limit(int(limit))

    session_factory = get_session_factory(settings.database_url)
    with session_factory() as session:
        articles = [dict(r) for r in session.execute(stmt).mappings().all()]

    flow_logger.info(f"Found {len(articles)} articles to backfill")

    total = 0
    for i in range(0, len(articles), batch_size):
        batch = articles[i : i + batch_size]
        count = embed_batch(batch, db, ollama)
        total += count
        flow_logger.info(f"Progress: {total}/{len(articles)} embeddings stored")

    flow_logger.info(f"Backfill complete: {total} embeddings stored")
    return total
