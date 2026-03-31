"""Backfill embeddings for existing processed articles and repos.

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
def embed_batch(items: list[dict], db: PostgresClient, ollama: OllamaClient, table: str) -> int:
    task_logger = get_run_logger()
    count = 0
    for item in items:
        try:
            title = item.get("title") or item.get("name") or ""
            summary = item.get("summary") or item.get("purpose") or item.get("description") or ""
            text = f"{title} {summary}".strip()
            if not text:
                continue
            embedding = ollama.embed(text)
            if embedding:
                if table == "articles":
                    db.store_embedding(str(item["id"]), embedding)
                else:
                    db.store_repo_embedding(str(item["id"]), embedding)
                count += 1
        except Exception as e:
            task_logger.warning(f"Failed to embed {item.get('url')}: {e}")
    return count


@flow(name="backfill-embeddings")
def backfill_embeddings(batch_size: int = 50, limit: int | None = None) -> dict:
    """Backfill embeddings for all content missing them (articles + repos).

    Args:
        batch_size: Items per embed_batch task call.
        limit: Max items to process per content type (None = all).
    """
    flow_logger = get_run_logger()
    settings = load_settings()
    db = PostgresClient(settings.database_url)
    ollama = OllamaClient(settings.ollama_base_url)

    import sqlalchemy as sa
    from data.postgres.engine import get_session_factory
    from data.postgres.models import Article, Repo

    session_factory = get_session_factory(settings.database_url)
    totals: dict[str, int] = {}

    # --- Articles ---
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

    with session_factory() as session:
        articles = [dict(r) for r in session.execute(stmt).mappings().all()]

    flow_logger.info(f"Found {len(articles)} articles to backfill")
    total_articles = 0
    for i in range(0, len(articles), batch_size):
        batch = articles[i : i + batch_size]
        total_articles += embed_batch(batch, db, ollama, "articles")
        flow_logger.info(f"Articles progress: {total_articles}/{len(articles)}")
    totals["articles"] = total_articles

    # --- Repos ---
    r = Repo.__table__
    repo_stmt = (
        sa.select(r.c.id, r.c.url, r.c.name, r.c.purpose, r.c.description)
        .where(
            r.c.embedding.is_(None),
            r.c.status == "processed",
            sa.or_(r.c.purpose.isnot(None), r.c.description.isnot(None)),
        )
        .order_by(r.c.updated_at.desc())
    )
    if limit:
        repo_stmt = repo_stmt.limit(int(limit))

    with session_factory() as session:
        repos = [dict(r) for r in session.execute(repo_stmt).mappings().all()]

    flow_logger.info(f"Found {len(repos)} repos to backfill")
    total_repos = 0
    for i in range(0, len(repos), batch_size):
        batch = repos[i : i + batch_size]
        total_repos += embed_batch(batch, db, ollama, "repos")
        flow_logger.info(f"Repos progress: {total_repos}/{len(repos)}")
    totals["repos"] = total_repos

    flow_logger.info(f"Backfill complete: {totals}")
    return totals
