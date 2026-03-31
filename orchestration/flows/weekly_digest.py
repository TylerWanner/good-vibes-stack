from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import requests
from prefect import flow, get_run_logger, task

from second_brain.llm import LLMClient
from data.postgres.client import PostgresClient
from shared.config import load_settings


@task
def fetch_recent_articles(days: int) -> list[dict]:
    settings = load_settings()
    db = PostgresClient(settings.database_url)
    return db.fetch_recent_processed(days=days, limit=settings.weekly_digest_article_limit)


@task(retries=2, retry_delay_seconds=3)
def build_digest(articles: list[dict]) -> dict:
    from shared.secrets import load_anthropic_api_key
    settings = load_settings()
    llm = LLMClient(
        provider=settings.llm_provider,
        model=settings.llm_model,
        anthropic_api_key=load_anthropic_api_key(),
        ollama_base_url=settings.ollama_base_url,
    )
    return llm.create_weekly_digest(articles)


@task
def persist_digest(days: int, digest: str, article_count: int) -> None:
    settings = load_settings()
    db = PostgresClient(settings.database_url)
    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=days)
    db.store_digest(
        period_start=period_start,
        period_end=period_end,
        content=digest,
        article_count=article_count,
    )


@task
def notify_telegram_if_configured(digest: str) -> bool:
    from integrations.telegram import notify_telegram
    return notify_telegram(digest)


@flow(name="weekly-digest")
def weekly_digest(days: int = 7) -> dict:
    logger = get_run_logger()
    articles = fetch_recent_articles(days=days)
    if not articles:
        logger.info("No processed articles found for digest window", extra={"days": days})
        return {"status": "processed", "article_count": 0, "digest": "No new items this week."}

    digest_result = build_digest(articles=articles)
    digest_text = digest_result.get("digest", "").strip()
    if not digest_text:
        raise RuntimeError("Digest generation returned empty output")

    persist_digest(days=days, digest=digest_text, article_count=len(articles))
    notified = notify_telegram_if_configured(digest=digest_text)

    return {
        "status": "processed",
        "article_count": len(articles),
        "digest": digest_text,
        "themes": digest_result.get("themes", []),
        "notified_telegram": bool(notified),
    }


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run weekly-digest flow locally")
    parser.add_argument("days", type=int, nargs="?", default=7, help="Digest window in days")
    args = parser.parse_args()
    result = weekly_digest(days=args.days)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
