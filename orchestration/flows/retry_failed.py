"""Retry failed and stale-pending article and repo ingests.

Queries the DB for:
  - articles with status='failed' OR stuck in 'pending' beyond the stale threshold
  - repos with status='failed'

Re-submits them all to POST /ingest (ingest-url deployment).
Driven entirely from DB state — not Prefect history.

Usage:
  From Prefect UI: trigger 'retry-failed' deployment
  With limit:      trigger with {"limit": 10}
  With threshold:  trigger with {"stale_after_minutes": 15}
"""
from __future__ import annotations

import time
import logging
import urllib.parse
import urllib.request
import json

from prefect import flow, task, get_run_logger

from data.postgres.client import PostgresClient
from shared.config import load_settings  # used inside tasks

logger = logging.getLogger(__name__)


@task
def get_failed_articles(limit: int) -> list[dict]:
    """Return failed articles with url, title, and failure count."""
    settings = load_settings()
    db = PostgresClient(settings.database_url)
    import sqlalchemy as sa
    from data.postgres.models import Article
    with db._session_factory() as session:
        rows = session.execute(
            sa.select(Article.url, Article.title, Article.failure_log)
            .where(Article.status == "failed")
            .order_by(Article.ingested_at.desc())
            .limit(limit)
        ).fetchall()
    results = [{"url": r[0], "title": r[1], "failure_count": len(r[2] or [])} for r in rows]
    logger.info("Found %d failed articles to retry", len(results))
    return results


@task
def get_failed_repos(limit: int) -> list[str]:
    """Return URLs of repos currently in 'failed' status."""
    settings = load_settings()
    db = PostgresClient(settings.database_url)
    import sqlalchemy as sa
    from data.postgres.models import Repo
    with db._session_factory() as session:
        rows = session.execute(
            sa.select(Repo.url)
            .where(Repo.status == "failed")
            .limit(limit)
        ).fetchall()
    urls = [r[0] for r in rows]
    logger.info("Found %d failed repos to retry", len(urls))
    return urls


@task
def get_stale_pending_articles(limit: int, stale_after_minutes: int) -> list[dict]:
    """Return stale-pending articles with url and title."""
    settings = load_settings()
    db = PostgresClient(settings.database_url)
    import sqlalchemy as sa
    from datetime import datetime, timezone, timedelta
    from data.postgres.models import Article
    threshold = datetime.now(timezone.utc) - timedelta(minutes=stale_after_minutes)
    with db._session_factory() as session:
        rows = session.execute(
            sa.select(Article.url, Article.title)
            .where(Article.status == "pending")
            .where(Article.ingested_at < threshold)
            .limit(limit)
        ).fetchall()
    results = [{"url": r[0], "title": r[1], "failure_count": 0} for r in rows]
    logger.info("Found %d stale-pending articles (threshold: %dm)", len(results), stale_after_minutes)
    return results


@task
def submit_ingest(url: str, base_url: str) -> bool:
    """Submit a single URL to POST /ingest with force=True."""
    try:
        body = json.dumps({"url": url, "force": True}).encode()
        req = urllib.request.Request(
            f"{base_url}/ingest",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            d = json.loads(resp.read())
            ingest_id = (d.get("ingest_id") or "?")[:8]
            logger.info("Queued %s → ingest %s", url, ingest_id)
            return True
    except Exception as exc:
        logger.warning("Failed to submit %s: %s", url, exc)
        return False


@flow(name="retry-failed", log_prints=True)
def retry_failed(limit: int = 50, delay_seconds: float = 1.5, stale_after_minutes: int = 30) -> dict:
    """Retry articles with status='failed' or stuck in 'pending' beyond the stale threshold.

    Args:
        limit: Maximum number of articles to retry per status in one run.
        delay_seconds: Seconds to wait between submissions (avoids burst 500s).
        stale_after_minutes: Articles pending longer than this are treated as stuck and re-queued.
    """
    run_logger = get_run_logger()
    settings = load_settings()
    base_url = settings.nervous_system_api_url.rstrip("/")

    failed_articles = get_failed_articles(limit=limit)
    stale_articles = get_stale_pending_articles(limit=limit, stale_after_minutes=stale_after_minutes)
    failed_repo_urls = get_failed_repos(limit=limit)

    # Merge and deduplicate — articles first, then repos
    seen: set[str] = set()
    items: list[dict] = []
    for item in failed_articles + stale_articles:
        if item["url"] not in seen:
            seen.add(item["url"])
            items.append(item)
    repo_items = [{"url": u, "title": None, "failure_count": 0} for u in failed_repo_urls if u not in seen]
    for item in repo_items:
        seen.add(item["url"])
    all_items = (items + repo_items)[:limit]

    if not all_items:
        run_logger.info("No failed or stale-pending articles/repos found — nothing to retry")
        return {"retried": 0, "submitted": 0, "failed_articles": len(failed_articles), "stale_pending": len(stale_articles), "failed_repos": len(failed_repo_urls)}

    run_logger.info(
        "Retrying %d total (%d failed articles, %d stale-pending, %d failed repos, delay=%.1fs)",
        len(all_items), len(failed_articles), len(stale_articles), len(failed_repo_urls), delay_seconds,
    )

    submitted = 0
    for i, item in enumerate(all_items):
        ok = submit_ingest(url=item["url"], base_url=base_url)
        if ok:
            submitted += 1
        if i < len(all_items) - 1:
            time.sleep(delay_seconds)

    run_logger.info("Submitted %d/%d for re-ingest", submitted, len(all_items))

    # Notify on completion — list each article with title and failure count
    try:
        import os
        from shared.secrets import load_telegram_bot_token
        import requests as _requests
        token = load_telegram_bot_token(bot="default")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if token and chat_id:
            lines = [f"🔁 *retry-failed* — {submitted}/{len(all_items)} queued"]
            if failed_articles:
                lines.append(f"\n*Failed ({len(failed_articles)}):*")
                for item in failed_articles[:limit]:
                    label = item.get("title") or item["url"].split("/")[-1][:50]
                    fc = item["failure_count"]
                    suffix = f" ×{fc}" if fc > 0 else ""
                    lines.append(f"• {label}{suffix}")
            if stale_articles:
                lines.append(f"\n*Stale-pending ({len(stale_articles)}):*")
                for item in stale_articles[:limit]:
                    label = item.get("title") or item["url"].split("/")[-1][:50]
                    lines.append(f"• {label}")
            if failed_repo_urls:
                lines.append(f"\n*Failed repos ({len(failed_repo_urls)}):*")
                for url in failed_repo_urls[:limit]:
                    lines.append(f"• {url.split('github.com/')[-1][:50]}")
            _requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": "\n".join(lines), "parse_mode": "Markdown"},
                timeout=10,
            )
    except Exception as e:
        run_logger.warning("Telegram notification failed: %s", e)

    return {"retried": len(all_items), "submitted": submitted, "failed_articles": len(failed_articles), "stale_pending": len(stale_articles), "failed_repos": len(failed_repo_urls)}


def main() -> None:
    retry_failed()


if __name__ == "__main__":
    main()
