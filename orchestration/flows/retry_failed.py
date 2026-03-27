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
def get_failed_articles(limit: int) -> list[str]:
    """Return URLs of articles currently in 'failed' status."""
    settings = load_settings()
    db = PostgresClient(settings.database_url)
    urls = db.get_article_urls_by_status(status="failed", limit=limit)
    logger.info("Found %d failed articles to retry", len(urls))
    return urls


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
def get_stale_pending_articles(limit: int, stale_after_minutes: int) -> list[str]:
    """Return URLs of articles stuck in 'pending' beyond the stale threshold."""
    settings = load_settings()
    db = PostgresClient(settings.database_url)
    urls = db.get_stale_pending_article_urls(limit=limit, stale_after_minutes=stale_after_minutes)
    logger.info("Found %d stale-pending articles (threshold: %dm)", len(urls), stale_after_minutes)
    return urls


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

    failed_urls = get_failed_articles(limit=limit)
    stale_urls = get_stale_pending_articles(limit=limit, stale_after_minutes=stale_after_minutes)
    failed_repo_urls = get_failed_repos(limit=limit)

    # Merge and deduplicate — articles first, then repos
    seen: set[str] = set()
    urls: list[str] = []
    for url in failed_urls + stale_urls + failed_repo_urls:
        if url not in seen:
            seen.add(url)
            urls.append(url)

    # Apply global cap — limit is total across all buckets, not per-bucket
    urls = urls[:limit]

    if not urls:
        run_logger.info("No failed or stale-pending articles/repos found — nothing to retry")
        return {"retried": 0, "submitted": 0, "failed_articles": len(failed_urls), "stale_pending": len(stale_urls), "failed_repos": len(failed_repo_urls)}

    run_logger.info(
        "Retrying %d total (%d failed articles, %d stale-pending, %d failed repos, delay=%.1fs)",
        len(urls), len(failed_urls), len(stale_urls), len(failed_repo_urls), delay_seconds,
    )

    submitted = 0
    for i, url in enumerate(urls):
        ok = submit_ingest(url=url, base_url=base_url)
        if ok:
            submitted += 1
        if i < len(urls) - 1:
            time.sleep(delay_seconds)

    run_logger.info("Submitted %d/%d for re-ingest", submitted, len(urls))

    # Notify on completion — summarises what was queued (not when each finishes,
    # since ingest subflows run independently after submission).
    try:
        from shared.secrets import load_telegram_credentials
        import requests as _requests
        creds = load_telegram_credentials()
        if creds:
            lines = [f"🔁 *retry-failed complete*", ""]
            if failed_urls:
                lines.append(f"• {len(failed_urls)} failed article(s)")
            if stale_urls:
                lines.append(f"• {len(stale_urls)} stale-pending article(s)")
            if failed_repo_urls:
                lines.append(f"• {len(failed_repo_urls)} failed repo(s)")
            lines += ["", f"Submitted {submitted}/{len(urls)} for re-ingest."]
            _requests.post(
                f"https://api.telegram.org/bot{creds.bot_token}/sendMessage",
                json={"chat_id": creds.chat_id, "text": "\n".join(lines), "parse_mode": "Markdown"},
                timeout=10,
            )
    except Exception as e:
        run_logger.warning("Telegram notification failed: %s", e)

    return {"retried": len(urls), "submitted": submitted, "failed_articles": len(failed_urls), "stale_pending": len(stale_urls), "failed_repos": len(failed_repo_urls)}


def main() -> None:
    retry_failed()


if __name__ == "__main__":
    main()
