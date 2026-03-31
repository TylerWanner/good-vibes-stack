"""Twitter posting flow.

Draws from recent second brain articles, drafts tweets via LLM, and posts
to Twitter. Optionally notifies via Telegram when a tweet goes out.

Prefect blocks required:
  twitter-credentials   — {api_key, api_secret, access_token, access_token_secret}

Optional:
  telegram-credentials  — {bot_token, chat_id} for post notifications
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from prefect import flow, get_run_logger, task

from second_brain.llm import LLMClient
from data.postgres.client import PostgresClient
from integrations.twitter import TwitterClient
from shared.config import load_settings


# ─── Tasks ───────────────────────────────────────────────────────────────────

@task
def fetch_source_articles(days: int = 7, limit: int = 20, min_pov: int = 4) -> list[dict[str, Any]]:
    """Pull recent high-quality articles from second brain as tweet fodder.

    Prefers articles with score_pov >= min_pov. Falls back to unscored recent
    articles if no scored articles are available (e.g. fresh DB).
    """
    settings = load_settings()
    db = PostgresClient(settings.database_url)

    # Primary: scored articles above the POV threshold
    articles = db.get_high_quality_articles(days=days, limit=limit, min_pov=min_pov)

    if not articles:
        # Fallback: unscored/recent — also catches fresh DBs before scoring runs
        articles = db.fetch_recent_processed(days=days, limit=limit)

    if not articles:
        raise RuntimeError(f"No processed articles found in the last {days} days")

    return articles


@task(retries=2, retry_delay_seconds=5)
def draft_tweets_from_articles(
    articles: list[dict[str, Any]],
    count: int = 3,
) -> list[dict[str, Any]]:
    """Use LLM to draft tweets from second brain articles."""
    from shared.secrets import load_anthropic_api_key
    settings = load_settings()
    llm = LLMClient(
        provider=settings.llm_provider,
        model=settings.llm_model,
        anthropic_api_key=load_anthropic_api_key(),
        ollama_base_url=settings.ollama_base_url,
    )
    drafts = llm.draft_tweets(articles=articles, count=count)
    if not drafts:
        raise RuntimeError("LLM returned no tweet drafts")
    return drafts


@task
def pick_tweet(drafts: list[dict[str, Any]]) -> dict[str, Any]:
    """Pick one tweet draft to post (random selection for now)."""
    return random.choice(drafts)


@task(retries=1, retry_delay_seconds=10)
def post_to_twitter(draft: dict[str, Any]) -> dict[str, Any]:
    """Post the selected tweet draft."""
    from shared.secrets import load_twitter_credentials
    
    creds = load_twitter_credentials()
    if not creds:
        raise RuntimeError(
            "Twitter credentials missing. Set up 'twitter-credentials' Prefect block."
        )
    client = TwitterClient(
        api_key=creds.api_key,
        api_secret=creds.api_secret,
        access_token=creds.access_token,
        access_token_secret=creds.access_token_secret,
    )
    text = draft["text"]
    result = client.post_tweet(text)
    return {**result, "source_url": draft.get("source_url", "")}


@task
def notify_tweet_posted(tweet: dict[str, Any]) -> bool:
    """Send a Telegram notification when a tweet is posted."""
    import os
    from shared.secrets import load_telegram_bot_token
    
    token = load_telegram_bot_token(bot="default")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return False

    tweet_id = tweet.get("id", "")
    tweet_url = f"https://twitter.com/user/status/{tweet_id}" if tweet_id else ""
    source = tweet.get("source_url", "")

    lines = ["🐦 *Tweet posted:*", "", tweet.get("text", "")]
    if tweet_url:
        lines += ["", f"[View tweet]({tweet_url})"]
    if source:
        lines += [f"[Source]({source})"]

    message = "\n".join(lines)
    response = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        },
        timeout=15,
    )
    response.raise_for_status()
    return True


# ─── Flows ───────────────────────────────────────────────────────────────────

@flow(name="post-tweet")
def post_tweet(
    days: int = 7,
    article_limit: int = 20,
    draft_count: int = 5,
    dry_run: bool = False,
    text: str | None = None,
) -> dict[str, Any]:
    """Draft and post a tweet from recent second brain content.

    If text is provided, skip drafting and post that text directly.
    Set dry_run=True to draft without posting — useful for testing.
    """
    logger = get_run_logger()

    if text:
        # Direct post — skip article fetching and LLM drafting
        logger.info("Direct post mode", extra={"text": text[:80]})
        selected = {"text": text, "source_url": ""}
    else:
        articles = fetch_source_articles(days=days, limit=article_limit)
        logger.info("Fetched source articles", extra={"count": len(articles)})

        drafts = draft_tweets_from_articles(articles=articles, count=draft_count)
        logger.info("Drafted tweets", extra={"count": len(drafts)})

        selected = pick_tweet(drafts=drafts)
        logger.info("Selected tweet", extra={"text": selected.get("text", "")[:80]})

    if dry_run:
        logger.info("dry_run=True — skipping post")
        return {
            "status": "dry_run",
            "draft": selected.get("text", ""),
            "source_url": selected.get("source_url", ""),
        }

    posted = post_to_twitter(draft=selected)
    logger.info("Posted tweet", extra={"tweet_id": posted.get("id")})

    notified = notify_tweet_posted(tweet=posted)

    return {
        "status": "posted",
        "tweet_id": posted.get("id"),
        "text": posted.get("text"),
        "source_url": posted.get("source_url"),
        "notified_telegram": bool(notified),
    }


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Post a tweet from second brain content")
    parser.add_argument("--days", type=int, default=7, help="Article window in days")
    parser.add_argument("--dry-run", action="store_true", help="Draft but don't post")
    args = parser.parse_args()

    if args.dry_run:
        from shared.secrets import load_anthropic_api_key
        settings = load_settings()
        db = PostgresClient(settings.database_url)
        articles = db.fetch_recent_processed(days=args.days, limit=20)
        llm = LLMClient(
            provider=settings.llm_provider,
            model=settings.llm_model,
            anthropic_api_key=load_anthropic_api_key(),
            ollama_base_url=settings.ollama_base_url,
        )
        drafts = llm.draft_tweets(articles=articles, count=5)
        print(json.dumps(drafts, indent=2))
    else:
        result = post_tweet(days=args.days)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
