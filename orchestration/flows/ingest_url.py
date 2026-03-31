from __future__ import annotations

import json
import logging
import re
import time
from typing import Any, Callable

from prefect import flow, get_run_logger, task

logger = logging.getLogger(__name__)
from shared.telemetry import setup_tracing

setup_tracing()

from integrations.chatgpt_share import fetch_chatgpt_share_document, is_chatgpt_share_url
from integrations.telegram import send_telegram_message

from integrations.github import fetch_github_repo_document, is_github_repo_url
from second_brain.llm import LLMClient
from data.postgres.client import PostgresClient
from integrations.reddit import fetch_reddit_document, is_reddit_url
from second_brain.acquisition.scrapling import ScraplingClient
from integrations.youtube import fetch_youtube_document, is_youtube_url
from shared.config import load_settings
from orchestration.flows.ingest_github_repo import (
    parse_owner_name,
    fetch_repo_metadata_task,
    fetch_repo_readme_task,
    fetch_repo_releases_task,
    fetch_repo_changelog_task,
    fetch_repo_tree_task,
    analyze_repo_task,
    store_repo_task,
)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _extract_text(doc: dict[str, Any]) -> str:
    for key in ("text", "content", "full_text", "html", "html_content", "summary", "transcript"):
        value = doc.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


from second_brain.classify import detect_source_type as _detect_source_type


def _detect_fetcher_mode(url: str) -> str:
    """Use stealthy mode for sites with aggressive anti-bot protection."""
    if "twitter.com" in url or "x.com" in url:
        return "stealthy"
    return "dynamic"


def _is_twitter_url(url: str) -> bool:
    return "twitter.com" in url or "x.com" in url


def _extract_tweet_id(url: str) -> str | None:
    match = re.search(r"/status/(\d+)", url)
    return match.group(1) if match else None


def _fetch_fxtwitter(tweet_id: str) -> dict[str, Any] | None:
    """Fetch tweet data from fxtwitter API. Returns tweet dict or None on failure."""
    import json as json_lib
    import urllib.request

    api_url = f"https://api.fxtwitter.com/status/{tweet_id}"
    try:
        req = urllib.request.Request(api_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json_lib.loads(resp.read())
            if data.get("code") == 200:
                return data.get("tweet")
    except Exception:
        pass  # Network/parse errors — caller handles None gracefully
    return None


def _extract_fxtwitter_text(tweet: dict[str, Any]) -> str:
    """Extract readable text from a fxtwitter tweet object (handles articles and normal tweets)."""
    article = tweet.get("article")
    if article:
        # Twitter Article — extract from content blocks
        blocks = article.get("content", {}).get("blocks", [])
        parts = [b.get("text", "") for b in blocks if b.get("text", "").strip()]
        text = "\n\n".join(parts)
        if not text:
            text = article.get("preview_text", "")
        return text
    # Normal tweet
    return tweet.get("raw_text", {}).get("text", "") or tweet.get("text", "")


def _extract_quote_urls(tweets: list[dict[str, Any]]) -> list[str]:
    """Extract quoted tweet URLs from a list of fxtwitter tweet objects."""
    urls = []
    for tweet in tweets:
        quote = tweet.get("quote")
        if quote:
            # Prefer the url field directly (handles Twitter Articles where id_str is None)
            url = quote.get("url")
            if not url:
                author = quote.get("author", {}).get("screen_name", "")
                qid = quote.get("id_str") or str(quote.get("id", ""))
                if author and qid:
                    url = f"https://x.com/{author}/status/{qid}"
            if url:
                urls.append(url)
    return urls


def _extract_body_urls(tweets: list[dict[str, Any]]) -> list[str]:
    """Extract URLs embedded in tweet body text (not captured by fxtwitter entities).

    fxtwitter often omits t.co-resolved URLs from entities but includes them raw in text.
    We extract http/https URLs directly from the text body, resolving t.co shortlinks.
    """
    import re
    url_pattern = re.compile(r'https?://[^\s\]>)"\']+')
    seen: set[str] = set()
    urls: list[str] = []
    # Only scan the main thread tweets — quoted tweet links belong to the quoted tweet's own ingest
    for text in [tweet.get("text", "") or "" for tweet in tweets]:
        for match in url_pattern.findall(text):
            # Strip trailing punctuation that got swept up
            match = match.rstrip(".,;:!?)")
            # Resolve t.co shortlinks to their final destination
            if "t.co/" in match:
                match = _resolve_redirect(match)
            if match not in seen:
                seen.add(match)
                urls.append(match)
    return urls


def _fetch_fxtwitter_thread(tweet_id: str, max_hops: int = 20) -> list[dict[str, Any]]:
    """Fetch a tweet and follow self-reply chain backwards to reconstruct thread order.

    Returns tweets in chronological order (oldest first).
    """
    tweets: list[dict[str, Any]] = []
    seen: set[str] = set()
    current_id = tweet_id

    while current_id and len(tweets) < max_hops:
        if current_id in seen:
            break
        seen.add(current_id)
        tweet = _fetch_fxtwitter(current_id)
        if not tweet:
            break
        tweets.append(tweet)
        # Follow chain only if author is replying to themselves
        replying_to_id = tweet.get("replying_to_status")
        replying_to_user = tweet.get("replying_to")
        author = tweet.get("author", {}).get("screen_name", "")
        if replying_to_id and replying_to_user and replying_to_user.lower() == author.lower():
            current_id = replying_to_id
        else:
            break

    tweets.reverse()  # chronological order
    return tweets


def _sanitize_text(text: str) -> str:
    """Strip dangerous HTML tags and prompt injection patterns.

    Applied only to Scrapling-fetched content before passing to the LLM.
    Removes <script>, <style> blocks, and common prompt injection phrases.
    """
    # Remove <script>...</script> blocks (including content)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.IGNORECASE | re.DOTALL)
    # Remove <style>...</style> blocks (including content)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.IGNORECASE | re.DOTALL)
    # Strip prompt injection patterns (line-level, case-insensitive where appropriate)
    injection_patterns = [
        r"ignore previous instructions[^\n]*",
        r"you are now[^\n]*",
        r"system:[^\n]*",
        r"###SYSTEM[^\n]*",
    ]
    for pattern in injection_patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE | re.MULTILINE)
    return text


from shared.url_utils import strip_tracking_params


def _normalize_tags(tags: Any) -> list[str]:
    """Normalize LLM-returned tags to a flat list of strings.

    Ollama sometimes returns tags as a list, sometimes as a dict, sometimes a string.
    Postgres TEXT[] requires a proper Python list — psycopg3 handles serialization.
    """
    if not tags:
        return []
    if isinstance(tags, list):
        return [str(t).strip() for t in tags if t]
    if isinstance(tags, dict):
        # Flatten dict values (e.g. {"categories": ["a", "b"]})
        result: list[str] = []
        for v in tags.values():
            if isinstance(v, list):
                result.extend(str(x).strip() for x in v if x)
            elif v:
                result.append(str(v).strip())
        return result
    if isinstance(tags, str) and tags.strip():
        return [tags.strip()]
    return []


def _is_allowed_url(url: str, domain_allowlist: frozenset[str]) -> bool:
    """Return True if URL passes domain allowlist. Empty allowlist = block all (fail safe)."""
    if not domain_allowlist:
        return False  # fail-safe: empty allowlist blocks everything
    from urllib.parse import urlparse
    try:
        # urlparse requires a scheme to extract hostname correctly.
        # Bare domains like "github.com" parse as path, not host — prepend https:// if needed.
        normalized = url if "://" in url else f"https://{url}"
        host = urlparse(normalized).hostname or ""
        host = host.lower().lstrip("www.")
        return host in domain_allowlist
    except Exception:
        return False  # Malformed URL — fail closed


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(retries=2, retry_delay_seconds=5, tags=["scrapling"])
def fetch_with_scrapling(url: str) -> dict[str, Any]:
    """Fetch any URL via the Scrapling sidecar (Playwright-backed, handles JS rendering)."""
    settings = load_settings()
    client = ScraplingClient(settings.scrapling_fetcher_url)
    result = client.fetch(url, fetcher=_detect_fetcher_mode(url))
    doc: dict[str, Any] = {
        "title": result.get("title", ""),
        "text": result.get("text", ""),
        "source_type": _detect_source_type(url),
        "url": url,
    }
    if result.get("publish_date"):
        doc["content_date"] = result["publish_date"]
    return doc


@task
def mark_pending(url: str) -> dict[str, Any] | None:
    """Check for an existing article and write a PENDING record if none exists.

    Returns the existing article dict if already in DB, or None if a new PENDING
    record was created and ingest should proceed.

    Combines the duplicate check and pending write into a single atomic DB call.
    """
    settings = load_settings()
    db = PostgresClient(settings.database_url)
    return db.mark_article_pending(url=url)


@task(retries=2, retry_delay_seconds=5)
def fetch_fxtwitter(url: str) -> dict[str, Any]:
    """Fetch a Twitter/X URL via fxtwitter API only.

    Raises RuntimeError if fxtwitter returns no content — this task is intentionally
    allowed to FAIL visibly in Prefect so fxtwitter failures are observable.
    The orchestrator will fall back to Scrapling if this task fails.
    """
    tweet_id = _extract_tweet_id(url)

    if tweet_id:
        tweets = _fetch_fxtwitter_thread(tweet_id)
        if tweets:
            first = tweets[0]
            if len(tweets) == 1:
                text = _extract_fxtwitter_text(first)
            else:
                parts = [_extract_fxtwitter_text(t) for t in tweets]
                parts = [p for p in parts if p.strip()]
                text = "\n\n---\n\n".join(parts)

            article = first.get("article")
            title = article.get("title", "") if article else ""
            if not title:
                author = first.get("author", {}).get("name", "")
                title = f"{author} on X" if author else ""

            if text.strip():
                return {
                    "title": title,
                    "text": text,
                    "source_type": "tweet",
                    "url": url,
                    "content_date": first.get("created_at"),  # e.g. "Mon Mar 23 12:27:38 +0000 2026"
                    "quote_urls": _extract_quote_urls(tweets),
                    "body_urls": _extract_body_urls(tweets),
                }

    raise RuntimeError(f"fxtwitter returned no content for {url}")


@task(retries=2, retry_delay_seconds=3)
def fetch_chatgpt_share(url: str) -> dict[str, Any]:
    settings = load_settings()
    return fetch_chatgpt_share_document(url, scrapling_fetcher_url=settings.scrapling_fetcher_url)


@task(retries=2, retry_delay_seconds=5)
def fetch_youtube(url: str) -> dict[str, Any]:
    return fetch_youtube_document(url)


@task(retries=2, retry_delay_seconds=3)
def fetch_reddit(url: str) -> dict[str, Any]:
    return fetch_reddit_document(url)


@task(retries=2, retry_delay_seconds=3)
def fetch_github(url: str) -> dict[str, Any]:
    return fetch_github_repo_document(url)


@task
def check_ollama_health(llm_provider: str | None = None) -> None:
    """Ping Ollama with a trivial inference call before attempting analysis.

    Fails fast (15s timeout) if Ollama is hung or unresponsive, rather than
    letting the full analysis task burn 2-3 minutes before timing out.
    Only runs when LLM provider is 'ollama'.
    """
    import requests
    from shared.config import load_settings
    settings = load_settings()
    effective_provider = llm_provider or settings.llm_provider
    if effective_provider != "ollama":
        return
    logger = get_run_logger()
    try:
        # Check reachability via /api/tags — instant, no inference, no contention.
        # We don't do a test generate here because that would add to Ollama queue pressure
        # when multiple flows start simultaneously.
        resp = requests.get(f"{settings.ollama_base_url}/api/tags", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        model_names = [m["name"] for m in data.get("models", [])]
        # Normalize: check with and without tag suffix (e.g. "qwen2.5:14b" vs "qwen2.5:14b:latest")
        model = settings.llm_model
        model_base = model.split(":")[0]
        if not any(m == model or m.startswith(model_base + ":") for m in model_names):
            raise RuntimeError(f"Model '{model}' not found in Ollama. Available: {model_names}")
        logger.info("Ollama health check passed — model '%s' available", model)
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(
            f"Ollama unreachable: {e}\n"
            "Restart Ollama on the host: `systemctl restart ollama` or `pkill ollama && ollama serve`"
        ) from e


@task(retries=2, retry_delay_seconds=3)
def analyze_document(url: str, doc: dict[str, Any], llm_provider: str | None = None, llm_model: str | None = None) -> dict[str, Any]:
    logger = get_run_logger()
    from shared.secrets import load_anthropic_api_key, load_anthropic_auth_token
    settings = load_settings()
    effective_provider = llm_provider or settings.llm_provider
    effective_model = llm_model or settings.llm_model
    if llm_provider or llm_model:
        logger.info("analyze_document: using override provider=%s model=%s for %s", effective_provider, effective_model, url)
    # Resolve Anthropic credentials: API key first, OAuth token as fallback
    anthropic_api_key = load_anthropic_api_key()
    anthropic_auth_token = None
    if effective_provider == "anthropic" and not anthropic_api_key:
        anthropic_auth_token = load_anthropic_auth_token()
        if anthropic_auth_token:
            logger.info("analyze_document: no API key found, falling back to Claude Max OAuth token")
        else:
            logger.warning("analyze_document: no Anthropic API key or OAuth token available")
    llm = LLMClient(
        provider=effective_provider,
        model=effective_model,
        anthropic_api_key=anthropic_api_key,
        anthropic_auth_token=anthropic_auth_token,
        ollama_base_url=settings.ollama_base_url,
        embedding_provider=settings.embedding_provider,
        embedding_model=settings.embedding_model,
    )
    title = doc.get("title") or ""
    content = _extract_text(doc)
    # Route long content through map-reduce (>16k chars → chunk + merge)
    LONG_CONTENT_THRESHOLD = 16000
    from shared.config import llm_concurrency
    # Update slot holder variable so it's queryable via GET /ops/ollama-slot
    try:
        import urllib.request as _ur
        from prefect.runtime import flow_run as _fr
        _slot_info = json.dumps({"url": url, "flow_run_id": str(_fr.id), "flow_run_name": str(_fr.name)})
        _raw_base = settings.prefect_api_url.rstrip("/")
        _base = _raw_base[:-4] if _raw_base.endswith("/api") else _raw_base
        _var_name = "ollama-slot-holder"
        _var_id = None
        try:
            _get_resp = _ur.urlopen(_ur.Request(f"{_base}/api/variables/name/{_var_name}"), timeout=3)
            _var_id = json.loads(_get_resp.read()).get("id")
        except Exception:
            pass
        if _var_id:
            _req = _ur.Request(f"{_base}/api/variables/{_var_id}", data=json.dumps({"value": _slot_info}).encode(), headers={"Content-Type": "application/json"}, method="PATCH")
        else:
            _req = _ur.Request(f"{_base}/api/variables/", data=json.dumps({"name": _var_name, "value": _slot_info}).encode(), headers={"Content-Type": "application/json"}, method="POST")
        _ur.urlopen(_req, timeout=3)
    except Exception as _e:
        logger.warning("Could not update ollama-slot-holder variable: %s", _e)

    if len(content) > LONG_CONTENT_THRESHOLD:
        logger.info("analyze_document: long content (%d chars), using map-reduce with per-chunk concurrency", len(content))
        import hashlib as _hashlib
        from prefect.variables import Variable as _Variable

        _chunk_var = "ingest-chunk-" + _hashlib.md5(url.encode()).hexdigest()[:12]

        # Check for existing progress to resume from
        _resume_from_chunk = 0
        _resume_running_summary: str | None = None
        try:
            _existing = _Variable.get(_chunk_var, default=None)
            if _existing:
                _progress = json.loads(_existing)
                _completed = _progress.get("completed_chunks", 0)
                if _completed > 0:
                    _resume_from_chunk = _completed
                    _resume_running_summary = _progress.get("running_summary")
                    logger.info("analyze_document: found existing progress %d chunks, resuming", _completed)
        except Exception as _e:
            logger.debug("failed to check existing chunk progress: %s", _e)

        _on_chunk_progress: Callable[[int, int, str], None]
        def _on_chunk_progress(completed: int, total: int, running_summary: str) -> None:
            try:
                payload = json.dumps({
                    "url": url,
                    "completed_chunks": completed,
                    "total_chunks": total,
                    "status": "in_progress",
                    "running_summary": running_summary,
                })
                _Variable.set(_chunk_var, payload, overwrite=True)
            except Exception as _e:
                logger.debug("chunk progress variable update failed: %s", _e)

        _on_complete: Callable[[], None]
        def _on_complete() -> None:
            try:
                _Variable.unset(_chunk_var)
            except Exception as _e:
                logger.debug("chunk variable cleanup failed: %s", _e)

        result = llm.summarize_long_content(
            url=url, title=title, content=content,
            on_chunk_progress=_on_chunk_progress,
            on_complete=_on_complete,
            concurrency_ctx=llm_concurrency,
            resume_from_chunk=_resume_from_chunk,
            resume_running_summary=_resume_running_summary,
        )
    else:
        with llm_concurrency():
            logger.info("analyze_document: acquired LLM slot for %s (%d chars)", url, len(content))
            result = llm.summarize_and_tag(url=url, title=title, content=content)
    logger.info("analyze_document result: summary_len=%d tags=%s scores=%s/%s/%s content_len=%d",
        len(result.get("summary") or ""),
        result.get("tags"),
        result.get("score_usefulness"), result.get("score_interest"), result.get("score_pov"),
        len(content),
    )
    # Log unexpected types so we can see raw LLM output when fields misbehave
    bad_fields = {k: type(v).__name__ for k, v in result.items()
                  if k in ("summary", "source_type") and not isinstance(v, str)}
    bad_fields.update({k: type(v).__name__ for k, v in result.items()
                       if k == "tags" and not isinstance(v, (list, type(None)))})
    if bad_fields:
        logger.warning("unexpected LLM field types: %s | raw result: %s", bad_fields, result)
    result["title"] = title
    result["raw_text"] = content
    # Preserve source_type from the fetch step if set
    if doc.get("source_type"):
        result["source_type"] = doc["source_type"]
    # Preserve has_transcript flag from YouTube fetcher
    if "has_transcript" in doc:
        result["has_transcript"] = doc["has_transcript"]
    # Preserve content_date from the fetch step (e.g. tweet created_at)
    if doc.get("content_date"):
        result["content_date"] = doc["content_date"]
    return result


@task
def store_article(url: str, analysis: dict[str, Any]) -> None:
    import logging
    logger = logging.getLogger(__name__)
    settings = load_settings()
    db = PostgresClient(settings.database_url)
    # Coerce all string fields — Ollama occasionally returns dicts/lists for string fields
    # when it misbehaves. str() ensures psycopg never sees a non-scalar where it expects one.
    db.upsert_article(
        url=url,
        readwise_id=None,
        title=str(analysis.get("title") or ""),
        summary=str(analysis.get("summary") or ""),
        tags=_normalize_tags(analysis.get("tags")),
        source_type=str(analysis.get("source_type") or "other"),
        raw_text=str(analysis.get("raw_text") or ""),
        score_usefulness=analysis.get("score_usefulness"),
        score_interest=analysis.get("score_interest"),
        score_pov=analysis.get("score_pov"),
        score_uniqueness=analysis.get("score_uniqueness"),
        content_date=analysis.get("content_date"),
    )
    # Generate and store embedding
    try:
        from integrations.ollama import OllamaClient
        title = analysis.get("title", "") or ""
        summary = analysis.get("summary", "") or ""
        embed_text = f"{title} {summary}".strip()
        if embed_text:
            ollama = OllamaClient(settings.ollama_base_url)
            embedding = ollama.embed(embed_text)
            if embedding:
                article_id = db.get_article_id_by_url(url)
                if article_id:
                    db.store_embedding(article_id, embedding)
                    logger.info(f"Stored embedding for {url}")
    except Exception as e:
        logger.warning(f"Failed to store embedding for {url}: {e}")


def _resolve_redirect(url: str, timeout: int = 5) -> str:
    """Follow redirects and return the final URL. Returns original URL on failure.

    Used to resolve URL shorteners (t.co, bit.ly, etc.) before allowlist check.
    """
    try:
        import urllib.request
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}, method="HEAD")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.url
    except Exception:
        try:
            # Fallback: GET request (some shorteners don't support HEAD)
            import urllib.request
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.url
        except Exception:
            return url  # Redirect failed — use original URL


# Domains that are pure URL shorteners — always resolve before allowlist check
_URL_SHORTENER_DOMAINS = frozenset(["t.co", "bit.ly", "tinyurl.com", "ow.ly", "buff.ly", "short.link"])

# URL path patterns that are never worth ingesting
_SKIP_PATH_PATTERNS = re.compile(r"/photo/\d+$|/video/\d+$|/analytics$")


def queue_found_links(
    source_url: str,
    found_links: list[str],
    domain_allowlist: frozenset[str],
    notify: dict[str, Any] | None = None,
) -> dict[str, list[str]]:
    """Filter found_links and kick off ingest for each new URL. Depth=1 only (no recursion).

    URL shorteners (t.co, bit.ly, etc.) are resolved to their final destination
    before the allowlist check so real content isn't blocked by the shortener domain.

    Returns dict with keys:
    - queued: links that were auto-ingested
    - blocked: links blocked by domain allowlist
    - already_processed: links already in the DB
    """
    logger = get_run_logger()
    if not found_links:
        return {"queued": [], "blocked": [], "already_processed": []}

    settings = load_settings()
    db = PostgresClient(settings.database_url)

    queued = []
    blocked = []
    already_processed = []

    for link in found_links:
        if link == source_url:
            continue
        # Skip media attachments and other non-content URLs
        from urllib.parse import urlparse
        parsed = urlparse(link)
        if _SKIP_PATH_PATTERNS.search(parsed.path):
            logger.debug("skipping non-content URL", extra={"url": link})
            continue
        # Resolve URL shorteners before allowlist check
        if parsed.netloc.lstrip("www.") in _URL_SHORTENER_DOMAINS:
            resolved = _resolve_redirect(link)
            if resolved != link:
                logger.info("resolved shortener", extra={"original": link, "resolved": resolved})
            link = resolved
        if not _is_allowed_url(link, domain_allowlist):
            from urllib.parse import urlparse as _urlparse
            _host = (_urlparse(link).hostname or "").lower().lstrip("www.")
            logger.info(f"found_link blocked by allowlist | url={link} | host={_host} | host_in_list={_host in domain_allowlist} | allowlist_size={len(domain_allowlist)}")
            blocked.append(link)
            continue
        existing = db.get_article_by_url(url=link)
        if existing and existing.get("status") == "processed":
            logger.debug("found_link already processed", extra={"url": link})
            already_processed.append(link)
            continue
        # Queue as independent Prefect flow run via REST API (fire-and-forget, no blocking subflow)
        try:
            from orchestration.prefect.client import trigger_deployment
            trigger_deployment(
                "ingest-url", "ingest-url",
                parameters={"url": link, "force": False, "auto_follow": False, "notify": notify or {}},
            )
            queued.append(link)
            logger.info("auto-followed found_link", extra={"url": link, "source": source_url})
        except Exception as exc:
            logger.warning("found_link queue failed | url=%s | error=%s", link, str(exc))

    return {"queued": queued, "blocked": blocked, "already_processed": already_processed}


def _notify_target(notify: dict[str, Any] | None) -> dict[str, Any]:
    import os
    from shared.secrets import load_telegram_bot_token
    
    notify = notify or {}
    bot = notify.get("bot", "default")
    
    # Load bot token from block
    bot_token = load_telegram_bot_token(bot=bot)
    
    # chat_id from notify dict or env var fallback
    chat_id = notify.get("chat_id") or os.getenv("TELEGRAM_CHAT_ID")

    return {
        "bot": bot,
        "channel": notify.get("channel") or "telegram",
        "account_id": notify.get("account_id") or "default",
        "chat_id": chat_id,
        "reply_to_message_id": notify.get("reply_to_message_id"),
        "telegram_bot_token": bot_token,
    }


def _send_ingest_notification(notify: dict[str, Any] | None, text: str, buttons: list[list[dict[str, str]]] | None = None) -> None:
    target = _notify_target(notify)
    if target.get("channel") != "telegram":
        return
    if target.get("telegram_bot_token") and target.get("chat_id"):
        send_telegram_message(
            target["telegram_bot_token"],
            target["chat_id"],
            text,
            buttons=buttons,
            reply_to_message_id=target.get("reply_to_message_id"),
        )


# ---------------------------------------------------------------------------
# Typed subflows — one per source type
# ---------------------------------------------------------------------------

@flow(name="ingest-github-subflow")
def ingest_github_subflow(url: str) -> dict[str, Any]:
    """Fetch, analyze, and store a GitHub repository. Returns the repo analysis dict."""
    metadata = fetch_repo_metadata_task(url=url)
    default_branch = metadata.get("default_branch", "main")
    readme = fetch_repo_readme_task(url=url, default_branch=default_branch)
    releases = fetch_repo_releases_task(url=url)
    changelog = fetch_repo_changelog_task(url=url, default_branch=default_branch)
    analysis = analyze_repo_task(
        metadata=metadata,
        readme=readme,
        releases=releases,
        changelog=changelog,
        tree=[],
    )
    store_repo_task(url=url, metadata=metadata, analysis=analysis, releases=releases)
    return analysis


@flow(name="ingest-youtube-subflow")
def ingest_youtube_subflow(url: str, llm_provider: str | None = None, llm_model: str | None = None) -> dict[str, Any]:
    """Fetch, analyze, and store a YouTube video. Returns the LLM analysis dict."""
    doc = fetch_youtube(url=url)
    analysis = analyze_document(url=url, doc=doc, llm_provider=llm_provider, llm_model=llm_model)
    store_article(url=url, analysis=analysis)
    return analysis


@flow(name="ingest-reddit-subflow")
def ingest_reddit_subflow(url: str, llm_provider: str | None = None, llm_model: str | None = None) -> dict[str, Any]:
    """Fetch, analyze, and store a Reddit post/thread. Returns the LLM analysis dict."""
    doc = fetch_reddit(url=url)
    analysis = analyze_document(url=url, doc=doc, llm_provider=llm_provider, llm_model=llm_model)
    store_article(url=url, analysis=analysis)
    return analysis


@flow(name="ingest-chatgpt-subflow")
def ingest_chatgpt_subflow(url: str, llm_provider: str | None = None, llm_model: str | None = None) -> dict[str, Any]:
    """Fetch, analyze, and store a ChatGPT share URL. Returns the LLM analysis dict."""
    doc = fetch_chatgpt_share(url=url)
    analysis = analyze_document(url=url, doc=doc, llm_provider=llm_provider, llm_model=llm_model)
    store_article(url=url, analysis=analysis)
    return analysis


@flow(name="ingest-tweet-subflow")
def ingest_tweet_subflow(url: str, auto_follow: bool = True, llm_provider: str | None = None, llm_model: str | None = None) -> dict[str, Any]:
    """Fetch via fxtwitter, analyze, and store a tweet/thread.

    Raises if fxtwitter returns no content — the orchestrator catches this and
    falls back to ingest_article_subflow (Scrapling path).

    Quoted tweets are queued as separate async ingests (not subflows) when auto_follow=True.
    Returns analysis dict with quote_follow_result merged in for notification visibility.
    """
    check_ollama_health(llm_provider=llm_provider)
    doc = fetch_fxtwitter(url=url)  # raises RuntimeError on failure (after retries)
    analysis = analyze_document(url=url, doc=doc, llm_provider=llm_provider, llm_model=llm_model)
    store_article(url=url, analysis=analysis)

    # Queue quoted tweets + body URLs as independent ingests
    quote_follow: dict[str, list[str]] = {"queued": [], "blocked": [], "already_processed": []}
    if auto_follow:
        quote_urls = doc.get("quote_urls", [])
        body_urls = doc.get("body_urls", [])
        found_links = list(dict.fromkeys(quote_urls + body_urls))  # dedup, preserve order
        if found_links:
            settings = load_settings()
            domain_allowlist = (
                settings.domain_allowlist
                if isinstance(settings.domain_allowlist, frozenset)
                else frozenset(d.strip() for d in settings.domain_allowlist.split(",") if d.strip())
            )
            # Quote tweet URLs are always x.com — allow them even if x.com isn't in the
            # domain allowlist (the allowlist is for article domains, not tweet-to-tweet links).
            tweet_allowlist = domain_allowlist | frozenset({"x.com", "twitter.com"})
            quote_follow = queue_found_links(
                source_url=url,
                found_links=found_links,
                domain_allowlist=tweet_allowlist,
                notify=None,
            )

    return {**analysis, "_quote_follow": quote_follow}


@flow(name="ingest-article-subflow")
def ingest_article_subflow(url: str, llm_provider: str | None = None, llm_model: str | None = None) -> dict[str, Any]:
    """Fetch via Scrapling (with sanitization), analyze, and store a generic article.

    Sanitization strips <script>/<style> blocks and prompt injection patterns
    before the content is passed to the LLM.
    """
    check_ollama_health(llm_provider=llm_provider)
    doc = fetch_with_scrapling(url=url)
    # Sanitize fetched text before LLM analysis
    if doc.get("text"):
        doc = {**doc, "text": _sanitize_text(doc["text"])}
    analysis = analyze_document(url=url, doc=doc, llm_provider=llm_provider, llm_model=llm_model)
    store_article(url=url, analysis=analysis)
    return analysis


# ---------------------------------------------------------------------------
# Orchestrator — routes only, no fetch logic
# ---------------------------------------------------------------------------

@flow(name="ingest-url")
def ingest_url(url: str, force: bool = False, auto_follow: bool = True, notify: dict[str, Any] | None = None, ingest_id: str | None = None, llm_provider: str | None = None, llm_model: str | None = None) -> dict[str, Any]:
    """Ingest a URL into the second brain. Routes to the appropriate subflow by source type.

    auto_follow=True (default): automatically ingest relevant links found in content.
    auto_follow=False: used for auto-followed links to prevent recursion (depth=1 only).
    ingest_id: UUID of the ingests record created by the API. When provided, the flow
               updates it to completed/failed on exit. Auto-followed links do not pass ingest_id.
    llm_provider/llm_model: Optional per-ingest LLM override. Bypasses global settings.
    Distributed tracing: API injects __OTEL_TRACEPARENT into flow run labels;
    Prefect's native telemetry picks it up and re-parents the flow span.
    """
    url = strip_tracking_params(url)
    logger = get_run_logger()
    start = time.time()

    def _mark_ingest_complete(destination: str) -> None:
        if not ingest_id:
            return
        try:
            settings = load_settings()
            db = PostgresClient(settings.database_url)
            db.complete_ingest(ingest_id, destination=destination)
        except Exception as e:
            logger.warning("failed to mark ingest complete: %s", e)

    def _mark_ingest_failed(error: str) -> None:
        if not ingest_id:
            return
        try:
            settings = load_settings()
            db = PostgresClient(settings.database_url)
            db.fail_ingest(ingest_id, error=error)
        except Exception as e:
            logger.warning("failed to mark ingest failed: %s", e)

    try:
        # --- GitHub: repos table, separate duplicate check, dedicated subflow ---
        # Check GitHub first to avoid creating ghost pending records in articles table
        if is_github_repo_url(url):
            owner, name = parse_owner_name(url)
            settings = load_settings()
            db = PostgresClient(settings.database_url)
            existing_repo = db.mark_repo_pending(url=url, owner=owner, name=name)
            if existing_repo and not force:
                logger.info("duplicate repo url skipped", extra={"url": url})
                result = {
                    "url": url,
                    "readwise_id": None,
                    "summary": existing_repo.get("purpose", ""),
                    "tags": existing_repo.get("stack", []),
                    "source_type": "github",
                    "status": "duplicate",
                    "elapsed_seconds": 0,
                    "found_links": [],
                    "auto_followed": [],
                    "blocked_links": [],
                    "already_processed_links": [],
                }
                try:
                    summary_preview = (result.get("summary") or "")[:300]
                    msg = f"ℹ️ Ingest skipped (duplicate): {url}\n\n{summary_preview}"
                    _send_ingest_notification(notify, msg)
                except Exception as tg_exc:
                    logger.warning("telegram notification failed: %s", tg_exc)
                return result
            analysis = ingest_github_subflow(url=url)
            elapsed = round(time.time() - start, 2)
            result = {
                "url": url,
                "readwise_id": None,
                "summary": analysis.get("purpose", ""),
                "tags": analysis.get("stack", []),
                "source_type": "github",
                "status": "processed",
                "elapsed_seconds": elapsed,
                "found_links": [],
                "auto_followed": [],
                "blocked_links": [],
                "already_processed_links": [],
            }
            try:
                summary_preview = (result.get("summary") or "")[:300]
                skills = analysis.get("skills", [])
                skill_str = f"\n🔧 Skills: {', '.join(skills)}" if skills else ""
                msg = f"✅ Ingest complete: {url}{skill_str}\n\n{summary_preview}"
                _send_ingest_notification(notify, msg)
            except Exception as tg_exc:
                logger.warning("telegram notification failed: %s", tg_exc)
            _mark_ingest_complete("repos")
            # Clean up any stale articles record for this github URL. These can accumulate
            # if the URL was ingested before GitHub routing existed, or if it errored before
            # routing — leaving a failed/pending ghost that retry-failed re-queues forever.
            try:
                import sqlalchemy as sa
                from data.postgres.models import Article
                with db._session_factory() as session:
                    updated = session.execute(
                        sa.update(Article)
                        .where(Article.url == url)
                        .where(Article.status.in_(["failed", "pending"]))
                        .values(status="processed", title="(github repo — indexed in repos table)")
                        .returning(Article.url)
                    ).fetchall()
                    session.commit()
                if updated:
                    logger.info("Cleaned up %d stale articles record(s) for github URL: %s", len(updated), url)
            except Exception as cleanup_exc:
                logger.warning("Failed to clean up stale articles record: %s", cleanup_exc)
            return result

        # --- Non-GitHub: articles table, mark pending and check duplicates ---
        existing = mark_pending(url=url)
        if existing and not force:
            logger.info("duplicate url skipped", extra={"url": url})
            result = {
                "url": url,
                "readwise_id": existing.get("readwise_id"),
                "summary": existing.get("summary", ""),
                "tags": existing.get("tags", []),
                "source_type": existing.get("source_type", "other"),
                "status": "duplicate",
                "elapsed_seconds": 0,
                "found_links": [],
                "auto_followed": [],
                "blocked_links": [],
                "already_processed_links": [],
            }
            try:
                summary_preview = (result.get("summary") or "")[:300]
                msg = f"ℹ️ Ingest skipped (duplicate): {url}\n\n{summary_preview}"
                _send_ingest_notification(notify, msg)
            except Exception as tg_exc:
                logger.warning("telegram notification failed: %s", tg_exc)
            return result

        # --- Route to the appropriate typed subflow ---
        fxtwitter_fallback = False
        if is_chatgpt_share_url(url):
            analysis = ingest_chatgpt_subflow(url=url, llm_provider=llm_provider, llm_model=llm_model)
        elif is_youtube_url(url):
            analysis = ingest_youtube_subflow(url=url, llm_provider=llm_provider, llm_model=llm_model)
        elif is_reddit_url(url):
            analysis = ingest_reddit_subflow(url=url, llm_provider=llm_provider, llm_model=llm_model)
        elif _is_twitter_url(url):
            # Try fxtwitter first — fast, structured, visible failure in Prefect.
            # Fall back to article/Scrapling path if the tweet subflow fails.
            try:
                analysis = ingest_tweet_subflow(url=url, auto_follow=auto_follow, llm_provider=llm_provider, llm_model=llm_model)
            except Exception:
                # fxtwitter API down or rate limited — Scrapling can still fetch the page
                logger.warning("fxtwitter failed for %s — falling back to Scrapling", url)
                fxtwitter_fallback = True
                analysis = ingest_article_subflow(url=url, llm_provider=llm_provider, llm_model=llm_model)
        else:
            analysis = ingest_article_subflow(url=url, llm_provider=llm_provider, llm_model=llm_model)

        # --- Auto-follow relevant links found in content ---
        # Skip LLM-extracted links for tweets — body URL extractor already handles those
        # deterministically and for free. LLM links are for articles/videos only.
        source_type = analysis.get("source_type", "")
        relevant_links = [] if source_type == "tweet" else analysis.get("relevant_links", [])
        queued_links: list[str] = []
        blocked_links: list[str] = []
        already_processed_links: list[str] = []

        if auto_follow and relevant_links:
            settings = load_settings()
            follow_result = queue_found_links(
                source_url=url,
                found_links=relevant_links,
                domain_allowlist=settings.domain_allowlist,
            )
            queued_links = follow_result["queued"]
            blocked_links = follow_result["blocked"]
            already_processed_links = follow_result["already_processed"]
            if queued_links:
                logger.info(
                    "auto-followed found_links",
                    extra={"count": len(queued_links), "urls": queued_links},
                )
            if blocked_links:
                logger.info(
                    "found_links blocked by allowlist",
                    extra={"count": len(blocked_links), "urls": blocked_links},
                )

        elapsed = round(time.time() - start, 2)
        logger.info("ingest completed", extra={"url": url, "elapsed_s": elapsed})

        result = {
            "url": url,
            "readwise_id": None,
            "summary": analysis.get("summary", ""),
            "tags": analysis.get("tags", []),
            "source_type": analysis.get("source_type", "other"),
            "status": "processed",
            "elapsed_seconds": elapsed,
            "found_links": relevant_links,
            "auto_followed": queued_links,
            "blocked_links": blocked_links,
            "already_processed_links": already_processed_links,
            "_quote_follow": analysis.get("_quote_follow", {}),
        }

        # Send Telegram notification from within the flow (works with timeout=0 dispatch)
        try:
            summary_preview = (result.get("summary") or "")[:300]
            # Scores live in analysis (stored to DB), not in result — pull from there
            scores = (
                analysis.get("score_usefulness"),
                analysis.get("score_interest"),
                analysis.get("score_pov"),
            )
            score_str = ""
            if any(s is not None for s in scores):
                u, i, p = scores
                score_str = f"\n⭐ U:{u or '?'} I:{i or '?'} P:{p or '?'}"
            transcript_warn = ""
            if analysis.get("source_type") == "video" and not analysis.get("has_transcript"):
                transcript_warn = "\n⚠️ No transcript — description only"
            fallback_warn = ""
            if _is_twitter_url(url) and fxtwitter_fallback:
                fallback_warn = "\n⚠️ fxtwitter failed — used Scrapling fallback"
            msg = f"✅ Ingest complete: {url}{score_str}{transcript_warn}{fallback_warn}\n\n{summary_preview}"
            # Found links from tweet body (quote tweets + inline URLs)
            quote_follow = result.pop("_quote_follow", {})
            quoted_queued = quote_follow.get("queued", [])
            quoted_blocked = quote_follow.get("blocked", [])
            quoted_already = quote_follow.get("already_processed", [])
            if quoted_queued:
                msg += f"\n\n🔗 Tweet links queued:\n" + "\n".join(f"  • {l}" for l in quoted_queued)
            if quoted_already:
                msg += f"\n\n🔗✅ Tweet links already ingested:\n" + "\n".join(f"  • {l}" for l in quoted_already)
            if quoted_blocked:
                from urllib.parse import urlparse
                qb_lines = []
                for bl in quoted_blocked:
                    try:
                        host = urlparse(bl).hostname or "unknown"
                    except Exception:
                        host = "unknown"  # Malformed URL in notification — cosmetic only
                    qb_lines.append(f"  • {host} (not in allowlist)")
                msg += f"\n\n🔗🚫 Tweet links blocked:\n" + "\n".join(qb_lines)
            # LLM-extracted found links
            if queued_links:
                msg += f"\n\n🔗 Auto-followed ({len(queued_links)}):\n" + "\n".join(f"  • {l}" for l in queued_links)
            if blocked_links:
                from urllib.parse import urlparse
                blocked_lines = []
                for bl in blocked_links:
                    try:
                        host = urlparse(bl).hostname or "unknown"
                    except Exception:
                        host = "unknown"  # Malformed URL in notification — cosmetic only
                    blocked_lines.append(f"  • {bl}\n    (domain not in allowlist: {host})")
                msg += f"\n\n🚫 Blocked ({len(blocked_links)}):\n" + "\n".join(blocked_lines)
            _send_ingest_notification(notify, msg)
        except Exception as tg_exc:
            logger.warning("telegram notification failed: %s", tg_exc)

        _mark_ingest_complete("articles")
        return result

    except Exception as exc:
        logger.exception("ingest failed", extra={"url": url})
        _mark_ingest_failed(f"{type(exc).__name__}: {exc}"[:500])
        try:
            settings = load_settings()
            db = PostgresClient(settings.database_url)
            error_message = f"{type(exc).__name__}: {exc}"
            db.mark_article_failed(url=url, readwise_id=None, error_message=error_message)
            msg = f"❌ Ingest failed: {url}\n\n{str(exc)[:200]}"
            # Retry buttons — only available when we have a tracked ingest_id
            buttons: list[list[dict[str, str]]] | None = None
            if ingest_id:
                buttons = [[
                    {"text": "🔄 Retry", "callback_data": f"ingest:retry:{ingest_id}"},
                    {"text": "✨ Retry with Sonnet", "callback_data": f"ingest:retry-sonnet:{ingest_id}"},
                ]]
            _send_ingest_notification(notify, msg, buttons=buttons)
        except Exception:
            pass  # Best-effort cleanup — don't mask original error
        raise


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Run ingest-url flow locally")
    parser.add_argument("url", help="URL to ingest")
    args = parser.parse_args()
    result = ingest_url(url=args.url)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
