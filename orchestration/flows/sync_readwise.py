from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any

from prefect import flow, get_run_logger, task

from data.postgres.client import PostgresClient
from integrations.readwise import ReadwiseClient
from orchestration.flows.ingest_url import ingest_url
from shared.config import load_settings


@task
def fetch_new_readwise_documents(updated_after: str | None = None) -> list[dict[str, Any]]:
    """Fetch documents from Readwise Reader added/updated since last sync."""
    from shared.secrets import load_readwise_token
    
    settings = load_settings()
    token = load_readwise_token()
    if not token:
        raise RuntimeError(
            "Readwise token missing. Set up 'readwise-credentials' Prefect block "
            "or set READWISE_API_TOKEN env var."
        )
    client = ReadwiseClient(token, settings.readwise_base_url)

    all_docs = []
    next_page_cursor = None

    while True:
        params: dict[str, Any] = {"page_size": 100, "withHtmlContent": "false"}
        if updated_after:
            params["updatedAfter"] = updated_after
        if next_page_cursor:
            params["pageCursor"] = next_page_cursor

        import requests as _requests
        response = _requests.get(
            f"{client.base_url}/list/",
            headers={"Authorization": f"Token {token}"},
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()

        all_docs.extend(payload.get("results", []))
        next_page_cursor = payload.get("nextPageCursor")
        if not next_page_cursor:
            break

    return all_docs


@task
def get_last_sync_cursor(db: PostgresClient) -> str | None:
    """Read the last sync timestamp from the DB (stored in a simple kv table)."""
    try:
        return db.get_setting("readwise_last_sync")
    except Exception:
        return None


@task
def save_sync_cursor(db: PostgresClient, cursor: str) -> None:
    """Persist the current sync timestamp so next run is incremental."""
    db.set_setting("readwise_last_sync", cursor)


@flow(name="sync-readwise")
async def sync_readwise(force_full: bool = False) -> dict[str, Any]:
    """Pull new bookmarks from Readwise Reader and ingest them into the second brain.

    Runs incrementally — only processes documents added/updated since the last sync.
    Set force_full=True to re-process all documents regardless of last sync time.

    Fan-out: each URL is ingested as a concurrent subflow via asyncio.gather.
    Cancelling this flow will cancel all in-progress ingest subflows (killswitch).
    Ollama concurrency is still controlled by the global 'ollama' tag limit (max 1).
    """
    logger = get_run_logger()
    settings = load_settings()
    db = PostgresClient(settings.database_url)
    start = time.time()

    # Get incremental cursor
    last_sync = None if force_full else get_last_sync_cursor(db=db)
    if last_sync:
        logger.info(f"Incremental sync from {last_sync}")
    else:
        logger.info("Full sync (no previous cursor)")

    docs = fetch_new_readwise_documents(updated_after=last_sync)
    logger.info(f"Fetched {len(docs)} documents from Readwise")

    # Build valid URL list
    valid_urls = []
    skipped = 0
    for doc in docs:
        url = doc.get("source_url") or doc.get("url")
        if not url or not url.startswith("http"):
            skipped += 1
            continue
        valid_urls.append(url)

    logger.info(f"Dispatching {len(valid_urls)} concurrent ingest subflows ({skipped} skipped)")

    # Fan out — each ingest_url runs as a subflow in its own thread.
    # asyncio.to_thread handles sync Prefect flows from async context.
    # Parent cancellation propagates to all in-progress subflows (killswitch).
    results = await asyncio.gather(
        *[asyncio.to_thread(ingest_url, url=url, force=False) for url in valid_urls],
        return_exceptions=True,
    )

    ingested = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "processed")
    skipped += sum(1 for r in results if isinstance(r, dict) and r.get("status") == "duplicate")
    errors = sum(1 for r in results if isinstance(r, Exception))

    # Save cursor for next incremental run
    new_cursor = datetime.now(timezone.utc).isoformat()
    save_sync_cursor(db=db, cursor=new_cursor)

    elapsed = round(time.time() - start, 2)
    logger.info(f"Sync complete: {ingested} ingested, {skipped} skipped, {errors} errors in {elapsed}s")

    return {
        "ingested": ingested,
        "skipped": skipped,
        "errors": errors,
        "elapsed_seconds": elapsed,
        "next_cursor": new_cursor,
    }


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Sync Readwise bookmarks to second brain")
    parser.add_argument("--force-full", action="store_true", help="Re-process all documents")
    args = parser.parse_args()
    result = sync_readwise(force_full=args.force_full)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
