"""Check a GitHub repo for meaningful changes since last ingest.

Fetches new releases (since last_release pointer) and current README,
runs Ollama analysis, and updates the repo record if something changed.
If changes are significant enough, triggers a full re-analysis.

Triggered on-demand via POST /repos/check-updates?url=<github_url>
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from prefect import flow, get_run_logger, task

from integrations.github import (
    fetch_repo_metadata,
    fetch_repo_readme,
    fetch_repo_releases,
    fetch_repo_changelog,
    fetch_repo_tree,
)
from second_brain.llm import LLMClient
from data.postgres.client import PostgresClient
from shared.config import load_settings
from orchestration.flows.ingest_github_repo import (
    analyze_repo_task,
    store_repo_task,
    parse_owner_name,
)


def _releases_since(releases: list[dict[str, Any]], last_known_tag: str | None) -> list[dict[str, Any]]:
    """Return releases newer than last_known_tag. If no pointer, returns all."""
    if not last_known_tag:
        return releases
    new = []
    for rel in releases:
        tag = rel.get("tag") or rel.get("tag_name") or ""
        if tag == last_known_tag:
            break
        new.append(rel)
    return new


@flow(name="check-repo-updates", log_prints=True)
def check_repo_updates(url: str) -> dict[str, Any]:
    """Check if a GitHub repo has meaningfully changed since last ingest.

    Returns a dict with:
    - changed: bool
    - update_summary: str (empty if no changes)
    - reanalyzed: bool (true if full re-analysis was triggered)
    - last_release: str (current tip)
    """
    logger = get_run_logger()
    settings = load_settings()
    db = PostgresClient(settings.database_url)

    # Normalize URL
    url = url.rstrip("/").split("?")[0]
    owner, name = parse_owner_name(url)
    canonical = f"https://github.com/{owner}/{name}"

    repo = db.get_repo_by_url(canonical)
    if not repo:
        logger.warning("repo not found in DB, ingesting fresh", extra={"url": canonical})
        # Fall back to fresh ingest — import here to avoid circular
        from orchestration.flows.ingest_github_repo import (
            fetch_repo_metadata_task,
            fetch_repo_readme_task,
            fetch_repo_releases_task,
            fetch_repo_changelog_task,
            fetch_repo_tree_task,
        )
        metadata = fetch_repo_metadata_task(canonical)
        readme = fetch_repo_readme_task(canonical, metadata.get("default_branch", "main"))
        releases = fetch_repo_releases_task(canonical)
        changelog = fetch_repo_changelog_task(canonical, metadata.get("default_branch", "main"))
        tree = fetch_repo_tree_task(canonical, metadata.get("default_branch", "main"))
        analysis = analyze_repo_task(metadata, readme, releases, changelog, tree)
        store_repo_task(canonical, metadata, analysis, releases)
        db.update_repo(canonical, readme_text=readme[:16000], last_checked_at=datetime.now(timezone.utc))
        return {"changed": True, "update_summary": "Fresh ingest (not previously in DB)", "reanalyzed": True, "last_release": (releases[0].get("tag") if releases else None)}

    last_known_tag = repo.get("last_release")
    logger.info("checking updates", extra={"url": canonical, "last_release": last_known_tag})

    # Fetch current state from GitHub
    metadata = fetch_repo_metadata(canonical)
    default_branch = metadata.get("default_branch", "main")
    current_readme = fetch_repo_readme(canonical, default_branch=default_branch)
    all_releases = fetch_repo_releases(canonical, limit=10)
    new_releases = _releases_since(all_releases, last_known_tag)

    logger.info(
        "fetched GitHub state",
        extra={"new_releases": len(new_releases), "readme_chars": len(current_readme)},
    )

    # Run Ollama update analysis
    from shared.secrets import load_anthropic_api_key
    llm = LLMClient(
        provider=settings.llm_provider,
        model=settings.llm_model,
        anthropic_api_key=load_anthropic_api_key(),
        ollama_base_url=settings.ollama_base_url,
    )

    from shared.config import llm_concurrency
    with llm_concurrency():
        result = llm.analyze_repo_update(
            repo=repo,
            new_releases=new_releases,
            current_readme=current_readme,
        )

    changed = result["changed"]
    update_summary = result["update_summary"]
    should_reanalyze = result["should_reanalyze"]
    reanalyzed = False

    # Determine new last_release pointer
    new_last_release = (new_releases[0].get("tag") or new_releases[0].get("tag_name")) if new_releases else last_known_tag
    new_last_release_at = new_releases[0].get("published_at") if new_releases else repo.get("last_release_at")

    if changed:
        logger.info("meaningful changes detected", extra={"summary": update_summary[:200]})

        if should_reanalyze:
            logger.info("triggering full re-analysis")
            # Full re-analysis pass
            from orchestration.flows.ingest_github_repo import (
                fetch_repo_changelog_task,
                fetch_repo_tree_task,
            )
            changelog = fetch_repo_changelog(canonical, default_branch=default_branch)
            tree = fetch_repo_tree(canonical, default_branch=default_branch)

            with llm_concurrency():
                full_analysis = llm.analyze_github_repo(
                    metadata=metadata,
                    readme=current_readme,
                    releases=all_releases[:3],
                    changelog=changelog,
                    tree=tree,
                )

            db.upsert_repo(
                url=canonical,
                owner=owner,
                name=name,
                description=metadata.get("description") or "",
                stars=metadata.get("stars") or 0,
                purpose=full_analysis.get("purpose") or "",
                architecture=full_analysis.get("architecture") or "",
                key_features=full_analysis.get("key_features") or [],
                stack=full_analysis.get("stack") or [],
                tradeoffs=full_analysis.get("tradeoffs") or "",
                fit_for_us=full_analysis.get("fit_for_us") or "",
                release_notes=full_analysis.get("release_summary") or "",
                last_release=new_last_release,
                last_release_at=new_last_release_at,
                status="processed",
            )
            reanalyzed = True
        else:
            # Minor update — advance pointer + store summary, skip full re-analysis
            db.update_repo(
                canonical,
                last_release=new_last_release,
                last_release_at=new_last_release_at,
                release_notes=update_summary,
            )

        # Always update readme snapshot + summary + checked_at
        db.update_repo(
            canonical,
            readme_text=current_readme[:16000],
            last_update_summary=update_summary,
        )

    else:
        logger.info("no meaningful changes detected")

    # Always advance last_checked_at
    db.update_repo(canonical, last_checked_at=datetime.now(timezone.utc))

    logger.info(
        "check complete",
        extra={"changed": changed, "reanalyzed": reanalyzed, "last_release": new_last_release},
    )

    return {
        "changed": changed,
        "update_summary": update_summary,
        "reanalyzed": reanalyzed,
        "last_release": new_last_release,
    }


@flow(name="check-all-watched-repos", log_prints=True)
def check_all_watched_repos() -> list[dict]:
    """Daily scheduled flow: check all watched repos for updates.

    Iterates repos where watched=True, runs check_repo_updates for each,
    sends a single Telegram summary if any changes were found.
    """
    from data.postgres.client import PostgresClient
    from integrations.telegram import send_telegram_message
    from shared.config import load_settings

    logger = get_run_logger()
    settings = load_settings()
    db = PostgresClient(settings.database_url)

    watched = db.list_repos(watched=True, limit=100)
    if not watched:
        logger.info("no watched repos")
        return []

    logger.info("checking %d watched repos", len(watched))
    results = []
    changed_summaries = []

    for repo in watched:
        url = repo["url"]
        try:
            result = check_repo_updates(url)
            results.append({"url": url, **result})
            if result.get("changed"):
                name = url.split("github.com/")[-1]
                changed_summaries.append(
                    f"• `{name}` @ `{result.get('last_release', '?')}`: {result.get('update_summary', '')[:120]}"
                )
        except Exception as exc:
            logger.error("check failed for %s: %s", url, exc)
            results.append({"url": url, "error": str(exc)})

    if changed_summaries:
        from integrations.telegram import notify_telegram
        message = "🔄 *Watched repo updates*\n\n" + "\n".join(changed_summaries)
        notify_telegram(message)
    else:
        logger.info("no meaningful changes across %d watched repos", len(watched))

    return results
