"""GitHub repo ingestion tasks.

These tasks are called directly from ingest_url flow — no separate @flow wrapper.
All orchestration, error handling, and Telegram notifications live in ingest_url.
"""
from __future__ import annotations

import re
from typing import Any

from prefect import get_run_logger, task

from integrations.github import (
    detect_skill_paths,
    fetch_repo_metadata,
    fetch_repo_readme,
    fetch_repo_releases,
    fetch_repo_changelog,
    fetch_repo_tree,
    fetch_skill_files,
    parse_skill_md,
)
from second_brain.llm import LLMClient
from data.postgres.client import PostgresClient
from shared.config import load_settings

_OWNER_NAME_RE = re.compile(r"https?://github\.com/([^/]+)/([^/\s?#]+)")


def parse_owner_name(url: str) -> tuple[str, str]:
    m = _OWNER_NAME_RE.match(url.rstrip("/"))
    if not m:
        raise ValueError(f"Could not parse owner/name from GitHub URL: {url}")
    return m.group(1), m.group(2)


@task
def fetch_repo_metadata_task(url: str) -> dict[str, Any]:
    get_run_logger().info("fetching repo metadata", extra={"url": url})
    return fetch_repo_metadata(url)


@task
def fetch_repo_readme_task(url: str, default_branch: str = "main") -> str:
    get_run_logger().info("fetching repo README", extra={"url": url})
    return fetch_repo_readme(url, default_branch=default_branch)


@task
def fetch_repo_releases_task(url: str) -> list[dict[str, Any]]:
    get_run_logger().info("fetching repo releases", extra={"url": url})
    return fetch_repo_releases(url, limit=3)


@task
def fetch_repo_changelog_task(url: str, default_branch: str = "main") -> str:
    get_run_logger().info("fetching repo changelog", extra={"url": url})
    return fetch_repo_changelog(url, default_branch=default_branch)


@task
def fetch_repo_tree_task(url: str, default_branch: str = "main") -> list[str]:
    get_run_logger().info("fetching repo tree", extra={"url": url})
    return fetch_repo_tree(url, default_branch=default_branch)


@task
def analyze_repo_task(
    metadata: dict[str, Any],
    readme: str,
    releases: list[dict[str, Any]],
    changelog: str,
    tree: list[str],
) -> dict[str, Any]:
    get_run_logger().info("analyzing repo with LLM", extra={"repo": metadata.get("full_name", "")})
    from shared.secrets import load_anthropic_api_key
    settings = load_settings()
    llm = LLMClient(
        provider=settings.llm_provider,
        model=settings.llm_model,
        anthropic_api_key=load_anthropic_api_key(),
        ollama_base_url=settings.ollama_base_url,
    )
    from shared.config import llm_concurrency
    with llm_concurrency():
        return llm.analyze_github_repo(
            metadata=metadata,
            readme=readme,
            releases=releases,
            changelog=changelog,
            tree=tree,
        )


@task
def store_repo_task(
    url: str,
    metadata: dict[str, Any],
    analysis: dict[str, Any],
    releases: list[dict[str, Any]],
) -> None:
    logger = get_run_logger()
    settings = load_settings()
    db = PostgresClient(settings.database_url)

    last_release = None
    last_release_at = None
    if releases:
        first = releases[0]
        last_release = first.get("tag") or first.get("tag_name") or None
        last_release_at = first.get("published_at") or None

    db.upsert_repo(
        url=url,
        owner=metadata.get("owner", ""),
        name=metadata.get("name", ""),
        description=metadata.get("description") or "",
        stars=metadata.get("stars") or 0,
        purpose=analysis.get("purpose") or "",
        architecture=analysis.get("architecture") or "",
        key_features=analysis.get("key_features") or [],
        stack=analysis.get("stack") or [],
        tradeoffs=analysis.get("tradeoffs") or "",
        fit_for_us=analysis.get("fit_for_us") or "",
        release_notes=analysis.get("release_summary") or "",
        last_release=last_release,
        last_release_at=last_release_at,
        last_push_at=metadata.get("pushed_at"),
        status="processed",
    )
    logger.info("repo stored", extra={"url": url})


@task
def detect_and_store_skills_task(
    url: str,
    tree: list[str],
    repo_id: str,
    default_branch: str = "main",
) -> list[str]:
    """Scan repo tree for SKILL.md files, fetch and store them. Returns list of skill names found."""
    logger = get_run_logger()
    skill_paths = detect_skill_paths(tree)
    if not skill_paths:
        logger.info("no SKILL.md files found in repo tree")
        return []

    logger.info("found %d SKILL.md file(s): %s", len(skill_paths), skill_paths)
    skill_files = fetch_skill_files(url, skill_paths, default_branch=default_branch)

    settings = load_settings()
    db = PostgresClient(settings.database_url)

    names = []
    for sf in skill_files:
        parsed = parse_skill_md(sf["content"], sf["skill_path"])
        db.upsert_skill(
            name=parsed["name"],
            source_url=sf["raw_url"],
            repo_id=repo_id,
            skill_path=sf["skill_path"],
            description=parsed["description"] or None,
            skill_md=sf["content"],
            install_cmd=parsed["install_cmd"] or None,
        )
        logger.info("stored skill: %s (%s)", parsed["name"], sf["skill_path"])
        names.append(parsed["name"])

    return names


from prefect import flow as prefect_flow


@prefect_flow(name="ingest-github-repo", log_prints=True)
def ingest_github_repo(url: str, force: bool = False) -> dict:
    """Top-level Prefect flow: ingest a GitHub repo into the second brain.

    Fetches metadata, README, releases, changelog, and directory tree,
    analyzes with LLM, and stores to the repos table.

    Args:
        url: GitHub repo URL (e.g. https://github.com/owner/repo)
        force: If True, re-ingest even if already processed.
    """
    from prefect import get_run_logger
    from data.postgres.client import PostgresClient
    from shared.config import load_settings

    logger = get_run_logger()
    settings = load_settings()
    db = PostgresClient(settings.database_url)

    # Normalize URL
    url = url.rstrip("/").split("?")[0]
    owner, name = parse_owner_name(url)
    canonical = f"https://github.com/{owner}/{name}"

    # Skip if already processed (unless force)
    if not force:
        existing = db.get_repo_by_url(canonical)
        if existing and existing.get("status") == "processed":
            logger.info("repo already processed, skipping (use force=True to re-ingest): %s", canonical)
            return {"status": "skipped", "url": canonical, "reason": "already processed"}

    logger.info("ingesting repo: %s", canonical)
    db.upsert_repo(url=canonical, owner=owner, name=name, status="pending")

    # Resolve repo_id for skill FK linkage
    repo_record = db.get_repo_by_url(canonical)
    repo_id = str(repo_record["id"]) if repo_record else None

    try:
        metadata = fetch_repo_metadata_task(canonical)
        default_branch = metadata.get("default_branch", "main")
        readme = fetch_repo_readme_task(canonical, default_branch)
        releases = fetch_repo_releases_task(canonical)
        changelog = fetch_repo_changelog_task(canonical, default_branch)
        tree = fetch_repo_tree_task(canonical, default_branch)
        analysis = analyze_repo_task(metadata, readme, releases, changelog, tree)
        store_repo_task(canonical, metadata, analysis, releases)

        # Skill detection — runs after store so repo_id is stable
        if repo_id is None:
            repo_record = db.get_repo_by_url(canonical)
            repo_id = str(repo_record["id"]) if repo_record else None
        skill_names: list[str] = []
        if repo_id:
            skill_names = detect_and_store_skills_task(canonical, tree, repo_id, default_branch)

        logger.info("repo ingest complete: %s (skills: %s)", canonical, skill_names or "none")
        return {"status": "processed", "url": canonical, "skills": skill_names}
    except Exception as exc:
        logger.error("repo ingest failed: %s — %s", canonical, exc)
        db.upsert_repo(url=canonical, owner=owner, name=name, status="failed", error_message=str(exc)[:500])
        raise
