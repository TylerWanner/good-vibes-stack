from __future__ import annotations

import re
from typing import Any

import requests

_GITHUB_REPO_RE = re.compile(
    r"https?://github\.com/([^/]+)/([^/\s?#]+)"
)

_URL_RE = re.compile(r'https?://[^\s\)\]\'"<>]+')

_NOISE_URL_RE = re.compile(
    r'(github\.com/(login|marketplace|features|pricing|about|contact)|'
    r'shields\.io|badge|img\.shields|githubusercontent\.com/u/|'
    r'gravatar|avatar|buymeacoffee|patreon|ko-fi|opencollective|'
    r'twitter\.com/intent|linkedin\.com/in/|discord\.gg|'
    r'mailto:|javascript:)'
)

_HEADERS = {
    "Accept": "application/vnd.github.v3+json",
    "User-Agent": "second-brain-ingest/0.2",
}


def _extract_readme_links(readme_text: str) -> list[str]:
    """Extract unique, non-noise URLs from README markdown."""
    seen: set[str] = set()
    links: list[str] = []
    for m in _URL_RE.finditer(readme_text):
        raw = m.group(0).rstrip(".,;:!?)")
        if _NOISE_URL_RE.search(raw):
            continue
        if raw not in seen:
            seen.add(raw)
            links.append(raw)
    return links[:50]  # cap at 50 to avoid prompt bloat


def is_github_repo_url(url: str) -> bool:
    """True for github.com/<owner>/<repo> URLs, including /tree and /blob paths.
    
    Returns False for issues, PRs, actions, settings, etc.
    """
    m = _GITHUB_REPO_RE.match(url.rstrip("/"))
    if not m:
        return False
    # Parse path after owner/repo
    path_parts = url.split("github.com/", 1)[-1].strip("/").split("/")
    if len(path_parts) <= 2:
        return True  # Just owner/repo
    # Allow tree/blob/commits (still repo browsing)
    sub_path = path_parts[2] if len(path_parts) > 2 else ""
    return sub_path in {"tree", "blob", "commits"}


def _parse_owner_repo(url: str) -> tuple[str, str]:
    m = _GITHUB_REPO_RE.match(url.rstrip("/"))
    if not m:
        raise RuntimeError(f"Not a valid GitHub repo URL: {url}")
    return m.group(1), m.group(2)


def fetch_repo_metadata(url: str) -> dict[str, Any]:
    """Fetch repo metadata from GitHub API: description, stars, language, topics."""
    owner, repo = _parse_owner_repo(url)
    try:
        r = requests.get(
            f"https://api.github.com/repos/{owner}/{repo}",
            headers=_HEADERS,
            timeout=15,
        )
        if r.status_code == 200:
            data = r.json()
            return {
                "owner": owner,
                "name": repo,
                "description": data.get("description") or "",
                "stars": data.get("stargazers_count", 0),
                "language": data.get("language") or "",
                "topics": data.get("topics") or [],
                "default_branch": data.get("default_branch") or "main",
                "full_name": data.get("full_name") or f"{owner}/{repo}",
                "pushed_at": data.get("pushed_at"),  # ISO8601 timestamp of last push
            }
    except Exception:
        pass
    return {
        "owner": owner,
        "name": repo,
        "description": "",
        "stars": 0,
        "language": "",
        "topics": [],
        "default_branch": "main",
        "full_name": f"{owner}/{repo}",
    }


def fetch_repo_readme(url: str, default_branch: str = "main") -> str:
    """Fetch README content (raw text, capped at 16k chars)."""
    owner, repo = _parse_owner_repo(url)
    branches = [default_branch] if default_branch != "main" else ["main", "master"]
    for branch in branches:
        try:
            raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/README.md"
            r = requests.get(raw_url, headers=_HEADERS, timeout=15)
            if r.status_code == 200:
                text = r.text
                if len(text) > 16000:
                    text = text[:16000] + "\n[README truncated]"
                return text
        except Exception:
            continue
    return ""


def fetch_repo_releases(url: str, limit: int = 3) -> list[dict[str, Any]]:
    """Fetch the latest 1-3 releases from GitHub API."""
    owner, repo = _parse_owner_repo(url)
    try:
        r = requests.get(
            f"https://api.github.com/repos/{owner}/{repo}/releases",
            headers=_HEADERS,
            params={"per_page": limit},
            timeout=15,
        )
        if r.status_code == 200:
            releases = []
            for rel in r.json()[:limit]:
                releases.append({
                    "tag": rel.get("tag_name") or "",
                    "name": rel.get("name") or "",
                    "published_at": rel.get("published_at") or "",
                    "body": (rel.get("body") or "")[:3000],
                })
            return releases
    except Exception:
        pass
    return []


def fetch_repo_changelog(url: str, default_branch: str = "main") -> str:
    """Fetch CHANGELOG.md if present (raw text, capped at 8k chars). Returns empty string if absent."""
    owner, repo = _parse_owner_repo(url)
    filenames = ["CHANGELOG.md", "CHANGELOG", "changelog.md", "CHANGES.md"]
    branches = [default_branch] if default_branch != "main" else ["main", "master"]
    for branch in branches:
        for filename in filenames:
            try:
                raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/{filename}"
                r = requests.get(raw_url, headers=_HEADERS, timeout=15)
                if r.status_code == 200:
                    text = r.text
                    if len(text) > 8000:
                        text = text[:8000] + "\n[CHANGELOG truncated]"
                    return text
            except Exception:
                continue
    return ""


def fetch_repo_tree(url: str, default_branch: str = "main") -> list[str]:
    """Fetch directory tree: top-level + src/, apps/, docs/ subdirs (paths only, no code)."""
    owner, repo = _parse_owner_repo(url)
    branch = default_branch

    try:
        r = requests.get(
            f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}",
            headers=_HEADERS,
            params={"recursive": "1"},
            timeout=15,
        )
        if r.status_code == 200:
            tree_data = r.json()
            all_paths = [item["path"] for item in tree_data.get("tree", []) if item.get("type") in ("blob", "tree")]

            # Include top-level entries + entries under src/, apps/, docs/
            result = []
            for path in all_paths:
                parts = path.split("/")
                # Top-level (no slash) or direct child of top-level dir
                if len(parts) <= 2:
                    result.append(path)
                    continue
                # Key subdirs
                top = parts[0]
                if top in ("src", "apps", "docs") and len(parts) <= 3:
                    result.append(path)

            return sorted(set(result))[:200]  # cap at 200 paths
    except Exception:
        pass

    # Fallback: just fetch top-level contents
    try:
        r = requests.get(
            f"https://api.github.com/repos/{owner}/{repo}/contents/",
            headers=_HEADERS,
            timeout=15,
        )
        if r.status_code == 200:
            return [item["name"] for item in r.json() if isinstance(item, dict)]
    except Exception:
        pass

    return []


def detect_skill_paths(tree: list[str]) -> list[str]:
    """Return paths in the tree that look like SKILL.md files."""
    results = []
    for path in tree:
        lower = path.lower()
        if lower == "skill.md" or lower.endswith("/skill.md"):
            results.append(path)
    return results


def fetch_skill_files(url: str, skill_paths: list[str], default_branch: str = "main") -> list[dict[str, Any]]:
    """Fetch content of SKILL.md files found in the repo tree.

    Returns a list of dicts with keys: skill_path, raw_url, content.
    Silently skips paths that return non-200.
    """
    owner, repo = _parse_owner_repo(url)
    results = []
    for path in skill_paths:
        raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{default_branch}/{path}"
        try:
            r = requests.get(raw_url, headers=_HEADERS, timeout=15)
            if r.status_code == 200:
                content = r.text
                if len(content) > 32000:
                    content = content[:32000] + "\n[SKILL.md truncated]"
                results.append({"skill_path": path, "raw_url": raw_url, "content": content})
        except Exception:
            continue
    return results


def parse_skill_md(content: str, path: str) -> dict[str, str]:
    """Extract name, description, and install_cmd from a SKILL.md.

    Attempts to parse YAML frontmatter first, then falls back to heuristics.
    """
    import re

    name = ""
    description = ""
    install_cmd = ""

    # Try YAML frontmatter (--- ... ---)
    fm_match = re.match(r"^---\s*\n(.*?)\n---", content, re.DOTALL)
    if fm_match:
        fm = fm_match.group(1)
        for line in fm.splitlines():
            if line.startswith("name:"):
                name = line.split(":", 1)[-1].strip().strip('"\'')
            elif line.startswith("description:"):
                description = line.split(":", 1)[-1].strip().strip('"\'')

    # Fall back: derive name from path dirname or first H1
    if not name:
        parts = path.split("/")
        if len(parts) >= 2:
            name = parts[-2]  # e.g. "skills/mission-control-manage/SKILL.md" → "mission-control-manage"
        else:
            name = parts[0].replace("SKILL.md", "").strip("/") or path

    # Fall back: first non-empty line after frontmatter as description
    if not description:
        body = content[fm_match.end():] if fm_match else content
        for line in body.splitlines():
            line = line.strip().lstrip("#").strip()
            if line and not line.startswith("---"):
                description = line[:300]
                break

    # Extract install command: look for lines like `openclaw skill install ...` or `install:` sections
    install_match = re.search(
        r"(openclaw\s+skill\s+install[^\n`]+|npm\s+install[^\n`]+|pip\s+install[^\n`]+)",
        content,
        re.IGNORECASE,
    )
    if install_match:
        install_cmd = install_match.group(1).strip()

    return {"name": name, "description": description, "install_cmd": install_cmd}


def fetch_github_repo_document(url: str) -> dict[str, Any]:
    """Fetch a GitHub repo's README and basic metadata via the GitHub API.

    No auth required for public repos. Falls back to raw README if API fails.
    Kept for backward compatibility with the existing ingest_url.py flow.
    """
    m = _GITHUB_REPO_RE.match(url.rstrip("/"))
    if not m:
        raise RuntimeError(f"Not a valid GitHub repo URL: {url}")

    owner, repo = m.group(1), m.group(2)

    # Get repo metadata
    repo_info = {}
    try:
        r = requests.get(
            f"https://api.github.com/repos/{owner}/{repo}",
            headers=_HEADERS,
            timeout=15,
        )
        if r.status_code == 200:
            repo_info = r.json()
    except Exception:
        pass

    # Get README content
    readme_text = ""
    default_branch = repo_info.get("default_branch", "main") if repo_info else "main"
    for branch in ([default_branch] if default_branch != "main" else ["main", "master"]):
        try:
            raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{branch}/README.md"
            r = requests.get(raw_url, headers=_HEADERS, timeout=15)
            if r.status_code == 200:
                readme_text = r.text
                break
        except Exception:
            continue

    if not readme_text and not repo_info:
        raise RuntimeError(f"Could not fetch README or metadata for {url}")

    title = repo_info.get("full_name") or f"{owner}/{repo}"
    description = repo_info.get("description") or ""
    stars = repo_info.get("stargazers_count", 0)
    language = repo_info.get("language") or ""
    topics = repo_info.get("topics") or []

    parts = [f"Repo: {title}"]
    if description:
        parts.append(f"Description: {description}")
    if language:
        parts.append(f"Language: {language}")
    if stars:
        parts.append(f"Stars: {stars}")
    if topics:
        parts.append(f"Topics: {', '.join(topics)}")
    if readme_text:
        readme_excerpt = readme_text[:12000]
        if len(readme_text) > 12000:
            readme_excerpt += "\n[README truncated]"
        parts.append(f"\nREADME:\n{readme_excerpt}")

        readme_links = _extract_readme_links(readme_text)
        if readme_links:
            parts.append(f"\nLinks found in README:\n" + "\n".join(readme_links))

    return {
        "title": title,
        "text": "\n".join(parts),
        "source_type": "github",
        "url": url,
    }
