from __future__ import annotations

import json
import logging
from typing import Any

from second_brain.prompts import (
    CHUNK_EXTRACT_PROMPT,
    COMPACT_SUMMARY_PROMPT,
    GITHUB_REPO_ANALYSIS_PROMPT,
    GITHUB_REPO_UPDATE_PROMPT,
    MERGE_SUMMARY_PROMPT,
    RESEARCH_PROMPT,
    SUMMARY_PROMPT,
    TWEET_DRAFT_PROMPT,
    WEEKLY_DIGEST_PROMPT,
)

logger = logging.getLogger(__name__)


def _clamp_score(val: Any) -> int | None:
    """Coerce LLM score to int 1-5, or None if unparseable."""
    try:
        v = int(val)
        return max(1, min(5, v))
    except (TypeError, ValueError):
        return None


def _extract_json(raw: str) -> dict[str, Any]:
    """Extract JSON object from LLM response. Handles markdown code blocks."""
    raw = raw.strip()
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(raw[start : end + 1])
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
    raise RuntimeError(f"Model did not return valid JSON: {raw[:500]}")


class LLMClient:
    """High-level LLM client for second brain operations.
    
    Supports multiple providers (anthropic, ollama) via integration clients.
    """

    def __init__(
        self,
        provider: str,
        model: str,
        anthropic_api_key: str | None = None,
        ollama_base_url: str = "http://127.0.0.1:11434",
        embedding_provider: str = "ollama",
        embedding_model: str = "nomic-embed-text",
    ) -> None:
        """Initialize LLM client.
        
        Args:
            provider: Text generation provider ("ollama" or "anthropic")
            model: Model name for text generation
            anthropic_api_key: Required if provider is "anthropic"
            ollama_base_url: Ollama API base URL
            embedding_provider: Provider for embeddings ("ollama" only for now)
            embedding_model: Model name for embeddings (default: nomic-embed-text)
        """
        self.provider = provider
        self.model = model
        self.embedding_provider = embedding_provider
        self.embedding_model = embedding_model
        self._ollama: Any = None
        self._anthropic: Any = None

        if provider == "ollama":
            from integrations.ollama import OllamaClient
            self._ollama = OllamaClient(ollama_base_url)
        elif provider == "anthropic":
            if not anthropic_api_key:
                raise RuntimeError("anthropic_api_key required for anthropic provider")
            from integrations.anthropic import AnthropicClient
            self._anthropic = AnthropicClient(anthropic_api_key)
        else:
            raise RuntimeError(f"Unsupported LLM provider: {provider}")

        # Initialize embedding client if needed (separate from generation provider)
        if embedding_provider == "ollama" and self._ollama is None:
            from integrations.ollama import OllamaClient
            self._ollama = OllamaClient(ollama_base_url)

    def embed(self, text: str, model: str | None = None) -> list[float] | None:
        """Generate embedding vector for text.
        
        Args:
            text: Text to embed
            model: Embedding model name (default: uses embedding_model from init)
            
        Returns:
            Embedding vector (768 dims for nomic-embed-text), or None on failure
        """
        if self.embedding_provider != "ollama":
            logger.warning("Only Ollama embeddings are currently supported (got %s)", self.embedding_provider)
            return None
        if not self._ollama:
            logger.warning("Ollama client not available for embeddings")
            return None
        return self._ollama.embed(text, model=model or self.embedding_model)

    def _chunk_text(self, text: str, chunk_size: int = 12000, overlap: int = 500) -> list[str]:
        """Split text into overlapping chunks at sentence/paragraph boundaries."""
        chunks = []
        start = 0
        while start < len(text):
            end = start + chunk_size
            if end < len(text):
                # Try to break at a paragraph boundary first, then sentence
                break_at = text.rfind("\n\n", start, end)
                if break_at == -1 or break_at <= start:
                    break_at = text.rfind(". ", start, end)
                if break_at == -1 or break_at <= start:
                    break_at = end
                else:
                    break_at += 1  # include the period/newline
            else:
                break_at = len(text)
            chunks.append(text[start:break_at].strip())
            start = max(break_at - overlap, start + 1)
        return [c for c in chunks if c]

    def summarize_long_content(self, *, url: str, title: str, content: str) -> dict[str, Any]:
        """Map-reduce summarization for long content (>16k chars).

        1. Split content into overlapping chunks
        2. Extract key knowledge from each chunk (map)
        3. Merge extracts into unified summary with scores (reduce)
        """
        COMPACT_THRESHOLD = 6000  # compact running summary when it exceeds this many chars

        # chunk_size=8000 chars (~2000 tokens) fits within num_ctx=10240
        # with room for running_summary and prompt overhead.
        chunks = self._chunk_text(content, chunk_size=8000, overlap=400)
        total = len(chunks)
        running_summary = "(nothing captured yet — this is the first section)"
        for i, chunk in enumerate(chunks):
            prompt = CHUNK_EXTRACT_PROMPT.format(
                chunk_num=i + 1,
                chunk_total=total,
                running_summary=running_summary,
                content=chunk,
            )
            extract = self._generate(prompt, timeout=180)  # 3min per chunk — larger chunks need more time
            running_summary = f"{running_summary}\n\n[Section {i+1}]\n{extract.strip()}"

            # Compact whenever running summary grows too large (not on final chunk)
            if len(running_summary) > COMPACT_THRESHOLD and i + 1 < total:
                compact_prompt = COMPACT_SUMMARY_PROMPT.format(running_summary=running_summary)
                running_summary = self._generate(compact_prompt, timeout=120).strip()

        # Final pass: turn the running summary into a structured KB entry
        merged_prompt = MERGE_SUMMARY_PROMPT.format(
            url=url,
            title=title or "(untitled)",
            extracts=running_summary,
        )
        raw = self._generate(merged_prompt, timeout=180)
        parsed = _extract_json(raw)

        result = {
            "summary": parsed.get("summary", ""),
            "tags": parsed.get("tags", []),
            "source_type": parsed.get("source_type", "other"),
            "relevant_links": parsed.get("relevant_links", []),
            "score_usefulness": _clamp_score(parsed.get("score_usefulness")),
            "score_interest": _clamp_score(parsed.get("score_interest")),
            "score_pov": _clamp_score(parsed.get("score_pov")),
            "score_uniqueness": _clamp_score(parsed.get("score_uniqueness")),
        }

        return result

    def summarize_and_tag(self, *, url: str, title: str, content: str) -> dict[str, Any]:
        prompt = SUMMARY_PROMPT.format(url=url, title=title or "(untitled)", content=content[:16000])
        raw = self._generate(prompt)
        parsed = _extract_json(raw)

        return {
            "summary": parsed.get("summary", ""),
            "tags": parsed.get("tags", []),
            "source_type": parsed.get("source_type", "other"),
            "relevant_links": parsed.get("relevant_links", []),
            "score_usefulness": _clamp_score(parsed.get("score_usefulness")),
            "score_interest": _clamp_score(parsed.get("score_interest")),
            "score_pov": _clamp_score(parsed.get("score_pov")),
            "score_uniqueness": _clamp_score(parsed.get("score_uniqueness")),
        }

    def analyze_github_repo(
        self,
        *,
        metadata: dict[str, Any],
        readme: str,
        releases: list[dict[str, Any]],
        changelog: str,
        tree: list[str],
    ) -> dict[str, Any]:
        """Structured extraction for a GitHub repo. Returns dict with purpose, architecture, etc."""
        releases_text = ""
        if releases:
            parts = []
            for rel in releases:
                tag = rel.get("tag") or rel.get("tag_name") or ""
                date = rel.get("published_at") or ""
                body = rel.get("body") or ""
                parts.append(f"### {tag} ({date})\n{body[:1500]}")
            releases_text = "\n\n".join(parts)
        else:
            releases_text = "(no releases found)"

        changelog_text = changelog[:2000] if changelog else "(no changelog found)"
        readme_text = readme[:6000] if readme else "(no README found)"

        prompt = GITHUB_REPO_ANALYSIS_PROMPT.format(
            full_name=metadata.get("full_name") or f"{metadata.get('owner', '')}/{metadata.get('name', '')}",
            description=metadata.get("description") or "",
            language=metadata.get("language") or "",
            topics=", ".join(metadata.get("topics") or []),
            stars=metadata.get("stars") or 0,
            readme=readme_text,
            releases=releases_text,
            changelog=changelog_text,
            tree="(omitted)",
        )

        raw = self._generate(prompt)
        parsed = _extract_json(raw)

        def _as_str(val: Any) -> str:
            if isinstance(val, str):
                return val
            if val is None:
                return ""
            return str(val)

        def _as_str_list(val: Any) -> list[str]:
            if isinstance(val, list):
                return [str(v).strip() for v in val if v]
            if isinstance(val, str) and val.strip():
                return [val.strip()]
            return []

        return {
            "purpose": _as_str(parsed.get("purpose")),
            "architecture": _as_str(parsed.get("architecture")),
            "key_features": _as_str_list(parsed.get("key_features")),
            "stack": _as_str_list(parsed.get("stack")),
            "tradeoffs": _as_str(parsed.get("tradeoffs")),
            "fit_for_us": _as_str(parsed.get("fit_for_us")),
            "release_summary": _as_str(parsed.get("release_summary")),
        }

    def analyze_repo_update(
        self,
        *,
        repo: dict[str, Any],
        new_releases: list[dict[str, Any]],
        current_readme: str,
    ) -> dict[str, Any]:
        """Check if a repo has meaningfully changed since last ingest. Returns {changed, update_summary, should_reanalyze}."""
        new_releases_text = ""
        if new_releases:
            parts = []
            for rel in new_releases:
                tag = rel.get("tag") or rel.get("tag_name") or ""
                date = rel.get("published_at") or ""
                body = rel.get("body") or ""
                parts.append(f"### {tag} ({date})\n{body[:1500]}")
            new_releases_text = "\n\n".join(parts)
        else:
            new_releases_text = "(no new releases)"

        key_features = repo.get("key_features") or []
        if isinstance(key_features, list):
            key_features_str = "\n".join(f"- {f}" for f in key_features)
        else:
            key_features_str = str(key_features)

        prompt = GITHUB_REPO_UPDATE_PROMPT.format(
            purpose=repo.get("purpose") or "(unknown)",
            architecture=repo.get("architecture") or "(unknown)",
            key_features=key_features_str,
            last_release=repo.get("last_release") or "(none)",
            new_releases=new_releases_text,
            stored_readme=(repo.get("readme_text") or "(not stored)")[:8000],
            current_readme=(current_readme or "(unavailable)")[:8000],
        )

        raw = self._generate(prompt)
        parsed = _extract_json(raw)
        return {
            "changed": bool(parsed.get("changed", False)),
            "update_summary": parsed.get("update_summary") or "",
            "should_reanalyze": bool(parsed.get("should_reanalyze", False)),
        }

    def draft_tweets(self, articles: list[dict[str, Any]], count: int = 3) -> list[dict[str, Any]]:
        packed = [
            {"title": a.get("title", ""), "summary": a.get("summary", ""), "url": a.get("url", "")}
            for a in articles
        ]
        prompt = TWEET_DRAFT_PROMPT.format(
            count=count,
            articles=json.dumps(packed, ensure_ascii=True, indent=2)[:12000],
        )
        raw = self._generate(prompt)
        parsed = _extract_json(raw)
        # Model should return {"tweets": [...]} — handle all fallback shapes
        if isinstance(parsed, list):
            return parsed
        for key in ("tweets", "drafts", "data", "list", "results", "items"):
            val = parsed.get(key)
            if isinstance(val, list) and val:
                return val
        return []

    def research_synthesis(self, query: str, sources: list[dict[str, Any]]) -> str:
        """Synthesize multiple fetched sources into a research report."""
        packed = []
        for i, s in enumerate(sources, 1):
            packed.append(f"[{i}] {s.get('url', '')}\nTitle: {s.get('title', '')}\n{s.get('content', '')[:3000]}")
        sources_text = "\n\n---\n\n".join(packed)
        prompt = RESEARCH_PROMPT.format(query=query, sources=sources_text[:20000])
        return self._generate_text(prompt)

    def create_weekly_digest(self, items: list[dict[str, Any]]) -> dict[str, Any]:
        packed = []
        for item in items:
            packed.append(
                {
                    "title": item.get("title", ""),
                    "summary": item.get("summary", ""),
                    "tags": item.get("tags", []),
                    "url": item.get("url", ""),
                }
            )
        prompt = WEEKLY_DIGEST_PROMPT.format(items=json.dumps(packed, ensure_ascii=True, indent=2)[:18000])
        raw = self._generate(prompt)
        parsed = _extract_json(raw)
        return {
            "digest": parsed.get("digest", ""),
            "themes": parsed.get("themes", []),
        }

    def _generate(self, prompt: str, timeout: int = 300) -> str:
        """Generate text with JSON format enforcement."""
        if self._anthropic:
            return self._anthropic.chat(self.model, [{"role": "user", "content": prompt}])
        if self._ollama:
            return self._ollama.chat(
                self.model,
                [{"role": "user", "content": prompt}],
                format="json",
                temperature=0.2,
                timeout=timeout,
            )
        raise RuntimeError("No LLM client configured")

    def _generate_text(self, prompt: str) -> str:
        """Generate plain text (no JSON enforcement)."""
        if self._anthropic:
            return self._anthropic.chat(self.model, [{"role": "user", "content": prompt}])
        if self._ollama:
            return self._ollama.chat(
                self.model,
                [{"role": "user", "content": prompt}],
                temperature=0.3,
                max_tokens=4096,
            )
        raise RuntimeError("No LLM client configured")


