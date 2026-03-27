import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Application settings loaded from environment variables.
    
    This is pure config — no secrets, no network calls.
    Secrets (API keys, tokens) belong in Prefect blocks, loaded via shared/secrets.py.
    """
    database_url: str
    readwise_base_url: str
    llm_provider: str
    llm_model: str
    ollama_base_url: str
    embedding_provider: str  # "ollama" | "none" — controls whether embeddings are generated
    embedding_model: str
    weekly_digest_article_limit: int
    prefect_api_url: str | None
    scrapling_fetcher_url: str
    domain_allowlist: frozenset[str]
    safe_docker_url: str | None
    nervous_system_api_url: str
    article_drafts_path: str


def load_settings() -> Settings:
    """Load settings from environment variables.
    
    For Prefect flows running in workers, call this at runtime.
    For the API server, use the `settings` module singleton instead.
    """
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError("DATABASE_URL is required")

    raw_allowlist = os.getenv("INGEST_DOMAIN_ALLOWLIST", "")
    domain_allowlist: frozenset[str] = frozenset(
        d.strip().lower().lstrip("www.")
        for d in raw_allowlist.split(",")
        if d.strip()
    )

    return Settings(
        database_url=database_url,
        readwise_base_url=os.getenv("READWISE_BASE_URL", "https://readwise.io/api/v3"),
        llm_provider=os.getenv("SECOND_BRAIN_LLM_PROVIDER", "anthropic").strip().lower(),
        llm_model=os.getenv("SECOND_BRAIN_LLM_MODEL", "claude-3-5-sonnet-latest"),
        ollama_base_url=os.getenv(
            "SECOND_BRAIN_OLLAMA_BASE_URL",
            os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434"),
        ),
        # Embedding provider is separate from LLM provider.
        # Set SECOND_BRAIN_EMBEDDING_PROVIDER=none to skip embeddings entirely
        # (semantic search won't work but everything else will).
        # Defaults to "ollama" regardless of LLM provider since Anthropic has no cheap embedding API.
        embedding_provider=os.getenv("SECOND_BRAIN_EMBEDDING_PROVIDER", "ollama").strip().lower(),
        embedding_model=os.getenv("SECOND_BRAIN_EMBEDDING_MODEL", "nomic-embed-text"),
        weekly_digest_article_limit=int(os.getenv("WEEKLY_DIGEST_ARTICLE_LIMIT", "100")),
        prefect_api_url=os.getenv("PREFECT_API_URL", "http://prefect-server:4200"),
        scrapling_fetcher_url=os.getenv("SCRAPLING_FETCHER_URL", "http://scrapling-fetcher:8002"),
        domain_allowlist=domain_allowlist,
        safe_docker_url=os.getenv("SAFE_DOCKER_URL"),
        nervous_system_api_url=os.getenv("NERVOUS_SYSTEM_API_URL", "http://nervous-system-api:8001"),
        article_drafts_path=os.getenv("ARTICLE_DRAFTS_PATH", "./docs/drafts"),
    )


# Module-level singleton — loaded once at import time.
# Use this in the API server. Prefect flows should call load_settings() instead.
settings = load_settings()


# --- Concurrency helpers ---

from contextlib import contextmanager
from typing import Generator


@contextmanager
def llm_concurrency() -> Generator[None, None, None]:
    """Concurrency gate for LLM calls — only applies to Ollama (local resource).
    
    Anthropic has its own rate limiting, so we skip the gate for cloud providers.
    """
    s = load_settings()
    if s.llm_provider == "ollama":
        from prefect.concurrency.sync import concurrency
        with concurrency("ollama", occupy=1):
            yield
    else:
        yield
