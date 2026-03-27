"""Tests for AsyncPostgresClient — method signatures and patterns."""
from __future__ import annotations

import pytest


class TestAsyncClientMethods:
    """Test that AsyncPostgresClient has all required methods."""

    def test_has_article_methods(self):
        from data.postgres.async_client import AsyncPostgresClient
        assert hasattr(AsyncPostgresClient, 'get_article_by_url')
        assert hasattr(AsyncPostgresClient, 'get_recent_articles')
        assert hasattr(AsyncPostgresClient, 'search_articles')
        assert hasattr(AsyncPostgresClient, 'get_articles_stats')
        assert hasattr(AsyncPostgresClient, 'upsert_article')
        assert hasattr(AsyncPostgresClient, 'get_top_tags')
        assert hasattr(AsyncPostgresClient, 'get_long_articles')

    def test_has_ingest_methods(self):
        from data.postgres.async_client import AsyncPostgresClient
        assert hasattr(AsyncPostgresClient, 'create_ingest')
        assert hasattr(AsyncPostgresClient, 'get_ingest')
        assert hasattr(AsyncPostgresClient, 'get_recent_ingests')
        assert hasattr(AsyncPostgresClient, 'update_ingest_flow_run')
        assert hasattr(AsyncPostgresClient, 'complete_ingest')
        assert hasattr(AsyncPostgresClient, 'fail_ingest')

    def test_has_repo_methods(self):
        from data.postgres.async_client import AsyncPostgresClient
        assert hasattr(AsyncPostgresClient, 'get_repo_by_url')
        assert hasattr(AsyncPostgresClient, 'get_repo_by_id')
        assert hasattr(AsyncPostgresClient, 'list_repos')
        assert hasattr(AsyncPostgresClient, 'update_repo')

    def test_has_skill_methods(self):
        from data.postgres.async_client import AsyncPostgresClient
        assert hasattr(AsyncPostgresClient, 'list_skills')
        assert hasattr(AsyncPostgresClient, 'search_skills')
        assert hasattr(AsyncPostgresClient, 'get_skills_for_repo')




class TestAsyncClientInit:
    """Test AsyncPostgresClient initialization."""

    def test_init_stores_dsn(self):
        from data.postgres.async_client import AsyncPostgresClient
        dsn = "postgresql://test:test@localhost:5432/test"
        client = AsyncPostgresClient(dsn)
        assert client._dsn == dsn

    def test_init_creates_session_factory(self):
        from data.postgres.async_client import AsyncPostgresClient
        dsn = "postgresql://test:test@localhost:5432/test"
        client = AsyncPostgresClient(dsn)
        assert client._session_factory is not None


class TestEngineFactory:
    """Test engine factory functions."""

    def test_get_async_engine_caches(self):
        """lru_cache guarantees same engine for same DSN."""
        from data.postgres.engine import get_async_engine
        dsn = "postgresql://cache-test:test@localhost:5432/test"
        engine1 = get_async_engine(dsn)
        engine2 = get_async_engine(dsn)
        assert engine1 is engine2

    def test_async_dsn_normalization(self):
        from data.postgres.engine import _normalize_async_dsn
        assert "postgresql+psycopg://" in _normalize_async_dsn("postgresql://user:pass@host/db")
        assert "postgresql+psycopg://" in _normalize_async_dsn("postgres://user:pass@host/db")

    def test_sync_dsn_normalization(self):
        from data.postgres.engine import _normalize_dsn
        assert "postgresql+psycopg2://" in _normalize_dsn("postgresql://user:pass@host/db")
        assert "postgresql+psycopg2://" in _normalize_dsn("postgres://user:pass@host/db")
