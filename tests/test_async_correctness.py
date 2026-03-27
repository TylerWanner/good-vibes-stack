"""Tests to verify async methods are actually async."""
from __future__ import annotations

import asyncio
import inspect
import pytest


class TestAsyncClientMethodsAreAsync:
    """Verify all AsyncPostgresClient methods are coroutines."""

    def test_article_methods_are_coroutines(self):
        from data.postgres.async_client import AsyncPostgresClient
        
        async_methods = [
            'get_article_by_url',
            'get_recent_articles', 
            'search_articles',
            'get_articles_stats',
            'upsert_article',
            'get_top_tags',
            'get_long_articles',
        ]
        
        for method_name in async_methods:
            method = getattr(AsyncPostgresClient, method_name)
            assert asyncio.iscoroutinefunction(method), f"{method_name} should be async"

    def test_ingest_methods_are_coroutines(self):
        from data.postgres.async_client import AsyncPostgresClient
        
        async_methods = [
            'create_ingest',
            'get_ingest',
            'get_recent_ingests',
            'update_ingest_flow_run',
            'complete_ingest',
            'fail_ingest',
        ]
        
        for method_name in async_methods:
            method = getattr(AsyncPostgresClient, method_name)
            assert asyncio.iscoroutinefunction(method), f"{method_name} should be async"

    def test_repo_methods_are_coroutines(self):
        from data.postgres.async_client import AsyncPostgresClient
        
        async_methods = [
            'get_repo_by_url',
            'get_repo_by_id',
            'list_repos',
            'update_repo',
        ]
        
        for method_name in async_methods:
            method = getattr(AsyncPostgresClient, method_name)
            assert asyncio.iscoroutinefunction(method), f"{method_name} should be async"

    def test_skill_methods_are_coroutines(self):
        from data.postgres.async_client import AsyncPostgresClient
        
        async_methods = [
            'list_skills',
            'search_skills',
            'get_skills_for_repo',
        ]
        
        for method_name in async_methods:
            method = getattr(AsyncPostgresClient, method_name)
            assert asyncio.iscoroutinefunction(method), f"{method_name} should be async"




class TestSyncClientMethodsAreSync:
    """Verify PostgresClient methods are NOT coroutines (sync is intentional)."""

    def test_sync_methods_are_not_coroutines(self):
        from data.postgres.client import PostgresClient
        
        # Sample of methods that should be sync
        sync_methods = [
            'get_article_by_url',
            'create_ingest',
        ]
        
        for method_name in sync_methods:
            if hasattr(PostgresClient, method_name):
                method = getattr(PostgresClient, method_name)
                assert not asyncio.iscoroutinefunction(method), f"{method_name} should be sync"
