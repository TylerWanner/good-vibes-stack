"""Tests for request model validation — bounds, defaults, required fields."""
from __future__ import annotations

import pytest


class TestReingestRequest:
    """Test ReingestRequest validation."""

    def test_required_query(self):
        from nervous_system.http.server import ReingestRequest
        with pytest.raises(Exception):  # ValidationError
            ReingestRequest(limit=10)  # missing query

    def test_limit_default(self):
        from nervous_system.http.server import ReingestRequest
        req = ReingestRequest(query="test")
        assert req.limit == 10

    def test_limit_bounds(self):
        from nervous_system.http.server import ReingestRequest
        from pydantic import ValidationError
        
        # Valid bounds
        req = ReingestRequest(query="test", limit=1)
        assert req.limit == 1
        req = ReingestRequest(query="test", limit=100)
        assert req.limit == 100
        
        # Invalid bounds
        with pytest.raises(ValidationError):
            ReingestRequest(query="test", limit=0)
        with pytest.raises(ValidationError):
            ReingestRequest(query="test", limit=101)


class TestDrainRequest:
    """Test DrainRequest validation."""

    def test_defaults(self):
        from nervous_system.http.server import DrainRequest
        req = DrainRequest()
        assert req.work_pool == "default-pool"
        assert req.cancel_running is True
        assert req.dry_run is False

    def test_override_defaults(self):
        from nervous_system.http.server import DrainRequest
        req = DrainRequest(work_pool="custom-pool", cancel_running=False, dry_run=True)
        assert req.work_pool == "custom-pool"
        assert req.cancel_running is False
        assert req.dry_run is True


class TestResearchRequest:
    """Test ResearchRequest validation."""

    def test_required_query(self):
        from nervous_system.http.server import ResearchRequest
        with pytest.raises(Exception):
            ResearchRequest(num_sources=5)

    def test_num_sources_default(self):
        from nervous_system.http.server import ResearchRequest
        req = ResearchRequest(query="test topic")
        assert req.num_sources == 5

    def test_num_sources_bounds(self):
        from nervous_system.http.server import ResearchRequest
        from pydantic import ValidationError
        
        req = ResearchRequest(query="test", num_sources=1)
        assert req.num_sources == 1
        req = ResearchRequest(query="test", num_sources=20)
        assert req.num_sources == 20
        
        with pytest.raises(ValidationError):
            ResearchRequest(query="test", num_sources=0)
        with pytest.raises(ValidationError):
            ResearchRequest(query="test", num_sources=21)


class TestIngestTextRequest:
    """Test IngestTextRequest validation."""

    def test_required_content(self):
        from nervous_system.http.server import IngestTextRequest
        with pytest.raises(Exception):
            IngestTextRequest()

    def test_defaults(self):
        from nervous_system.http.server import IngestTextRequest
        req = IngestTextRequest(content="Hello world")
        assert req.content == "Hello world"
        assert req.title == ""
        assert req.source_type == "note"
        assert req.contributor == "user"
        assert req.privacy == "private"
        assert req.tags == ""
