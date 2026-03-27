"""Tests for API endpoint contracts and response models."""
from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch


class TestHealthEndpoint:
    """Test /health endpoint."""

    def test_health_returns_ok(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"

    def test_health_response_model(self, client):
        """Health response should match HealthResponse model."""
        response = client.get("/health")
        data = response.json()
        assert "status" in data
        # Should only have fields from HealthResponse
        assert set(data.keys()) == {"status"}


class TestPydanticModels:
    """Test that Pydantic models are correctly defined."""

    def test_article_response_fields(self):
        from nervous_system.http.server import ArticleResponse
        fields = ArticleResponse.model_fields
        assert "id" in fields
        assert "url" in fields
        assert "title" in fields
        assert "summary" in fields
        assert "tags" in fields
        assert "score_usefulness" in fields

    def test_ingest_created_response_fields(self):
        from nervous_system.http.server import IngestCreatedResponse
        fields = IngestCreatedResponse.model_fields
        assert "status" in fields
        assert "url" in fields
        assert "ingest_id" in fields
        assert "flow_run_id" in fields

    def test_dispatch_response_fields(self):
        from nervous_system.http.server import DispatchResponse
        fields = DispatchResponse.model_fields
        assert "status" in fields
        assert "flow_run_id" in fields

    def test_drain_response_fields(self):
        from nervous_system.http.server import DrainResponse
        fields = DrainResponse.model_fields
        assert "status" in fields
        assert "work_pool" in fields
        assert "cancelled" in fields
        assert "by_state" in fields


class TestRequestModels:
    """Test that request models validate correctly."""

    def test_save_content_request_requires_url(self):
        from nervous_system.http.server import SaveContentRequest
        from pydantic import ValidationError
        import pytest
        # URL is required and must be non-empty
        with pytest.raises(ValidationError):
            SaveContentRequest(force=True)
        with pytest.raises(ValidationError):
            SaveContentRequest(url="", force=True)
        # Valid request
        req = SaveContentRequest(url="https://example.com", force=True)
        assert req.url == "https://example.com"
        assert req.force is True

    def test_reingest_request_validation(self):
        from nervous_system.http.server import ReingestRequest
        req = ReingestRequest(query="test", limit=50)
        assert req.query == "test"
        assert req.limit == 50

    def test_reingest_request_limit_bounds(self):
        from nervous_system.http.server import ReingestRequest
        # Should enforce ge=1, le=100
        with pytest.raises(Exception):  # ValidationError
            ReingestRequest(query="test", limit=0)
        with pytest.raises(Exception):
            ReingestRequest(query="test", limit=101)

    def test_drain_request_defaults(self):
        from nervous_system.http.server import DrainRequest
        req = DrainRequest()
        assert req.work_pool == "default-pool"
        assert req.cancel_running is True
        assert req.dry_run is False


class TestOpenApiTags:
    """Test that endpoints have proper OpenAPI tags."""

    def test_endpoints_have_tags(self, client):
        """All endpoints should be tagged for OpenAPI grouping."""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        spec = response.json()
        
        # Check that tags are defined
        assert "tags" in spec
        tag_names = [t["name"] for t in spec["tags"]]
        assert "health" in tag_names
        assert "articles" in tag_names
        assert "ingests" in tag_names
        assert "repos" in tag_names

    def test_health_endpoint_tagged(self, client):
        """Health endpoint should have 'health' tag."""
        response = client.get("/openapi.json")
        spec = response.json()
        health_path = spec["paths"].get("/health", {})
        if "get" in health_path:
            tags = health_path["get"].get("tags", [])
            assert "health" in tags


class TestErrorHandling:
    """Test error handling patterns."""

    def test_not_found_returns_404(self, client):
        """Nonexistent endpoints should return 404."""
        response = client.get("/nonexistent/endpoint")
        assert response.status_code == 404

    def test_invalid_method_returns_405(self, client):
        """Wrong HTTP method should return 405."""
        response = client.delete("/health")
        assert response.status_code == 405
