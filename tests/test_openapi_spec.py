"""Tests for OpenAPI specification quality and completeness."""
from __future__ import annotations

import pytest


class TestOpenApiTags:
    """Test that all endpoints are properly tagged."""

    def test_all_tag_groups_defined(self, client):
        """All expected tag groups should be defined in the spec."""
        response = client.get("/openapi.json")
        assert response.status_code == 200
        spec = response.json()
        
        tag_names = {t["name"] for t in spec.get("tags", [])}
        expected_tags = {"health", "articles", "ingests", "repos", "skills", "flows", "ops", "private"}
        
        for tag in expected_tags:
            assert tag in tag_names, f"Missing tag group: {tag}"

    def test_tags_have_descriptions(self, client):
        """Each tag group should have a description."""
        response = client.get("/openapi.json")
        spec = response.json()
        
        for tag in spec.get("tags", []):
            assert "description" in tag, f"Tag '{tag['name']}' missing description"
            assert len(tag["description"]) > 0, f"Tag '{tag['name']}' has empty description"


class TestOpenApiEndpoints:
    """Test that endpoints have proper documentation."""

    def test_all_endpoints_have_tags(self, client):
        """Every endpoint should have at least one tag."""
        response = client.get("/openapi.json")
        spec = response.json()
        
        untagged = []
        for path, methods in spec.get("paths", {}).items():
            for method, details in methods.items():
                if method in ("get", "post", "put", "patch", "delete"):
                    tags = details.get("tags", [])
                    if not tags:
                        untagged.append(f"{method.upper()} {path}")
        
        assert not untagged, f"Untagged endpoints: {untagged}"

    def test_endpoints_have_summaries(self, client):
        """Endpoints should have summary or description."""
        response = client.get("/openapi.json")
        spec = response.json()
        
        missing_docs = []
        for path, methods in spec.get("paths", {}).items():
            for method, details in methods.items():
                if method in ("get", "post", "put", "patch", "delete"):
                    has_summary = bool(details.get("summary"))
                    has_description = bool(details.get("description"))
                    if not (has_summary or has_description):
                        missing_docs.append(f"{method.upper()} {path}")
        
        # Allow some undocumented endpoints, but flag if too many
        assert len(missing_docs) < 10, f"Too many undocumented endpoints: {missing_docs}"


class TestOpenApiSchemas:
    """Test that response schemas are properly defined."""

    def test_response_models_defined(self, client):
        """Endpoints with response_model should have schema in spec."""
        response = client.get("/openapi.json")
        spec = response.json()
        
        schemas = spec.get("components", {}).get("schemas", {})
        
        # Check that our main response models exist
        expected_schemas = [
            "HealthResponse",
            "IngestCreatedResponse", 
            "DispatchResponse",
            "DrainResponse",
            "ReingestResponse",
        ]
        
        for schema_name in expected_schemas:
            assert schema_name in schemas, f"Missing schema: {schema_name}"

    def test_request_models_defined(self, client):
        """Request body models should have schema in spec."""
        response = client.get("/openapi.json")
        spec = response.json()
        
        schemas = spec.get("components", {}).get("schemas", {})
        
        expected_request_schemas = [
            "SaveContentRequest",
            "ReingestRequest",
            "DrainRequest",
            "ResearchRequest",
        ]
        
        for schema_name in expected_request_schemas:
            assert schema_name in schemas, f"Missing request schema: {schema_name}"


class TestOpenApiInfo:
    """Test API metadata."""

    def test_api_has_title(self, client):
        response = client.get("/openapi.json")
        spec = response.json()
        assert spec.get("info", {}).get("title") == "Nervous System API"

    def test_api_has_version(self, client):
        response = client.get("/openapi.json")
        spec = response.json()
        assert spec.get("info", {}).get("version") is not None

    def test_api_has_description(self, client):
        response = client.get("/openapi.json")
        spec = response.json()
        assert spec.get("info", {}).get("description") is not None
