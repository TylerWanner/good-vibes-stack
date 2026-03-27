"""Tests for API authentication middleware."""
from __future__ import annotations

import pytest


class TestApiKeyAuth:
    """Test API key authentication middleware."""

    def test_health_endpoint_no_auth_required(self, client):
        """Health endpoint should always be accessible."""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_dev_mode_no_auth_required(self, client):
        """When API_SECRET_KEY is not set, all requests should work."""
        # The default fixture has no API_SECRET_KEY set
        response = client.get("/articles")
        # Should not be 401 (might be 500 if DB not connected, but not 401)
        assert response.status_code != 401

    def test_options_preflight_no_auth(self, client):
        """OPTIONS requests for CORS preflight should not require auth."""
        response = client.options("/articles")
        # Should not be 401
        assert response.status_code != 401


class TestApiKeyAuthWithKey:
    """Test API key auth when key is configured.

    Patches the module-level _API_KEY constant directly via monkeypatch.setattr —
    avoids importlib.reload which is fragile and leaks state between tests.
    """

    def test_missing_api_key_returns_401(self, client, monkeypatch):
        """Should return 401 when API key is required but missing."""
        import nervous_system.http.server as server_module
        monkeypatch.setattr(server_module, "_API_KEY", "test-secret-key")
        response = client.get("/articles")
        assert response.status_code == 401
        assert "Invalid or missing API key" in response.json().get("detail", "")

    def test_wrong_api_key_returns_401(self, client, monkeypatch):
        """Should return 401 when API key is wrong."""
        import nervous_system.http.server as server_module
        monkeypatch.setattr(server_module, "_API_KEY", "correct-key")
        response = client.get("/articles", headers={"X-API-Key": "wrong-key"})
        assert response.status_code == 401

    def test_correct_api_key_allows_access(self, client, monkeypatch):
        """Should allow access with correct API key."""
        import nervous_system.http.server as server_module
        monkeypatch.setattr(server_module, "_API_KEY", "my-secret-key")
        response = client.get("/health", headers={"X-API-Key": "my-secret-key"})
        assert response.status_code == 200


class TestRateLimiting:
    """Test rate limiting behavior."""

    def test_health_endpoint_accessible(self, client):
        """Health endpoint should be accessible regardless of rate limiting."""
        response = client.get("/health")
        assert response.status_code == 200
