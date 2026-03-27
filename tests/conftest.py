"""Shared pytest fixtures for the test suite."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    """Create a test client for the API."""
    # Import here to avoid import errors during collection
    from nervous_system.http.server import app
    return TestClient(app)
