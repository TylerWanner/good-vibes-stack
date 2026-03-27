from __future__ import annotations

from typing import Any

import requests


class ScraplingClient:
    """HTTP client for the scrapling-fetcher sidecar service."""

    # Give the client 15s more than the server's hard cap so we get the
    # server's 504 error rather than a client-side connection timeout.
    TIMEOUT_DYNAMIC = 75
    TIMEOUT_STEALTHY = 105

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def fetch(self, url: str, *, fetcher: str = "dynamic") -> dict[str, Any]:
        """Fetch a URL via the sidecar and return title + text.

        Args:
            url: The URL to fetch.
            fetcher: "dynamic" (default) for standard JS rendering,
                     "stealthy" for Cloudflare / anti-bot sites.

        Returns:
            dict with keys: url, title, text
        """
        timeout = self.TIMEOUT_STEALTHY if fetcher == "stealthy" else self.TIMEOUT_DYNAMIC
        response = self.session.post(
            f"{self.base_url}/fetch",
            json={"url": url, "fetcher": fetcher, "network_idle": True},
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()
