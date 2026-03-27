from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests


class ReadwiseClient:
    def __init__(self, api_token: str, base_url: str = "https://readwise.io/api/v3") -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Token {api_token}",
                "Content-Type": "application/json",
            }
        )

    def save_url(self, url: str, note: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"url": url}
        if note:
            payload["notes"] = note
        response = self.session.post(f"{self.base_url}/save/", json=payload, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_document(self, doc_id: str) -> dict[str, Any]:
        response = self.session.get(
            f"{self.base_url}/list/",
            params={"id": doc_id, "withHtmlContent": "true", "page_size": 1},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        results = payload.get("results", [])
        if not results:
            raise RuntimeError(f"Readwise document {doc_id} not found")
        return results[0]

    def list_recent_documents(self, days: int = 7, page_size: int = 100) -> list[dict[str, Any]]:
        response = self.session.get(
            f"{self.base_url}/list/",
            params={"page_size": page_size},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        results = payload.get("results", [])

        now = datetime.now(timezone.utc)
        cutoff = now.timestamp() - (days * 24 * 60 * 60)

        recent = []
        for item in results:
            created_at = item.get("created_at")
            if not created_at:
                continue
            try:
                ts = datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp()
            except ValueError:
                continue
            if ts >= cutoff:
                recent.append(item)
        return recent
