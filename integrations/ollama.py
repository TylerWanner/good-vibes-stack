"""Ollama API client.

Low-level HTTP client for Ollama's REST API.
Used by second_brain/llm.py for text generation and embeddings.
"""
from __future__ import annotations

import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)


class OllamaClient:
    """HTTP client for Ollama API."""

    def __init__(self, base_url: str = "http://127.0.0.1:11434"):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()

    def chat(
        self,
        model: str,
        messages: list[dict[str, Any]],
        *,
        format: str | None = None,
        temperature: float = 0.2,
        max_tokens: int = 2048,
        timeout: int = 480,
    ) -> str:
        """Generate a chat completion.

        Args:
            model: Model name (e.g., "llama3", "mistral")
            messages: List of Ollama chat message dicts. For multimodal models,
                a message may also include an ``images`` array of base64 strings.
            format: Optional output format ("json" for JSON mode)
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate
            timeout: Request timeout in seconds

        Returns:
            Generated text content
        """
        payload: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
                "num_ctx": 10240,  # qwen2.5:14b supports 32k natively; 10240 balances context headroom vs memory pressure with Docker Desktop 10GB VM on 24GB unified memory
            },
        }
        if format:
            payload["format"] = format

        prompt_preview = messages[-1]["content"][:100] if messages else "(no messages)"
        try:
            response = self.session.post(
                f"{self.base_url}/api/chat",
                json=payload,
                timeout=timeout,
            )
            response.raise_for_status()
        except requests.exceptions.ReadTimeout:
            raise RuntimeError(
                f"Ollama timeout after {timeout}s on model={model}. Prompt: {prompt_preview}..."
            ) from None
        except requests.exceptions.ConnectionError:
            raise RuntimeError(
                f"Ollama connection failed at {self.base_url}. Is Ollama running?"
            ) from None
        except requests.exceptions.HTTPError as e:
            raise RuntimeError(
                f"Ollama HTTP {e.response.status_code} on model={model}. Prompt: {prompt_preview}..."
            ) from None
        result = response.json()
        message = result.get("message", {})
        return (message.get("content") or "").strip()

    def embed(
        self,
        text: str,
        model: str = "nomic-embed-text",
        timeout: int = 30,
    ) -> list[float] | None:
        """Generate embedding vector for text.

        Args:
            text: Text to embed
            model: Embedding model name
            timeout: Request timeout in seconds

        Returns:
            Embedding vector (768 dims for nomic-embed-text), or None on failure
        """
        if not text or not text.strip():
            return None

        try:
            response = self.session.post(
                f"{self.base_url}/api/embeddings",
                json={"model": model, "prompt": text},
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()["embedding"]
        except Exception as e:
            logger.warning("Ollama embedding failed: %s", e)
            return None
