"""Anthropic API client.

Thin wrapper around the official anthropic SDK.
Used by second_brain/llm.py for text generation.
"""
from __future__ import annotations

import logging

import anthropic

logger = logging.getLogger(__name__)


class AnthropicClient:
    """Client for Anthropic Messages API."""

    def __init__(self, api_key: str):
        self.client = anthropic.Anthropic(api_key=api_key)

    def chat(
        self,
        model: str,
        messages: list[dict[str, str]],
        *,
        max_tokens: int = 2048,
        timeout: int = 90,
    ) -> str:
        """Generate a chat completion.

        Args:
            model: Model name (e.g., "claude-sonnet-4-6")
            messages: List of {"role": "user"|"assistant", "content": "..."}
            max_tokens: Maximum tokens to generate
            timeout: Request timeout in seconds

        Returns:
            Generated text content
        """
        response = self.client.messages.create(
            model=model,
            max_tokens=max_tokens,
            messages=messages,
            timeout=timeout,
        )
        # Extract text from response
        text_parts = [
            block.text
            for block in response.content
            if hasattr(block, "text")
        ]
        return "\n".join(text_parts).strip()
