"""Anthropic API client.

Thin wrapper around the official anthropic SDK.
Used by second_brain/llm.py for text generation.

Supports two auth modes:
- API key:    AnthropicClient(api_key="sk-ant-...")
- OAuth token: AnthropicClient(auth_token="Bearer ...")  (Claude Max / Claude Code OAuth)
"""
from __future__ import annotations

import logging

import anthropic

logger = logging.getLogger(__name__)


class AnthropicClient:
    """Client for Anthropic Messages API."""

    def __init__(self, api_key: str | None = None, *, auth_token: str | None = None):
        if auth_token:
            # Claude Max OAuth — strip "Bearer " prefix if present
            token = auth_token.removeprefix("Bearer ").strip()
            self.client = anthropic.Anthropic(api_key="placeholder", auth_token=token)
            logger.info("AnthropicClient using OAuth token (Claude Max)")
        elif api_key:
            self.client = anthropic.Anthropic(api_key=api_key)
        else:
            raise ValueError("Either api_key or auth_token must be provided")

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
        prompt_preview = messages[-1]["content"][:100] if messages else "(no messages)"
        try:
            response = self.client.messages.create(
                model=model,
                max_tokens=max_tokens,
                messages=messages,
                timeout=timeout,
            )
        except anthropic.APIConnectionError:
            raise RuntimeError("Anthropic connection failed. Check network/API status.") from None
        except anthropic.RateLimitError:
            raise RuntimeError("Anthropic rate limited. Try again later.") from None
        except anthropic.AuthenticationError:
            raise RuntimeError("Anthropic auth failed. Check API key or OAuth token.") from None
        except anthropic.APIStatusError as e:
            raise RuntimeError(
                f"Anthropic API error {e.status_code}: {str(e)[:100]}. Prompt: {prompt_preview}..."
            ) from None
        
        # Extract text from response
        text_parts = [
            block.text
            for block in response.content
            if hasattr(block, "text")
        ]
        return "\n".join(text_parts).strip()
