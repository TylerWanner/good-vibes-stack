"""Tests for LLM response parsing — JSON edge cases, score clamping, etc."""
from __future__ import annotations

import pytest


class TestScoreClamping:
    """Test that scores are clamped to valid 1-5 range."""

    def test_clamp_score_in_range(self):
        from second_brain.llm import _clamp_score
        assert _clamp_score(3) == 3
        assert _clamp_score(1) == 1
        assert _clamp_score(5) == 5

    def test_clamp_score_below_range(self):
        from second_brain.llm import _clamp_score
        assert _clamp_score(0) == 1
        assert _clamp_score(-5) == 1

    def test_clamp_score_above_range(self):
        from second_brain.llm import _clamp_score
        assert _clamp_score(6) == 5
        assert _clamp_score(100) == 5

    def test_clamp_score_none(self):
        from second_brain.llm import _clamp_score
        assert _clamp_score(None) is None

    def test_clamp_score_string(self):
        from second_brain.llm import _clamp_score
        # Should handle string numbers
        assert _clamp_score("3") == 3
        assert _clamp_score("0") == 1
        assert _clamp_score("10") == 5


class TestJsonParsing:
    """Test JSON extraction from LLM responses."""

    def test_extract_json_clean(self):
        from second_brain.llm import _extract_json
        result = _extract_json('{"summary": "test", "tags": ["a", "b"]}')
        assert result["summary"] == "test"
        assert result["tags"] == ["a", "b"]

    def test_extract_json_with_markdown(self):
        from second_brain.llm import _extract_json
        text = '''Here's the analysis:
```json
{"summary": "test", "tags": ["a"]}
```
That's my response.'''
        result = _extract_json(text)
        assert result["summary"] == "test"

    def test_extract_json_with_thinking(self):
        from second_brain.llm import _extract_json
        text = '''<think>Let me analyze this...</think>
{"summary": "after thinking", "tags": []}'''
        result = _extract_json(text)
        assert result["summary"] == "after thinking"

    def test_extract_json_nested_braces(self):
        from second_brain.llm import _extract_json
        text = '{"summary": "test {with} braces", "data": {"nested": true}}'
        result = _extract_json(text)
        assert result["summary"] == "test {with} braces"
        assert result["data"]["nested"] is True

    def test_extract_json_invalid_raises(self):
        from second_brain.llm import _extract_json
        with pytest.raises(RuntimeError, match="did not return valid JSON"):
            _extract_json("This is not JSON at all")

    def test_extract_json_empty_string_raises(self):
        from second_brain.llm import _extract_json
        with pytest.raises(RuntimeError, match="did not return valid JSON"):
            _extract_json("")


class TestProviderRouting:
    """Test that LLM provider routing works correctly."""

    def test_settings_has_llm_provider_field(self):
        """Verify the Settings dataclass has the expected LLM fields."""
        from shared.config import Settings
        import dataclasses
        field_names = {f.name for f in dataclasses.fields(Settings)}
        assert "llm_provider" in field_names
        assert "llm_model" in field_names
        assert "ollama_base_url" in field_names

    def test_llm_client_accepts_ollama_provider(self):
        """LLMClient should accept 'ollama' as a provider."""
        from second_brain.llm import LLMClient
        # Just verify construction doesn't raise
        client = LLMClient(provider="ollama", model="test", ollama_base_url="http://localhost:11434")
        assert client.provider == "ollama"

    def test_llm_client_accepts_anthropic_provider(self):
        """LLMClient should accept 'anthropic' as a provider."""
        from second_brain.llm import LLMClient
        client = LLMClient(provider="anthropic", model="test", anthropic_api_key="test-key")
        assert client.provider == "anthropic"
