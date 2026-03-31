"""Tests for LLMClient._chunk_text.

These are pure unit tests — no LLM calls, no network, no Prefect.
The chunker is pure Python logic and should be fast and deterministic.
"""
import pytest
from unittest.mock import MagicMock
from second_brain.llm import LLMClient


@pytest.fixture
def llm():
    """LLMClient with no real backends — only used for _chunk_text."""
    client = LLMClient.__new__(LLMClient)
    client._ollama = None
    client._anthropic = None
    client.model = "test"
    client.embedding_model = "test"
    return client


class TestChunkText:
    def test_short_text_single_chunk(self, llm):
        """Text shorter than chunk_size produces exactly one chunk."""
        text = "hello world"
        chunks = llm._chunk_text(text, chunk_size=8000, overlap=400)
        assert len(chunks) == 1
        assert chunks[0] == text

    def test_empty_text(self, llm):
        chunks = llm._chunk_text("", chunk_size=8000, overlap=400)
        assert chunks == []

    def test_exact_chunk_size(self, llm):
        """Text exactly at chunk_size produces one chunk, not two."""
        text = "a" * 8000
        chunks = llm._chunk_text(text, chunk_size=8000, overlap=400)
        assert len(chunks) == 1

    def test_motherduck_length_produces_few_chunks(self, llm):
        """22k chars at 8k chunk_size should produce ~3 chunks, not 403.

        This is the regression test for the infinite loop bug where
        break_at >= len(text) but the loop continued creating single-char chunks.
        """
        # Simulate motherduck article length with realistic content
        text = "word " * (22655 // 5)  # ~22k chars of word-like content
        chunks = llm._chunk_text(text, chunk_size=8000, overlap=400)
        assert len(chunks) <= 5, f"Expected ~3 chunks, got {len(chunks)} — infinite loop bug may be back"
        assert len(chunks) >= 2, f"Expected at least 2 chunks for 22k text, got {len(chunks)}"

    def test_no_duplicate_content_at_boundaries(self, llm):
        """Chunks should cover the full text without absurd overlap."""
        text = ("sentence one. sentence two. " * 500)[:20000]  # ~20k chars
        chunks = llm._chunk_text(text, chunk_size=8000, overlap=400)
        # Sanity: shouldn't create more chunks than chars / (chunk_size - overlap)
        max_reasonable = (len(text) // (8000 - 400)) + 2
        assert len(chunks) <= max_reasonable, (
            f"Too many chunks: {len(chunks)} (max reasonable: {max_reasonable}). "
            f"Likely infinite loop regression."
        )

    def test_chunks_cover_full_content(self, llm):
        """Every part of the text should appear in at least one chunk."""
        text = "abcdefghij" * 3000  # 30k chars
        chunks = llm._chunk_text(text, chunk_size=8000, overlap=400)
        # Check first and last chars appear
        assert text[:100] in "".join(chunks)
        assert text[-100:] in "".join(chunks)

    def test_paragraph_boundary_preferred(self, llm):
        """Chunker should prefer breaking at paragraph boundaries."""
        paragraph = "This is a sentence. " * 20  # ~400 chars per paragraph
        text = ("\n\n".join([paragraph] * 25))  # ~10k chars with clear boundaries
        chunks = llm._chunk_text(text, chunk_size=8000, overlap=400)
        # At least one chunk should end near a paragraph boundary
        for chunk in chunks[:-1]:  # all but last
            assert not chunk.endswith("This is a sentenc")  # shouldn't cut mid-word

    def test_terminates_for_pathological_input(self, llm):
        """No paragraph or sentence breaks — should still terminate."""
        text = "a" * 50000  # 50k chars, no breaks at all
        chunks = llm._chunk_text(text, chunk_size=8000, overlap=400)
        assert len(chunks) <= 10  # 50k / 8k = ~7 chunks expected
        assert len(chunks) >= 5
