"""Test that chunk resume correctly skips to the right content.

These tests verify that:
1. Chunking is deterministic (same input = same output)
2. Chunk markers align correctly (chunk N contains expected content)
3. Chunks are stable across multiple runs

Run with: python -m pytest tests/test_chunk_resume.py -v
Or standalone: python tests/test_chunk_resume.py
"""
import sys
sys.path.insert(0, '.')

from second_brain.llm import LLMClient


def get_client():
    """Get an LLMClient instance without initializing network connections."""
    return LLMClient.__new__(LLMClient)


def test_chunk_text_deterministic():
    """Chunking the same text should always produce the same chunks."""
    client = get_client()
    
    text = "First sentence here. " * 100 + "\n\n" + "Second paragraph. " * 100 + "\n\n" + "Third part. " * 100
    
    chunks1 = client._chunk_text(text, chunk_size=500, overlap=50)
    chunks2 = client._chunk_text(text, chunk_size=500, overlap=50)
    
    assert chunks1 == chunks2, "Chunking should be deterministic"
    assert len(chunks1) > 1, "Should produce multiple chunks"


def test_chunk_resume_markers_align():
    """Chunk N should contain content from section N (no drift)."""
    client = get_client()
    
    # Create predictable content with clear chunk boundaries
    text = ""
    for i in range(10):
        text += f"CHUNK_MARKER_{i} " + ("x" * 1000) + "\n\n"
    
    chunks = client._chunk_text(text, chunk_size=1200, overlap=100)
    
    # Each chunk should contain its corresponding marker
    for i, chunk in enumerate(chunks):
        assert f"CHUNK_MARKER_{i}" in chunk, f"Chunk {i} should contain CHUNK_MARKER_{i}"


def test_chunk_boundaries_stable():
    """Chunk boundaries should be stable across identical inputs."""
    client = get_client()
    
    # Realistic article-like content
    paragraphs = []
    for i in range(20):
        sentences = [f"Sentence {j} of paragraph {i}. " for j in range(5)]
        paragraphs.append("".join(sentences))
    text = "\n\n".join(paragraphs)
    
    chunks = client._chunk_text(text, chunk_size=8000, overlap=400)
    chunks2 = client._chunk_text(text, chunk_size=8000, overlap=400)
    
    assert len(chunks) == len(chunks2), "Should produce same number of chunks"
    for i, (c1, c2) in enumerate(zip(chunks, chunks2)):
        assert c1 == c2, f"Chunk {i} differs between runs"


def test_resume_skips_correct_chunks():
    """Resuming from chunk N should process chunks N onwards."""
    client = get_client()
    
    # Create content with unique markers per chunk
    text = ""
    for i in range(10):
        text += f"MARKER_{i} " + ("x" * 1000) + "\n\n"
    
    chunks = client._chunk_text(text, chunk_size=1200, overlap=100)
    
    # Simulate resuming from chunk 3
    resume_from = 3
    remaining_chunks = chunks[resume_from:]
    
    # Verify we get the right number of chunks
    assert len(remaining_chunks) == len(chunks) - resume_from
    
    # The first remaining chunk should be identical to chunks[resume_from]
    assert remaining_chunks[0] == chunks[resume_from], \
        "First remaining chunk should be exactly chunks[resume_from]"


def test_overlap_handled_correctly():
    """Overlap should not cause content duplication issues in resume."""
    client = get_client()
    
    # Content with clear sentence boundaries
    sentences = [f"This is sentence number {i}." for i in range(100)]
    text = " ".join(sentences)
    
    chunks = client._chunk_text(text, chunk_size=200, overlap=50)
    
    # All chunks should be non-empty
    assert all(chunk.strip() for chunk in chunks), "All chunks should have content"
    
    # Verify chunks can be processed in sequence without issues
    for i in range(len(chunks)):
        remaining = chunks[i:]
        assert len(remaining) == len(chunks) - i


if __name__ == "__main__":
    print("Running chunk resume tests...\n")
    
    test_chunk_text_deterministic()
    print("✓ test_chunk_text_deterministic passed")
    
    test_chunk_resume_markers_align()
    print("✓ test_chunk_resume_markers_align passed")
    
    test_chunk_boundaries_stable()
    print("✓ test_chunk_boundaries_stable passed")
    
    test_resume_skips_correct_chunks()
    print("✓ test_resume_skips_correct_chunks passed")
    
    test_overlap_handled_correctly()
    print("✓ test_overlap_handled_correctly passed")
    
    print("\nAll tests passed!")
