"""Content classification utilities."""
from __future__ import annotations


def detect_source_type(url: str) -> str:
    """Detect the source type from a URL.
    
    Returns one of: tweet, github, video, paper, newsletter, article
    """
    url_lower = url.lower()
    
    # Social / microblogging
    if "twitter.com" in url_lower or "x.com" in url_lower:
        return "tweet"
    if "threadreaderapp.com" in url_lower:
        return "tweet"
    
    # Code
    if "github.com" in url_lower:
        return "github"
    
    # Video
    if "youtube.com" in url_lower or "youtu.be" in url_lower:
        return "video"
    
    # Papers / PDFs
    if "arxiv.org" in url_lower:
        return "paper"
    if url_lower.endswith(".pdf"):
        return "paper"
    
    # Newsletters
    if ".substack.com" in url_lower:
        return "newsletter"
    
    # Default to article
    return "article"
