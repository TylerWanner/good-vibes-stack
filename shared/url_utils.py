"""URL manipulation utilities."""
from urllib.parse import urlparse, urlunparse


def strip_tracking_params(url: str) -> str:
    """Strip query params and fragments from Twitter/X URLs.
    
    These platforms add tracking params like ?s=46 that create duplicate URLs
    for the same content. Other URLs are returned unchanged.
    """
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if any(h in host for h in ("x.com", "twitter.com")):
        return urlunparse(parsed._replace(query="", fragment=""))
    return url
