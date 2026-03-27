"""Tests for domain allowlist logic in ingest_url.py."""
import pytest

from orchestration.flows.ingest_url import _is_allowed_url


class TestIsAllowedUrl:
    """Test domain allowlist filtering."""

    def test_empty_allowlist_blocks_all(self):
        """Empty allowlist = fail-safe, block everything."""
        allowlist: frozenset[str] = frozenset()
        assert _is_allowed_url("https://github.com/foo", allowlist) is False
        assert _is_allowed_url("https://x.com/user", allowlist) is False

    def test_matching_domain_allowed(self):
        allowlist = frozenset(["github.com", "x.com"])
        assert _is_allowed_url("https://github.com/owner/repo", allowlist) is True
        assert _is_allowed_url("https://x.com/user/status/123", allowlist) is True

    def test_non_matching_domain_blocked(self):
        allowlist = frozenset(["github.com"])
        assert _is_allowed_url("https://x.com/user/status/123", allowlist) is False
        assert _is_allowed_url("https://reddit.com/r/python", allowlist) is False

    def test_subdomain_not_matched(self):
        """Subdomains require explicit allowlist entry (exact match)."""
        allowlist = frozenset(["github.com"])
        # gist.github.com != github.com — need to add explicitly
        assert _is_allowed_url("https://gist.github.com/user/abc", allowlist) is False
        
        # With explicit subdomain in allowlist
        allowlist_with_gist = frozenset(["github.com", "gist.github.com"])
        assert _is_allowed_url("https://gist.github.com/user/abc", allowlist_with_gist) is True

    def test_www_stripped(self):
        """www. prefix should be stripped before matching."""
        allowlist = frozenset(["youtube.com"])
        assert _is_allowed_url("https://www.youtube.com/watch?v=abc", allowlist) is True

    def test_case_insensitive(self):
        allowlist = frozenset(["github.com"])
        assert _is_allowed_url("https://GitHub.com/owner/repo", allowlist) is True
        assert _is_allowed_url("https://GITHUB.COM/owner/repo", allowlist) is True

    def test_invalid_url_blocked(self):
        """Malformed URLs are blocked."""
        allowlist = frozenset(["github.com"])
        assert _is_allowed_url("not a url", allowlist) is False
        assert _is_allowed_url("", allowlist) is False
