"""Tests for shared/url_utils.py — URL manipulation utilities."""
import pytest

from shared.url_utils import strip_tracking_params


class TestStripTrackingParams:
    """Test Twitter/X tracking param removal."""

    def test_strips_twitter_query_params(self):
        url = "https://twitter.com/user/status/123?s=46&t=abc"
        assert strip_tracking_params(url) == "https://twitter.com/user/status/123"

    def test_strips_x_query_params(self):
        url = "https://x.com/user/status/123?s=20"
        assert strip_tracking_params(url) == "https://x.com/user/status/123"

    def test_strips_fragment(self):
        url = "https://x.com/user/status/123#section"
        assert strip_tracking_params(url) == "https://x.com/user/status/123"

    def test_strips_both_query_and_fragment(self):
        url = "https://twitter.com/user/status/123?s=46#top"
        assert strip_tracking_params(url) == "https://twitter.com/user/status/123"

    def test_preserves_non_twitter_urls(self):
        url = "https://github.com/owner/repo?tab=readme"
        assert strip_tracking_params(url) == url

    def test_preserves_youtube_params(self):
        # YouTube params are meaningful (video ID, timestamp)
        url = "https://youtube.com/watch?v=abc123&t=120"
        assert strip_tracking_params(url) == url

    def test_handles_url_without_params(self):
        url = "https://x.com/user/status/123"
        assert strip_tracking_params(url) == url

    def test_handles_mobile_twitter(self):
        url = "https://mobile.twitter.com/user/status/123?s=46"
        assert strip_tracking_params(url) == "https://mobile.twitter.com/user/status/123"
