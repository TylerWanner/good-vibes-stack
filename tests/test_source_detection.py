"""Tests for URL and source type detection."""
from __future__ import annotations

import pytest


class TestSourceTypeDetection:
    """Test that URLs are correctly classified by source type."""

    def test_youtube_detection(self):
        from second_brain.classify import detect_source_type
        assert detect_source_type("https://www.youtube.com/watch?v=abc123") == "video"
        assert detect_source_type("https://youtu.be/abc123") == "video"
        assert detect_source_type("https://youtube.com/shorts/abc") == "video"

    def test_twitter_detection(self):
        from second_brain.classify import detect_source_type
        assert detect_source_type("https://twitter.com/user/status/123") == "tweet"
        assert detect_source_type("https://x.com/user/status/123") == "tweet"

    def test_github_detection(self):
        from second_brain.classify import detect_source_type
        assert detect_source_type("https://github.com/owner/repo") == "github"
        assert detect_source_type("https://github.com/owner/repo/blob/main/file.py") == "github"

    def test_arxiv_detection(self):
        from second_brain.classify import detect_source_type
        assert detect_source_type("https://arxiv.org/abs/2301.00001") == "paper"
        assert detect_source_type("https://arxiv.org/pdf/2301.00001.pdf") == "paper"

    def test_pdf_detection(self):
        from second_brain.classify import detect_source_type
        assert detect_source_type("https://example.com/paper.pdf") == "paper"
        assert detect_source_type("https://example.com/doc.PDF") == "paper"

    def test_substack_detection(self):
        from second_brain.classify import detect_source_type
        assert detect_source_type("https://example.substack.com/p/article") == "newsletter"

    def test_default_article(self):
        from second_brain.classify import detect_source_type
        assert detect_source_type("https://example.com/blog/post") == "article"
        assert detect_source_type("https://news.ycombinator.com/item?id=123") == "article"


class TestUrlNormalization:
    """Test URL normalization and cleaning."""

    def test_strip_twitter_tracking(self):
        from shared.url_utils import strip_tracking_params
        url = "https://x.com/user/status/123?s=46&t=abc"
        assert strip_tracking_params(url) == "https://x.com/user/status/123"

    def test_strip_twitter_tracking_twitter_domain(self):
        from shared.url_utils import strip_tracking_params
        url = "https://twitter.com/user/status/123?s=20"
        assert strip_tracking_params(url) == "https://twitter.com/user/status/123"

    def test_preserve_non_twitter_params(self):
        from shared.url_utils import strip_tracking_params
        url = "https://example.com/page?id=123&ref=abc"
        # Non-twitter URLs should be unchanged
        assert strip_tracking_params(url) == url

    def test_handle_empty_url(self):
        from shared.url_utils import strip_tracking_params
        assert strip_tracking_params("") == ""


class TestGithubUrlValidation:
    """Test GitHub URL validation."""

    def test_valid_github_repo(self):
        from integrations.github import is_github_repo_url
        assert is_github_repo_url("https://github.com/owner/repo") is True
        assert is_github_repo_url("https://github.com/owner/repo/") is True

    def test_github_with_tree_path(self):
        from integrations.github import is_github_repo_url
        assert is_github_repo_url("https://github.com/owner/repo/tree/main") is True
        assert is_github_repo_url("https://github.com/owner/repo/tree/feature/branch") is True

    def test_github_with_blob_path(self):
        from integrations.github import is_github_repo_url
        assert is_github_repo_url("https://github.com/owner/repo/blob/main/file.py") is True

    def test_github_with_commits_path(self):
        from integrations.github import is_github_repo_url
        assert is_github_repo_url("https://github.com/owner/repo/commits/main") is True

    def test_github_issues_rejected(self):
        from integrations.github import is_github_repo_url
        assert is_github_repo_url("https://github.com/owner/repo/issues") is False
        assert is_github_repo_url("https://github.com/owner/repo/issues/123") is False

    def test_github_pull_requests_rejected(self):
        from integrations.github import is_github_repo_url
        assert is_github_repo_url("https://github.com/owner/repo/pull/123") is False
        assert is_github_repo_url("https://github.com/owner/repo/pulls") is False

    def test_github_actions_rejected(self):
        from integrations.github import is_github_repo_url
        assert is_github_repo_url("https://github.com/owner/repo/actions") is False

    def test_github_settings_rejected(self):
        from integrations.github import is_github_repo_url
        assert is_github_repo_url("https://github.com/owner/repo/settings") is False

    def test_invalid_github_urls(self):
        from integrations.github import is_github_repo_url
        assert is_github_repo_url("https://github.com") is False
        assert is_github_repo_url("https://github.com/owner") is False
        assert is_github_repo_url("https://example.com/owner/repo") is False
        assert is_github_repo_url("not a url") is False
