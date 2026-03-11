"""
Copyright (c) 2024 Aiven Ltd
See LICENSE for details

Unit tests for API middlewares
"""

import pytest

from karapace.api.middlewares import _should_skip_auth


class TestShouldSkipAuth:
    """Tests for the _should_skip_auth function"""

    # Backward compatibility - Exact path matching
    def test_should_skip_auth_exact_match_health_endpoint(self):
        """Test exact match for /_health endpoint (backward compatibility)"""
        assert _should_skip_auth("/_health", ["/_health", "/metrics"]) is True

    def test_should_skip_auth_exact_match_metrics_endpoint(self):
        """Test exact match for /metrics endpoint (backward compatibility)"""
        assert _should_skip_auth("/metrics", ["/_health", "/metrics"]) is True

    def test_should_skip_auth_exact_match_not_found(self):
        """Test exact match returns False when path not in list"""
        assert _should_skip_auth("/subjects", ["/_health", "/metrics"]) is False

    def test_should_skip_auth_exact_match_multiple_patterns(self):
        """Test exact match with multiple patterns"""
        patterns = ["/api/health", "/api/metrics", "/api/status"]
        assert _should_skip_auth("/api/health", patterns) is True
        assert _should_skip_auth("/api/metrics", patterns) is True
        assert _should_skip_auth("/api/status", patterns) is True
        assert _should_skip_auth("/api/other", patterns) is False

    # NEW FEATURE: Regex pattern matching
    def test_should_skip_auth_regex_simple_pattern(self):
        """Test simple regex pattern matching"""
        assert _should_skip_auth("/api/v1/health", ["^/api/v1/.*"]) is True
        assert _should_skip_auth("/api/v2/health", ["^/api/v1/.*"]) is False

    def test_should_skip_auth_regex_versioned_api(self):
        """Test regex pattern for versioned APIs"""
        pattern = "/api/v[0-9]+/subjects/.*"
        assert _should_skip_auth("/api/v1/subjects/test", [pattern]) is True
        assert _should_skip_auth("/api/v2/subjects/test-subject", [pattern]) is True
        assert _should_skip_auth("/api/v99/subjects/foo", [pattern]) is True
        assert _should_skip_auth("/api/vX/subjects/test", [pattern]) is False

    def test_should_skip_auth_regex_wildcard_pattern(self):
        """Test wildcard regex patterns"""
        assert _should_skip_auth("/internal/any/path", ["/internal/.*"]) is True
        # Note: /internal/ matches the pattern as  prefix
        assert _should_skip_auth("/internal/", ["/internal/.*"]) is True  # .* matches zero or more
        assert _should_skip_auth("/internal/x", ["/internal/.*"]) is True
        assert _should_skip_auth("/external/path", ["/internal/.*"]) is False

    def test_should_skip_auth_regex_anchored_pattern(self):
        """Test anchored regex patterns"""
        assert _should_skip_auth("/metrics", ["^/metrics$"]) is True
        assert _should_skip_auth("/metrics/detailed", ["^/metrics$"]) is False
        assert _should_skip_auth("prefix/metrics", ["^/metrics$"]) is False

    def test_should_skip_auth_regex_optional_groups(self):
        """Test regex with optional groups"""
        pattern = "/api/(v[0-9]+/)?health"
        assert _should_skip_auth("/api/health", [pattern]) is True
        assert _should_skip_auth("/api/v1/health", [pattern]) is True

    def test_should_skip_auth_regex_special_characters(self):
        """Test regex with special characters"""
        # Test query parameters
        pattern = "/path\\?.*"
        assert _should_skip_auth("/path?param=value", [pattern]) is True
        assert _should_skip_auth("/path", [pattern]) is False

    def test_should_skip_auth_multiple_regex_patterns(self):
        """Test multiple regex patterns"""
        patterns = ["/api/v[0-9]+/.*", "/internal/.*", "^/admin/.*"]
        assert _should_skip_auth("/api/v1/test", patterns) is True
        assert _should_skip_auth("/internal/service", patterns) is True
        assert _should_skip_auth("/admin/panel", patterns) is True
        assert _should_skip_auth("/public/resource", patterns) is False

    # NEW FEATURE: Invalid regex handling
    def test_should_skip_auth_invalid_regex_continues(self):
        """Test that invalid regex is skipped gracefully"""
        # Invalid regex should not crash, should continue to next pattern
        patterns = ["[invalid(regex", "/_health"]
        assert _should_skip_auth("/_health", patterns) is True

    def test_should_skip_auth_mixed_valid_invalid_regex(self):
        """Test mixed valid and invalid regex patterns"""
        patterns = ["[invalid(", "/api/v[0-9]+/.*", "((broken", "/metrics"]
        assert _should_skip_auth("/api/v1/test", patterns) is True
        assert _should_skip_auth("/metrics", patterns) is True
        assert _should_skip_auth("/other", patterns) is False

    def test_should_skip_auth_all_invalid_regex(self):
        """Test with all invalid regex patterns"""
        patterns = ["[invalid(", "((broken", "*unclosed"]
        assert _should_skip_auth("/_health", patterns) is False
        assert _should_skip_auth("/any/path", patterns) is False

    def test_should_skip_auth_invalid_regex_does_not_match_path(self):
        """Test that invalid regex doesn't accidentally match"""
        patterns = ["[invalid(regex"]
        assert _should_skip_auth("[invalid(regex", patterns) is True  # exact match
        assert _should_skip_auth("/some/path", patterns) is False

    # Edge cases
    def test_should_skip_auth_empty_pattern_list(self):
        """Test with empty pattern list"""
        assert _should_skip_auth("/_health", []) is False
        assert _should_skip_auth("/any/path", []) is False

    def test_should_skip_auth_empty_path(self):
        """Test with empty path"""
        assert _should_skip_auth("", ["/_health", "/metrics"]) is False
        assert _should_skip_auth("", [""]) is True  # exact match on empty
        assert _should_skip_auth("", ["^$"]) is True  # regex match on empty

    def test_should_skip_auth_path_with_trailing_slash(self):
        """Test paths with and without trailing slashes"""
        patterns = ["/health"]
        assert _should_skip_auth("/health", patterns) is True
        # With regex, /health pattern matches /health/ as prefix
        # If you want exact match, use anchored regex
        assert _should_skip_auth("/health/", patterns) is True  # regex matches as prefix

        # With regex to handle both explicitly
        patterns_regex = ["/health/?$"]  # anchored with optional slash
        assert _should_skip_auth("/health", patterns_regex) is True
        assert _should_skip_auth("/health/", patterns_regex) is True

    def test_should_skip_auth_case_sensitive(self):
        """Test that path matching is case sensitive"""
        patterns = ["/_health"]
        assert _should_skip_auth("/_health", patterns) is True
        assert _should_skip_auth("/_Health", patterns) is False
        assert _should_skip_auth("/_HEALTH", patterns) is False

    def test_should_skip_auth_pattern_priority_exact_before_regex(self):
        """Test that exact match is tried before regex"""
        # When path matches exactly, it should return True immediately
        # This is implicit in the implementation order
        patterns = ["/_health", "^/.*"]
        assert _should_skip_auth("/_health", patterns) is True

    # Real-world patterns
    def test_should_skip_auth_common_health_check_patterns(self):
        """Test common health check endpoints"""
        patterns = ["/_health", "/health", "/healthz", "/ready", "/alive"]
        assert _should_skip_auth("/_health", patterns) is True
        assert _should_skip_auth("/health", patterns) is True
        assert _should_skip_auth("/healthz", patterns) is True
        assert _should_skip_auth("/ready", patterns) is True
        assert _should_skip_auth("/alive", patterns) is True

    def test_should_skip_auth_monitoring_endpoints(self):
        """Test common monitoring endpoints"""
        patterns = ["/metrics", "/prometheus", "^/actuator/.*"]
        assert _should_skip_auth("/metrics", patterns) is True
        assert _should_skip_auth("/prometheus", patterns) is True
        assert _should_skip_auth("/actuator/health", patterns) is True
        assert _should_skip_auth("/actuator/info", patterns) is True
        assert _should_skip_auth("/actuator", patterns) is False

    def test_should_skip_auth_api_versioning(self):
        """Test API versioning patterns"""
        patterns = ["/api/v[0-9]+/(health|metrics|status)"]
        assert _should_skip_auth("/api/v1/health", patterns) is True
        assert _should_skip_auth("/api/v2/metrics", patterns) is True
        assert _should_skip_auth("/api/v3/status", patterns) is True
        assert _should_skip_auth("/api/v1/subjects", patterns) is False

    def test_should_skip_auth_path_with_query_parameters(self):
        """Test paths that might include query parameters"""
        # Exact match won't work with query params
        patterns = ["/_health"]
        # Note: re.match matches from start, so /_health matches /_health?... as prefix
        assert _should_skip_auth("/_health?detail=true", patterns) is True  # regex prefix match

        # Regex can handle query params explicitly with anchored pattern
        patterns_regex = ["^/_health(\\?.*)?$"]
        assert _should_skip_auth("/_health", patterns_regex) is True
        assert _should_skip_auth("/_health?detail=true", patterns_regex) is True

    def test_should_skip_auth_unicode_paths(self):
        """Test Unicode characters in paths"""
        patterns = ["/健康", "/健康/.*"]
        assert _should_skip_auth("/健康", patterns) is True
        assert _should_skip_auth("/健康/詳細", patterns) is True

    def test_should_skip_auth_url_encoded_paths(self):
        """Test URL-encoded paths"""
        # Exact match on encoded path
        patterns = ["/path%20with%20spaces"]
        assert _should_skip_auth("/path%20with%20spaces", patterns) is True
        assert _should_skip_auth("/path with spaces", patterns) is False

    def test_should_skip_auth_default_config_patterns(self):
        """Test with default configuration patterns"""
        default_patterns = ["/_health", "/metrics"]
        assert _should_skip_auth("/_health", default_patterns) is True
        assert _should_skip_auth("/metrics", default_patterns) is True
        assert _should_skip_auth("/subjects", default_patterns) is False
        assert _should_skip_auth("/config", default_patterns) is False
