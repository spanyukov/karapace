"""
Test config

Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

import os

from karapace.core.config import Config
from karapace.core.constants import DEFAULT_AIOHTTP_CLIENT_MAX_SIZE, DEFAULT_PRODUCER_MAX_REQUEST


def test_http_request_max_size() -> None:
    config = Config()
    config.karapace_rest = False
    config.producer_max_request_size = DEFAULT_PRODUCER_MAX_REQUEST + 1024
    assert config.get_max_request_size() == DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = Config()
    config.karapace_rest = False
    config.http_request_max_size = 1024
    assert config.get_max_request_size() == 1024

    config = Config()
    config.karapace_rest = True
    assert config.get_max_request_size() == DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = Config()
    config.karapace_rest = True
    config.producer_max_request_size = 1024
    assert config.get_max_request_size() == DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = Config()
    config.karapace_rest = True
    config.producer_max_request_size = DEFAULT_PRODUCER_MAX_REQUEST + 1024
    assert config.get_max_request_size() == DEFAULT_PRODUCER_MAX_REQUEST + 1024 + DEFAULT_AIOHTTP_CLIENT_MAX_SIZE

    config = Config()
    config.karapace_rest = True
    config.http_request_max_size = 1024
    config.producer_max_request_size = DEFAULT_PRODUCER_MAX_REQUEST + 1024
    assert config.get_max_request_size() == 1024


def test_sasl_oauthbearer_skip_auth_paths_default() -> None:
    """Test default value for sasl_oauthbearer_skip_auth_paths"""
    config = Config()
    assert config.sasl_oauthbearer_skip_auth_paths == ["/_health", "/metrics"]
    assert isinstance(config.sasl_oauthbearer_skip_auth_paths, list)
    assert len(config.sasl_oauthbearer_skip_auth_paths) == 2


def test_sasl_oauthbearer_skip_auth_paths_custom() -> None:
    """Test custom value for sasl_oauthbearer_skip_auth_paths"""
    custom_paths = ["/custom/health", "/custom/metrics", "/api/v1/.*"]
    config = Config(sasl_oauthbearer_skip_auth_paths=custom_paths)
    assert config.sasl_oauthbearer_skip_auth_paths == custom_paths
    assert len(config.sasl_oauthbearer_skip_auth_paths) == 3


def test_sasl_oauthbearer_skip_auth_paths_empty() -> None:
    """Test empty list for sasl_oauthbearer_skip_auth_paths"""
    config = Config(sasl_oauthbearer_skip_auth_paths=[])
    assert config.sasl_oauthbearer_skip_auth_paths == []
    assert isinstance(config.sasl_oauthbearer_skip_auth_paths, list)


def test_sasl_oauthbearer_skip_auth_paths_from_env(monkeypatch) -> None:
    """Test loading sasl_oauthbearer_skip_auth_paths from environment variable"""
    # Set environment variable
    monkeypatch.setenv("KARAPACE_SASL_OAUTHBEARER_SKIP_AUTH_PATHS", '["/_custom_health", "/custom_metrics"]')

    config = Config()
    assert config.sasl_oauthbearer_skip_auth_paths == ["/_custom_health", "/custom_metrics"]


def test_sasl_oauthbearer_skip_auth_paths_single_value_from_env(monkeypatch) -> None:
    """Test loading single value for sasl_oauthbearer_skip_auth_paths from environment"""
    monkeypatch.setenv("KARAPACE_SASL_OAUTHBEARER_SKIP_AUTH_PATHS", '["/_health_only"]')

    config = Config()
    assert config.sasl_oauthbearer_skip_auth_paths == ["/_health_only"]
    assert len(config.sasl_oauthbearer_skip_auth_paths) == 1


def test_sasl_oauthbearer_skip_auth_paths_regex_patterns() -> None:
    """Test regex patterns in sasl_oauthbearer_skip_auth_paths"""
    regex_patterns = ["/api/v[0-9]+/health", "^/internal/.*", "/metrics"]
    config = Config(sasl_oauthbearer_skip_auth_paths=regex_patterns)
    assert config.sasl_oauthbearer_skip_auth_paths == regex_patterns
    # Verify patterns are stored as strings (not compiled regex objects)
    for pattern in config.sasl_oauthbearer_skip_auth_paths:
        assert isinstance(pattern, str)
