import pytest

from src import cors_config


@pytest.fixture(autouse=True)
def clear_cors_env(monkeypatch):
    """Ensure each test starts without CORS-specific environment variables."""
    monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)
    monkeypatch.delenv("CORS_ALLOW_ORIGIN_REGEXES", raising=False)


def test_defaults_include_localhost_and_vercel():
    explicit, regex = cors_config.get_cors_settings()

    assert "http://localhost" in explicit
    assert cors_config.DEFAULT_REGEX_ORIGINS[0] in regex


def test_blank_regex_env_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("CORS_ALLOW_ORIGIN_REGEXES", "   ")

    _, regex = cors_config.get_cors_settings()

    assert regex == list(cors_config.DEFAULT_REGEX_ORIGINS)


def test_blank_explicit_env_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("CORS_ALLOW_ORIGINS", "")

    explicit, _ = cors_config.get_cors_settings()

    assert explicit == list(cors_config.DEFAULT_EXPLICIT_ORIGINS)


def test_custom_env_values_override_defaults(monkeypatch):
    monkeypatch.setenv("CORS_ALLOW_ORIGINS", "https://example.com, https://api.example.com")
    monkeypatch.setenv("CORS_ALLOW_ORIGIN_REGEXES", "https://(.+\\.)?example\\.com")

    explicit, regex = cors_config.get_cors_settings()

    assert explicit == ["https://example.com", "https://api.example.com"]
    assert regex == ["https://(.+\\.)?example\\.com"]
