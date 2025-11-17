"""
Shared helpers for configuring CORS across the Flask and FastAPI entrypoints.
"""
from __future__ import annotations

import os
from typing import List, Sequence, Tuple

DEFAULT_EXPLICIT_ORIGINS: Sequence[str] = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:4173",
    "http://localhost:5173",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:4173",
    "http://127.0.0.1:5173",
]

# Allow any Vercel preview/production deployment by default so that hosted
# frontends can communicate with the API without additional configuration.
DEFAULT_REGEX_ORIGINS: Sequence[str] = [r"https://(.+\.)?vercel\.app"]


def _split_env_list(raw_value: str | None) -> List[str]:
    if raw_value is None:
        return []
    return [entry.strip() for entry in raw_value.split(",") if entry.strip()]


def get_cors_settings() -> Tuple[List[str], List[str]]:
    """
    Returns the explicit origins and regex-based origins allowed by the server.
    Environment variables can override both lists:

    * CORS_ALLOW_ORIGINS controls the explicit list (comma-separated).
    * CORS_ALLOW_ORIGIN_REGEXES controls regex patterns (comma-separated).
    """
    raw_explicit = os.environ.get("CORS_ALLOW_ORIGINS")
    explicit_candidates = _split_env_list(raw_explicit)
    if raw_explicit is None:
        explicit = list(DEFAULT_EXPLICIT_ORIGINS)
    elif explicit_candidates:
        explicit = explicit_candidates
    else:
        # Treat blank/whitespace-only env vars as "use defaults" to avoid
        # accidentally stripping the safe local/Vercel origins.
        explicit = list(DEFAULT_EXPLICIT_ORIGINS)

    raw_regexes = os.environ.get("CORS_ALLOW_ORIGIN_REGEXES")
    regex_candidates = _split_env_list(raw_regexes)
    if raw_regexes is None:
        regexes = list(DEFAULT_REGEX_ORIGINS)
    elif regex_candidates:
        regexes = regex_candidates
    else:
        regexes = list(DEFAULT_REGEX_ORIGINS)

    # Drop obvious wildcards; explicit '*' entries should live in the explicit list.
    regexes = [pattern for pattern in regexes if pattern and pattern != "*"]

    return explicit, regexes


def combine_regex_patterns(patterns: Sequence[str]) -> str | None:
    """
    Returns a single non-capturing regex that matches any of the supplied patterns.
    FastAPI's CORSMiddleware accepts only one regex, so the helper consolidates
    multiple entries when needed.
    """
    if not patterns:
        return None
    wrapped_patterns = [f"(?:{pattern})" for pattern in patterns]
    return "|".join(wrapped_patterns)
