"""Lightweight structured logging for API processes."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        payload: Dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Attach common attributes when present
        for attr in ("request_path", "method", "status_code", "latency_ms", "client"):
            value = getattr(record, attr, None)
            if value is not None:
                payload[attr] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=True)


def setup_logging(default_level: str | int = logging.INFO) -> None:
    """Configure root logger for JSON output; reuse existing handlers when present."""
    level = os.environ.get("LOG_LEVEL", default_level)
    root = logging.getLogger()
    root.setLevel(level)

    # Avoid duplicating handlers if this is called more than once (e.g., in tests)
    if any(isinstance(handler, logging.StreamHandler) for handler in root.handlers):
        for handler in root.handlers:
            handler.setFormatter(JsonFormatter())
        return

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
