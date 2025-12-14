"""Basic API security utilities (rate limiting, env validation)."""

from __future__ import annotations

import os
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Deque, Dict


@dataclass
class RateLimitConfig:
    requests_per_minute: int = 120
    window_seconds: int = 60


class RateLimiter:
    """Thread-safe sliding-window rate limiter keyed by client identifier."""

    def __init__(self, config: RateLimitConfig | None = None):
        self.config = config or RateLimitConfig()
        self._lock = threading.Lock()
        self._requests: Dict[str, Deque[float]] = defaultdict(deque)

    def is_allowed(self, client_id: str) -> bool:
        now = time.time()
        with self._lock:
            window = self._requests[client_id]
            # Drop timestamps outside the window
            cutoff = now - self.config.window_seconds
            while window and window[0] < cutoff:
                window.popleft()
            if len(window) >= self.config.requests_per_minute:
                return False
            window.append(now)
            return True

    @classmethod
    def from_env(cls) -> "RateLimiter":
        raw_limit = os.environ.get("RATE_LIMIT_PER_MINUTE")
        try:
            limit = int(raw_limit) if raw_limit else RateLimitConfig.requests_per_minute
            if limit <= 0:
                raise ValueError
        except Exception:
            limit = RateLimitConfig.requests_per_minute
        return cls(RateLimitConfig(requests_per_minute=limit))
