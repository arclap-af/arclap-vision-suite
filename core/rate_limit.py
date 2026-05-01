"""In-memory token-bucket rate limiter.

Simple per-IP throttle to stop a misbehaving frontend (or one user
hammering F5) from accidentally DoSing the suite. Thread-safe, no
external dependency.

Default budget: 100 requests / second per IP, with a burst of 200.
Tighter for /api/upload* / /api/swiss/import-zip (10/s) — those touch
disk and YOLO inference.
"""
from __future__ import annotations

import time
from collections import defaultdict
from threading import Lock


class TokenBucket:
    __slots__ = ("rate", "burst", "tokens", "ts")

    def __init__(self, rate: float, burst: float):
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.ts = time.monotonic()

    def take(self, n: float = 1.0) -> bool:
        now = time.monotonic()
        elapsed = now - self.ts
        self.ts = now
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False


_buckets: dict[tuple[str, str], TokenBucket] = defaultdict(lambda: TokenBucket(100, 200))
_lock = Lock()

_HEAVY_PATHS = ("/api/upload", "/api/swiss/import-zip", "/api/datasets/upload",
                "/api/models/upload", "/api/upload-image", "/api/images/batch-upload")


def check(ip: str, path: str) -> tuple[bool, dict]:
    """Returns (allowed, headers). Headers are X-RateLimit-* style."""
    heavy = any(path.startswith(p) for p in _HEAVY_PATHS)
    key = (ip, "heavy" if heavy else "normal")
    with _lock:
        b = _buckets.get(key)
        if b is None:
            b = TokenBucket(rate=10, burst=20) if heavy else TokenBucket(rate=100, burst=200)
            _buckets[key] = b
        ok = b.take()
        remaining = max(0, int(b.tokens))
    return ok, {
        "X-RateLimit-Limit": str(int(b.burst)),
        "X-RateLimit-Remaining": str(remaining),
        "X-RateLimit-Bucket": "heavy" if heavy else "normal",
    }
