from __future__ import annotations
import logging
import threading
import time

import httpx

from pipeline.config import settings

logger = logging.getLogger(__name__)


class RateLimitedClient:
    """HTTP client with token-bucket rate limiting.

    Maintains a bucket of tokens replenished at `rate` per minute.
    Each GET request consumes one token. If the bucket is empty,
    the call blocks until a token is available.

    Usage:
        with RateLimitedClient() as client:
            data = client.get("https://...")
    """

    def __init__(self, rate: int = settings.API_RATE_LIMIT) -> None:
        self._rate = rate
        self._tokens = float(rate)
        self._lock = threading.Lock()
        self._last_refill = time.monotonic()
        self._client = httpx.Client(
            timeout=httpx.Timeout(30.0),
            headers={
                "X-API-KEY": settings.sportsdb_api_key,
                "Content-Type": "application/json",
            },
        )

    def _refill(self) -> None:
        """Add tokens proportional to elapsed time since last refill."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            float(self._rate),
            self._tokens + elapsed * (self._rate / 60.0),
        )
        self._last_refill = now

    def get(self, url: str) -> dict:
        """Make a rate-limited GET request. Returns parsed JSON.

        Blocks if no tokens are available. Raises on HTTP errors.
        """
        with self._lock:
            self._refill()
            if self._tokens < 1:
                wait = (1 - self._tokens) * (60.0 / self._rate)
                logger.debug("Rate limit: sleeping %.2fs", wait)
                time.sleep(wait)
                self._refill()
            self._tokens -= 1

        logger.debug("GET %s", url)
        response = self._client.get(url)
        response.raise_for_status()
        return response.json()

    def __enter__(self) -> "RateLimitedClient":
        return self

    def __exit__(self, *_) -> None:
        self._client.close()
