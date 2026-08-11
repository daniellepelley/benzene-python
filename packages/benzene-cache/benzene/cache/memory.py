"""In-memory cache — a process-local :class:`Cache` (mirrors an in-memory ``IDistributedCache``).

For tests and single-process services: a dict with optional per-entry TTL and an injectable clock, so
expiry is asserted without sleeping. The same shape as
:class:`benzene.resilience.InMemoryIdempotencyStore` — a network-backed :class:`benzene.cache.RedisCache`
slots in behind the identical three async methods when a shared store is needed.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

Clock = Callable[[], float]


class InMemoryCache:
    """A process-local :class:`Cache` with an optional per-entry TTL.

    Suitable for tests and single-instance services. ``ttl`` on :meth:`set` (seconds) bounds how long a
    key lives; ``None`` keeps it for the process lifetime. ``clock`` is injectable so a test expires
    entries without sleeping — pass a manual clock and advance it by hand.
    """

    def __init__(self, *, clock: Clock = time.monotonic) -> None:
        self._clock = clock
        self._entries: dict[str, tuple[Any, float | None]] = {}

    async def get(self, key: str) -> Any | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at is not None and self._clock() >= expires_at:
            del self._entries[key]
            return None
        return value

    async def set(self, key: str, value: Any, *, ttl: float | None = None) -> None:
        expires_at = None if ttl is None else self._clock() + ttl
        self._entries[key] = (value, expires_at)

    async def delete(self, key: str) -> None:
        self._entries.pop(key, None)
