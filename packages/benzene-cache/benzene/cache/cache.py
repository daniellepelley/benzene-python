"""The cache abstraction — a keyed async store (mirrors ``Benzene.Cache.Core``).

:class:`Cache` is the one port every backend implements: three async methods keyed on a string, with
an optional per-``set`` TTL. It is deliberately narrow — get, set, delete — so an in-memory dict
(:class:`benzene.cache.InMemoryCache`) and a shared Redis instance (:class:`benzene.cache.RedisCache`)
are interchangeable behind it, and the cache-aside helpers in :mod:`benzene.cache.aside` compose over
it without caring which one is wired in.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Cache(Protocol):
    """A keyed async cache — the seam a backend implements (mirrors ``Benzene.Cache.Core``).

    Every method is async so a network-backed store (Redis, Memcached) is a drop-in; an in-memory
    implementation simply completes synchronously. Values are arbitrary — a backend that serializes
    (:class:`benzene.cache.RedisCache`) round-trips them through JSON, so store what JSON can carry.
    """

    async def get(self, key: str) -> Any | None:
        """Return the value stored under ``key``, or ``None`` on a miss (never raises for a miss)."""
        ...

    async def set(self, key: str, value: Any, *, ttl: float | None = None) -> None:
        """Store ``value`` under ``key``; ``ttl`` (seconds) bounds its lifetime, ``None`` is forever."""
        ...

    async def delete(self, key: str) -> None:
        """Evict ``key``; a no-op if it is already absent."""
        ...
