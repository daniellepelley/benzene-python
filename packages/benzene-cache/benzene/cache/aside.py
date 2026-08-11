"""Cache-aside — read-through-on-miss over any :class:`Cache` (mirrors ``Benzene.Cache.Core``).

Cache-aside is the workhorse caching pattern: look in the cache, and only on a miss run the expensive
loader, store what it returns, and hand it back. :func:`get_or_load` is that pattern in one call;
:class:`CacheAside` binds a cache and a default TTL so callers stop repeating them.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from .cache import Cache

#: A loader is sync-or-async — a plain callable or a coroutine function; both are awaited transparently.
Loader = Callable[[], "Any | Awaitable[Any]"]


async def get_or_load(
    cache: Cache,
    key: str,
    loader: Loader,
    *,
    ttl: float | None = None,
) -> Any:
    """Return the cached value for ``key``, else load it, store it, and return it (cache-aside).

    On a hit the value comes straight from ``cache`` and ``loader`` is never called. On a miss
    ``loader`` runs — it may be sync or async (an awaitable return is awaited) — and its value is
    written back under ``key`` with ``ttl`` before being returned. Note that ``None`` is the cache's
    miss sentinel (:meth:`Cache.get` returns ``None`` for an absent key), so a loader that yields
    ``None`` is returned to *this* caller but re-loads on the next call rather than replaying.
    """
    cached = await cache.get(key)
    if cached is not None:
        return cached
    loaded = loader()
    if inspect.isawaitable(loaded):
        loaded = await loaded
    await cache.set(key, loaded, ttl=ttl)
    return loaded


class CacheAside:
    """A :class:`Cache` bound to a default TTL, exposing :meth:`get_or_load`.

    A thin convenience over :func:`get_or_load` for the common case of one cache and one TTL — the
    per-call ``ttl`` still wins when given, otherwise the bound ``default_ttl`` applies.
    """

    def __init__(self, cache: Cache, *, default_ttl: float | None = None) -> None:
        self._cache = cache
        self._default_ttl = default_ttl

    async def get_or_load(
        self,
        key: str,
        loader: Loader,
        *,
        ttl: float | None = None,
    ) -> Any:
        """Cache-aside through the bound cache, defaulting ``ttl`` to the instance's ``default_ttl``."""
        return await get_or_load(
            self._cache,
            key,
            loader,
            ttl=self._default_ttl if ttl is None else ttl,
        )
