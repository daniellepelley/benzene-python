"""``benzene.cache`` — a cache-aside abstraction plus a Redis backend.

Caching in Benzene is one narrow port and the read-through pattern over it. :class:`Cache` is three
async methods keyed on a string — ``get`` / ``set`` (with an optional TTL) / ``delete`` — and every
backend is interchangeable behind it: :class:`InMemoryCache` (a dict with an injectable clock, for
tests and single-process services) and :class:`RedisCache` (a shared store over ``redis.asyncio``,
values round-tripped through JSON) present the identical surface.

Reach for it when a value is expensive to produce and cheap to reuse — a downstream lookup, a rendered
projection, a token — and staleness within a TTL is acceptable. :func:`get_or_load` is the cache-aside
workhorse: return the cached value, or run the loader (sync *or* async) once, store it, and hand it
back; :class:`CacheAside` binds a cache and default TTL so callers stop repeating them.

The ``redis`` SDK is an optional ``[redis]`` extra imported lazily, so importing this package and
exercising it in memory needs no SDK and no network. Mirrors .NET's ``Benzene.Cache.Core`` (the
abstraction) and ``Benzene.Cache.Redis`` (the backend). Depends only on ``benzene-core``, and
contributes the ``benzene.cache`` subpackage to the shared ``benzene`` namespace.
"""

from __future__ import annotations

from .aside import CacheAside, get_or_load
from .cache import Cache
from .memory import InMemoryCache
from .redis import RedisCache

__all__ = [
    "Cache",
    "CacheAside",
    "InMemoryCache",
    "RedisCache",
    "get_or_load",
]
