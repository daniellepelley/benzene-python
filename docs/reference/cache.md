# `benzene.cache`

A cache-aside abstraction over one narrow async `Cache` port, with an in-memory backend for tests and
single-process services and a **Redis** backend for a shared store. **Distribution: `benzene-cache`
(depends only on `benzene-core`).**

```bash
pip install benzene-cache            # in-memory only
pip install "benzene-cache[redis]"   # + the redis.asyncio backend
```

## Overview

Caching in Benzene is one narrow port and the read-through pattern over it. `Cache` is three async
methods keyed on a string — `get` / `set` (with an optional TTL) / `delete` — and every backend is
interchangeable behind it: `InMemoryCache` (a dict with an injectable clock) and `RedisCache` (a shared
store over `redis.asyncio`, values round-tripped through JSON) present the identical surface.

Reach for it when a value is expensive to produce and cheap to reuse — a downstream lookup, a rendered
projection, a token — and staleness within a TTL is acceptable. `get_or_load` is the cache-aside
workhorse: return the cached value, or run the loader (sync *or* async) once, store it, and hand it
back; `CacheAside` binds a cache and default TTL so callers stop repeating them.

The `redis` SDK is an optional `[redis]` extra imported lazily, so importing this package and
exercising it in memory needs no SDK and no network. Mirrors .NET's `Benzene.Cache.Core` (the
abstraction) and `Benzene.Cache.Redis` (the backend).

## The `Cache` port

```python
from typing import Any, Protocol, runtime_checkable

@runtime_checkable
class Cache(Protocol):
    async def get(self, key: str) -> Any | None: ...
    async def set(self, key: str, value: Any, *, ttl: float | None = None) -> None: ...
    async def delete(self, key: str) -> None: ...
```

- `get(key)` returns the stored value, or `None` on a miss — it **never raises for a miss**.
- `set(key, value, *, ttl=None)` stores a value; `ttl` (seconds) bounds its lifetime, `None` is
  forever.
- `delete(key)` evicts a key; a no-op if it is already absent.

Values are arbitrary, but a serializing backend (`RedisCache`) round-trips them through JSON, so store
what JSON can carry. `Cache` is `runtime_checkable`, so `isinstance(obj, Cache)` recognises any duck.

## `InMemoryCache`

A process-local `Cache` for tests and single-instance services: a dict with an optional per-entry TTL
and an injectable clock, so expiry is asserted without sleeping.

```python
from benzene.cache import InMemoryCache

cache = InMemoryCache()                    # or InMemoryCache(clock=manual_clock)
await cache.set("profile:42", {"name": "Alice"}, ttl=300)
await cache.get("profile:42")              # {"name": "Alice"}
await cache.get("missing")                 # None
```

```python
InMemoryCache(*, clock: Clock = time.monotonic)     # Clock = Callable[[], float]
```

`ttl` is per-`set`; pass a manual `clock` and advance it by hand to expire entries deterministically in
a test. It is the same shape as `benzene.resilience`'s `InMemoryIdempotencyStore`.

## `RedisCache`

A `Cache` over `redis.asyncio` for a store shared across instances. Values round-trip through JSON
(`benzene.core.encode_body` on the way in — the same wire-naming entry point outbound clients use —
`json.loads` on the way out), and TTL maps to Redis' native `SET ... EX`/`PX`.

```python
from benzene.cache import RedisCache

cache = RedisCache("redis://localhost")    # the redis SDK is imported lazily here
# or inject an already-built client (this is how tests use an in-memory fake):
cache = RedisCache(client=redis_client)
```

```python
RedisCache(url: str | None = None, *, client: Any | None = None)
```

- Construct from a connection `url` (the `redis` SDK is imported lazily then), or hand it an
  already-built async `client`. Passing neither raises `ValueError`.
- Only the client's `get` / `set` / `delete` are used, so any duck-typed stand-in works — that is how
  the whole surface runs against an in-memory fake with no SDK and no network.
- TTL maps to `SET ... EX` for whole seconds and `SET ... PX` for sub-second values (mirroring the .NET
  options).

## Cache-aside — `get_or_load` and `CacheAside`

Cache-aside is the workhorse pattern: look in the cache, and only on a miss run the expensive loader,
store what it returns, and hand it back.

```python
from benzene.cache import InMemoryCache, get_or_load, CacheAside

cache = InMemoryCache()

async def load_profile() -> dict:          # the loader may be sync or async
    return await profiles.fetch(user_id)

# Hits the cache; only runs the loader on a miss, then caches it for 300s.
profile = await get_or_load(cache, f"profile:{user_id}", load_profile, ttl=300)

# Bind a cache + default TTL when you're repeating them.
profiles_cache = CacheAside(cache, default_ttl=300)
profile = await profiles_cache.get_or_load(f"profile:{user_id}", load_profile)
```

```python
async def get_or_load(cache: Cache, key: str, loader: Loader, *, ttl: float | None = None) -> Any
```

- On a **hit** the value comes straight from the cache and `loader` is never called.
- On a **miss** `loader` runs (a plain callable or a coroutine function — an awaitable return is
  awaited transparently), its value is written back under `key` with `ttl`, and returned.

`CacheAside(cache, *, default_ttl=None)` is a thin convenience for one cache and one TTL; its
`get_or_load(key, loader, *, ttl=None)` defaults `ttl` to `default_ttl`, and a per-call `ttl` still
wins when given.

### Caveat: a cached `None` is the miss sentinel

`Cache.get` returns `None` for an absent key, so **`None` is indistinguishable from a miss**. A loader
that yields `None` is returned to *this* caller, but it is not cached as a hit — the next call re-runs
the loader rather than replaying `None`. If "not found" is a valid, cacheable answer for you, cache a
sentinel value (for example `{"found": False}`) instead of `None`.

## Troubleshooting

- **`ImportError` / `ModuleNotFoundError` for `redis`** — the `[redis]` extra is not installed. Run
  `pip install "benzene-cache[redis]"`, or inject a client with `RedisCache(client=...)`.
- **`ValueError: RedisCache requires either a url or an injected client`** — you constructed
  `RedisCache()` with no arguments; pass a URL or a client.
- **A loader keeps running on every call** — its value is `None` (the miss sentinel, above), or the
  backend's TTL has already elapsed. Cache a non-`None` sentinel for the "empty" case.

## Exports

`Cache`, `InMemoryCache`, `RedisCache`, `get_or_load`, `CacheAside`.

## See also

- [`benzene.core`](core.md) — `encode_body`, the wire-naming JSON encoder `RedisCache` serializes with.
- [`benzene.resilience`](resilience.md) — `InMemoryIdempotencyStore`, the same injectable-clock shape
  as `InMemoryCache`.
