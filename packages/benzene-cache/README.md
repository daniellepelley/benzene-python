# benzene-cache

Caching for [Benzene Python](https://github.com/daniellepelley/benzene-python): a **cache-aside**
abstraction over one narrow async `Cache` port, with an in-memory backend for tests and single-process
services and a **Redis** backend for a shared store. Depends only on `benzene-core`.

```bash
pip install benzene-cache          # in-memory only
pip install benzene-cache[redis]   # + the redis.asyncio backend
```

`Cache` is three async methods keyed on a string — `get`, `set` (with an optional TTL), `delete` — and
every backend is interchangeable behind it. `get_or_load` is the cache-aside workhorse: return the
cached value, or run the loader once, store it, and hand it back.

```python
from benzene.cache import InMemoryCache, RedisCache, get_or_load, CacheAside

cache = InMemoryCache()  # or RedisCache("redis://localhost")


async def load_profile() -> dict:  # the loader may be sync or async
    return await profiles.fetch(user_id)


# Cache-aside: hits the cache, and only runs the loader on a miss (then caches for 300s).
profile = await get_or_load(cache, f"profile:{user_id}", load_profile, ttl=300)

# Bind a cache + default TTL when you're repeating them.
profiles_cache = CacheAside(cache, default_ttl=300)
profile = await profiles_cache.get_or_load(f"profile:{user_id}", load_profile)
```

- **`Cache`** — the abstraction (mirrors `Benzene.Cache.Core`): `async get(key) -> Any | None`,
  `async set(key, value, *, ttl=None)`, `async delete(key)`. A miss returns `None` and never raises.
- **`InMemoryCache`** — a dict with optional per-entry TTL and an injectable `clock`, so expiry is
  asserted without sleeping. The same shape as `benzene.resilience`'s `InMemoryIdempotencyStore`.
- **`RedisCache`** — a `Cache` over `redis.asyncio` (mirrors `Benzene.Cache.Redis`). Values round-trip
  through JSON (`benzene.core.encode_body` on the way in, `json.loads` on the way out) and TTL maps to
  `SET ... EX`/`PX`. The `redis` SDK is imported lazily, and an already-built client can be injected —
  so it is exercised against an in-memory fake with no SDK and no network.
- **`get_or_load` / `CacheAside`** — read-through-on-miss over any `Cache`; the loader is sync or async
  and runs exactly once per miss.

Nothing here needs Redis or a real clock to test: the in-memory cache takes an injectable `clock` and
the Redis cache takes an injected client, so the whole surface runs deterministically in memory.
Mirrors .NET's `Benzene.Cache.Core` and `Benzene.Cache.Redis`, and contributes the `benzene.cache`
subpackage to the shared `benzene` namespace.
