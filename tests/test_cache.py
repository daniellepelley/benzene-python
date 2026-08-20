"""The cache surface — in-memory round-trips, TTL expiry, cache-aside, and the Redis backend.

The in-memory cache drives an injectable clock so TTL expiry is asserted without sleeping, cache-aside
is checked for loading exactly once against both sync and async loaders, and the Redis backend is
exercised against a small in-memory fake client — no redis, no network — asserting the JSON round-trip
and that TTL is passed through.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any

import pytest
from benzene.cache import CacheAside, InMemoryCache, RedisCache, get_or_load


def run(coro):
    return asyncio.run(coro)


class ManualClock:
    """A clock the test advances by hand — ``clock()`` reads seconds, ``clock.advance(dt)`` moves it."""

    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, dt: float) -> None:
        self.now += dt


class FakeRedis:
    """An in-memory stand-in for a ``redis.asyncio`` client, recording ``ex``/``px`` on set.

    Only the ``get`` / ``set`` / ``delete`` methods :class:`RedisCache` calls are implemented, and
    values are stored as the raw JSON strings the cache writes — so a test can assert the wire form.
    """

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.set_calls: list[tuple[str, str, int | None, int | None]] = []

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(
        self,
        key: str,
        value: str,
        *,
        ex: int | None = None,
        px: int | None = None,
    ) -> None:
        self.store[key] = value
        self.set_calls.append((key, value, ex, px))

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)


# --- InMemoryCache round-trips -------------------------------------------------------------------


def test_in_memory_get_set_delete_round_trip() -> None:
    cache = InMemoryCache()

    assert run(cache.get("missing")) is None

    run(cache.set("k", {"a": 1}))
    assert run(cache.get("k")) == {"a": 1}

    run(cache.delete("k"))
    assert run(cache.get("k")) is None

    # Deleting an absent key is a no-op, not an error.
    run(cache.delete("k"))


def test_in_memory_ttl_expires_on_the_injected_clock() -> None:
    clock = ManualClock()
    cache = InMemoryCache(clock=clock)

    run(cache.set("k", "v", ttl=10))
    assert run(cache.get("k")) == "v"

    clock.advance(9)
    assert run(cache.get("k")) == "v"  # still inside the window

    clock.advance(1)  # now at the expiry instant
    assert run(cache.get("k")) is None  # expired without sleeping


def test_in_memory_no_ttl_is_kept() -> None:
    clock = ManualClock()
    cache = InMemoryCache(clock=clock)

    run(cache.set("k", "v"))  # no ttl -> forever
    clock.advance(1_000_000)
    assert run(cache.get("k")) == "v"


# --- cache-aside ---------------------------------------------------------------------------------


def test_get_or_load_calls_loader_once_then_caches() -> None:
    cache = InMemoryCache()
    calls = 0

    def loader() -> str:
        nonlocal calls
        calls += 1
        return "loaded"

    first = run(get_or_load(cache, "k", loader))
    assert first == "loaded"
    assert calls == 1

    second = run(get_or_load(cache, "k", loader))
    assert second == "loaded"
    assert calls == 1  # a cache hit — the loader did not run again


def test_get_or_load_accepts_an_async_loader() -> None:
    cache = InMemoryCache()
    calls = 0

    async def loader() -> dict[str, int]:
        nonlocal calls
        calls += 1
        return {"n": 42}

    assert run(get_or_load(cache, "k", loader)) == {"n": 42}
    assert run(get_or_load(cache, "k", loader)) == {"n": 42}
    assert calls == 1


def test_get_or_load_passes_ttl_through() -> None:
    clock = ManualClock()
    cache = InMemoryCache(clock=clock)
    calls = 0

    def loader() -> str:
        nonlocal calls
        calls += 1
        return "v"

    run(get_or_load(cache, "k", loader, ttl=5))
    clock.advance(5)
    run(get_or_load(cache, "k", loader, ttl=5))  # expired -> loads again
    assert calls == 2


def test_cache_aside_applies_its_default_ttl() -> None:
    clock = ManualClock()
    cache = InMemoryCache(clock=clock)
    aside = CacheAside(cache, default_ttl=5)
    calls = 0

    def loader() -> str:
        nonlocal calls
        calls += 1
        return "v"

    assert run(aside.get_or_load("k", loader)) == "v"
    assert calls == 1

    clock.advance(4)
    run(aside.get_or_load("k", loader))
    assert calls == 1  # still cached under the default ttl

    clock.advance(1)
    run(aside.get_or_load("k", loader))
    assert calls == 2  # default ttl elapsed


# --- RedisCache against a fake client ------------------------------------------------------------


def test_redis_cache_json_round_trip() -> None:
    fake = FakeRedis()
    cache = RedisCache(client=fake)

    value: dict[str, Any] = {"id": 7, "name": "widget"}
    run(cache.set("k", value))

    # Stored as a JSON string on the wire, not a Python object.
    assert fake.store["k"] == '{"id": 7, "name": "widget"}'
    # And parsed back out on get.
    assert run(cache.get("k")) == value


def test_redis_cache_miss_returns_none() -> None:
    cache = RedisCache(client=FakeRedis())
    assert run(cache.get("missing")) is None


def test_redis_cache_passes_ttl_as_ex() -> None:
    fake = FakeRedis()
    cache = RedisCache(client=fake)

    run(cache.set("k", "v", ttl=30))
    key, _value, ex, px = fake.set_calls[-1]
    assert key == "k"
    assert ex == 30
    assert px is None


def test_redis_cache_subsecond_ttl_uses_px() -> None:
    fake = FakeRedis()
    cache = RedisCache(client=fake)

    run(cache.set("k", "v", ttl=0.25))
    _key, _value, ex, px = fake.set_calls[-1]
    assert ex is None
    assert px == 250


def test_redis_cache_no_ttl_sets_without_expiry() -> None:
    fake = FakeRedis()
    cache = RedisCache(client=fake)

    run(cache.set("k", "v"))
    _key, _value, ex, px = fake.set_calls[-1]
    assert ex is None
    assert px is None


def test_redis_cache_delete_evicts() -> None:
    fake = FakeRedis()
    cache = RedisCache(client=fake)

    run(cache.set("k", "v"))
    run(cache.delete("k"))
    assert run(cache.get("k")) is None


def test_redis_cache_requires_url_or_client() -> None:
    try:
        RedisCache()
    except ValueError:
        pass
    else:  # pragma: no cover - the constructor must reject an empty construction
        raise AssertionError("RedisCache() should require a url or client")


def test_redis_cache_rounds_a_subsecond_ttl_up_to_at_least_one_millisecond() -> None:
    fake = FakeRedis()
    cache = RedisCache(client=fake)

    # 0.4ms rounds *up* to 1ms: `px=0` is rejected by Redis ("invalid expire time"), and a TTL that
    # was asked for must never become "no expiry at all".
    run(cache.set("k", "v", ttl=0.0004))
    _key, _value, ex, px = fake.set_calls[-1]
    assert ex is None
    assert px == 1


def test_redis_cache_rounds_a_fractional_ttl_up_not_down() -> None:
    fake = FakeRedis()
    cache = RedisCache(client=fake)

    # 1.9s must not silently become 1s — a truncated TTL expires the entry roughly half a second
    # early, which is a correctness bug in the caller's cache, not a rounding detail.
    run(cache.set("k", "v", ttl=1.9))
    _key, _value, ex, px = fake.set_calls[-1]
    assert ex == 2
    assert px is None


def test_redis_cache_missing_sdk_raises_a_teaching_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A forgotten `[redis]` extra is a deployment error: it must fail loudly at construction with a
    # message naming the extra, never surface later as a mysterious attribute error.
    monkeypatch.setitem(sys.modules, "redis", None)
    with pytest.raises(ImportError, match=r"benzene-cache\[redis\]"):
        RedisCache("redis://localhost")
