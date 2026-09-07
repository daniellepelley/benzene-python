"""A shared :class:`~benzene.resilience.IdempotencyStore` over Redis — cross-instance dedupe.

:class:`~benzene.resilience.InMemoryIdempotencyStore` is a dict, so on any multi-instance
deployment — two pods, two Lambda invocations, two SQS consumer replicas — the two deliveries of one
message dedupe against *different dictionaries* and both run the handler. Nothing errors, nothing
logs; the card is charged twice. This store is the shared backing that makes the middleware's
guarantee hold in the shape it is actually deployed in.

The whole of its correctness is one command: ``SET key value NX EX ttl``. Redis applies it
atomically, so of two deliveries racing for the same key exactly one gets a non-nil reply and runs
the handler — the reservation and its expiry are set together, and there is no window in which a
crash could leave a key claimed forever. It is emphatically **not** ``EXISTS`` (or ``GET``) followed
by ``SET``: two callers would read "absent" across the await between them and both would claim it,
which is the exact race the middleware reserves against.

A shared store **relocates** the race; it does not remove it. Two instances still both reach Redis,
and it is Redis' single-threaded conditional write — not the framework — that orders them. Handlers
should still be written to tolerate running twice: a store outage, a key that lapsed between the two
deliveries, or a redelivery beyond ``ttl`` all end with the handler running again.

``redis`` is an optional ``[redis]`` extra imported lazily, so importing this module and exercising
it against an injected fake needs no SDK and no network.
"""

from __future__ import annotations

import math
from typing import Any

from benzene.results import Result

from .result_codec import decode_result, encode_result

#: Key namespace, so an idempotency store and a :class:`benzene.cache.RedisCache` can share one Redis.
DEFAULT_PREFIX = "benzene:idem:"

#: One day — comfortably past the redelivery window of every at-least-once transport this port binds.
DEFAULT_TTL = 86_400.0


def _client_from(url: str) -> Any:
    """Build a ``redis.asyncio`` client for ``url``, importing the optional SDK lazily.

    A missing SDK is a *deployment* error, not a dedupe outcome: it must fail loudly at construction
    naming the exact extra, never surface later as a mysterious attribute error on a store that has
    silently stopped deduplicating. (The same guard :class:`benzene.cache.RedisCache` uses.)
    """
    try:
        from redis.asyncio import Redis  # noqa: PLC0415 - lazy: optional [redis] extra
    except ImportError as exc:
        raise ImportError(
            "RedisIdempotencyStore requires redis — install it with "
            "'pip install benzene-resilience[redis]'."
        ) from exc

    return Redis.from_url(url)


class RedisIdempotencyStore:
    """An :class:`~benzene.resilience.IdempotencyStore` over ``redis.asyncio``.

    Construct it from a connection URL (``RedisIdempotencyStore("redis://localhost")``) — the
    ``redis`` SDK is imported lazily then — or hand it an already-built async client
    (``RedisIdempotencyStore(client=...)``), which is how the tests inject an in-memory fake. Only
    the client's ``get`` / ``set`` / ``delete`` methods are used, so any duck-typed stand-in works::

        from benzene.resilience import RedisIdempotencyStore, idempotency_interception

        store = RedisIdempotencyStore("redis://cache:6379", ttl=3600)
        definition.middleware += [idempotency_interception(store)]

    ``ttl`` (seconds, default one day) is how long a key is remembered, and it must **exceed the
    transport's maximum redelivery window** — a redelivery that arrives after the key lapsed finds
    nothing and runs the handler again. ``None`` means no expiry, which leaks a key per message
    forever; pass it only for a store you prune yourself. Sub-second values are sent as ``PX``
    milliseconds and whole seconds as ``EX``, both rounded *up* (Redis rejects a zero expiry, and a
    truncated TTL would forget a key sooner than the caller asked).

    Values are the JSON of :func:`~benzene.resilience.encode_result`; see that module for what a
    stored result keeps and what it drops.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        client: Any | None = None,
        ttl: float | None = DEFAULT_TTL,
        prefix: str = DEFAULT_PREFIX,
    ) -> None:
        if client is None and url is None:
            raise ValueError("RedisIdempotencyStore requires either a url or an injected client")
        if ttl is not None and ttl <= 0:
            raise ValueError("RedisIdempotencyStore ttl must be positive (or None for no expiry)")
        self._client = client if client is not None else _client_from(url or "")
        self._ttl = ttl
        self._prefix = prefix

    async def get(self, key: str) -> Result | None:
        raw = await self._client.get(self._prefix + key)
        if raw is None:
            return None
        return decode_result(raw)

    async def put(self, key: str, result: Result) -> None:
        """Store ``result`` under ``key``, replacing any reservation and restarting the TTL."""
        await self._client.set(self._prefix + key, encode_result(result), **self._expiry())

    async def put_if_absent(self, key: str, result: Result) -> bool:
        """Claim ``key`` with one atomic ``SET ... NX``; ``True`` when this caller won it.

        Redis answers nil (``None`` in ``redis-py``) when ``NX`` finds the key already present, so
        the reply *is* the answer — no read is involved on either path.
        """
        reply = await self._client.set(
            self._prefix + key, encode_result(result), nx=True, **self._expiry()
        )
        return bool(reply)

    async def delete(self, key: str) -> None:
        await self._client.delete(self._prefix + key)

    def _expiry(self) -> dict[str, int]:
        """The expiry argument for ``SET`` — in the same command, never a follow-up ``EXPIRE``."""
        if self._ttl is None:
            return {}
        if self._ttl < 1:
            return {"px": max(1, math.ceil(self._ttl * 1000))}
        return {"ex": math.ceil(self._ttl)}
