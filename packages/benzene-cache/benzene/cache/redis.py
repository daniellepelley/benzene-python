"""Redis-backed cache — a shared :class:`Cache` over ``redis.asyncio`` (mirrors ``Benzene.Cache.Redis``).

The distributed backend behind the :class:`benzene.cache.Cache` port: values are serialized to JSON on
the way in (via :func:`benzene.core.encode_body`, the same wire-naming entry point outbound clients use)
and parsed back on the way out, and TTL is honoured with Redis' native ``SET ... EX``. The ``redis``
package is an optional extra imported lazily inside the methods, so importing this module — and running
the tests against an injected fake client — needs no SDK and no network.
"""

from __future__ import annotations

import json
import math
from typing import Any

from benzene.core import encode_body


def _client_from(url: str) -> Any:
    """Build a ``redis.asyncio`` client for ``url``, importing the optional SDK lazily.

    Isolated so the import cost — and the ``redis`` dependency — is paid only when a caller actually
    constructs a :class:`RedisCache` from a URL rather than injecting their own client. A missing SDK
    is a *deployment* error, not a cache outcome, so it is re-raised as an ImportError naming the exact
    extra to install (the same guard the other optional-dependency sites use).
    """
    try:
        from redis.asyncio import Redis  # noqa: PLC0415 - lazy: optional [redis] extra
    except ImportError as exc:
        raise ImportError(
            "RedisCache requires redis — install it with 'pip install benzene-cache[redis]'."
        ) from exc

    return Redis.from_url(url)


class RedisCache:
    """A :class:`Cache` over ``redis.asyncio`` (mirrors ``Benzene.Cache.Redis``).

    Construct it from a connection URL (``RedisCache("redis://localhost")``) — the ``redis`` SDK is
    imported lazily then — or hand it an already-built async client (``RedisCache(client=...)``), which
    is how the tests inject an in-memory fake. Values round-trip through JSON: :meth:`set` encodes with
    :func:`benzene.core.encode_body`, :meth:`get` parses with :func:`json.loads` and returns ``None``
    for a missing key. Only the ``get`` / ``set`` / ``delete`` methods of the client are used, so any
    duck-typed stand-in works.
    """

    def __init__(self, url: str | None = None, *, client: Any | None = None) -> None:
        if client is None and url is None:
            raise ValueError("RedisCache requires either a url or an injected client")
        self._client = client if client is not None else _client_from(url or "")

    async def get(self, key: str) -> Any | None:
        raw = await self._client.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    async def set(self, key: str, value: Any, *, ttl: float | None = None) -> None:
        payload = encode_body(value)
        if ttl is None:
            await self._client.set(key, payload)
        else:
            # Sub-second TTLs use PX (milliseconds); whole seconds use EX, mirroring the .NET options.
            # Both round *up* (never down, never to zero): Redis rejects ``px=0``/``ex=0`` outright,
            # and a truncated TTL would expire a value earlier than the caller asked for.
            if ttl < 1:
                await self._client.set(key, payload, px=max(1, math.ceil(ttl * 1000)))
            else:
                await self._client.set(key, payload, ex=math.ceil(ttl))

    async def delete(self, key: str) -> None:
        await self._client.delete(key)
