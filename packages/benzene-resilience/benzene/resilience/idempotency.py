"""Idempotency — dedupe redelivered messages (mirrors ``Benzene.Idempotency``).

At-least-once transports (SQS, Pub/Sub, Kafka, Service Bus) redeliver: the same logical message can
reach the handler twice. Idempotency middleware makes the *second* delivery a no-op by keying each
invocation on a caller-supplied idempotency key (a header) and **replaying the first result** instead
of running the handler again — so "charge the card" or "ship the order" happens once even though the
message arrived twice.

Only outcomes matching ``remember_when`` are stored (successes, by default): a transient failure is
*not* remembered, so a redelivery is free to retry it — dedupe must never pin a failure in place. A
message with no idempotency key passes straight through (nothing to dedupe on).

The store is a pluggable port (:class:`IdempotencyStore`); an in-memory implementation ships here for
tests and single-process services, and a shared backend (e.g. Redis, once ``benzene-cache`` lands)
slots in behind the same two async methods.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Protocol

from benzene.core import Context, Middleware, Next
from benzene.results import Result

Clock = Callable[[], float]

#: Header names tried in order to find an invocation's idempotency key.
DEFAULT_KEY_HEADERS: tuple[str, ...] = ("idempotency-key", "message-id")


class IdempotencyStore(Protocol):
    """A keyed store of prior results — the seam a shared backend implements.

    Both methods are async so a network-backed store (Redis, DynamoDB) is a drop-in; the in-memory
    implementation simply completes synchronously.
    """

    async def get(self, key: str) -> Result | None: ...
    async def put(self, key: str, result: Result) -> None: ...


class InMemoryIdempotencyStore:
    """A process-local :class:`IdempotencyStore` with an optional per-entry TTL.

    Suitable for tests and single-instance services. ``ttl`` (seconds) bounds how long a key is
    remembered; ``None`` keeps entries for the process lifetime. ``clock`` is injectable so a test
    expires entries without sleeping.
    """

    def __init__(self, *, ttl: float | None = None, clock: Clock = time.monotonic) -> None:
        self._ttl = ttl
        self._clock = clock
        self._entries: dict[str, tuple[Result, float | None]] = {}

    async def get(self, key: str) -> Result | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        result, expires_at = entry
        if expires_at is not None and self._clock() >= expires_at:
            del self._entries[key]
            return None
        return result

    async def put(self, key: str, result: Result) -> None:
        expires_at = None if self._ttl is None else self._clock() + self._ttl
        self._entries[key] = (result, expires_at)


def _key_of(headers: dict[str, str], candidates: Sequence[str]) -> str | None:
    for name in candidates:
        value = headers.get(name.lower())
        if value:
            return value
    return None


def idempotency(
    store: IdempotencyStore,
    *,
    key_headers: Sequence[str] = DEFAULT_KEY_HEADERS,
    remember_when: Callable[[Result], bool] | None = None,
) -> Middleware:
    """Middleware that replays the stored result for a repeated idempotency key.

    The key is read from the first present of ``key_headers``. A first delivery runs the handler and,
    if its result satisfies ``remember_when`` (default: the result is successful), stores it; a repeat
    delivery short-circuits with the stored result and never touches the handler. A keyless message is
    passed through unchanged. Install it ahead of the message router.
    """
    should_remember = remember_when or (lambda result: result.is_successful)

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        key = _key_of(context.headers, key_headers)
        if key is None:
            await next()
            return
        remembered = await store.get(key)
        if remembered is not None:
            context.result = remembered  # replay: the handler does not run a second time
            return
        await next()
        result = context.result if context.result is not None else Result.ok()
        if should_remember(result):
            await store.put(key, result)

    return middleware
