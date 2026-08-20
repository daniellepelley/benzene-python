"""Idempotency — dedupe redelivered messages (mirrors ``Benzene.Idempotency``).

At-least-once transports (SQS, Pub/Sub, Kafka, Service Bus) redeliver: the same logical message can
reach the handler twice. Idempotency middleware makes the *second* delivery a no-op by keying each
invocation on a caller-supplied idempotency key (a header) and **replaying the first result** instead
of running the handler again — so "charge the card" or "ship the order" happens once even though the
message arrived twice.

Redeliveries also *overlap*: the same key can be in flight twice at once (a visibility timeout
expires while the first delivery is still working). A check-then-run would let both run the handler,
so the middleware **reserves** the key atomically first (:meth:`IdempotencyStore.put_if_absent`) and
only the delivery that wins the reservation runs the handler. The loser is told its twin is still
working — ``conflict``, "duplicate delivery is already in flight" — rather than being given a result
that does not exist yet; the transport redelivers, and by then the first delivery's result is stored
and replays. Charging once beats answering promptly.

Only outcomes matching ``remember_when`` are stored (successes, by default): a transient failure is
*not* remembered — the reservation is released instead, so a redelivery is free to retry it and
dedupe never pins a failure in place. A handler that *raises* likewise releases the key, so one bad
delivery cannot wedge a key forever. A message with no idempotency key passes straight through
(nothing to dedupe on).

The store is a pluggable port (:class:`IdempotencyStore`); an in-memory implementation ships here for
tests and single-process services, and a shared backend (e.g. Redis, once ``benzene-cache`` lands)
slots in behind the same async methods.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from typing import Protocol

from benzene.core import Context, Middleware, Next
from benzene.results import Result, Status

Clock = Callable[[], float]

#: Header names tried in order to find an invocation's idempotency key.
DEFAULT_KEY_HEADERS: tuple[str, ...] = ("idempotency-key", "message-id")

#: The value reserved under a key while its first delivery is still running, and the result handed
#: to a concurrent duplicate. It is a plain ``Result`` so any store — including one that serialises
#: entries — round-trips it by value; the middleware recognises it by equality, never by identity.
IN_PROGRESS: Result[None] = Result.failure(
    Status.CONFLICT, "duplicate delivery is already in flight"
)


class IdempotencyStore(Protocol):
    """A keyed store of prior results — the seam a shared backend implements.

    Every method is async so a network-backed store (Redis, DynamoDB) is a drop-in; the in-memory
    implementation simply completes synchronously.

    :meth:`put_if_absent` must be **atomic** — that is the whole point of the method: it is what
    stops two concurrent deliveries of one key from both running the handler. In-memory that is a
    ``dict`` write with no ``await`` between the read and the write; on Redis it is ``SET key value
    NX`` (with the TTL in the same command), on DynamoDB a conditional put.
    """

    async def get(self, key: str) -> Result | None: ...
    async def put(self, key: str, result: Result) -> None: ...
    async def put_if_absent(self, key: str, result: Result) -> bool:
        """Store ``result`` only if ``key`` is unset; return whether this caller claimed it."""
        ...

    async def delete(self, key: str) -> None:
        """Forget ``key`` — used to release a reservation whose outcome is not remembered."""
        ...


class InMemoryIdempotencyStore:
    """A process-local :class:`IdempotencyStore` with an optional per-entry TTL.

    Suitable for tests and single-instance services. ``ttl`` (seconds) bounds how long a key is
    remembered; ``None`` keeps entries for the process lifetime. ``clock`` is injectable so a test
    expires entries without sleeping.

    :meth:`put_if_absent` is race-free by construction: it reads and writes the dict with no ``await``
    in between, so no other task can interleave on the single-threaded event loop.
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
        self._entries[key] = (result, self._expires_at())

    async def put_if_absent(self, key: str, result: Result) -> bool:
        entry = self._entries.get(key)
        if entry is not None and not self._is_expired(entry[1]):
            return False
        self._entries[key] = (result, self._expires_at())
        return True

    async def delete(self, key: str) -> None:
        self._entries.pop(key, None)

    def _expires_at(self) -> float | None:
        return None if self._ttl is None else self._clock() + self._ttl

    def _is_expired(self, expires_at: float | None) -> bool:
        return expires_at is not None and self._clock() >= expires_at


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
    """Middleware that runs one delivery per idempotency key and replays its result.

    Prefer the :func:`idempotency_interception` spelling — it is the same function under the
    convention every other middleware factory follows.

    The key is read from the first present of ``key_headers``. A first delivery reserves the key,
    runs the handler and, if its result satisfies ``remember_when`` (default: the result is
    successful), stores it under the reservation; any other outcome — including a raised exception —
    releases the key so a redelivery may retry. A repeat delivery short-circuits and never touches
    the handler: it replays the stored result, or, while the first delivery is still running, gets
    ``conflict`` ("duplicate delivery is already in flight") so the transport redelivers it later.
    A keyless message is passed through unchanged. Install it ahead of the message router.
    """
    should_remember = remember_when or (lambda result: result.is_successful)

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        key = _key_of(context.headers, key_headers)
        if key is None:
            await next()
            return
        if not await store.put_if_absent(key, IN_PROGRESS):
            settled = await store.get(key)
            # A settled result replays. The marker — or an entry that vanished between the failed
            # reservation and this read — means the twin is still working, so let it finish.
            in_flight = settled is None or settled == IN_PROGRESS
            context.result = IN_PROGRESS if in_flight else settled
            return
        stored = False
        try:
            await next()
            result = context.result if context.result is not None else Result.ok()
            stored = should_remember(result)
            if stored:
                await store.put(key, result)  # overwrite the marker with the real outcome
        finally:
            # Release the reservation for every outcome we do not remember — a not-remembered
            # failure *and* a raising handler — so the key is never wedged in progress forever.
            if not stored:
                await store.delete(key)

    return middleware


#: Preferred spelling of :func:`idempotency`, matching the ``*_interception`` convention the other
#: middleware factories follow. The bare ``idempotency`` name keeps working.
idempotency_interception = idempotency
