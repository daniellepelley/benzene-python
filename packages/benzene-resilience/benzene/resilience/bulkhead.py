"""Bulkhead — a concurrency limiter (resilience §, mirrors ``Benzene.Resilience.Polly``'s bulkhead).

A bulkhead caps how many invocations run *at once* so a slow dependency can't exhaust the whole
service's capacity: at most ``max_concurrency`` calls execute concurrently, up to ``max_queue`` more
may wait for a slot, and anything beyond that is shed immediately with ``too-many-requests`` (the
back-pressure signal the status vocabulary reserves for exactly this) rather than piling up unbounded.

Like the circuit breaker it exposes one ``execute(run)`` seam, so the same bulkhead guards an inbound
pipeline (:func:`bulkhead_interception`) and an outbound client (:class:`BulkheadMessageSender`).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from benzene.core import Context, MessageSender, Middleware, Next
from benzene.results import Result

Run = Callable[[], Awaitable[Result]]


class Bulkhead:
    """A semaphore-backed concurrency limiter with a bounded wait queue.

    ``max_concurrency`` calls run at once; ``max_queue`` more may await a slot; the next call after
    that is shed with ``too-many-requests``. The admission check is a synchronous compare-and-increment
    before any ``await``, so the shed decision is race-free under cooperative asyncio scheduling.
    """

    def __init__(self, max_concurrency: int, *, max_queue: int = 0) -> None:
        self._max_concurrency = max(1, max_concurrency)
        self._max_queue = max(0, max_queue)
        self._semaphore = asyncio.Semaphore(self._max_concurrency)
        self._in_flight = 0  # holders + waiters admitted (not yet returned)

    @property
    def in_flight(self) -> int:
        """Admitted calls currently running or queued (excludes shed calls)."""
        return self._in_flight

    async def execute(self, run: Run) -> Result:
        """Run ``run`` when a slot (or a queue place) is free, else shed with ``too-many-requests``."""
        capacity = self._max_concurrency + self._max_queue
        if self._in_flight >= capacity:
            return Result.too_many_requests("bulkhead is full")
        self._in_flight += 1
        try:
            async with self._semaphore:
                return await run()
        finally:
            self._in_flight -= 1


def bulkhead_interception(bulkhead: Bulkhead) -> Middleware:
    """Middleware capping in-flight invocations; sheds excess with ``too-many-requests`` (no handler run)."""

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        async def run() -> Result:
            await next()
            return context.result if context.result is not None else Result.ok()

        context.result = await bulkhead.execute(run)

    return middleware


class BulkheadMessageSender:
    """Wraps a :class:`~benzene.core.MessageSender`, capping concurrent outbound sends."""

    def __init__(self, inner: MessageSender, bulkhead: Bulkhead) -> None:
        self._inner = inner
        self._bulkhead = bulkhead

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        return await self._bulkhead.execute(
            lambda: self._inner.send_message(topic, message, headers)
        )


def with_bulkhead(
    inner: MessageSender, bulkhead: Bulkhead | None = None, **options: Any
) -> BulkheadMessageSender:
    """Sugar for :class:`BulkheadMessageSender` — ``with_bulkhead(sender, max_concurrency=10)``."""
    return BulkheadMessageSender(inner, bulkhead or Bulkhead(**options))
