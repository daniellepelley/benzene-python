"""Rate limiting — a token bucket wired to ``too-many-requests`` (mirrors ``Benzene.RateLimiting``).

A token bucket admits a steady ``refill_rate`` calls per second while tolerating a short ``burst``:
each call spends one token, tokens refill continuously up to the burst ceiling, and a call that finds
the bucket empty is rejected with ``too-many-requests`` — the same back-pressure status a downstream
would return, now enforced at the edge. This is the concrete home the roadmap wanted for that status:
before this package, ``too-many-requests`` existed in the vocabulary but nothing in the port produced
it.

One ``execute(run)`` seam again, so the limiter guards inbound (:func:`rate_limit_interception`) and
outbound (:class:`RateLimitingMessageSender`) alike.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import Any

from benzene.core import Context, MessageSender, Middleware, Next
from benzene.results import Result

Run = Callable[[], Awaitable[Result]]
Clock = Callable[[], float]


class RateLimiter:
    """A continuous-refill token bucket.

    ``refill_rate`` tokens are added per second up to a ceiling of ``burst`` (which also seeds the
    bucket full, so an idle service absorbs a burst immediately). ``clock`` is injectable so tests
    drive the refill deterministically without sleeping.
    """

    def __init__(
        self,
        *,
        refill_rate: float,
        burst: int = 1,
        clock: Clock = time.monotonic,
    ) -> None:
        if refill_rate <= 0:
            raise ValueError("refill_rate must be positive")
        self._refill_rate = refill_rate
        self._burst = max(1, burst)
        self._clock = clock
        self._tokens = float(self._burst)
        self._updated_at = clock()

    @property
    def available(self) -> float:
        """Tokens available right now (after refilling for the elapsed time)."""
        self._refill()
        return self._tokens

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._updated_at
        if elapsed > 0:
            self._tokens = min(self._burst, self._tokens + elapsed * self._refill_rate)
            self._updated_at = now

    def _try_acquire(self) -> bool:
        self._refill()
        if self._tokens >= 1:
            self._tokens -= 1
            return True
        return False

    async def execute(self, run: Run) -> Result:
        """Spend a token and run, or reject with ``too-many-requests`` when the bucket is empty."""
        if not self._try_acquire():
            return Result.too_many_requests("rate limit exceeded")
        return await run()


def rate_limit_interception(limiter: RateLimiter) -> Middleware:
    """Middleware rate-limiting the pipeline; rejects with ``too-many-requests`` (no handler run)."""

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        async def run() -> Result:
            await next()
            return context.result if context.result is not None else Result.ok()

        context.result = await limiter.execute(run)

    return middleware


class RateLimitingMessageSender:
    """Wraps a :class:`~benzene.core.MessageSender`, rate-limiting outbound sends."""

    def __init__(self, inner: MessageSender, limiter: RateLimiter) -> None:
        self._inner = inner
        self._limiter = limiter

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        return await self._limiter.execute(
            lambda: self._inner.send_message(topic, message, headers)
        )


def with_rate_limit(
    inner: MessageSender, limiter: RateLimiter | None = None, **options: Any
) -> RateLimitingMessageSender:
    """Sugar for :class:`RateLimitingMessageSender` — ``with_rate_limit(sender, refill_rate=100)``."""
    return RateLimitingMessageSender(inner, limiter or RateLimiter(**options))
