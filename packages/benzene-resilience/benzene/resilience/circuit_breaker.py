"""Circuit breaker (resilience §, mirrors ``Benzene.Resilience.Polly``'s circuit-breaker policy).

A circuit breaker stops a service from hammering a dependency that is already failing: after
``failure_threshold`` consecutive *tripping* failures it **opens** and rejects calls fast for
``reset_timeout`` seconds, then lets a single probe through (**half-open**) — a success closes the
circuit, a failure re-opens it. Only *server-side* failures trip it by default; a ``bad-request`` or
``not-found`` is a handled outcome the dependency answered correctly, so it resets the breaker rather
than counting against it (the ``handle-these-results`` semantics Polly expresses).

The state machine is exposed as a plain :class:`CircuitBreaker` object with one
``execute(run)`` seam, so the *same* breaker guards an inbound pipeline (via
:func:`circuit_breaker_interception`) and an outbound client (via :class:`CircuitBreakingMessageSender`)
— resilience is a decorator over the one ``Result``-returning shape, exactly like retry
(``benzene.core.with_retry``).
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable, Iterable
from enum import Enum
from typing import Any

from benzene.core import Context, MessageSender, Middleware, Next
from benzene.results import Result, Status

#: A guarded unit of work: produces a :class:`~benzene.results.Result` (an outbound send, or the
#: pipeline's ``next()`` reduced to the context result).
Run = Callable[[], Awaitable[Result]]

#: A monotonic clock reading seconds; injectable so tests advance time without sleeping.
Clock = Callable[[], float]

#: Statuses that trip the breaker by default — transient/server-side failures a retry-then-back-off
#: strategy is meant for. Client errors (``bad-request``, ``not-found``, …) are handled outcomes and
#: deliberately do **not** trip it.
DEFAULT_TRIP_ON: frozenset[str] = frozenset(
    {
        Status.SERVICE_UNAVAILABLE,
        Status.TIMEOUT,
        Status.TOO_MANY_REQUESTS,
        Status.UNEXPECTED_ERROR,
    }
)


class CircuitState(str, Enum):
    """The three breaker states (core-concepts §, resilience)."""

    CLOSED = "closed"  # normal: calls pass through
    OPEN = "open"  # tripped: calls rejected fast until the reset timeout elapses
    HALF_OPEN = "half-open"  # probing: a single call is allowed to test recovery


class CircuitOpenError(Exception):
    """Raised nowhere — the breaker never *raises* on rejection (it returns a failure result).

    Kept as a named type so callers that prefer to branch on a status can still recognise the
    reason string the breaker attaches (``"circuit breaker is open"``).
    """


class CircuitBreaker:
    """A consecutive-failure circuit breaker over any ``Result``-returning call.

    ``failure_threshold`` consecutive tripping failures open the circuit; it stays open for
    ``reset_timeout`` seconds, then admits one probe. ``trip_on`` selects which failure statuses
    count (default :data:`DEFAULT_TRIP_ON`); every other outcome — success *or* a non-tripping
    failure — resets the consecutive count. ``clock`` is injectable for tests.

    The object is deliberately lock-free: every state transition is a synchronous read-modify-write
    around the single ``await run()``, matching the cooperative, single-threaded asyncio model the
    rest of the pipeline assumes.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        reset_timeout: float = 30.0,
        trip_on: Iterable[str] = DEFAULT_TRIP_ON,
        clock: Clock = time.monotonic,
    ) -> None:
        self._failure_threshold = max(1, failure_threshold)
        self._reset_timeout = reset_timeout
        self._trip_on = frozenset(trip_on)
        self._clock = clock
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._probing = False

    @property
    def state(self) -> CircuitState:
        """The current state, resolving an elapsed open window to ``half-open`` on read."""
        if self._state is CircuitState.OPEN and self._reset_elapsed():
            return CircuitState.HALF_OPEN
        return self._state

    def _reset_elapsed(self) -> bool:
        return (self._clock() - self._opened_at) >= self._reset_timeout

    async def execute(self, run: Run) -> Result:
        """Run ``run`` under the breaker, or reject fast with ``service-unavailable`` when open.

        A tripping failure (or a raised exception) advances the breaker toward open; anything else
        closes/keeps it closed. An exception is re-raised after being recorded so the surrounding
        envelope maps it — the breaker only *observes* faults, it does not swallow them.
        """
        if not self._admit():
            return Result.service_unavailable("circuit breaker is open")
        try:
            result = await run()
        except Exception:
            self._on_failure()
            raise
        self._record(result)
        return result

    def _admit(self) -> bool:
        """Decide whether a call may proceed, transitioning ``open`` → ``half-open`` when due."""
        if self._state is CircuitState.OPEN:
            if not self._reset_elapsed():
                return False
            # The open window has elapsed: admit exactly one probe.
            self._state = CircuitState.HALF_OPEN
            self._probing = True
            return True
        if self._state is CircuitState.HALF_OPEN:
            # A probe is already in flight — keep rejecting until it resolves.
            if self._probing:
                return False
            self._probing = True
            return True
        return True  # CLOSED

    def _record(self, result: Result) -> None:
        if result.is_successful or result.status not in self._trip_on:
            self._on_success()
        else:
            self._on_failure()

    def _on_success(self) -> None:
        self._consecutive_failures = 0
        self._probing = False
        self._state = CircuitState.CLOSED

    def _on_failure(self) -> None:
        self._probing = False
        if self._state is CircuitState.HALF_OPEN:
            self._trip()
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._failure_threshold:
            self._trip()

    def _trip(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._clock()


def circuit_breaker_interception(breaker: CircuitBreaker) -> Middleware:
    """Middleware guarding the pipeline with ``breaker``: when open, short-circuit fast.

    While the circuit is open the handler never runs — ``next()`` is not called, so the router is
    skipped and the caller gets ``service-unavailable`` immediately. Install it ahead of the message
    router (and typically ahead of retry, so a tripped breaker isn't itself retried).
    """

    async def middleware(context: Context, next: Next) -> None:  # noqa: A002 - spec name
        async def run() -> Result:
            await next()
            return context.result if context.result is not None else Result.ok()

        context.result = await breaker.execute(run)

    return middleware


class CircuitBreakingMessageSender:
    """Wraps a :class:`~benzene.core.MessageSender`, guarding outbound sends with a breaker.

    Composes with the other outbound decorators (``with_retry``, ``with_correlation_id``): put the
    breaker *inside* retry so a fast rejection short-circuits, or *outside* to count retried
    failures as one — both are one line.
    """

    def __init__(self, inner: MessageSender, breaker: CircuitBreaker) -> None:
        self._inner = inner
        self._breaker = breaker

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        return await self._breaker.execute(
            lambda: self._inner.send_message(topic, message, headers)
        )


def with_circuit_breaker(
    inner: MessageSender, breaker: CircuitBreaker | None = None, **options: Any
) -> CircuitBreakingMessageSender:
    """Sugar for :class:`CircuitBreakingMessageSender` — ``with_circuit_breaker(sender, failure_threshold=3)``.

    Pass a shared :class:`CircuitBreaker`, or keyword options to build a fresh one for this client.
    """
    return CircuitBreakingMessageSender(inner, breaker or CircuitBreaker(**options))
