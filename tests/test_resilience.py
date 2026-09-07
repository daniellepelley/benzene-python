"""The resilience policies — circuit breaker, bulkhead, rate limiting, idempotency, saga.

Each policy is exercised twice where it ships twice (inbound middleware + outbound decorator) and the
time-based ones drive an injectable clock, so recovery windows and token refills are asserted
deterministically without sleeping. No broker, no real clock, no third-party package.
"""

from __future__ import annotations

import asyncio
import sys
import threading
from typing import Any

import pytest
from benzene.core import Context, MiddlewarePipeline, Registry, message_router
from benzene.resilience import (
    IN_PROGRESS,
    Bulkhead,
    CircuitBreaker,
    CircuitState,
    DynamoDbIdempotencyStore,
    InMemoryIdempotencyStore,
    RateLimiter,
    RedisIdempotencyStore,
    Saga,
    circuit_breaker_interception,
    decode_result,
    encode_result,
    idempotency,
    idempotency_interception,
    rate_limit_interception,
    with_bulkhead,
    with_circuit_breaker,
    with_rate_limit,
)
from benzene.results import BenzeneError, ProblemDetails, Result, Status


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


class ScriptedSender:
    """A :class:`MessageSender` that returns a scripted sequence of results, recording call count."""

    def __init__(self, *results: Result) -> None:
        self._results = list(results)
        self.calls = 0

    async def send_message(self, topic, message, headers=None) -> Result:
        self.calls += 1
        return self._results[min(self.calls - 1, len(self._results) - 1)]


# --- circuit breaker ---------------------------------------------------------------------------


def test_breaker_opens_after_consecutive_failures_and_rejects_fast() -> None:
    clock = ManualClock()
    sender = ScriptedSender(Result.service_unavailable("down"))
    guarded = with_circuit_breaker(sender, failure_threshold=3, reset_timeout=30, clock=clock)

    for _ in range(3):
        assert run(guarded.send_message("t", {})).status == Status.SERVICE_UNAVAILABLE
    assert sender.calls == 3  # three real attempts tripped the breaker

    # Now open: the next call is rejected without touching the inner sender.
    rejected = run(guarded.send_message("t", {}))
    assert rejected.status == Status.SERVICE_UNAVAILABLE
    assert "circuit breaker is open" in rejected.messages[0]
    assert sender.calls == 3  # unchanged — fast reject, no inner call


def test_breaker_half_opens_after_timeout_and_closes_on_success() -> None:
    clock = ManualClock()
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=10, clock=clock)

    async def fail() -> Result:
        return Result.timeout("slow")

    async def ok() -> Result:
        return Result.ok()

    run(breaker.execute(fail))
    assert breaker.state is CircuitState.OPEN

    clock.advance(10)  # reset window elapsed → half-open on next read
    # The assert above narrows breaker.state to OPEN, and a type checker cannot see that advancing
    # the clock re-reads it. The state genuinely changes; the narrowing is the fiction.
    assert breaker.state is CircuitState.HALF_OPEN  # type: ignore[comparison-overlap]

    # The probe succeeds → circuit closes and normal traffic resumes.
    assert run(breaker.execute(ok)).is_successful
    assert breaker.state is CircuitState.CLOSED


def test_breaker_reopens_when_the_probe_fails() -> None:
    clock = ManualClock()
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=10, clock=clock)

    async def fail() -> Result:
        return Result.service_unavailable()

    run(breaker.execute(fail))  # opens
    clock.advance(10)
    run(breaker.execute(fail))  # the probe fails → straight back to open
    assert breaker.state is CircuitState.OPEN


def test_breaker_ignores_client_errors() -> None:
    breaker = CircuitBreaker(failure_threshold=2)

    async def bad_request() -> Result:
        return Result.bad_request("nope")

    for _ in range(5):
        run(breaker.execute(bad_request))
    # A bad-request is a handled outcome, not a trip — the breaker stays closed however many arrive.
    assert breaker.state is CircuitState.CLOSED


def test_breaker_records_and_reraises_exceptions() -> None:
    breaker = CircuitBreaker(failure_threshold=1)

    async def boom() -> Result:
        raise RuntimeError("kaboom")

    try:
        run(breaker.execute(boom))
        raise AssertionError("expected the exception to propagate")
    except RuntimeError:
        pass
    assert breaker.state is CircuitState.OPEN  # the fault counted toward tripping


def test_half_open_admits_exactly_one_probe() -> None:
    """Half-open is a single-probe state: a second caller is rejected while the probe is in flight."""
    clock = ManualClock()
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=10, clock=clock)
    ran = {"n": 0}
    entered = asyncio.Event()
    release = asyncio.Event()

    async def fail() -> Result:
        return Result.service_unavailable("down")

    async def probe() -> Result:
        ran["n"] += 1
        entered.set()
        await release.wait()
        return Result.ok()

    async def second_caller() -> Result:  # pragma: no cover - must never run
        ran["n"] += 1
        return Result.ok()

    async def scenario() -> Result:
        await breaker.execute(fail)  # opens at t=0
        clock.advance(10)  # the window elapsed → the next call is admitted as the probe
        probing = asyncio.create_task(breaker.execute(probe))
        await entered.wait()  # the probe is parked inside the breaker
        rejected = await breaker.execute(second_caller)
        release.set()
        await probing
        return rejected

    rejected = run(scenario())
    assert rejected.status == Status.SERVICE_UNAVAILABLE
    assert "circuit breaker is open" in rejected.messages[0]
    assert ran["n"] == 1  # only the probe ran; the second caller's work never started
    assert breaker.state is CircuitState.CLOSED  # the probe succeeded → closed


def test_breaker_releases_the_probe_when_it_is_cancelled() -> None:
    """A cancelled probe must not wedge the breaker into rejecting 100% of traffic."""
    clock = ManualClock()
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=10, clock=clock)
    entered = asyncio.Event()

    async def fail() -> Result:
        return Result.service_unavailable("down")

    async def parked_probe() -> Result:  # pragma: no cover - cancelled, never returns
        entered.set()
        await asyncio.Event().wait()  # never set: the task is cancelled instead
        return Result.ok()

    async def ok() -> Result:
        return Result.ok()

    async def scenario() -> Result:
        await breaker.execute(fail)  # opens at t=0
        clock.advance(10)
        probing = asyncio.create_task(breaker.execute(parked_probe))
        await entered.wait()
        probing.cancel()
        await asyncio.gather(probing, return_exceptions=True)
        # The probe proved nothing: back to open with a fresh window, not stuck half-open.
        assert breaker.state is CircuitState.OPEN
        clock.advance(10)
        return await breaker.execute(ok)

    assert run(scenario()).is_successful  # admitted again — not wedged
    assert breaker.state is CircuitState.CLOSED


def test_breaker_ignores_a_stale_success_from_before_it_tripped() -> None:
    """An in-flight call admitted while closed must not erase an open window it predates."""
    clock = ManualClock()
    breaker = CircuitBreaker(failure_threshold=2, reset_timeout=10, clock=clock)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_ok() -> Result:
        entered.set()
        await release.wait()
        return Result.ok()

    async def fail() -> Result:
        return Result.service_unavailable("down")

    async def trip_while_the_slow_call_is_in_flight() -> None:
        await entered.wait()
        await breaker.execute(fail)
        await breaker.execute(fail)  # threshold reached → open
        assert breaker.state is CircuitState.OPEN
        release.set()

    async def scenario() -> None:
        await asyncio.gather(breaker.execute(slow_ok), trip_while_the_slow_call_is_in_flight())

    run(scenario())
    assert breaker.state is CircuitState.OPEN  # the stale success is ignored


def test_breaker_ignores_a_stale_failure_while_open() -> None:
    """A stale failure must not re-trip the breaker and silently extend the open window."""
    clock = ManualClock()
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=10, clock=clock)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_fail() -> Result:
        entered.set()
        await release.wait()
        return Result.timeout("slow")

    async def fail() -> Result:
        return Result.service_unavailable("down")

    async def trip_while_the_slow_call_is_in_flight() -> None:
        await entered.wait()
        await breaker.execute(fail)  # opens at t=0
        clock.advance(5)  # halfway through the open window
        release.set()

    async def scenario() -> None:
        await asyncio.gather(breaker.execute(slow_fail), trip_while_the_slow_call_is_in_flight())

    run(scenario())
    clock.advance(5)  # t=10: the window opened at t=0 has now fully elapsed
    assert breaker.state is CircuitState.HALF_OPEN  # not pushed out to t=15 by the stale failure


def test_breaker_interception_short_circuits_the_pipeline() -> None:
    breaker = CircuitBreaker(failure_threshold=1, reset_timeout=1000, clock=ManualClock())
    handler_calls = {"n": 0}

    async def handler(_request) -> Result:
        handler_calls["n"] += 1
        return Result.service_unavailable()

    registry = Registry().register("t", handler)
    pipeline = MiddlewarePipeline([circuit_breaker_interception(breaker)]).use(
        message_router(registry)
    )

    run(pipeline.handle(Context("t", {})))  # trips the breaker (1 failure)
    ctx = Context("t", {})
    run(pipeline.handle(ctx))  # breaker open → handler must not run again
    assert handler_calls["n"] == 1
    assert ctx.result is not None and ctx.result.status == Status.SERVICE_UNAVAILABLE


# --- bulkhead ----------------------------------------------------------------------------------


def test_bulkhead_sheds_load_past_capacity() -> None:
    bulkhead = Bulkhead(max_concurrency=2, max_queue=0)
    release = asyncio.Event()
    admitted = 0

    async def slow() -> Result:
        nonlocal admitted
        admitted += 1
        await release.wait()
        return Result.ok()

    async def scenario() -> list[Result]:
        # Two calls occupy the two slots and park; a third finds no slot or queue place.
        first = asyncio.create_task(bulkhead.execute(slow))
        second = asyncio.create_task(bulkhead.execute(slow))
        await asyncio.sleep(0)  # let both enter and park on the event
        third = await bulkhead.execute(slow)  # rejected immediately
        release.set()
        await asyncio.gather(first, second)
        return [third]

    (third,) = run(scenario())
    assert third.status == Status.TOO_MANY_REQUESTS
    assert admitted == 2  # only the two that fit ever ran


def test_bulkhead_admits_within_capacity() -> None:
    bulkhead = Bulkhead(max_concurrency=3)
    sender = ScriptedSender(Result.ok())
    guarded = with_bulkhead(sender, bulkhead=bulkhead)
    assert run(guarded.send_message("t", {})).is_successful
    assert bulkhead.in_flight == 0  # settled back to zero after the call returned


def test_bulkhead_queue_lets_extra_callers_wait() -> None:
    bulkhead = Bulkhead(max_concurrency=1, max_queue=1)
    release = asyncio.Event()

    async def slow() -> Result:
        await release.wait()
        return Result.ok()

    async def scenario() -> Result:
        holder = asyncio.create_task(bulkhead.execute(slow))  # takes the one slot
        await asyncio.sleep(0)
        waiter = asyncio.create_task(bulkhead.execute(slow))  # takes the one queue place
        await asyncio.sleep(0)
        shed = await bulkhead.execute(slow)  # no slot, no queue → shed
        release.set()
        await asyncio.gather(holder, waiter)
        return shed

    assert run(scenario()).status == Status.TOO_MANY_REQUESTS


# --- rate limiting -----------------------------------------------------------------------------


def test_rate_limiter_allows_a_burst_then_rejects() -> None:
    clock = ManualClock()
    limiter = RateLimiter(refill_rate=1, burst=3, clock=clock)

    async def ok() -> Result:
        return Result.ok()

    # The bucket starts full (burst=3): three pass, the fourth is rejected.
    for _ in range(3):
        assert run(limiter.execute(ok)).is_successful
    assert run(limiter.execute(ok)).status == Status.TOO_MANY_REQUESTS


def test_rate_limiter_refills_over_time() -> None:
    clock = ManualClock()
    limiter = RateLimiter(refill_rate=2, burst=1, clock=clock)

    async def ok() -> Result:
        return Result.ok()

    assert run(limiter.execute(ok)).is_successful  # spends the one token
    assert run(limiter.execute(ok)).status == Status.TOO_MANY_REQUESTS  # empty
    clock.advance(0.5)  # 0.5s * 2/s = 1 token refilled
    assert run(limiter.execute(ok)).is_successful


def test_rate_limit_decorator_wraps_a_sender() -> None:
    clock = ManualClock()
    sender = ScriptedSender(Result.ok())
    guarded = with_rate_limit(sender, refill_rate=1, burst=1, clock=clock)
    assert run(guarded.send_message("t", {})).is_successful
    assert run(guarded.send_message("t", {})).status == Status.TOO_MANY_REQUESTS
    assert sender.calls == 1  # the rejected call never reached the inner sender


def test_rate_limit_interception_rejects_without_running_the_handler() -> None:
    clock = ManualClock()
    limiter = RateLimiter(refill_rate=1, burst=1, clock=clock)
    handler_calls = {"n": 0}

    async def handler(_request) -> Result:
        handler_calls["n"] += 1
        return Result.ok()

    registry = Registry().register("t", handler)
    pipeline = MiddlewarePipeline([rate_limit_interception(limiter)]).use(message_router(registry))

    run(pipeline.handle(Context("t", {})))  # allowed
    ctx = Context("t", {})
    run(pipeline.handle(ctx))  # bucket empty → rejected, handler skipped
    assert handler_calls["n"] == 1
    assert ctx.result is not None and ctx.result.status == Status.TOO_MANY_REQUESTS


# --- idempotency -------------------------------------------------------------------------------


def _dedupe_pipeline(store, handler):
    registry = Registry().register("t", handler)
    return MiddlewarePipeline([idempotency(store)]).use(message_router(registry))


def test_idempotency_replays_the_first_result_for_a_repeat_key() -> None:
    store = InMemoryIdempotencyStore()
    runs = {"n": 0}

    async def handler(_request) -> Result:
        runs["n"] += 1
        return Result.created({"attempt": runs["n"]})

    pipeline = _dedupe_pipeline(store, handler)
    headers = {"idempotency-key": "abc"}

    first = Context("t", {}, headers=headers)
    run(pipeline.handle(first))
    second = Context("t", {}, headers=headers)
    run(pipeline.handle(second))

    assert runs["n"] == 1  # the handler ran once
    assert first.result is not None and second.result is not None
    assert second.result.payload == {"attempt": 1}  # the first result, replayed


def test_idempotency_does_not_remember_failures() -> None:
    store = InMemoryIdempotencyStore()
    outcomes = iter([Result.service_unavailable("down"), Result.ok({"ok": True})])

    async def handler(_request) -> Result:
        return next(outcomes)

    pipeline = _dedupe_pipeline(store, handler)
    headers = {"idempotency-key": "abc"}

    first = Context("t", {}, headers=headers)
    run(pipeline.handle(first))  # fails → not remembered
    second = Context("t", {}, headers=headers)
    run(pipeline.handle(second))  # retried → succeeds

    assert first.result is not None and not first.result.is_successful
    assert second.result is not None and second.result.is_successful


def test_idempotency_passes_keyless_messages_through() -> None:
    store = InMemoryIdempotencyStore()
    runs = {"n": 0}

    async def handler(_request) -> Result:
        runs["n"] += 1
        return Result.ok()

    pipeline = _dedupe_pipeline(store, handler)
    run(pipeline.handle(Context("t", {})))  # no idempotency header
    run(pipeline.handle(Context("t", {})))
    assert runs["n"] == 2  # nothing to dedupe on → both ran


def test_idempotency_runs_the_handler_once_for_concurrent_duplicates() -> None:
    """Two deliveries of one key in flight at once: the handler runs once, the twin gets a conflict."""
    store = InMemoryIdempotencyStore()
    runs = {"n": 0}
    entered = asyncio.Event()
    release = asyncio.Event()

    async def handler(_request) -> Result:
        runs["n"] += 1
        if runs["n"] == 1:
            entered.set()
            await release.wait()  # park the first delivery inside the handler
        return Result.created({"attempt": runs["n"]})

    pipeline = _dedupe_pipeline(store, handler)
    headers = {"idempotency-key": "abc"}
    first = Context("t", {}, headers=headers)
    second = Context("t", {}, headers=headers)

    async def deliver_the_twin() -> None:
        await entered.wait()  # the first delivery is inside the handler, nothing stored yet
        await pipeline.handle(second)
        release.set()

    async def scenario() -> None:
        await asyncio.gather(pipeline.handle(first), deliver_the_twin())

    run(scenario())

    assert runs["n"] == 1  # "charge the card" happened exactly once
    assert first.result is not None and first.result.payload == {"attempt": 1}
    assert second.result is not None and second.result.status == Status.CONFLICT
    assert "in flight" in second.result.messages[0]


def test_idempotency_replays_a_finished_result_after_the_reservation() -> None:
    store = InMemoryIdempotencyStore()
    runs = {"n": 0}

    async def handler(_request) -> Result:
        runs["n"] += 1
        return Result.created({"attempt": runs["n"]})

    pipeline = _dedupe_pipeline(store, handler)
    headers = {"idempotency-key": "abc"}
    run(pipeline.handle(Context("t", {}, headers=headers)))
    second = Context("t", {}, headers=headers)
    run(pipeline.handle(second))

    # A settled key replays the stored result — the in-progress marker never outlives the delivery.
    assert runs["n"] == 1
    assert second.result is not None and second.result.payload == {"attempt": 1}


def test_idempotency_frees_the_key_when_the_handler_raises() -> None:
    """A raising delivery must not leave the key reserved forever — the redelivery has to run."""
    store = InMemoryIdempotencyStore()
    middleware = idempotency(store)

    async def explode() -> None:
        raise RuntimeError("boom")

    context = Context("t", {}, headers={"idempotency-key": "abc"})
    try:
        run(middleware(context, explode))
        raise AssertionError("expected the exception to propagate")
    except RuntimeError:
        pass

    assert run(store.get("abc")) is None  # the reservation was released

    runs = {"n": 0}

    async def handler(_request) -> Result:
        runs["n"] += 1
        return Result.ok()

    pipeline = _dedupe_pipeline(store, handler)
    run(pipeline.handle(Context("t", {}, headers={"idempotency-key": "abc"})))
    assert runs["n"] == 1  # the redelivery ran, rather than seeing a wedged reservation


def test_idempotency_falls_back_to_the_message_id_header() -> None:
    store = InMemoryIdempotencyStore()
    runs = {"n": 0}

    async def handler(_request) -> Result:
        runs["n"] += 1
        return Result.ok({"attempt": runs["n"]})

    pipeline = _dedupe_pipeline(store, handler)
    headers = {"message-id": "m-1"}  # no idempotency-key → the transport's message id keys it
    run(pipeline.handle(Context("t", {}, headers=headers)))
    second = Context("t", {}, headers=headers)
    run(pipeline.handle(second))

    assert runs["n"] == 1
    assert second.result is not None and second.result.payload == {"attempt": 1}

    # An explicit idempotency-key wins over the fallback: same message-id, new key → a fresh run.
    third = Context("t", {}, headers={"message-id": "m-1", "idempotency-key": "k-2"})
    run(pipeline.handle(third))
    assert runs["n"] == 2
    assert third.result is not None and third.result.payload == {"attempt": 2}


def test_idempotency_remembers_failures_when_remember_when_says_so() -> None:
    store = InMemoryIdempotencyStore()
    outcomes = iter([Result.not_found("gone"), Result.ok({"retried": True})])

    async def handler(_request) -> Result:
        return next(outcomes)

    registry = Registry().register("t", handler)
    pipeline = MiddlewarePipeline(
        [idempotency(store, remember_when=lambda result: True)]  # remember every outcome
    ).use(message_router(registry))
    headers = {"idempotency-key": "abc"}

    first = Context("t", {}, headers=headers)
    run(pipeline.handle(first))
    second = Context("t", {}, headers=headers)
    run(pipeline.handle(second))

    assert first.result is not None and first.result.status == Status.NOT_FOUND
    # The custom predicate pinned the failure, so the redelivery replays it instead of retrying.
    assert second.result is not None and second.result.status == Status.NOT_FOUND


def test_idempotency_store_expires_entries() -> None:
    clock = ManualClock()
    store = InMemoryIdempotencyStore(ttl=5, clock=clock)
    run(store.put("k", Result.ok({"v": 1})))
    assert run(store.get("k")) is not None
    clock.advance(5)
    assert run(store.get("k")) is None  # TTL elapsed → forgotten


def test_idempotency_interception_is_the_preferred_alias() -> None:
    # D8: the middleware factories are named ``*_interception``; the old name stays working.
    assert idempotency_interception is idempotency


# --- durable idempotency stores (Redis, DynamoDB) ------------------------------------------------
#
# The in-memory store is single-process, so on a multi-instance deployment dedupe is a silent no-op.
# These two stores are the shared backing that makes the guarantee hold across processes, and the
# whole of their correctness is that ``put_if_absent`` is ONE atomic conditional write. Both fakes
# below therefore model the *real* command semantics — Redis' ``SET NX`` and DynamoDB's conditional
# ``PutItem`` — with a switch point that a ``get``-then-``put`` emulation cannot survive.


class FakeRedis:
    """A ``redis.asyncio`` stand-in modelling SET's real NX / EX / PX semantics on a manual clock.

    The ``await asyncio.sleep(0)`` at the top of every command is load-bearing: it hands control to
    any other ready task *before* the command decides, so two callers can be inside ``set`` at once.
    A store that emulated ``SET NX`` as ``get``-then-``set`` would have both callers read "absent"
    across that switch point and both would claim the key; only a single atomic command survives it.
    """

    def __init__(self, clock: ManualClock | None = None) -> None:
        self.clock = clock or ManualClock()
        self.entries: dict[str, tuple[str, float | None]] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def _live(self, key: str) -> str | None:
        entry = self.entries.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at is not None and self.clock() >= expires_at:
            del self.entries[key]  # Redis drops a lapsed key; a reader never sees it
            return None
        return value

    async def get(self, key: str) -> str | None:
        self.calls.append(("get", {"key": key}))
        await asyncio.sleep(0)
        return self._live(key)

    async def set(
        self,
        key: str,
        value: str,
        *,
        nx: bool = False,
        ex: int | None = None,
        px: int | None = None,
    ) -> bool | None:
        self.calls.append(("set", {"key": key, "value": value, "nx": nx, "ex": ex, "px": px}))
        await asyncio.sleep(0)  # the switch point a non-atomic emulation loses on
        if nx and self._live(key) is not None:
            return None  # redis answers nil when NX finds the key present
        ttl = float(ex) if ex is not None else (px / 1000 if px is not None else None)
        self.entries[key] = (value, None if ttl is None else self.clock() + ttl)
        return True

    async def delete(self, key: str) -> None:
        self.calls.append(("delete", {"key": key}))
        await asyncio.sleep(0)
        self.entries.pop(key, None)

    def command_names(self) -> list[str]:
        return [name for name, _ in self.calls]


class ConditionalCheckFailedException(Exception):
    """What boto3 exposes as ``client.exceptions.ConditionalCheckFailedException``."""


class FakeDynamoDb:
    """A boto3 DynamoDB client stand-in modelling conditional ``PutItem``.

    ``put_item`` evaluates its ``ConditionExpression`` and writes under one lock, the way DynamoDB
    applies a conditional write to a single item, and raises the same
    ``ConditionalCheckFailedException`` the SDK hangs off ``client.exceptions``. It accepts *only*
    the store's atomic expression, so an implementation that dropped the condition — or read first
    and wrote unconditionally — is caught here rather than in production.

    ``rendezvous`` (a :class:`threading.Barrier`) parks every caller inside ``put_item`` until the
    expected number have arrived, so a racing pair is genuinely concurrent instead of being
    accidentally serialised by the thread pool.
    """

    CONDITION = "attribute_not_exists(#pk) OR expiresAt <= :now"

    def __init__(self, *, rendezvous: threading.Barrier | None = None) -> None:
        self.items: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.threads: list[int] = []
        self.exceptions = self
        self.ConditionalCheckFailedException = ConditionalCheckFailedException
        self._rendezvous = rendezvous
        self._lock = threading.Lock()

    def _record(self, name: str, kwargs: dict[str, Any]) -> None:
        self.calls.append((name, kwargs))
        self.threads.append(threading.get_ident())

    def put_item(self, **kwargs: Any) -> dict[str, Any]:
        self._record("put_item", kwargs)
        item = kwargs["Item"]
        condition = kwargs.get("ConditionExpression")
        if self._rendezvous is not None:
            self._rendezvous.wait(timeout=5)  # both callers inside the command at once
        with self._lock:  # DynamoDB evaluates and writes a single item atomically
            names = kwargs.get("ExpressionAttributeNames") or {}
            key = item[names.get("#pk", "pk")]["S"]
            if condition is None:
                self.items[key] = item
                return {}
            if condition != self.CONDITION:
                raise AssertionError(f"unexpected ConditionExpression: {condition!r}")
            now = float(kwargs["ExpressionAttributeValues"][":now"]["N"])
            existing = self.items.get(key)
            if existing is not None:
                expires_at = existing.get("expiresAt")
                lapsed = expires_at is not None and float(expires_at["N"]) <= now
                if not lapsed:
                    raise ConditionalCheckFailedException("The conditional request failed")
            self.items[key] = item
            return {}

    def get_item(self, **kwargs: Any) -> dict[str, Any]:
        self._record("get_item", kwargs)
        key = next(iter(kwargs["Key"].values()))["S"]
        item = self.items.get(key)
        return {} if item is None else {"Item": item}

    def delete_item(self, **kwargs: Any) -> dict[str, Any]:
        self._record("delete_item", kwargs)
        key = next(iter(kwargs["Key"].values()))["S"]
        self.items.pop(key, None)
        return {}

    def command_names(self) -> list[str]:
        return [name for name, _ in self.calls]


def _run_concurrent_duplicates(store: Any) -> tuple[int, Context, Context]:
    """Deliver one idempotency key twice, overlapping, through the real middleware.

    The same scenario ``test_idempotency_runs_the_handler_once_for_concurrent_duplicates`` runs
    against the in-memory store, so pointing it at a durable store proves the swap is
    behaviour-preserving rather than merely type-compatible.
    """
    runs = {"n": 0}
    entered = asyncio.Event()
    release = asyncio.Event()

    async def handler(_request) -> Result:
        runs["n"] += 1
        if runs["n"] == 1:
            entered.set()
            await release.wait()  # park the first delivery inside the handler
        return Result.created({"attempt": runs["n"]})

    pipeline = _dedupe_pipeline(store, handler)
    headers = {"idempotency-key": "abc"}
    first = Context("t", {}, headers=headers)
    second = Context("t", {}, headers=headers)

    async def deliver_the_twin() -> None:
        await entered.wait()  # the first delivery is inside the handler, nothing stored yet
        await pipeline.handle(second)
        release.set()

    async def scenario() -> None:
        await asyncio.gather(pipeline.handle(first), deliver_the_twin())

    run(scenario())
    return runs["n"], first, second


# --- the result codec both stores serialise through ---------------------------------------------


def test_encode_result_round_trips_the_status_payload_and_every_error() -> None:
    result = Result(
        Status.VALIDATION_ERROR,
        {"orderId": "o-1"},
        (BenzeneError("too small", field="quantity", code="min"), BenzeneError("unknown sku")),
    )
    restored = decode_result(encode_result(result))
    assert restored == result
    # A list would compare unequal to every stored tuple — including the in-flight marker.
    assert isinstance(restored.errors, tuple)
    assert restored.errors[0].field == "quantity" and restored.errors[0].code == "min"


def test_encode_result_round_trips_the_in_flight_marker_by_equality() -> None:
    # The middleware recognises the reservation with ``settled == IN_PROGRESS``, so a store that
    # serialises entries has to restore something that compares equal, not merely similar.
    assert decode_result(encode_result(IN_PROGRESS)) == IN_PROGRESS


def test_encode_result_keeps_an_application_authored_problem_document() -> None:
    document = ProblemDetails(
        benzene_status=Status.CONFLICT,
        type="https://orders.example/problems/already-shipped",
        title="Already shipped",
        detail="order o-1 left the warehouse",
        errors=(BenzeneError("already shipped", code="shipped"),),
    )
    restored = decode_result(encode_result(Result.problem(document)))
    assert restored == Result.problem(document)
    assert restored.problem_document is not None
    assert restored.problem_document.type == "https://orders.example/problems/already-shipped"


def test_encode_result_keeps_an_explicit_success_classification() -> None:
    # ``isSuccessful`` is authoritative on the wire; a replayed duplicate must carry the same
    # classification the first delivery did, not one re-derived from the status text.
    result = Result.set("cache-warm", {"warmed": 3}, successful=True)
    restored = decode_result(encode_result(result))
    assert restored.successful is True and restored.is_successful


def test_decode_result_rejects_a_corrupt_entry_loudly() -> None:
    with pytest.raises(ValueError, match="idempotency"):
        decode_result("[]")


# --- Redis store ---------------------------------------------------------------------------------


def test_redis_store_reserves_a_key_for_exactly_one_of_two_racing_callers() -> None:
    """The race the whole capability exists for: two deliveries, one reservation."""
    fake = FakeRedis()
    store = RedisIdempotencyStore(client=fake)

    async def scenario() -> list[bool]:
        return list(
            await asyncio.gather(
                store.put_if_absent("abc", IN_PROGRESS),
                store.put_if_absent("abc", IN_PROGRESS),
            )
        )

    claimed = run(scenario())
    assert sorted(claimed) == [False, True]  # exactly one delivery may run the handler
    # One command per caller, and it is SET ... NX: a read followed by a write would show up here
    # as ``["get", "get", "set", "set"]`` and both callers would have won above.
    assert fake.command_names() == ["set", "set"]
    assert all(call["nx"] is True for _, call in fake.calls)


def test_redis_store_lets_a_lapsed_reservation_be_reclaimed() -> None:
    clock = ManualClock()
    fake = FakeRedis(clock)
    store = RedisIdempotencyStore(client=fake, ttl=30)

    assert run(store.put_if_absent("abc", IN_PROGRESS)) is True
    assert run(store.put_if_absent("abc", IN_PROGRESS)) is False  # held for the TTL
    clock.advance(30)
    assert run(store.get("abc")) is None  # the TTL elapsed → Redis forgot it
    assert run(store.put_if_absent("abc", IN_PROGRESS)) is True  # reclaimable


def test_redis_store_sets_the_ttl_in_the_same_command_as_the_reservation() -> None:
    # A separate EXPIRE would leave a window where a crash pins the key forever.
    fake = FakeRedis()
    run(RedisIdempotencyStore(client=fake, ttl=1.9).put_if_absent("abc", IN_PROGRESS))
    _, call = fake.calls[0]
    assert call["nx"] is True and call["ex"] == 2 and call["px"] is None  # rounds up, never down


def test_redis_store_uses_px_for_a_sub_second_ttl() -> None:
    fake = FakeRedis()
    run(RedisIdempotencyStore(client=fake, ttl=0.25).put_if_absent("abc", IN_PROGRESS))
    _, call = fake.calls[0]
    assert call["px"] == 250 and call["ex"] is None


def test_redis_store_round_trips_a_result_through_the_wire_form() -> None:
    fake = FakeRedis()
    store = RedisIdempotencyStore(client=fake)
    result = Result.created({"orderId": "o-1"})
    run(store.put("abc", result))
    assert run(store.get("abc")) == result
    assert "benzene:idem:abc" in fake.entries  # namespaced, so it shares a Redis with the cache


def test_redis_store_reports_a_miss_as_none() -> None:
    assert run(RedisIdempotencyStore(client=FakeRedis()).get("nothing-here")) is None


def test_redis_store_delete_releases_the_reservation() -> None:
    fake = FakeRedis()
    store = RedisIdempotencyStore(client=fake)
    run(store.put_if_absent("abc", IN_PROGRESS))
    run(store.delete("abc"))
    assert run(store.put_if_absent("abc", IN_PROGRESS)) is True


def test_redis_store_requires_a_url_or_a_client() -> None:
    with pytest.raises(ValueError, match="url or an injected client"):
        RedisIdempotencyStore()


def test_redis_store_missing_sdk_raises_a_teaching_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A forgotten extra is a deployment error, never a message outcome: it must fail loudly at
    # construction with a message naming the extra to install.
    monkeypatch.setitem(sys.modules, "redis", None)
    with pytest.raises(ImportError, match=r"benzene-resilience\[redis\]"):
        RedisIdempotencyStore("redis://localhost")


def test_idempotency_over_the_redis_store_runs_the_handler_once_for_concurrent_duplicates() -> None:
    calls, first, second = _run_concurrent_duplicates(RedisIdempotencyStore(client=FakeRedis()))
    assert calls == 1  # "charge the card" happened exactly once, across the shared store
    assert first.result is not None and first.result.payload == {"attempt": 1}
    assert second.result is not None and second.result.status == Status.CONFLICT
    assert "in flight" in second.result.messages[0]


def test_idempotency_over_the_redis_store_replays_the_first_result() -> None:
    store = RedisIdempotencyStore(client=FakeRedis())
    runs = {"n": 0}

    async def handler(_request) -> Result:
        runs["n"] += 1
        return Result.created({"attempt": runs["n"]})

    pipeline = _dedupe_pipeline(store, handler)
    headers = {"idempotency-key": "abc"}
    first = Context("t", {}, headers=headers)
    run(pipeline.handle(first))
    second = Context("t", {}, headers=headers)
    run(pipeline.handle(second))

    assert runs["n"] == 1
    assert second.result is not None and second.result.status == Status.CREATED
    assert second.result.payload == {"attempt": 1}  # decoded back out of the store


# --- DynamoDB store ------------------------------------------------------------------------------


def test_dynamodb_store_reserves_a_key_for_exactly_one_of_two_racing_callers() -> None:
    """Both callers sit inside ``PutItem`` at once; the conditional write picks one."""
    fake = FakeDynamoDb(rendezvous=threading.Barrier(2))
    store = DynamoDbIdempotencyStore("orders-idempotency", client=fake, clock=ManualClock())

    async def scenario() -> list[bool]:
        return list(
            await asyncio.gather(
                store.put_if_absent("abc", IN_PROGRESS),
                store.put_if_absent("abc", IN_PROGRESS),
            )
        )

    claimed = run(scenario())
    assert sorted(claimed) == [False, True]
    # One conditional write per caller — no read-then-write, which would both show up here and let
    # both callers claim the key above.
    assert fake.command_names() == ["put_item", "put_item"]
    assert {call["ConditionExpression"] for _, call in fake.calls} == {FakeDynamoDb.CONDITION}
    assert all(call["ExpressionAttributeNames"] == {"#pk": "pk"} for _, call in fake.calls)


def test_dynamodb_store_treats_a_lapsed_record_as_absent() -> None:
    # DynamoDB's own TTL sweeper lags by up to 48 hours. A store that waited for it would leave an
    # expired key unreclaimable for two days, so the record's own ``expiresAt`` is what decides.
    clock = ManualClock()
    fake = FakeDynamoDb()
    store = DynamoDbIdempotencyStore("orders-idempotency", client=fake, ttl=30, clock=clock)

    assert run(store.put_if_absent("abc", IN_PROGRESS)) is True
    assert run(store.put_if_absent("abc", IN_PROGRESS)) is False
    clock.advance(30)
    assert run(store.get("abc")) is None
    assert "abc" in fake.items  # still physically present: the read expired it, not the sweeper
    assert run(store.put_if_absent("abc", IN_PROGRESS)) is True


def test_dynamodb_store_writes_a_ttl_attribute_and_the_marker_status() -> None:
    clock = ManualClock(1_700_000_000.0)
    fake = FakeDynamoDb()
    store = DynamoDbIdempotencyStore("orders-idempotency", client=fake, ttl=60, clock=clock)
    run(store.put_if_absent("abc", IN_PROGRESS))

    item = fake.items["abc"]
    assert item["pk"] == {"S": "abc"}
    assert item["expiresAt"] == {"N": "1700000060"}  # whole epoch seconds, what TTL requires
    # Mirrors the .NET record so a mixed-language fleet can share one table.
    assert item["status"] == {"S": "InProgress"}
    assert item["wasSuccessful"] == {"BOOL": False}

    run(store.put("abc", Result.created({"orderId": "o-1"})))
    settled = fake.items["abc"]
    assert settled["status"] == {"S": "Completed"} and settled["wasSuccessful"] == {"BOOL": True}


def test_dynamodb_store_reads_consistently_and_round_trips_a_result() -> None:
    fake = FakeDynamoDb()
    store = DynamoDbIdempotencyStore("orders-idempotency", client=fake)
    result = Result.created({"orderId": "o-1"})
    run(store.put("abc", result))
    assert run(store.get("abc")) == result
    _, read = fake.calls[-1]
    # An eventually-consistent read could miss the reservation the other instance just wrote.
    assert read["ConsistentRead"] is True


def test_dynamodb_store_synthesises_a_result_for_a_record_written_by_another_port() -> None:
    # A .NET service sharing the table writes no ``result`` attribute; its record still has to read
    # as "this key is taken" rather than crashing or looking absent.
    fake = FakeDynamoDb()
    fake.items["abc"] = {
        "pk": {"S": "abc"},
        "status": {"S": "Completed"},
        "wasSuccessful": {"BOOL": True},
        "expiresAt": {"N": "9999999999"},
    }
    settled = run(DynamoDbIdempotencyStore("orders-idempotency", client=fake).get("abc"))
    assert settled is not None and settled.is_successful

    fake.items["def"] = dict(fake.items["abc"], pk={"S": "def"}, status={"S": "InProgress"})
    in_flight = run(DynamoDbIdempotencyStore("orders-idempotency", client=fake).get("def"))
    assert in_flight == IN_PROGRESS  # the middleware's marker, recognised by equality


def test_dynamodb_store_delete_releases_the_reservation() -> None:
    fake = FakeDynamoDb()
    store = DynamoDbIdempotencyStore("orders-idempotency", client=fake)
    run(store.put_if_absent("abc", IN_PROGRESS))
    run(store.delete("abc"))
    assert fake.command_names()[-1] == "delete_item"
    assert run(store.put_if_absent("abc", IN_PROGRESS)) is True


def test_dynamodb_store_offloads_every_blocking_call_off_the_event_loop() -> None:
    # The rule tests/test_egress_offloads_the_event_loop.py pins for the senders: a blocking boto3
    # call must never run on the loop that an ASGI server is sharing.
    fake = FakeDynamoDb()
    store = DynamoDbIdempotencyStore("orders-idempotency", client=fake)
    run(store.put_if_absent("abc", IN_PROGRESS))
    run(store.get("abc"))
    run(store.delete("abc"))
    assert fake.threads and all(ident != threading.get_ident() for ident in fake.threads)


def test_dynamodb_store_missing_sdk_raises_a_teaching_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "boto3", None)
    store = DynamoDbIdempotencyStore("orders-idempotency")
    with pytest.raises(ImportError, match=r"benzene-resilience\[dynamodb\]"):
        run(store.get("abc"))


def test_idempotency_over_the_dynamodb_store_runs_the_handler_once_for_duplicates() -> None:
    store = DynamoDbIdempotencyStore("orders-idempotency", client=FakeDynamoDb())
    calls, first, second = _run_concurrent_duplicates(store)
    assert calls == 1
    assert first.result is not None and first.result.payload == {"attempt": 1}
    assert second.result is not None and second.result.status == Status.CONFLICT


# --- saga --------------------------------------------------------------------------------------


def test_saga_runs_every_step_on_success() -> None:
    async def reserve(state) -> Result:
        state["reserved"] = True
        return Result.ok()

    async def charge(state) -> Result:
        state["charged"] = True
        return Result.ok()

    outcome = run(Saga().step("reserve", reserve).step("charge", charge).execute())
    assert outcome.is_successful
    assert outcome.completed == ["reserve", "charge"]
    assert outcome.state == {"reserved": True, "charged": True}


def test_saga_compensates_completed_steps_in_reverse_on_failure() -> None:
    undone: list[str] = []

    async def reserve(state) -> Result:
        return Result.ok()

    async def undo_reserve(state) -> None:
        undone.append("reserve")

    async def charge(state) -> Result:
        return Result.ok()

    async def undo_charge(state) -> None:
        undone.append("charge")

    async def ship(state) -> Result:
        return Result.service_unavailable("courier down")

    outcome = run(
        Saga()
        .step("reserve", reserve, undo_reserve)
        .step("charge", charge, undo_charge)
        .step("ship", ship)
        .execute()
    )
    assert not outcome.is_successful
    assert outcome.failed_step == "ship"
    assert outcome.result.status == Status.SERVICE_UNAVAILABLE
    assert undone == ["charge", "reserve"]  # reverse order
    assert outcome.compensated == ["charge", "reserve"]


def test_saga_maps_a_raising_step_to_unexpected_error_and_rolls_back() -> None:
    undone: list[str] = []

    async def step_one(state) -> Result:
        return Result.ok()

    async def undo_one(state) -> None:
        undone.append("one")

    async def step_two(state) -> Result:
        raise RuntimeError("boom")

    outcome = run(Saga().step("one", step_one, undo_one).step("two", step_two).execute())
    assert outcome.result.status == Status.UNEXPECTED_ERROR
    assert outcome.failed_step == "two"
    assert undone == ["one"]


def test_saga_surfaces_a_failing_compensation() -> None:
    async def step_one(state) -> Result:
        return Result.ok()

    async def undo_one(state) -> None:
        raise RuntimeError("compensation failed")

    async def step_two(state) -> Result:
        return Result.bad_request("stop")

    outcome = run(Saga().step("one", step_one, undo_one).step("two", step_two).execute())
    assert outcome.compensation_failures == ["one"]
    assert outcome.compensated == []  # the compensation raised, so it isn't counted as done
    assert not outcome.is_successful
