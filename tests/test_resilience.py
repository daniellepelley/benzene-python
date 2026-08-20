"""The resilience policies — circuit breaker, bulkhead, rate limiting, idempotency, saga.

Each policy is exercised twice where it ships twice (inbound middleware + outbound decorator) and the
time-based ones drive an injectable clock, so recovery windows and token refills are asserted
deterministically without sleeping. No broker, no real clock, no third-party package.
"""

from __future__ import annotations

import asyncio

from benzene.core import Context, MiddlewarePipeline, Registry, message_router
from benzene.resilience import (
    Bulkhead,
    CircuitBreaker,
    CircuitState,
    InMemoryIdempotencyStore,
    RateLimiter,
    Saga,
    circuit_breaker_interception,
    idempotency,
    rate_limit_interception,
    with_bulkhead,
    with_circuit_breaker,
    with_rate_limit,
)
from benzene.results import Result, Status


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


def test_idempotency_store_expires_entries() -> None:
    clock = ManualClock()
    store = InMemoryIdempotencyStore(ttl=5, clock=clock)
    run(store.put("k", Result.ok({"v": 1})))
    assert run(store.get("k")) is not None
    clock.advance(5)
    assert run(store.get("k")) is None  # TTL elapsed → forgotten


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
