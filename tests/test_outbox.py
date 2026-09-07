"""The transactional outbox — staging a send with the state write, and forwarding it afterwards.

The tests are written as the story the package exists to tell:

1. **The gap.** A handler that writes state and then sends loses the send when the broker is down —
   the state write stands, the message is gone, and nothing anywhere records that it was owed.
2. **The fix.** The same handler with the outbox in front of its sender: the send is captured
   durably, the caller is told ``accepted``, and a later dispatcher run forwards it once the broker
   is back.
3. **The crash window.** A dispatcher that dies between "the broker accepted" and "the store was
   told" re-forwards the envelope after its lease lapses — so the message is never lost — and both
   copies carry the *same* ``idempotency-key``, so a consumer running the shipped idempotency
   middleware runs its handler once. At-least-once delivery, deduped at the far end; never a silent
   double-effect.
4. **The poison bound.** A staged message that can never be sent parks after a bounded number of
   attempts (immediately, when its failure is not the kind a retry can fix) and never blocks the
   envelopes behind it.

Everything runs in memory on a manual clock — no broker, no sleeping, no third-party package. The
durable SQL store is exercised against stdlib ``sqlite3`` through a ~30-line adapter that presents
the same ``async with engine.begin() as conn`` shape SQLAlchemy 2.x does, which is also the proof
that the store abstracts nothing about the caller's database.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import sqlite3
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any

import pytest
from benzene.core import Context, MiddlewarePipeline, Registry, encode_body, message_router
from benzene.outbox import (
    CREATE_TABLE_SQL,
    BufferedOutboxStage,
    InMemoryOutboxStore,
    OutboxDispatcher,
    OutboxDispatchOutcome,
    OutboxEnvelope,
    OutboxOptions,
    OutboxStatus,
    SqlOutboxStage,
    SqlOutboxStore,
    outbox_dispatcher_worker,
    outbox_interception,
    outbox_transaction,
    run_outbox_dispatcher_loop,
    with_outbox,
)
from benzene.resilience import InMemoryIdempotencyStore, idempotency_interception
from benzene.results import Result, Status


def run(coro: Any) -> Any:
    return asyncio.run(coro)


class ManualClock:
    """A clock the test advances by hand — ``clock()`` reads seconds, ``clock.advance(dt)`` moves it."""

    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, dt: float) -> None:
        self.now += dt


class FakeBroker:
    """A ``MessageSender`` that records what it accepted, and can be taken offline."""

    def __init__(self, *, online: bool = True) -> None:
        self.online = online
        self.sent: list[tuple[str, Any, dict[str, str]]] = []

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        if not self.online:
            raise ConnectionError("broker unreachable")
        self.sent.append((topic, message, dict(headers or {})))
        return Result.ok()


class RefusingBroker:
    """A ``MessageSender`` that always answers with one failure status (never raises)."""

    def __init__(self, status: str = Status.SERVICE_UNAVAILABLE) -> None:
        self.status = status
        self.calls = 0

    async def send_message(
        self, topic: str, message: Any, headers: dict[str, str] | None = None
    ) -> Result:
        self.calls += 1
        return Result.failure(self.status, "nope")


# --- 1/2: the gap, and the fix -------------------------------------------------------------------


ORDER = {"order_id": "o-1", "total": 10}


async def _place_order(orders: dict[str, Any], sender: Any) -> Result:
    """The handler this whole package exists for: write state, then tell someone about it."""
    orders["o-1"] = ORDER  # the state write — committed, and it stays committed
    try:
        return await sender.send_message("orders:placed", ORDER)
    except Exception:  # the try/catch-and-log a service without an outbox is left with
        return Result.ok()


def test_without_an_outbox_a_send_that_fails_after_the_state_write_is_lost() -> None:
    orders: dict[str, Any] = {}
    broker = FakeBroker(online=False)

    result = run(_place_order(orders, broker))

    assert result.is_successful  # the caller was told the order was placed
    assert orders["o-1"] == ORDER  # and the state write stands
    assert broker.sent == []  # but the message never left, and nothing owes it
    broker.online = True
    assert broker.sent == []  # the broker coming back changes nothing: it was never recorded


def test_with_an_outbox_the_same_send_is_staged_and_forwarded_when_the_broker_returns() -> None:
    orders: dict[str, Any] = {}
    clock = ManualClock()
    broker = FakeBroker(online=False)
    store = InMemoryOutboxStore(clock=clock)

    result = run(_place_order(orders, with_outbox(broker, store, clock=clock)))

    assert result.status == Status.ACCEPTED  # deferred, and the caller is told so
    assert broker.sent == []  # nothing has been sent yet
    pending = run(store.claim_due(10, lease=0.0))
    assert [e.topic for e in pending] == ["orders:placed"]  # but the send is recorded, durably

    broker.online = True
    dispatched = run(OutboxDispatcher(store, broker, clock=clock).run_once())

    assert dispatched.dispatched == 1
    assert broker.sent[0][0] == "orders:placed"
    assert broker.sent[0][1] == ORDER
    envelope = run(store.get(pending[0].id))
    assert envelope is not None and envelope.status is OutboxStatus.DISPATCHED


# --- 3: the crash window -------------------------------------------------------------------------


def test_a_second_dispatcher_cannot_claim_an_envelope_another_is_still_forwarding() -> None:
    clock = ManualClock()
    store = InMemoryOutboxStore(clock=clock)
    broker = FakeBroker()
    run(with_outbox(broker, store, clock=clock).send_message("orders:placed", ORDER))

    claimed = run(store.claim_due(10, lease=120.0))
    assert len(claimed) == 1

    # A sweep on another instance, mid-forward: the lease is live, so it sees nothing to do.
    assert run(store.claim_due(10, lease=120.0)) == []
    assert run(OutboxDispatcher(store, broker, clock=clock).dispatch_one(claimed[0].id)) is (
        OutboxDispatchOutcome.CLAIM_REFUSED
    )


def test_a_dispatcher_that_crashes_mid_forward_re_forwards_with_the_same_idempotency_key() -> None:
    clock = ManualClock()
    store = InMemoryOutboxStore(clock=clock)
    broker = FakeBroker()
    run(with_outbox(broker, store, clock=clock).send_message("orders:placed", ORDER))

    # Dispatcher A: claims, the broker accepts... and the process dies before mark_dispatched.
    claimed = run(store.claim_due(10, lease=120.0))
    run(broker.send_message(claimed[0].topic, json.loads(claimed[0].payload), claimed[0].headers))
    assert len(broker.sent) == 1

    # Nothing is lost: once A's lease lapses, dispatcher B re-claims and forwards it again.
    clock.advance(121.0)
    assert run(OutboxDispatcher(store, broker, clock=clock).run_once()).dispatched == 1
    assert len(broker.sent) == 2

    # Both copies carry the same key, so the far end is what makes the effect happen once.
    keys = {sent[2]["idempotency-key"] for sent in broker.sent}
    assert keys == {claimed[0].id}

    runs = {"n": 0}

    async def handler(_request: Any) -> Result:
        runs["n"] += 1
        return Result.created({"run": runs["n"]})

    consumer = MiddlewarePipeline(
        [idempotency_interception(InMemoryIdempotencyStore())]
    ).use(message_router(Registry().register("orders:placed", handler)))
    for topic, message, headers in broker.sent:
        run(consumer.handle(Context(topic, message, headers=headers)))

    assert runs["n"] == 1  # delivered twice, applied once


# --- 4: the poison bound -------------------------------------------------------------------------


def test_a_poison_envelope_parks_and_never_blocks_the_ones_behind_it() -> None:
    clock = ManualClock()
    store = InMemoryOutboxStore(clock=clock)
    broker = RefusingBroker()
    options = OutboxOptions(max_attempts=3, backoff_base=10.0, claim_lease=5.0)
    sender = with_outbox(broker, store, clock=clock, options=options)
    run(sender.send_message("orders:poison", {"bad": True}))
    clock.advance(1.0)
    run(sender.send_message("orders:placed", ORDER))

    dispatcher = OutboxDispatcher(store, broker, options=options, clock=clock)

    first = run(dispatcher.run_once())
    assert (first.dispatched, first.rescheduled, first.parked) == (0, 2, 0)

    # The good envelope only failed because the broker was refusing everything; it goes through as
    # soon as the broker recovers, without waiting for the poison one to give up.
    healthy = FakeBroker()
    clock.advance(20.0)
    recovered = OutboxDispatcher(store, healthy, options=options, clock=clock)
    assert run(recovered.run_once()).dispatched == 2


def test_a_permanently_failing_envelope_parks_after_the_attempt_budget() -> None:
    clock = ManualClock()
    store = InMemoryOutboxStore(clock=clock)
    broker = RefusingBroker()
    options = OutboxOptions(max_attempts=3, backoff_base=10.0, backoff_cap=100.0, claim_lease=5.0)
    run(with_outbox(broker, store, clock=clock).send_message("orders:poison", {"bad": True}))
    dispatcher = OutboxDispatcher(store, broker, options=options, clock=clock)

    assert run(dispatcher.run_once()).rescheduled == 1
    clock.advance(10.0)  # backoff_base * 2^0
    assert run(dispatcher.run_once()).rescheduled == 1
    clock.advance(20.0)  # backoff_base * 2^1
    assert run(dispatcher.run_once()).parked == 1

    assert broker.calls == 3  # bounded — it is not retried forever
    envelope = run(store.get(run(_only_id(store))))
    assert envelope is not None
    assert envelope.status is OutboxStatus.PARKED
    assert envelope.last_error is not None and "service-unavailable" in envelope.last_error

    # Parked is terminal: never claimed again, and never swept away by retention either.
    clock.advance(10_000_000.0)
    assert run(dispatcher.run_once()).dispatched == 0
    assert run(store.claim_due(10, lease=5.0)) == []
    assert run(store.get(envelope.id)) is not None


def test_a_failure_a_retry_cannot_fix_parks_on_the_first_attempt() -> None:
    clock = ManualClock()
    store = InMemoryOutboxStore(clock=clock)
    broker = RefusingBroker(Status.BAD_REQUEST)
    run(with_outbox(broker, store, clock=clock).send_message("orders:placed", ORDER))

    result = run(OutboxDispatcher(store, broker, clock=clock).run_once())

    assert (result.parked, result.rescheduled) == (1, 0)
    assert broker.calls == 1  # a malformed message fails identically every time; don't burn 10


async def _only_id(store: InMemoryOutboxStore) -> str:
    ids = list(store.ids())
    assert len(ids) == 1
    return ids[0]


# --- capture semantics ---------------------------------------------------------------------------


def test_capture_never_calls_the_inner_sender_and_records_the_wire_body() -> None:
    clock = ManualClock()
    store = InMemoryOutboxStore(clock=clock)
    broker = FakeBroker()

    result = run(with_outbox(broker, store, clock=clock).send_message("t", {"orderId": "o-1"}))

    assert result.status == Status.ACCEPTED
    assert broker.sent == []
    envelope = run(store.get(run(_only_id(store))))
    assert envelope is not None
    assert envelope.payload == encode_body({"orderId": "o-1"})
    assert envelope.created_at == clock.now
    assert envelope.status is OutboxStatus.PENDING


def test_the_envelope_id_is_stamped_as_the_idempotency_key_only_when_absent() -> None:
    store = InMemoryOutboxStore()
    sender = with_outbox(FakeBroker(), store)

    run(sender.send_message("t", {}, {"X-Correlation-Id": "c-1"}))
    run(sender.send_message("t", {}, {"Idempotency-Key": "mine"}))

    stamped, supplied = sorted(run(store.claim_due(10, lease=0.0)), key=lambda e: e.created_at)
    assert stamped.headers["idempotency-key"] == stamped.id
    assert stamped.headers["x-correlation-id"] == "c-1"  # headers snapshot, lower-cased
    assert supplied.headers["idempotency-key"] == "mine"  # the caller's key is untouched


def test_the_dispatcher_replays_the_stored_headers_over_the_relay_hosts_own() -> None:
    from benzene.core import with_correlation_id

    clock = ManualClock()
    store = InMemoryOutboxStore(clock=clock)
    broker = FakeBroker()
    # The documented composition: stamping decorators go OUTSIDE the outbox, so what they stamp at
    # business time is what the envelope captures.
    sender = with_correlation_id(with_outbox(broker, store), new_id=lambda: "business-time")
    run(sender.send_message("t", {}))

    relay = with_correlation_id(broker, new_id=lambda: "relay-time")
    run(OutboxDispatcher(store, relay, clock=clock).run_once())

    assert broker.sent[0][2]["x-correlation-id"] == "business-time"


# --- the in-memory store -------------------------------------------------------------------------


def _envelope(id: str = "e-1", *, created_at: float = 0.0) -> OutboxEnvelope:  # noqa: A002
    return OutboxEnvelope(
        id=id, topic="t", payload="{}", headers={}, created_at=created_at
    )


def test_the_in_memory_store_claims_in_creation_order_and_leases_what_it_claims() -> None:
    clock = ManualClock()
    store = InMemoryOutboxStore(clock=clock)
    run(store.add([_envelope("b", created_at=2.0), _envelope("a", created_at=1.0)]))

    assert [e.id for e in run(store.claim_due(1, lease=30.0))] == ["a"]
    assert [e.id for e in run(store.claim_due(1, lease=30.0))] == ["b"]
    assert run(store.claim_due(10, lease=30.0)) == []  # both leased
    clock.advance(31.0)
    assert {e.id for e in run(store.claim_due(10, lease=30.0))} == {"a", "b"}  # leases lapsed


def test_exactly_one_of_two_concurrent_claims_for_one_envelope_wins() -> None:
    store = InMemoryOutboxStore()
    run(store.add([_envelope()]))

    async def race() -> list[OutboxEnvelope | None]:
        return list(await asyncio.gather(store.claim("e-1", 30.0), store.claim("e-1", 30.0)))

    won = [claim for claim in run(race()) if claim is not None]
    assert len(won) == 1


def test_retention_deletes_dispatched_envelopes_and_leaves_parked_ones_alone() -> None:
    clock = ManualClock()
    store = InMemoryOutboxStore(clock=clock)
    run(store.add([_envelope("dispatched"), _envelope("parked")]))
    run(store.mark_dispatched("dispatched"))
    run(store.park("parked", "never going to work"))

    assert run(store.delete_dispatched_before(clock.now - 1.0)) == 0  # inside the window
    clock.advance(10.0)
    assert run(store.delete_dispatched_before(clock.now)) == 1
    assert run(store.get("dispatched")) is None
    assert run(store.get("parked")) is not None


def test_lifecycle_calls_for_an_unknown_envelope_are_a_no_op() -> None:
    store = InMemoryOutboxStore()
    run(store.mark_dispatched("gone"))
    run(store.park("gone", "x"))
    run(store.reschedule("gone", 1, 1.0, "x"))


# --- the staging seam ----------------------------------------------------------------------------


def test_transactional_capture_stages_instead_of_writing_and_the_commit_is_the_callers() -> None:
    store = InMemoryOutboxStore()
    committed: list[Sequence[OutboxEnvelope]] = []

    async def scenario() -> None:
        async with outbox_transaction(commit=committed.append) as stage:
            sender = with_outbox(
                FakeBroker(), store, options=OutboxOptions(write_mode="transactional")
            )
            await sender.send_message("t", ORDER)
            assert isinstance(stage, BufferedOutboxStage) and stage.staged  # buffered...
            assert await store.claim_due(10, lease=0.0) == []  # ...and nothing written yet

    run(scenario())
    assert len(committed) == 1 and len(committed[0]) == 1


def test_an_exception_in_the_transaction_discards_the_staged_envelope() -> None:
    store = InMemoryOutboxStore()
    committed: list[Sequence[OutboxEnvelope]] = []

    async def scenario() -> None:
        with contextlib.suppress(RuntimeError):
            async with outbox_transaction(commit=committed.append):
                sender = with_outbox(
                    FakeBroker(), store, options=OutboxOptions(write_mode="transactional")
                )
                await sender.send_message("t", ORDER)
                raise RuntimeError("the handler's own write failed")

    run(scenario())
    assert committed == []
    assert run(store.claim_due(10, lease=0.0)) == []


def test_transactional_capture_with_no_stage_in_scope_refuses_loudly() -> None:
    store = InMemoryOutboxStore()
    sender = with_outbox(FakeBroker(), store, options=OutboxOptions(write_mode="transactional"))

    with pytest.raises(LookupError) as excinfo:
        run(sender.send_message("t", ORDER))

    assert "outbox_transaction" in str(excinfo.value)


def test_a_stage_dropped_with_undrained_envelopes_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    stage = BufferedOutboxStage()
    run(stage.stage(_envelope()))
    with caplog.at_level(logging.WARNING, logger="benzene.outbox"):
        stage.close()
    assert "never drained" in caplog.text


def test_the_middleware_commits_staged_sends_only_when_the_handler_succeeded() -> None:
    store = InMemoryOutboxStore()
    sender = with_outbox(FakeBroker(), store, options=OutboxOptions(write_mode="transactional"))

    async def ok(_request: Any) -> Result:
        await sender.send_message("orders:placed", ORDER)
        return Result.ok()

    async def fails(_request: Any) -> Result:
        await sender.send_message("orders:placed", ORDER)
        return Result.failure(Status.SERVICE_UNAVAILABLE, "the state write did not happen")

    registry = Registry().register("ok", ok).register("fails", fails)
    pipeline = MiddlewarePipeline([outbox_interception(store)]).use(message_router(registry))

    run(pipeline.handle(Context("fails", {})))
    assert run(store.claim_due(10, lease=0.0)) == []  # discarded with the failed invocation

    run(pipeline.handle(Context("ok", {})))
    assert len(run(store.claim_due(10, lease=0.0))) == 1


# --- the SQL store: the caller's own transaction --------------------------------------------------


class _SqliteResult:
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        self._cursor = cursor

    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount

    def fetchall(self) -> list[Any]:
        return self._cursor.fetchall()


class SqliteConnection:
    """The one method the store drives, over a stdlib ``sqlite3`` connection."""

    def __init__(self, raw: sqlite3.Connection) -> None:
        self.raw = raw

    async def execute(
        self, statement: Any, parameters: Mapping[str, Any] | None = None
    ) -> _SqliteResult:
        return _SqliteResult(self.raw.execute(str(statement), dict(parameters or {})))


class SqliteEngine:
    """``async with engine.begin() as conn`` — SQLAlchemy 2.x's shape, in thirty lines of stdlib."""

    def __init__(self, raw: sqlite3.Connection) -> None:
        self.raw = raw

    @contextlib.asynccontextmanager
    async def begin(self) -> AsyncIterator[SqliteConnection]:
        try:
            yield SqliteConnection(self.raw)
        except Exception:
            self.raw.rollback()
            raise
        else:
            self.raw.commit()


def _sqlite() -> tuple[SqliteEngine, sqlite3.Connection]:
    raw = sqlite3.connect(":memory:")
    raw.executescript(CREATE_TABLE_SQL)
    raw.execute("CREATE TABLE orders (id TEXT PRIMARY KEY, total INTEGER NOT NULL)")
    raw.commit()
    return SqliteEngine(raw), raw


def test_the_sql_store_runs_the_whole_lifecycle() -> None:
    engine, _ = _sqlite()
    clock = ManualClock()
    store = SqlOutboxStore(engine, clock=clock)
    broker = FakeBroker()
    run(with_outbox(broker, store, clock=clock).send_message("orders:placed", ORDER))

    claimed = run(store.claim_due(10, lease=30.0))
    assert len(claimed) == 1 and claimed[0].topic == "orders:placed"
    assert run(store.claim_due(10, lease=30.0)) == []  # leased — a second sweeper backs off
    assert run(store.claim(claimed[0].id, 30.0)) is None

    run(store.reschedule(claimed[0].id, 1, 60.0, "service-unavailable"))
    assert run(store.claim_due(10, lease=30.0)) == []  # not due yet
    clock.advance(61.0)
    again = run(store.claim_due(10, lease=30.0))
    assert again[0].attempt_count == 1 and again[0].last_error == "service-unavailable"

    run(store.mark_dispatched(again[0].id))
    stored = run(store.get(again[0].id))
    assert stored is not None and stored.status is OutboxStatus.DISPATCHED
    assert run(store.delete_dispatched_before(clock.now - 1.0)) == 0
    clock.advance(5.0)
    assert run(store.delete_dispatched_before(clock.now)) == 1
    assert run(store.get(again[0].id)) is None


def test_the_sql_store_never_deletes_a_parked_envelope() -> None:
    engine, _ = _sqlite()
    clock = ManualClock()
    store = SqlOutboxStore(engine, clock=clock)
    run(store.add([_envelope("parked")]))
    run(store.park("parked", "poison"))

    clock.advance(10_000_000.0)
    assert run(store.delete_dispatched_before(clock.now)) == 0
    parked = run(store.get("parked"))
    assert parked is not None and parked.status is OutboxStatus.PARKED
    assert run(store.claim_due(10, lease=30.0)) == []


def test_two_sql_stores_over_one_database_cannot_both_claim_one_envelope() -> None:
    engine, raw = _sqlite()
    clock = ManualClock()
    first = SqlOutboxStore(engine, clock=clock)
    second = SqlOutboxStore(SqliteEngine(raw), clock=clock)
    run(first.add([_envelope()]))

    won = [run(first.claim("e-1", 30.0)), run(second.claim("e-1", 30.0))]
    assert [claim is not None for claim in won] == [True, False]


def test_the_state_write_and_the_staged_send_commit_or_roll_back_together() -> None:
    engine, raw = _sqlite()
    store = SqlOutboxStore(engine)
    sender = with_outbox(
        FakeBroker(), store, options=OutboxOptions(write_mode="transactional"), new_id=lambda: "e-1"
    )

    async def place_order(*, fail: bool) -> None:
        # The caller's own transaction, on the caller's own connection. Benzene never opens it,
        # never commits it, and knows nothing about the `orders` table.
        connection = SqliteConnection(raw)
        try:
            await connection.execute(
                "INSERT INTO orders (id, total) VALUES (:id, :total)", {"id": "o-1", "total": 10}
            )
            async with outbox_transaction(stage=SqlOutboxStage(connection)):
                await sender.send_message("orders:placed", ORDER)
            if fail:
                raise RuntimeError("something later in the handler blew up")
        except RuntimeError:
            raw.rollback()
        else:
            raw.commit()

    run(place_order(fail=True))
    assert raw.execute("SELECT count(*) FROM orders").fetchone()[0] == 0
    assert run(store.claim_due(10, lease=0.0)) == []  # neither side happened

    run(place_order(fail=False))
    assert raw.execute("SELECT count(*) FROM orders").fetchone()[0] == 1
    assert [e.id for e in run(store.claim_due(10, lease=0.0))] == ["e-1"]  # both did


def test_building_a_sql_store_from_a_url_without_sqlalchemy_names_the_extra() -> None:
    try:
        import sqlalchemy  # noqa: F401
    except ImportError:
        pass
    else:
        pytest.skip("sqlalchemy is installed, so the teaching ImportError cannot fire")

    with pytest.raises(ImportError) as excinfo:
        SqlOutboxStore.from_url("sqlite+aiosqlite:///:memory:")

    assert "benzene-outbox[sql]" in str(excinfo.value)


# --- the dispatcher loop -------------------------------------------------------------------------


def test_the_dispatcher_loop_polls_until_it_is_told_to_stop_and_survives_a_failing_run() -> None:
    clock = ManualClock()
    store = InMemoryOutboxStore(clock=clock)
    broker = FakeBroker()
    run(with_outbox(broker, store, clock=clock).send_message("orders:placed", ORDER))

    runs = {"n": 0}

    class OneBadRun:
        """A dispatcher whose first run raises — the loop must log it and keep going."""

        def __init__(self, inner: OutboxDispatcher) -> None:
            self._inner = inner

        async def run_once(self) -> Any:
            runs["n"] += 1
            if runs["n"] == 1:
                raise ConnectionError("the outbox table was briefly unreachable")
            return await self._inner.run_once()

    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    run(
        run_outbox_dispatcher_loop(
            OneBadRun(OutboxDispatcher(store, broker, clock=clock)),
            should_continue=lambda: runs["n"] < 3,
            poll_interval=5.0,
            sleep=sleep,
        )
    )

    assert runs["n"] == 3
    assert slept == [5.0, 5.0, 5.0]
    assert len(broker.sent) == 1  # the failed run cost nothing but a poll interval


def test_dispatch_one_forwards_a_single_envelope_for_a_stream_triggered_relay() -> None:
    clock = ManualClock()
    store = InMemoryOutboxStore(clock=clock)
    broker = FakeBroker()
    run(with_outbox(broker, store, clock=clock).send_message("orders:placed", ORDER))
    envelope_id = run(_only_id(store))

    outcome = run(OutboxDispatcher(store, broker, clock=clock).dispatch_one(envelope_id))

    assert outcome is OutboxDispatchOutcome.DISPATCHED
    assert run(OutboxDispatcher(store, broker, clock=clock).dispatch_one("unknown")) is (
        OutboxDispatchOutcome.CLAIM_REFUSED
    )


def test_the_dispatcher_returns_what_it_did() -> None:
    clock = ManualClock()
    store = InMemoryOutboxStore(clock=clock)
    result = run(OutboxDispatcher(store, FakeBroker(), clock=clock).run_once())
    assert (result.dispatched, result.rescheduled, result.parked, result.deleted) == (0, 0, 0, 0)


# --- the seams that must stay lined up ------------------------------------------------------------


def test_the_header_capture_stamps_is_the_one_the_idempotency_middleware_reads() -> None:
    """The two packages click together by value, not by a dependency — so pin the value."""
    from benzene.outbox import IDEMPOTENCY_KEY_HEADER
    from benzene.resilience import DEFAULT_KEY_HEADERS

    assert IDEMPOTENCY_KEY_HEADER == DEFAULT_KEY_HEADERS[0] == "idempotency-key"


def test_a_transaction_cannot_be_both_staged_and_buffered() -> None:
    async def scenario() -> None:
        async with outbox_transaction(stage=BufferedOutboxStage(), commit=lambda _: None):
            pass  # pragma: no cover - the context manager raises before the body runs

    with pytest.raises(ValueError, match="not both"):
        run(scenario())


def test_the_middleware_refuses_to_be_built_with_nowhere_to_commit() -> None:
    with pytest.raises(ValueError, match="somewhere to commit"):
        outbox_interception()


def test_the_worker_factory_refuses_the_stop_signal_the_host_owns() -> None:
    dispatcher = OutboxDispatcher(InMemoryOutboxStore(), FakeBroker())
    with pytest.raises(TypeError, match="should_continue"):
        outbox_dispatcher_worker(dispatcher, should_continue=lambda: True)
