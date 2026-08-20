"""The RabbitMQ binding — inbound decode + consumer loop + outbound producer (no broker, no SDK).

Drives the real binding logic against duck-typed fakes: a delivery decodes to the Benzene envelope with
the topic lifted from the `topic` header, the consumer loop dispatches one delivery per scope and acks
only successful deliveries (at-least-once), and the outbound sender forwards headers onto AMQP headers
and maps a publish failure to service-unavailable.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import threading
import types
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest
from benzene.core import BenzeneMessageApplication, MiddlewarePipeline, Registry
from benzene.rabbitmq import (
    TOPIC_HEADER,
    RabbitMqConsumerApp,
    RabbitMqMessageSender,
    decode_rabbitmq_message,
    run_consumer_loop,
)
from benzene.rabbitmq import consumer as consumer_module
from benzene.rabbitmq.testing import (
    FakeRabbitMqMethod,
    FakeRabbitMqProperties,
    RabbitMqMessageBuilder,
    RecordingRabbitMqChannel,
)
from benzene.results import Result, Status


@dataclass
class PlaceOrder:
    sku: str = ""


def _app(handler=None) -> BenzeneMessageApplication:
    async def default(request: PlaceOrder) -> Result:
        return Result.created({"sku": request.sku})

    registry = Registry().register("orders:place", handler or default, request_type=PlaceOrder)
    return BenzeneMessageApplication(registry, MiddlewarePipeline())


# --- inbound decode ----------------------------------------------------------------------------


def test_decode_lifts_topic_from_the_header_and_keeps_the_rest() -> None:
    message = (
        RabbitMqMessageBuilder("orders:place")
        .with_header("x-correlation-id", "c1")
        .with_body({"sku": "A"})
        .build()
    )
    envelope = decode_rabbitmq_message(message.method, message.properties, message.body)
    assert envelope["topic"] == "orders:place"
    assert envelope["headers"] == {"x-correlation-id": "c1"}  # topic removed, rest preserved
    assert json.loads(envelope["body"]) == {"sku": "A"}


def test_decode_tolerates_absent_headers_and_body() -> None:
    envelope = decode_rabbitmq_message(
        FakeRabbitMqMethod(), FakeRabbitMqProperties(headers=None), b""
    )
    assert envelope == {"topic": "", "headers": {}, "body": ""}


# --- consumer dispatch + loop ------------------------------------------------------------------


def test_handle_message_runs_the_pipeline_and_maps_the_result() -> None:
    app = RabbitMqConsumerApp(_app())
    message = RabbitMqMessageBuilder("orders:place").with_body({"sku": "A"}).build()
    result = asyncio.run(app.handle_message(message.method, message.properties, message.body))
    assert result.status == Status.CREATED


def test_a_poison_delivery_never_raises() -> None:
    async def boom(_request: PlaceOrder) -> Result:
        raise RuntimeError("handler blew up")

    app = RabbitMqConsumerApp(_app(boom))
    message = RabbitMqMessageBuilder("orders:place").with_body({"sku": "A"}).build()
    result = asyncio.run(
        app.handle_message(message.method, message.properties, message.body)
    )  # must not raise
    assert not result.is_successful  # the fault is a failure result, not a crash


def test_loop_acks_only_successful_deliveries() -> None:
    async def flaky(request: PlaceOrder) -> Result:
        return (
            Result.created({})
            if request.sku == "ok"
            else Result.failure(Status.SERVICE_UNAVAILABLE)
        )

    app = RabbitMqConsumerApp(_app(flaky))
    good = RabbitMqMessageBuilder("orders:place", delivery_tag=1).with_body({"sku": "ok"}).build()
    bad = RabbitMqMessageBuilder("orders:place", delivery_tag=2).with_body({"sku": "bad"}).build()
    channel = RecordingRabbitMqChannel(deliveries=[good, bad])

    pulls = {"n": 0}

    def should_continue() -> bool:
        pulls["n"] += 1
        return pulls["n"] <= 3  # two deliveries + one empty pull

    asyncio.run(
        run_consumer_loop(
            app, channel, queue="orders", idle_sleep=0, should_continue=should_continue
        )
    )
    # at-least-once: the good delivery is acked, the failed one is nacked for redelivery.
    assert channel.acked == [1]
    assert channel.nacked == [2]
    # service-unavailable is transient, so the nack keeps the delivery on the queue.
    assert channel.nacks == [{"delivery_tag": 2, "requeue": True}]


def test_loop_leaves_failures_unacked_when_ack_is_manual() -> None:
    async def failing(_request: PlaceOrder) -> Result:
        return Result.failure(Status.SERVICE_UNAVAILABLE)

    app = RabbitMqConsumerApp(_app(failing))
    bad = RabbitMqMessageBuilder("orders:place", delivery_tag=7).with_body({"sku": "x"}).build()
    channel = RecordingRabbitMqChannel(deliveries=[bad])
    seen: list[Any] = []

    asyncio.run(
        run_consumer_loop(
            app,
            channel,
            queue="orders",
            should_continue=lambda: bool(channel.deliveries),
            ack=False,
            on_result=lambda m, r: seen.append(r),
        )
    )
    assert len(seen) == 1 and not seen[0].is_successful  # dispatched, but the loop touched no acks
    assert channel.acked == []
    assert channel.nacked == []


# --- outbound producer -------------------------------------------------------------------------


class _FakeChannel:
    def __init__(self, *, fail: bool = False) -> None:
        self.published: list[dict[str, Any]] = []
        self._fail = fail

    def basic_publish(self, *, exchange, routing_key, body, properties=None) -> None:
        if self._fail:
            raise RuntimeError("broker down")
        self.published.append(
            {
                "exchange": exchange,
                "routing_key": routing_key,
                "body": body,
                "properties": properties,
            }
        )


def test_sender_forwards_headers_and_tags_the_topic() -> None:
    channel = _FakeChannel()
    sender = RabbitMqMessageSender("orders-events", "orders", channel=channel)
    result = asyncio.run(
        sender.send_message("orders:created", {"sku": "A"}, headers={"x-correlation-id": "c1"})
    )
    assert result.is_successful
    published = channel.published[0]
    assert published["exchange"] == "orders-events"  # the physical AMQP exchange
    assert published["routing_key"] == "orders"
    header_map = published["properties"].headers
    assert header_map[TOPIC_HEADER] == "orders:created"  # Benzene topic carried in the header
    assert header_map["x-correlation-id"] == "c1"
    assert json.loads(published["body"].decode()) == {"sku": "A"}


def test_sender_maps_a_publish_failure_to_service_unavailable() -> None:
    sender = RabbitMqMessageSender("orders-events", channel=_FakeChannel(fail=True))
    result = asyncio.run(sender.send_message("orders:created", {"sku": "A"}))
    assert result.status == Status.SERVICE_UNAVAILABLE


def test_recording_channel_serves_as_the_publish_sink_too() -> None:
    channel = RecordingRabbitMqChannel()
    sender = RabbitMqMessageSender("orders-events", channel=channel)
    result = asyncio.run(sender.send_message("orders:created", {"sku": "A"}))
    assert result.is_successful
    assert channel.published[0]["properties"].headers[TOPIC_HEADER] == "orders:created"


# --- poison handling, idle backoff, and off-loop dispatch ---------------------------------------


@dataclass
class _RequeueingChannel:
    """A channel that honours ``requeue``: a nacked-with-requeue delivery returns to the queue head.

    The real broker behaviour the poison hot-loop depends on — without it a test cannot tell a
    dead-lettered delivery from a requeued one, because both simply disappear from a fixed list.
    """

    deliveries: list[Any] = field(default_factory=list)
    acked: list[int] = field(default_factory=list)
    nacks: list[dict[str, Any]] = field(default_factory=list)
    gets: int = 0
    in_flight: Any = None

    def basic_get(self, queue: str = "", *, auto_ack: bool = False) -> tuple[Any, Any, Any]:
        self.gets += 1
        if self.deliveries:
            self.in_flight = self.deliveries.pop(0)
            return self.in_flight.method, self.in_flight.properties, self.in_flight.body
        return None, None, None

    def basic_ack(self, *, delivery_tag: int, multiple: bool = False) -> None:
        self.acked.append(delivery_tag)

    def basic_nack(
        self, *, delivery_tag: int, multiple: bool = False, requeue: bool = True
    ) -> None:
        self.nacks.append({"delivery_tag": delivery_tag, "requeue": requeue})
        if requeue:  # back to the head of the queue, exactly as the broker would
            self.deliveries.insert(0, self.in_flight)


def _loop_bounded_to(pulls: int) -> Callable[[], bool]:
    counter = {"n": 0}

    def should_continue() -> bool:
        counter["n"] += 1
        return counter["n"] <= pulls

    return should_continue


def test_loop_dead_letters_a_non_retryable_failure_instead_of_requeueing_it() -> None:
    # A deterministic failure (bad-request/not-found) never gets better on redelivery: requeueing it
    # spins the loop at full speed forever. It must be nacked with requeue=False so the queue's
    # dead-letter exchange takes it.
    async def rejecting(_request: PlaceOrder) -> Result:
        return Result.failure(Status.BAD_REQUEST, "malformed")

    app = RabbitMqConsumerApp(_app(rejecting))
    poison = RabbitMqMessageBuilder("orders:place", delivery_tag=9).with_body({"sku": "x"}).build()
    channel = _RequeueingChannel(deliveries=[poison])

    asyncio.run(
        run_consumer_loop(
            app, channel, queue="orders", idle_sleep=0, should_continue=_loop_bounded_to(4)
        )
    )

    assert channel.nacks == [{"delivery_tag": 9, "requeue": False}]  # dropped to the DLX, once
    assert channel.deliveries == []  # and never served back to the loop
    assert channel.gets == 4  # the poison delivery, then three empty pulls — no hot loop


def test_loop_requeues_a_transient_failure_for_redelivery() -> None:
    # The other half of the rule: a transient status is still requeued (at-least-once preserved).
    async def unavailable(_request: PlaceOrder) -> Result:
        return Result.failure(Status.SERVICE_UNAVAILABLE, "broker hiccup")

    app = RabbitMqConsumerApp(_app(unavailable))
    delivery = RabbitMqMessageBuilder("orders:place", delivery_tag=3).with_body({"sku": "x"}).build()
    channel = _RequeueingChannel(deliveries=[delivery])

    asyncio.run(
        run_consumer_loop(app, channel, queue="orders", should_continue=_loop_bounded_to(2))
    )

    assert [n["requeue"] for n in channel.nacks] == [True, True]  # requeued, then served again
    assert channel.deliveries  # still on the queue for the next worker


def test_loop_sleeps_instead_of_busy_polling_an_empty_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # basic_get on an empty queue returns instantly; with no delay the loop spins at full speed
    # (unlike Kafka's poll_timeout or SQS's long poll). Recorded sleeps, never wall-clock time.
    slept: list[float] = []
    real_sleep = asyncio.sleep

    async def spy(delay: float, *args: Any, **kwargs: Any) -> Any:
        slept.append(delay)
        return await real_sleep(0)

    monkeypatch.setattr(consumer_module.asyncio, "sleep", spy)

    app = RabbitMqConsumerApp(_app())
    channel = RecordingRabbitMqChannel()  # empty queue: every get is a miss

    asyncio.run(
        run_consumer_loop(app, channel, queue="orders", should_continue=_loop_bounded_to(3))
    )
    assert slept == [1.0, 1.0, 1.0]  # the default idle backoff, once per empty pull

    slept.clear()
    asyncio.run(
        run_consumer_loop(
            app, channel, queue="orders", idle_sleep=0.25, should_continue=_loop_bounded_to(2)
        )
    )
    assert slept == [0.25, 0.25]  # and it is tunable


def test_loop_does_not_sleep_while_deliveries_keep_arriving(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slept: list[float] = []
    real_sleep = asyncio.sleep

    async def spy(delay: float, *args: Any, **kwargs: Any) -> Any:
        slept.append(delay)
        return await real_sleep(0)

    monkeypatch.setattr(consumer_module.asyncio, "sleep", spy)

    app = RabbitMqConsumerApp(_app())
    message = RabbitMqMessageBuilder("orders:place").with_body({"sku": "A"}).build()
    channel = RecordingRabbitMqChannel(deliveries=[message])

    asyncio.run(
        run_consumer_loop(
            app, channel, queue="orders", should_continue=lambda: bool(channel.deliveries)
        )
    )
    assert slept == []  # a busy queue never pays the idle backoff


def test_loop_runs_the_blocking_pika_calls_via_to_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    # basic_get/basic_ack are blocking network round-trips: they must be dispatched off the event
    # loop so a co-hosted server (uvicorn in the k8s_orders example) is never starved.
    routed: list[str] = []
    real_to_thread = asyncio.to_thread

    async def spy(func: Any, *args: Any, **kwargs: Any) -> Any:
        routed.append(getattr(func, "__name__", repr(func)))
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(consumer_module.asyncio, "to_thread", spy)

    app = RabbitMqConsumerApp(_app())
    message = RabbitMqMessageBuilder("orders:place").with_body({"sku": "A"}).build()
    channel = RecordingRabbitMqChannel(deliveries=[message])

    asyncio.run(
        run_consumer_loop(
            app, channel, queue="orders", should_continue=lambda: bool(channel.deliveries)
        )
    )
    assert routed == ["basic_get", "basic_ack"]  # both SDK calls left the event loop, in order
    assert channel.acked == [1]


def test_loop_warns_about_a_failed_delivery_distinguishing_dlx_from_redelivery(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Without on_result wired, a failing delivery used to vanish silently.
    async def rejecting(_request: PlaceOrder) -> Result:
        return Result.failure(Status.BAD_REQUEST, "malformed")

    async def unavailable(_request: PlaceOrder) -> Result:
        return Result.failure(Status.SERVICE_UNAVAILABLE, "hiccup")

    poison = RabbitMqMessageBuilder("orders:place", delivery_tag=1).with_body({"sku": "x"}).build()
    transient = RabbitMqMessageBuilder("orders:place", delivery_tag=2).with_body({"sku": "x"}).build()

    with caplog.at_level(logging.WARNING, logger="benzene.rabbitmq.consumer"):
        asyncio.run(
            run_consumer_loop(
                RabbitMqConsumerApp(_app(rejecting)),
                _RequeueingChannel(deliveries=[poison]),
                queue="orders",
                should_continue=_loop_bounded_to(1),
            )
        )
        asyncio.run(
            run_consumer_loop(
                RabbitMqConsumerApp(_app(unavailable)),
                _RequeueingChannel(deliveries=[transient]),
                queue="orders",
                should_continue=_loop_bounded_to(1),
            )
        )

    messages = [record.getMessage() for record in caplog.records]
    assert len(messages) == 2
    assert "orders:place" in messages[0] and Status.BAD_REQUEST in messages[0]
    assert "dead-letter" in messages[0]  # dropped: the DLX (if any) takes it
    assert "redelivery" in messages[1]  # requeued: it comes back
    assert "dead-letter" not in messages[1]


def test_the_queue_name_is_required() -> None:
    # An empty queue name is almost never what a caller meant; the binding refuses to guess.
    app = RabbitMqConsumerApp(_app())
    with pytest.raises(TypeError):
        run_consumer_loop(app, RecordingRabbitMqChannel()).close()  # type: ignore[call-arg]


# --- producer: one channel, many coroutines ----------------------------------------------------


class _ConcurrencyRecordingChannel:
    """Records how many publishes are inside ``basic_publish`` at once, and whether the lock is held."""

    def __init__(self, sender_lock: Any) -> None:
        self._sender_lock = sender_lock
        self._guard = threading.Lock()
        self.in_flight = 0
        self.max_in_flight = 0
        self.lock_held: list[bool] = []
        self.published: list[dict[str, Any]] = []

    def basic_publish(self, *, exchange, routing_key, body, properties=None) -> None:
        with self._guard:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            self.lock_held.append(self._sender_lock().locked())
            self.published.append({"body": body, "properties": properties})
        with self._guard:
            self.in_flight -= 1


def test_concurrent_publishes_are_serialized_over_the_shared_channel() -> None:
    # pika channels are not thread-safe: two to_thread workers publishing on one channel interleave
    # AMQP frames. The sender holds an asyncio.Lock across client creation + publish.
    sender = RabbitMqMessageSender("orders-events", "orders")
    # White-box on purpose: the fake reads the documented lock to prove it is held during the publish.
    channel = _ConcurrencyRecordingChannel(lambda: sender._lock)
    sender._channel = channel

    async def publish_all() -> list[Result]:
        return list(
            await asyncio.gather(
                *(sender.send_message("orders:created", {"sku": str(n)}) for n in range(5))
            )
        )

    results = asyncio.run(publish_all())
    assert all(r.is_successful for r in results)
    assert len(channel.published) == 5
    assert channel.max_in_flight == 1  # never two workers on one channel
    assert channel.lock_held == [True] * 5  # each publish ran under the sender's lock


def test_the_blocking_connection_is_opened_on_the_worker_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # pika.BlockingConnection is blocking network I/O: opening it inline stalls the event loop.
    opened_on: list[int] = []

    class _StubChannel:
        def __init__(self) -> None:
            self.published: list[Any] = []

        def basic_publish(self, *, exchange, routing_key, body, properties=None) -> None:
            self.published.append(body)

    class _StubConnection:
        def __init__(self, _parameters: Any) -> None:
            opened_on.append(threading.get_ident())
            self._channel = _StubChannel()

        def channel(self) -> _StubChannel:
            return self._channel

    pika = types.ModuleType("pika")
    pika.BlockingConnection = _StubConnection  # type: ignore[attr-defined]
    pika.ConnectionParameters = lambda **kwargs: kwargs  # type: ignore[attr-defined]
    pika.BasicProperties = FakeRabbitMqProperties  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pika", pika)

    sender = RabbitMqMessageSender("orders-events", host="broker")
    result = asyncio.run(sender.send_message("orders:created", {"sku": "A"}))

    assert result.is_successful
    assert opened_on and opened_on[0] != threading.get_ident()  # off the event loop's thread
    asyncio.run(sender.send_message("orders:created", {"sku": "B"}))
    assert len(opened_on) == 1  # and the connection is opened once, then reused


def test_a_missing_pika_raises_a_teaching_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # A forgotten extra is a deployment error, not a per-message service-unavailable that retry
    # middleware and circuit breakers then hammer.
    monkeypatch.setitem(sys.modules, "pika", None)
    sender = RabbitMqMessageSender("orders-events")

    with pytest.raises(ImportError) as caught:
        asyncio.run(sender.send_message("orders:created", {"sku": "A"}))
    assert "benzene-rabbitmq[rabbitmq]" in str(caught.value)
