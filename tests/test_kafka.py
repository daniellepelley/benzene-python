"""The Kafka binding — inbound decode + consumer loop + outbound producer (no broker, no SDK).

Drives the real binding logic against duck-typed fakes: a record decodes to the Benzene envelope with
the topic lifted from the `topic` header, the consumer loop dispatches one record per scope and keeps
at-least-once honest (it commits a successful offset but seeks back to a failure rather than
committing past it), and the outbound sender forwards headers onto Kafka headers, maps a delivery
failure to service-unavailable and an unacknowledged flush to a timeout.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import types
from dataclasses import dataclass
from typing import Any

import benzene.kafka.consumer as consumer_module
import pytest
from benzene.core import BenzeneMessageApplication, MiddlewarePipeline, Registry
from benzene.kafka import (
    TOPIC_HEADER,
    KafkaConsumerApp,
    KafkaMessageSender,
    decode_kafka_message,
    run_consumer_loop,
)
from benzene.kafka.testing import FakeKafkaMessage, KafkaMessageBuilder, RecordingKafkaConsumer
from benzene.results import Result, Status


@dataclass
class PlaceOrder:
    sku: str = ""


def _app(handler=None) -> BenzeneMessageApplication:
    async def default(request: PlaceOrder) -> Result:
        return Result.created({"sku": request.sku})

    registry = Registry().register("orders:place", handler or default, request_type=PlaceOrder)
    return BenzeneMessageApplication(registry, MiddlewarePipeline())


async def _flaky(request: PlaceOrder) -> Result:
    """Succeeds for the ``ok`` sku, fails for anything else (drives the loop's commit decisions)."""
    return Result.created({}) if request.sku == "ok" else Result.failure(Status.SERVICE_UNAVAILABLE)


def _n_polls(n: int) -> Any:
    """A ``should_continue`` that stops the loop after ``n`` polls."""
    polls = {"n": 0}

    def should_continue() -> bool:
        polls["n"] += 1
        return polls["n"] <= n

    return should_continue


# --- inbound decode ----------------------------------------------------------------------------


def test_decode_lifts_topic_from_the_header_and_keeps_the_rest() -> None:
    message = (
        KafkaMessageBuilder("orders:place")
        .with_header("x-correlation-id", "c1")
        .with_body({"sku": "A"})
        .build()
    )
    envelope = decode_kafka_message(message)
    assert envelope["topic"] == "orders:place"
    assert envelope["headers"] == {"x-correlation-id": "c1"}  # topic removed, rest preserved
    assert json.loads(envelope["body"]) == {"sku": "A"}


def test_decode_tolerates_absent_headers_and_value() -> None:
    envelope = decode_kafka_message(FakeKafkaMessage(_headers=[], _value=b""))
    assert envelope == {"topic": "", "headers": {}, "body": ""}


# --- consumer dispatch + loop ------------------------------------------------------------------


def test_handle_message_runs_the_pipeline_and_maps_the_result() -> None:
    app = KafkaConsumerApp(_app())
    message = KafkaMessageBuilder("orders:place").with_body({"sku": "A"}).build()
    result = asyncio.run(app.handle_message(message))
    assert result.status == Status.CREATED


def test_a_poison_record_never_raises() -> None:
    async def boom(_request: PlaceOrder) -> Result:
        raise RuntimeError("handler blew up")

    app = KafkaConsumerApp(_app(boom))
    message = KafkaMessageBuilder("orders:place").with_body({"sku": "A"}).build()
    result = asyncio.run(app.handle_message(message))  # must not raise
    assert not result.is_successful  # the fault is a failure result, not a crash


def test_loop_commits_only_successful_offsets() -> None:
    async def flaky(request: PlaceOrder) -> Result:
        return (
            Result.created({})
            if request.sku == "ok"
            else Result.failure(Status.SERVICE_UNAVAILABLE)
        )

    app = KafkaConsumerApp(_app(flaky))
    good = KafkaMessageBuilder("orders:place").with_body({"sku": "ok"}).build()
    bad = KafkaMessageBuilder("orders:place").with_body({"sku": "bad"}).build()
    consumer = RecordingKafkaConsumer(records=[good, bad])

    polls = {"n": 0}

    def should_continue() -> bool:
        polls["n"] += 1
        return polls["n"] <= 3  # two records + one empty poll

    asyncio.run(run_consumer_loop(app, consumer, should_continue=should_continue))
    # at-least-once: the good record's offset is committed, the failed one is left for redelivery.
    assert consumer.committed == [good]


def test_loop_never_commits_past_an_uncommitted_failure() -> None:
    # Kafka commits are watermarks, not per-message acks: committing the *later* record's offset
    # would also mark the earlier failed one consumed, silently dropping it. The loop must instead
    # seek back to the failure and never commit an offset beyond it.
    app = KafkaConsumerApp(_app(_flaky))
    bad = (
        KafkaMessageBuilder("orders:place", partition=3, offset=5).with_body({"sku": "bad"}).build()
    )
    good = (
        KafkaMessageBuilder("orders:place", partition=3, offset=6).with_body({"sku": "ok"}).build()
    )
    consumer = RecordingKafkaConsumer(records=[bad, good])

    asyncio.run(run_consumer_loop(app, consumer, should_continue=_n_polls(2)))

    assert consumer.committed == []  # committing record 6 would have buried the failure at 5
    assert [(t.topic, t.partition, t.offset) for t in consumer.seeks] == [("benzene", 3, 5)]


def test_loop_commits_the_failed_record_and_its_successor_after_redelivery() -> None:
    attempts = {"n": 0}

    async def fails_once(_request: PlaceOrder) -> Result:
        attempts["n"] += 1
        return Result.failure(Status.SERVICE_UNAVAILABLE) if attempts["n"] == 1 else Result.ok()

    app = KafkaConsumerApp(_app(fails_once))
    first = (
        KafkaMessageBuilder("orders:place", partition=0, offset=5).with_body({"sku": "x"}).build()
    )
    redelivered = (
        KafkaMessageBuilder("orders:place", partition=0, offset=5).with_body({"sku": "x"}).build()
    )
    later = (
        KafkaMessageBuilder("orders:place", partition=0, offset=6).with_body({"sku": "y"}).build()
    )
    consumer = RecordingKafkaConsumer(records=[first, redelivered, later])

    asyncio.run(run_consumer_loop(app, consumer, should_continue=_n_polls(3)))

    # The seek re-served offset 5; once it succeeded the partition unblocked and both commits landed.
    assert consumer.committed == [redelivered, later]
    assert [t.offset for t in consumer.seeks] == [5]


def test_a_failure_on_one_partition_does_not_block_another() -> None:
    app = KafkaConsumerApp(_app(_flaky))
    bad = (
        KafkaMessageBuilder("orders:place", partition=1, offset=9).with_body({"sku": "bad"}).build()
    )
    good = (
        KafkaMessageBuilder("orders:place", partition=2, offset=4).with_body({"sku": "ok"}).build()
    )
    consumer = RecordingKafkaConsumer(records=[bad, good])

    asyncio.run(run_consumer_loop(app, consumer, should_continue=_n_polls(2)))

    # Offsets are per-partition watermarks: partition 2's success cannot bury partition 1's failure.
    assert consumer.committed == [good]
    assert [t.partition for t in consumer.seeks] == [1]


def test_loop_with_commit_disabled_lets_the_caller_control_commits() -> None:
    async def failing(_request: PlaceOrder) -> Result:
        return Result.failure(Status.SERVICE_UNAVAILABLE)

    app = KafkaConsumerApp(_app(failing))
    bad = KafkaMessageBuilder("orders:place", offset=11).with_body({"sku": "x"}).build()
    consumer = RecordingKafkaConsumer(records=[bad])
    seen: list[Any] = []

    asyncio.run(
        run_consumer_loop(
            app,
            consumer,
            should_continue=lambda: bool(consumer.records),
            commit=False,
            on_result=lambda m, r: seen.append(r),
        )
    )
    assert len(seen) == 1 and not seen[0].is_successful  # dispatched, but the loop owns no offsets
    assert consumer.committed == []
    assert consumer.seeks == []


def test_loop_warns_when_a_record_is_left_for_redelivery(
    caplog: pytest.LogCaptureFixture,
) -> None:
    app = KafkaConsumerApp(_app(_flaky))
    bad = KafkaMessageBuilder("orders:place", offset=2).with_body({"sku": "bad"}).build()
    consumer = RecordingKafkaConsumer(records=[bad])

    with caplog.at_level(logging.WARNING, logger="benzene.kafka.consumer"):
        asyncio.run(
            run_consumer_loop(app, consumer, should_continue=lambda: bool(consumer.records))
        )

    # A poison record must not loop invisibly: the failure is logged even with no on_result wired.
    assert len(caplog.records) == 1
    logged = caplog.records[0].getMessage()
    assert "orders:place" in logged and "service-unavailable" in logged


def test_loop_runs_the_blocking_kafka_calls_via_to_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    # poll/commit are synchronous confluent-kafka calls: run directly on the event loop, an idle
    # topic would spin without a single await point and starve every coroutine sharing the loop.
    routed: list[str] = []
    real_to_thread = asyncio.to_thread

    async def spy(func: Any, *args: Any, **kwargs: Any) -> Any:
        routed.append(getattr(func, "__name__", repr(func)))
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr(consumer_module.asyncio, "to_thread", spy)

    app = KafkaConsumerApp(_app())
    message = KafkaMessageBuilder("orders:place").with_body({"sku": "A"}).build()
    consumer = RecordingKafkaConsumer(records=[message])

    asyncio.run(run_consumer_loop(app, consumer, should_continue=lambda: bool(consumer.records)))

    assert routed == ["poll", "commit"]
    assert consumer.committed == [message]


def test_seek_target_uses_the_real_topic_partition_when_the_sdk_is_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # confluent's Consumer.seek only accepts a genuine TopicPartition; without the SDK the loop
    # falls back to a same-shaped record so the binding still runs (and tests) with plain fakes.
    class _TopicPartition:
        def __init__(self, topic: str, partition: int, offset: int) -> None:
            self.topic, self.partition, self.offset = topic, partition, offset

    stub = types.ModuleType("confluent_kafka")
    stub.TopicPartition = _TopicPartition  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "confluent_kafka", stub)

    message = KafkaMessageBuilder(
        "orders:place", kafka_topic="orders-events", partition=2, offset=17
    ).build()
    target = consumer_module._seek_target(message)
    assert isinstance(target, _TopicPartition)
    assert (target.topic, target.partition, target.offset) == ("orders-events", 2, 17)


def test_loop_skips_records_carrying_a_broker_error() -> None:
    app = KafkaConsumerApp(_app())
    errored = FakeKafkaMessage(
        _headers=[(TOPIC_HEADER, b"orders:place")], _value=b"{}", _error="EOF"
    )
    consumer = RecordingKafkaConsumer(records=[errored])
    seen: list[Any] = []

    asyncio.run(
        run_consumer_loop(
            app,
            consumer,
            should_continue=lambda: bool(consumer.records),
            on_result=lambda m, r: seen.append(r),
        )
    )
    assert seen == []  # the error record was skipped, never dispatched
    assert consumer.committed == []


# --- outbound producer -------------------------------------------------------------------------


class _FakeProducer:
    """A duck-typed ``confluent_kafka.Producer``: ``produce`` + ``flush`` → messages still in flight."""

    def __init__(self, *, fail: bool = False, remaining: int = 0, deliver: bool = True) -> None:
        self.produced: list[dict[str, Any]] = []
        self.flushed: list[float] = []
        self._fail = fail
        self._remaining = remaining
        self._deliver = deliver

    def produce(self, topic, value, headers, on_delivery) -> None:
        self.produced.append({"topic": topic, "value": value, "headers": headers})
        if self._deliver:  # a broker that never acks leaves the callback unfired
            on_delivery("boom" if self._fail else None, None)

    def flush(self, timeout) -> int:
        self.flushed.append(timeout)
        return self._remaining


def test_sender_forwards_headers_and_tags_the_topic() -> None:
    producer = _FakeProducer()
    sender = KafkaMessageSender("orders-events", producer=producer)
    result = asyncio.run(
        sender.send_message("orders:created", {"sku": "A"}, headers={"x-correlation-id": "c1"})
    )
    assert result.is_successful
    record = producer.produced[0]
    assert record["topic"] == "orders-events"  # the physical Kafka topic
    header_map = {k: v.decode() for k, v in record["headers"]}
    assert header_map[TOPIC_HEADER] == "orders:created"  # Benzene topic carried in the header
    assert header_map["x-correlation-id"] == "c1"
    assert json.loads(record["value"].decode()) == {"sku": "A"}


def test_sender_maps_a_delivery_failure_to_service_unavailable() -> None:
    sender = KafkaMessageSender("orders-events", producer=_FakeProducer(fail=True))
    result = asyncio.run(sender.send_message("orders:created", {"sku": "A"}))
    assert result.status == Status.SERVICE_UNAVAILABLE


def test_sender_maps_an_unacknowledged_flush_to_a_timeout() -> None:
    # Broker unreachable: produce() only buffers locally, flush times out with the message still in
    # flight and no delivery callback ever fires — reporting ok here would lose the message silently.
    producer = _FakeProducer(remaining=1, deliver=False)
    sender = KafkaMessageSender("orders-events", producer=producer, flush_timeout=0.25)
    result = asyncio.run(sender.send_message("orders:created", {"sku": "A"}))
    assert result.status == Status.TIMEOUT
    assert producer.flushed == [0.25]  # the configured bound was actually passed to flush


def test_sender_is_successful_when_flush_drains_and_the_callback_fires() -> None:
    sender = KafkaMessageSender("orders-events", producer=_FakeProducer(remaining=0))
    result = asyncio.run(sender.send_message("orders:created", {"sku": "A"}))
    assert result.status == Status.OK


def test_sender_without_the_sdk_raises_a_teaching_import_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A missing optional dependency is a deployment error, not a per-message Result: it must escape
    # send_message's failure mapper so it can't be retried or tripped over by a circuit breaker.
    monkeypatch.setitem(sys.modules, "confluent_kafka", None)
    sender = KafkaMessageSender("orders-events", bootstrap_servers="localhost:9092")
    with pytest.raises(ImportError, match=r"benzene-kafka\[kafka\]"):
        asyncio.run(sender.send_message("orders:created", {"sku": "A"}))
