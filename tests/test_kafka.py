"""The Kafka binding — inbound decode + consumer loop + outbound producer (no broker, no SDK).

Drives the real binding logic against duck-typed fakes: a record decodes to the Benzene envelope with
the topic lifted from the `topic` header, the consumer loop dispatches one record per scope and commits
only successful offsets (at-least-once), and the outbound sender forwards headers onto Kafka headers and
maps a delivery failure to service-unavailable.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

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
    def __init__(self, *, fail: bool = False) -> None:
        self.produced: list[dict[str, Any]] = []
        self._fail = fail

    def produce(self, topic, value, headers, on_delivery) -> None:
        self.produced.append({"topic": topic, "value": value, "headers": headers})
        on_delivery("boom" if self._fail else None, None)

    def flush(self, _timeout) -> None:
        return None


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
