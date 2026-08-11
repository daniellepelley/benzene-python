"""The RabbitMQ binding — inbound decode + consumer loop + outbound producer (no broker, no SDK).

Drives the real binding logic against duck-typed fakes: a delivery decodes to the Benzene envelope with
the topic lifted from the `topic` header, the consumer loop dispatches one delivery per scope and acks
only successful deliveries (at-least-once), and the outbound sender forwards headers onto AMQP headers
and maps a publish failure to service-unavailable.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

from benzene.core import BenzeneMessageApplication, MiddlewarePipeline, Registry
from benzene.rabbitmq import (
    TOPIC_HEADER,
    RabbitMqConsumerApp,
    RabbitMqMessageSender,
    decode_rabbitmq_message,
    run_consumer_loop,
)
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

    asyncio.run(run_consumer_loop(app, channel, should_continue=should_continue))
    # at-least-once: the good delivery is acked, the failed one is nacked for redelivery.
    assert channel.acked == [1]
    assert channel.nacked == [2]


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
