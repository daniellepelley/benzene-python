"""Proof that the public harness reads like the gold-standard shape — identically on every cloud.

This is the test-champion's own dogfood: it boots the *same* real app from ``OrdersStartUp``, fakes
*only* the outbound edge with a ``FakeMessageSender``, and drives ingress → handler → egress through
the public harness. The setup (lines that call ``create_test_host(...).with_services(...)`` and the
egress assertions) is **byte-for-byte identical** across AWS, GCP, and Azure — the *only* thing that
changes per cloud is the single ``build_<cloud>()`` call and the native ``send_*`` shape. That
parallelism is the product; this test fails the moment it drifts.
"""

from __future__ import annotations

import asyncio
import inspect
import json

import pytest
from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host
from orders_domain import ORDER_CREATED_TOPIC, OrderEventLog, OrderService, OrdersStartUp


def _make_host(build: str):
    """Identical across clouds: boot the real app, fake only the edge, specialize in ONE call."""
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []

    def overrides(services):
        services.add_instance(OrderService, service)
        services.add_instance(MessageSender, sender)  # only the external edge is faked
        services.add_instance(OrderEventLog, seen)

    builder = create_test_host(OrdersStartUp).with_services(overrides)
    host = getattr(builder, build)()  # the ONE cloud-specific line
    return host, service, sender, seen


# (build_<cloud>, name of the native async-message send_*) — the only per-cloud differences.
CLOUDS = [
    ("build_aws", "send_sqs"),
    ("build_gcp", "send_pubsub"),
    ("build_azure", "send_service_bus"),
]


@pytest.mark.parametrize("build,send_async", CLOUDS, ids=[c[0] for c in CLOUDS])
def test_http_place_order_publishes_on_every_cloud(build: str, send_async: str) -> None:
    """Gold-standard shape: front door in, native response out, assert response AND egress."""
    host, service, sender, _ = _make_host(build)

    response = host.send_http("POST", "/orders", body={"sku": "ABC", "quantity": 2})

    # 6a. assert on the transport response — same attribute access on every cloud
    assert response.status_code == 201
    order = json.loads(response.body)
    # 6b. assert on the faked client's captured egress — same on every cloud
    assert sender.last_topic == ORDER_CREATED_TOPIC
    assert sender.last_message.id == order["id"]
    assert order["id"] in service.orders


@pytest.mark.parametrize("build,send_async", CLOUDS, ids=[c[0] for c in CLOUDS])
def test_native_async_message_reaches_handler_on_every_cloud(build: str, send_async: str) -> None:
    """The native async trigger (SQS / Pub/Sub / Service Bus) routes to the subscriber the same way.

    ``send_*`` takes (topic, payload, headers) on every cloud — the harmonised trio — so this single
    parametrised body drives all three.
    """
    host, _, _, seen = _make_host(build)

    getattr(host, send_async)(
        ORDER_CREATED_TOPIC,
        {"id": "ord-xcloud", "sku": "ABC"},
        headers={"trace-id": "t-123"},
    )

    assert seen == ["ord-xcloud"]


# (build_<host>, name of the *awaitable* native send_*) — the self-hosted consumer bindings.
# Their bindings are poll loops rather than request/response hosts, so ``send_*`` is a coroutine,
# but the argument trio and the ingress → handler path must stay identical to the cloud hosts'.
CONSUMERS = [
    ("build_kafka", "send_kafka"),
    ("build_rabbitmq", "send_rabbitmq"),
    ("build_sqs_consumer", "send_sqs_consumer"),
]


@pytest.mark.parametrize("build,send_async", CONSUMERS, ids=[c[0] for c in CONSUMERS])
def test_native_async_message_reaches_handler_on_every_consumer(build: str, send_async: str) -> None:
    """The self-hosted consumers (Kafka / RabbitMQ / SQS poller) route the same way as the clouds.

    Same ``(topic, body, headers)`` trio, same ``seen`` shape — the only difference is that these
    ``send_*`` are awaitable (the bindings are loops, so there is no synchronous response channel);
    each returns the mapped :class:`~benzene.results.Result` the loop would act on.
    """
    host, _, _, seen = _make_host(build)

    result = asyncio.run(
        getattr(host, send_async)(
            ORDER_CREATED_TOPIC,
            {"id": "ord-xcloud", "sku": "ABC"},
            headers={"trace-id": "t-123"},
        )
    )

    assert result.is_successful
    assert seen == ["ord-xcloud"]


# Every host whose native ``send_*`` carries a Benzene topic — the cloud triggers, the self-hosted
# consumer loops, and gRPC. (``send_http`` is deliberately absent: an HTTP request is addressed by
# method + path, and the topic is resolved from the route.)
TOPIC_SENDERS = [
    *CLOUDS,
    *CONSUMERS,
    ("build_grpc", "send_grpc"),
]


@pytest.mark.parametrize("build,send", TOPIC_SENDERS, ids=[c[0] for c in TOPIC_SENDERS])
def test_topic_senders_share_one_signature(build: str, send: str) -> None:
    """One loop, no per-cloud cases: every topic-carrying ``send_*`` opens ``(topic, body, headers)``.

    This is the consistency law at the signature level — a reader who learned ``send_sqs`` already
    knows ``send_kafka``. Extra trailing parameters (``message_id`` on ``send_sqs``) are fine; the
    order and naming of the leading trio is not negotiable.
    """
    host, _, _, _ = _make_host(build)

    parameters = list(inspect.signature(getattr(host, send)).parameters)

    assert parameters[:3] == ["topic", "body", "headers"]
