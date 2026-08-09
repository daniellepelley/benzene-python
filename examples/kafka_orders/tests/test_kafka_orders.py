"""Dogfooded, in-memory tests for the Kafka orders example.

No broker, no network: drive the real Kafka consumer binding via the shared harness
(``create_test_host(...).build_kafka()`` + ``send_kafka``) and fake only the outbound edge with
``benzene.testing.FakeMessageSender`` (port-quality-standards §4). The setup is identical to the
AWS/GCP/Azure suites bar the single ``.build_kafka()`` call — the test-champion consistency law.
"""

from __future__ import annotations

import asyncio

from benzene.core import MessageSender
from benzene.results import Status
from benzene.testing import FakeMessageSender, create_test_host
from orders_domain import ORDER_CREATED_TOPIC, ORDER_EVENTS_KEY, OrderService, OrdersStartUp


def make_host():
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []

    def overrides(services):
        services.add_instance(OrderService, service)
        services.add_instance(MessageSender, sender)  # only the external edge is faked
        services.add_instance(ORDER_EVENTS_KEY, seen)

    host = create_test_host(OrdersStartUp).with_services(overrides).build_kafka()
    return host, service, sender, seen


def test_kafka_place_order_creates_and_publishes() -> None:
    host, service, sender, _ = make_host()

    result = asyncio.run(host.send_kafka("orders:place", body={"sku": "ABC", "quantity": 2}))

    assert result.status == Status.CREATED
    # Egress: the handler published OrderCreated over Kafka (ingress -> handler -> egress).
    assert sender.last_topic == ORDER_CREATED_TOPIC
    assert sender.last_message.sku == "ABC"
    assert sender.last_message.id in service.orders


def test_kafka_place_order_validates_sku() -> None:
    host, _, sender, _ = make_host()
    result = asyncio.run(host.send_kafka("orders:place", body={"quantity": 1}))
    assert result.status == Status.BAD_REQUEST
    assert not sender.sent  # nothing published on a rejected record


def test_kafka_order_created_subscriber_records_the_id() -> None:
    # The domain also subscribes to orders:created; a record on that topic reaches the subscriber.
    host, _, _, seen = make_host()
    result = asyncio.run(
        host.send_kafka("orders:created", body={"id": "o-1", "sku": "ABC", "quantity": 1})
    )
    assert result.is_successful
    assert seen == ["o-1"]
