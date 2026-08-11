"""Dogfooded, in-memory tests for the SQS orders example.

No queue, no network: drive the real self-hosted SQS consumer binding via the shared harness
(``create_test_host(...).build_sqs_consumer()`` + ``send_sqs_consumer``) and fake only the outbound
edge with ``benzene.testing.FakeMessageSender`` (port-quality-standards §4). The setup is identical to
the Kafka/AWS/GCP/Azure suites bar the single ``.build_sqs_consumer()`` call — the test-champion
consistency law.
"""

from __future__ import annotations

import asyncio

from benzene.core import MessageSender
from benzene.results import Status
from benzene.testing import FakeMessageSender, create_test_host
from orders_domain import ORDER_CREATED_TOPIC, OrderEventLog, OrderService, OrdersStartUp


def make_host():
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []

    def overrides(services):
        services.add_instance(OrderService, service)
        services.add_instance(MessageSender, sender)  # only the external edge is faked
        services.add_instance(OrderEventLog, seen)

    host = create_test_host(OrdersStartUp).with_services(overrides).build_sqs_consumer()
    return host, service, sender, seen


def test_sqs_place_order_creates_and_publishes() -> None:
    host, service, sender, _ = make_host()

    result = asyncio.run(host.send_sqs_consumer("orders:place", body={"sku": "ABC", "quantity": 2}))

    assert result.status == Status.CREATED
    # Egress: the handler published OrderCreated over SQS (ingress -> handler -> egress).
    assert sender.last_topic == ORDER_CREATED_TOPIC
    assert sender.last_message.sku == "ABC"
    assert sender.last_message.id in service.orders


def test_sqs_place_order_validates_sku() -> None:
    host, _, sender, _ = make_host()
    result = asyncio.run(host.send_sqs_consumer("orders:place", body={"quantity": 1}))
    assert result.status == Status.BAD_REQUEST
    assert not sender.sent  # nothing published on a rejected message


def test_sqs_order_created_subscriber_records_the_id() -> None:
    # The domain also subscribes to orders:created; a message on that topic reaches the subscriber.
    host, _, _, seen = make_host()
    result = asyncio.run(
        host.send_sqs_consumer("orders:created", body={"id": "o-1", "sku": "ABC", "quantity": 1})
    )
    assert result.is_successful
    assert seen == ["o-1"]
