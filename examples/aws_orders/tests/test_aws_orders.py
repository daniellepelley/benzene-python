"""Dogfooded, in-memory tests for the AWS orders example.

Drives the real Lambda bindings via ``benzene.aws.testing`` and fakes only the outbound edge.
Covers all three event sources (API Gateway, SQS, SNS), ingress → handler → egress, and the SQS
partial-batch-failure protocol.
"""

from __future__ import annotations

import json

from benzene.aws.testing import SqsEventBuilder
from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host
from orders_domain import ORDER_CREATED_TOPIC, ORDER_EVENTS_KEY, OrderService, OrdersStartUp


def make_host():
    """Boot the real app from OrdersStartUp, fake only the edges, specialize to AWS.

    Identical to the GCP and Azure suites except the single ``.build_aws()`` call and the ``send_*``
    shape — the test-champion consistency law in action.
    """
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []

    def overrides(services):
        services.add_instance(OrderService, service)
        services.add_instance(MessageSender, sender)
        services.add_instance(ORDER_EVENTS_KEY, seen)

    host = create_test_host(OrdersStartUp).with_services(overrides).build_aws()
    return host, service, sender, seen


def test_api_gateway_place_order_creates_and_publishes() -> None:
    host, service, sender, _ = make_host()

    response = host.send_http("POST", "/orders", body={"sku": "ABC", "quantity": 2})

    assert response.status_code == 201
    order = json.loads(response.body)
    assert order["sku"] == "ABC"
    assert sender.last_topic == ORDER_CREATED_TOPIC        # ingress -> handler -> egress
    assert sender.last_message.id == order["id"]
    assert order["id"] in service.orders


def test_api_gateway_get_order_round_trip() -> None:
    host, _, _, _ = make_host()
    created = json.loads(host.send_http("POST", "/orders", body={"sku": "XYZ"}).body)
    response = host.send_http("GET", f"/orders/{created['id']}")
    assert response.status_code == 200
    assert json.loads(response.body)["id"] == created["id"]


def test_sqs_order_created_is_handled() -> None:
    host, _, _, seen = make_host()
    result = host.send_sqs(ORDER_CREATED_TOPIC, {"id": "ord-sqs", "sku": "ABC"})
    assert result.batch_item_failures == []
    assert seen == ["ord-sqs"]


def test_sns_order_created_is_handled() -> None:
    host, _, _, seen = make_host()
    host.send_sns(ORDER_CREATED_TOPIC, {"id": "ord-sns", "sku": "ABC"})
    assert seen == ["ord-sns"]


def test_sqs_partial_batch_failure_reports_only_failed_record() -> None:
    host, _, _, seen = make_host()
    event = (
        SqsEventBuilder()
        .with_message(ORDER_CREATED_TOPIC, {"id": "ok-1", "sku": "A"}, message_id="m1")
        .with_message("orders:unknown", {}, message_id="m2")     # no handler -> not-found -> fails
        .build()
    )
    result = host.send_sqs_event(event)
    assert result.batch_item_failures == [{"itemIdentifier": "m2"}]
    assert seen == ["ok-1"]                                       # the good record still processed
