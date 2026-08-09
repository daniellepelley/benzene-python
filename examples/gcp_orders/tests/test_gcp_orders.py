"""Dogfooded, in-memory tests for the GCP orders example.

No cloud, no network: drive the real Cloud Functions bindings via ``benzene.gcp.testing`` and fake
only the outbound edge with ``benzene.testing.FakeMessageSender`` (port-quality-standards §4).
Proves HTTP ingress, ingress → handler → egress, Pub/Sub ingress, and the failure/redelivery rule.
"""

from __future__ import annotations

import json

import pytest
from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host
from orders_domain import ORDER_CREATED_TOPIC, OrderEventLog, OrderService, OrdersStartUp


def make_host():
    """Boot the real app from OrdersStartUp, fake only the edges, specialize to GCP.

    Note the setup is identical to the AWS and Azure suites except the single ``.build_gcp()`` call
    and the ``send_*`` shape — the test-champion consistency law in action.
    """
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []

    def overrides(services):
        services.add_instance(OrderService, service)  # override ANY registration...
        services.add_instance(MessageSender, sender)  # ...only the external edge is faked
        services.add_instance(OrderEventLog, seen)

    host = create_test_host(OrdersStartUp).with_services(overrides).build_gcp()
    return host, service, sender, seen


def test_http_place_order_creates_and_publishes() -> None:
    host, service, sender, _ = make_host()

    response = host.send_http("POST", "/orders", body={"sku": "ABC", "quantity": 2})

    assert response.status_code == 201
    order = json.loads(response.body)
    assert order["sku"] == "ABC"
    assert order["quantity"] == 2
    # Egress: the handler published OrderCreated carrying the new order's id (ingress->handler->egress).
    assert sender.last_topic == ORDER_CREATED_TOPIC
    assert sender.last_message.id == order["id"]
    assert sender.last_message.sku == "ABC"
    assert order["id"] in service.orders


def test_http_place_order_validates_sku() -> None:
    host, _, sender, _ = make_host()
    response = host.send_http("POST", "/orders", body={"quantity": 1})
    assert response.status_code == 400
    assert not sender.sent  # nothing published on a rejected request


def test_http_get_order_round_trip() -> None:
    host, _, _, _ = make_host()
    created = json.loads(host.send_http("POST", "/orders", body={"sku": "XYZ"}).body)

    response = host.send_http("GET", f"/orders/{created['id']}")

    assert response.status_code == 200
    assert json.loads(response.body)["id"] == created["id"]


def test_http_get_unknown_order_is_404() -> None:
    host, _, _, _ = make_host()
    response = host.send_http("GET", "/orders/does-not-exist")
    assert response.status_code == 404


def test_pubsub_order_created_is_handled() -> None:
    host, _, _, seen = make_host()

    host.send_pubsub(ORDER_CREATED_TOPIC, body={"id": "ord-1", "sku": "ABC"})

    assert seen == ["ord-1"]


def test_pubsub_unroutable_topic_raises_for_redelivery() -> None:
    host, _, _, _ = make_host()
    # No handler for this topic -> not-found -> the binding raises so Pub/Sub redelivers.
    with pytest.raises(RuntimeError):
        host.send_pubsub("orders:unknown", body={})
