"""Dogfooded, in-memory tests for the standalone HTTP orders example.

Boot the real app from ``OrdersStartUp`` (the same composition root as the cloud examples) and drive
it through the HTTP front door with the shared harness — ``create_test_host(...).build_http()`` and
``send_http``, the *identical* setup to the AWS/GCP/Azure suites bar the one specialization call.
Only the outbound edge is faked (``FakeMessageSender``), proving ingress → handler → egress. No
cloud, no socket: the same test the CI gate runs.
"""

from __future__ import annotations

import json

from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host
from orders_domain import ORDER_CREATED_TOPIC, ORDER_EVENTS_KEY, OrderService, OrdersStartUp


def make_host():
    """Boot the real app from OrdersStartUp, fake only the edges, specialize to standalone HTTP.

    Note the setup is identical to the AWS/GCP/Azure suites except the single ``.build_http()`` call
    and the ``send_http`` shape — the test-champion consistency law in action.
    """
    service = OrderService()
    sender = FakeMessageSender()
    seen: list[str] = []

    def overrides(services):
        services.add_instance(OrderService, service)     # override ANY registration...
        services.add_instance(MessageSender, sender)      # ...only the external edge is faked
        services.add_instance(ORDER_EVENTS_KEY, seen)

    host = create_test_host(OrdersStartUp).with_services(overrides).build_http()
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
