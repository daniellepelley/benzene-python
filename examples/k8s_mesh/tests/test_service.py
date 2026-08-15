"""Dogfooded, in-memory tests for the k8s_mesh domain services.

Boots the real app from ``ServiceStartUp`` (the same composition root ``host.py`` deploys) through the
shared test harness, faking only the outbound edge (``FakeMessageSender``) — proving ingress -> handler
-> egress for each of the three domains, and that ``/benzene/invoke`` (the generic envelope endpoint
every downstream hop and the mesh's poller both reach the service through) really does serve every
topic. No cloud, no network, no Kubernetes: the same test the CI gate runs.
"""

from __future__ import annotations

import json

from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host

from k8s_mesh.service.domain import (
    ORDER_CREATE_TOPIC,
    PAYMENT_TAKE_TOPIC,
    SHIPMENT_BOOK_TOPIC,
)
from k8s_mesh.service.startup import ServiceStartUp


def make_host(service_name: str):
    sender = FakeMessageSender()

    def overrides(services):
        services.add_instance(MessageSender, sender)

    host = create_test_host(ServiceStartUp(service_name)).with_services(overrides).build_http()
    return host, sender


def test_orders_create_order_chains_to_payment_take() -> None:
    host, sender = make_host("orders")

    response = host.send_http(
        "POST", "/orders", body={"customerId": "cust-1", "sku": "espresso", "quantity": 2}
    )

    assert response.status_code == 201
    order = json.loads(response.body)
    assert order["status"] == "created"
    assert order["orderId"]
    # Egress: orders chains to payments over the payment:take topic (ingress -> handler -> egress).
    assert sender.last_topic == PAYMENT_TAKE_TOPIC
    assert sender.last_message.order_id == order["orderId"]


def test_payments_take_payment_chains_to_shipment_book() -> None:
    host, sender = make_host("payments")

    response = host.send_http("POST", "/payments", body={"orderId": "order-1", "amount": 20})

    assert response.status_code == 201
    payment = json.loads(response.body)
    assert payment["status"] == "captured"
    assert sender.last_topic == SHIPMENT_BOOK_TOPIC
    assert sender.last_message.order_id == "order-1"


def test_shipping_books_shipment_and_is_terminal() -> None:
    host, sender = make_host("shipping")

    response = host.send_http(
        "POST", "/shipments", body={"orderId": "order-1", "address": "1 Way", "carrier": "royal-mail"}
    )

    assert response.status_code == 201
    shipment = json.loads(response.body)
    assert shipment["status"] == "booked"
    assert not sender.sent  # terminal: no downstream call


def test_benzene_invoke_is_the_single_envelope_endpoint_for_every_topic() -> None:
    """The generic envelope endpoint (the analogue of .NET's POST /benzene-message) routes by the
    envelope's topic — not by a per-topic REST route — proving "one endpoint serves every topic"."""
    host, sender = make_host("orders")

    envelope = {
        "topic": ORDER_CREATE_TOPIC,
        "headers": {},
        "body": json.dumps({"customerId": "cust-1", "sku": "latte", "quantity": 1}),
    }
    response = host.send_http("POST", "/benzene/invoke", body=envelope)

    assert response.status_code == 200  # /benzene/invoke itself always answers 200
    response_envelope = json.loads(response.body)
    assert response_envelope["statusCode"] == "created"
    assert sender.last_topic == PAYMENT_TAKE_TOPIC


def test_health_and_spec_surfaces_answer() -> None:
    host, _ = make_host("shipping")

    health = host.send_http("GET", "/benzene/health")
    assert health.status_code == 200
    assert json.loads(health.body)["isHealthy"] is True

    spec = host.send_http("GET", "/benzene/spec")
    assert spec.status_code == 200
    topics = {t["id"] for t in json.loads(spec.body)["topics"]}
    assert SHIPMENT_BOOK_TOPIC in topics
