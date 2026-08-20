"""Dogfooded, in-memory tests for the six azure_functions_mesh domain services.

Boots the real app from ``ServiceStartUp`` (the same composition root ``service/host.py`` deploys)
through the shared Azure test harness (``create_test_host(...).build_azure()``), faking only the
outbound edge (``FakeMessageSender``) — proving ingress -> handler -> egress over the *native* Azure
Functions trigger shapes (HTTP, Service Bus, Event Hub, Event Grid) each service actually receives in
deployment, plus the real HTTP Cloud Service Profile surfaces the mesh's HTTP-polling interrogation
depends on. No cloud, no network: the same test the CI gate runs.
"""

from __future__ import annotations

import json

from benzene.core import MessageSender
from benzene.testing import FakeMessageSender, create_test_host

from azure_functions_mesh.service.domain import (
    ORDER_PLACED_TOPIC,
    PAYMENT_CAPTURED_TOPIC,
    PAYMENT_TAKE_TOPIC,
    SHIPMENT_BOOK_TOPIC,
    SHIPMENT_DISPATCHED_TOPIC,
)
from azure_functions_mesh.service.startup import ServiceStartUp


def make_host(service_name: str):
    sender = FakeMessageSender()

    def overrides(services):
        services.add_instance(MessageSender, sender)

    host = create_test_host(ServiceStartUp(service_name)).with_services(overrides).build_azure()
    return host, sender


# --- orders: HTTP ingress -> Service Bus + Event Hub egress -----------------------------------------


def test_orders_create_order_sends_payment_take_and_order_placed() -> None:
    host, sender = make_host("orders")

    response = host.send_http("POST", "/orders", body={"orderId": "order-1"})

    assert response.status_code == 201
    order = json.loads(response.body)
    assert order["status"] == "created"
    assert order["orderId"] == "order-1"
    topics = [sent.topic for sent in sender.sent]
    assert topics == [PAYMENT_TAKE_TOPIC, ORDER_PLACED_TOPIC]
    assert all(sent.message.order_id == "order-1" for sent in sender.sent)


def test_orders_mints_an_order_id_when_none_given() -> None:
    host, _ = make_host("orders")
    response = host.send_http("POST", "/orders", body={})
    assert json.loads(response.body)["orderId"]


# --- payments: Service Bus ingress -> Service Bus + Event Grid egress -------------------------------


def test_payments_take_sends_shipment_book_and_payment_captured() -> None:
    host, sender = make_host("payments")

    host.send_service_bus(PAYMENT_TAKE_TOPIC, {"orderId": "order-2"})

    topics = [sent.topic for sent in sender.sent]
    assert topics == [SHIPMENT_BOOK_TOPIC, PAYMENT_CAPTURED_TOPIC]
    assert all(sent.message.order_id == "order-2" for sent in sender.sent)


# --- shipping: Service Bus ingress -> Event Grid egress, terminal in this direction ------------------


def test_shipping_book_sends_shipment_dispatched() -> None:
    host, sender = make_host("shipping")

    host.send_service_bus(SHIPMENT_BOOK_TOPIC, {"orderId": "order-3"})

    assert len(sender.sent) == 1
    assert sender.sent[0].topic == SHIPMENT_DISPATCHED_TOPIC
    assert sender.sent[0].message.order_id == "order-3"


# --- terminal consumers: Event Hub + Event Grid ingress, no egress ----------------------------------


def test_inventory_consumes_order_placed_event_hub_and_shipment_dispatched_event_grid() -> None:
    host, sender = make_host("inventory")

    host.send_event_hub(ORDER_PLACED_TOPIC, {"orderId": "order-4"})
    host.send_event_grid({"eventType": SHIPMENT_DISPATCHED_TOPIC, "data": {"orderId": "order-5"}})

    assert not sender.sent  # a pure consumer sends nothing downstream


def test_notifications_consumes_all_three_fan_out_topics() -> None:
    host, sender = make_host("notifications")

    host.send_event_hub(ORDER_PLACED_TOPIC, {"orderId": "order-6"})
    host.send_event_grid({"eventType": PAYMENT_CAPTURED_TOPIC, "data": {"orderId": "order-6"}})
    host.send_event_grid({"eventType": SHIPMENT_DISPATCHED_TOPIC, "data": {"orderId": "order-6"}})

    assert not sender.sent


def test_analytics_consumes_payment_captured_and_shipment_dispatched() -> None:
    host, sender = make_host("analytics")

    host.send_event_grid({"eventType": PAYMENT_CAPTURED_TOPIC, "data": {"orderId": "order-7"}})
    host.send_event_grid({"eventType": SHIPMENT_DISPATCHED_TOPIC, "data": {"orderId": "order-7"}})

    assert not sender.sent


# --- the mesh's own interrogation surfaces: real HTTP, not a direct invoke --------------------------


def test_http_standard_paths_answer_health_and_spec() -> None:
    """Every service exposes /benzene/health + /benzene/spec over HTTP (host.json's routePrefix "" puts
    it at the site root) — the surface AzureDiscovery + HttpServiceSource actually interrogate."""
    host, _ = make_host("shipping")

    health = host.send_http("GET", "/benzene/health")
    assert health.status_code == 200
    assert json.loads(health.body)["isHealthy"] is True

    spec = host.send_http("GET", "/benzene/spec")
    assert spec.status_code == 200
    body = json.loads(spec.body)
    # R5's Contract Document: the service name is info.title and the topics are requests[].topic.
    assert body["info"]["title"] == "shipping"
    assert {r["topic"] for r in body["requests"]} == {SHIPMENT_BOOK_TOPIC}


def test_http_standard_paths_invoke_also_answers() -> None:
    host, sender = make_host("analytics")

    response = host.send_http(
        "POST",
        "/benzene/invoke",
        body={"topic": PAYMENT_CAPTURED_TOPIC, "headers": {}, "body": json.dumps({"orderId": "order-8"})},
    )
    assert response.status_code == 200
    assert not sender.sent
